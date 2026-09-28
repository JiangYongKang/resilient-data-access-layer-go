package store

import "sort"

// Commit 提交事务（第一提交者胜出 + 快照隔离下的行级合并发布）。
//
// 依赖检查（任一不满足即整体中止，不发布任何改动）：
//  1. 整表读依赖（List/Count/HasTable/Columns 及在既有表上的 DDL）：
//     表当前版本必须等于快照版本，防止范围谓词幻影与结构并发变更；
//  2. 行读依赖（Get/FindOne）：读过的每行当前版本必须等于读取时版本，行被删即冲突；
//  3. 行写依赖（Update/Delete）：写过的每行自快照后未被他人提交修改；
//  4. 插入 id 未被占用，且唯一约束对已提交世界复核通过；
//  5. 新建表的表名在提交时仍不存在。
//
// 发布：
//   - 新建表：整表发布，起始表版本 1；
//   - 在既有表上做过 DDL：已通过整表版本检查（当前状态==快照），整表发布；
//   - 仅 DML 的既有表：以当前已提交状态为基准做行级合并（插入并入、更新覆盖、
//     删除移除），若期间列被 DDL 扩展，则按当前结构为新行补默认值，随后表版本 +1。
//
// 全部通过后一次性发布：DDL 与 DML 同生共死，不允许部分提交。
func (tx *Tx) Commit() error {
	if tx.closed {
		return ErrTxClosed
	}
	tx.closed = true

	s := tx.store
	s.mu.Lock()
	defer s.mu.Unlock()

	// 1) 整表读依赖（本事务新建的表不依赖已提交世界，跳过）
	for t := range tx.fullRead {
		if _, isNew := tx.newTables[t]; isNew {
			continue
		}
		cur, ok := s.tables[t]
		if !ok || tx.baseVer[t] != cur.tableV {
			return ErrConflict
		}
	}

	// 2)+3) 行读 / 行写依赖
	for _, deps := range []map[string]map[int64]int64{tx.rowRead, tx.writeRows} {
		for t, rows := range deps {
			if _, isNew := tx.newTables[t]; isNew {
				continue
			}
			cur, ok := s.tables[t]
			if !ok {
				return ErrConflict
			}
			for id, seen := range rows {
				now, exists := cur.rowV[id]
				if !exists || now != seen {
					return ErrConflict
				}
			}
		}
	}

	// 4) 插入 id 占用检查（全局分配下通常恒不占用，这里做防御式复核）
	for t, rows := range tx.insertRows {
		if _, isNew := tx.newTables[t]; isNew {
			continue
		}
		cur, ok := s.tables[t]
		if !ok {
			return ErrConflict
		}
		for id := range rows {
			if _, occupied := cur.rows[id]; occupied {
				return ErrConflict
			}
		}
	}

	// 5) 新建表名碰撞
	for t := range tx.newTables {
		if _, exists := s.tables[t]; exists {
			return ErrConflict
		}
	}

	// 唯一约束复核：本事务写入产出的终值不得与已提交世界的其他行撞唯一键。
	for tName, privT := range tx.priv {
		if _, isNew := tx.newTables[tName]; isNew {
			continue
		}
		curT := s.tables[tName]
		uniqueCols := privT.uniqueColumnNames()
		if len(uniqueCols) == 0 {
			continue
		}
		for id := range tx.writtenRowIDs(tName) {
			privRow := privT.rows[id] // 删除行不在此集合（删除不产生终值）
			if privRow == nil {
				continue
			}
			for _, col := range uniqueCols {
				v := privRow[col]
				if v == nil {
					continue
				}
				for cid, crow := range curT.rows {
					if cid == id {
						continue
					}
					if crow[col] == v {
						return ErrUniqueViolation
					}
				}
			}
		}
	}

	// ---------- 发布 ----------
	published := make(map[string]struct{})

	for tName := range tx.newTables {
		st := tx.priv[tName]
		st.tableV = 1
		s.tables[tName] = st
		published[tName] = struct{}{}
	}
	for tName := range tx.ddlTables {
		st := tx.priv[tName]
		st.tableV = tx.baseVer[tName] + 1
		s.tables[tName] = st
		published[tName] = struct{}{}
	}
	for _, group := range []map[string]map[int64]struct{}{tx.insertRows, tx.deleteRows} {
		for tName := range group {
			if _, done := published[tName]; done {
				continue
			}
			if _, isNew := tx.newTables[tName]; isNew {
				continue
			}
			if err := tx.publishMerged(s, tName); err != nil {
				return err
			}
			published[tName] = struct{}{}
		}
	}
	for tName := range tx.writeRows {
		if _, done := published[tName]; done {
			continue
		}
		if _, isNew := tx.newTables[tName]; isNew {
			continue
		}
		if err := tx.publishMerged(s, tName); err != nil {
			return err
		}
		published[tName] = struct{}{}
	}
	return nil
}

// publishMerged 以当前已提交状态为基准合并发布一张纯 DML 表。
// 调用方已持有 store 写锁，且所有依赖检查、唯一复核均已通过。
func (tx *Tx) publishMerged(s *Store, table string) error {
	cur := s.tables[table]
	priv := tx.priv[table]
	merged := copyTableState(cur) // 以最新已提交数据与结构为基准
	merged.columns = cur.columns

	inserts := tx.insertRows[table]
	updates := tx.writeRows[table]
	deletes := tx.deleteRows[table]

	for id := range deletes {
		delete(merged.rows, id)
		delete(merged.rowV, id)
	}

	// 插入：以最新列结构规整；事务期间若有并发 DDL 加列，按默认值补齐。
	for id := range inserts {
		src := priv.rows[id]
		nr := make(Row, len(merged.columns))
		for _, col := range merged.columns {
			if v, ok := src[col.Name]; ok {
				nr[col.Name] = v
			} else {
				def, err := defaultFor(col)
				if err != nil {
					return err
				}
				nr[col.Name] = def
			}
		}
		merged.rows[id] = nr
		merged.rowV[id] = 1
	}

	// 更新：把私副本中被触碰的列覆盖到最新已提交行；未触碰列保留现值。
	for id := range updates {
		if _, removed := deletes[id]; removed {
			continue
		}
		dst, ok := merged.rows[id]
		if !ok {
			return ErrConflict // 依赖检查阶段已排除
		}
		src := priv.rows[id]
		for _, col := range merged.columns {
			if v, touched := src[col.Name]; touched {
				dst[col.Name] = v
			}
		}
		merged.rowV[id] = priv.rowV[id] // 私副本中该行版本已 +1
	}

	ids := make([]int64, 0, len(merged.rows))
	for id := range merged.rows {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	merged.order = ids
	if priv.nextID > merged.nextID {
		merged.nextID = priv.nextID
	}
	if g := s.nextID[table]; g > merged.nextID {
		merged.nextID = g
	}
	merged.tableV = cur.tableV + 1
	s.tables[table] = merged
	return nil
}

// Rollback 丢弃事务的全部工作副本。
func (tx *Tx) Rollback() error {
	if tx.closed {
		return ErrTxClosed
	}
	tx.closed = true
	tx.priv = nil
	return nil
}

// ---- 依赖登记辅助 ----

func (tx *Tx) markFullRead(name string) { tx.fullRead[name] = struct{}{} }

func (tx *Tx) markRowRead(name string, id, ver int64) {
	m := tx.rowRead[name]
	if m == nil {
		m = make(map[int64]int64)
		tx.rowRead[name] = m
	}
	if _, recorded := m[id]; !recorded {
		m[id] = ver
	}
}

func (tx *Tx) markRowWrite(name string, id, snapshotVer int64) {
	m := tx.writeRows[name]
	if m == nil {
		m = make(map[int64]int64)
		tx.writeRows[name] = m
	}
	if _, recorded := m[id]; !recorded {
		m[id] = snapshotVer // 以首次写时（快照视角）的行版本为基准
	}
}

// tableForRead 取得读用表；调用方随后按需调用 mark* 登记具体依赖。
func (tx *Tx) tableForRead(name string) (*tableState, error) {
	if tx.closed {
		return nil, ErrTxClosed
	}
	t, ok := tx.priv[name]
	if !ok {
		return nil, ErrTableNotFound
	}
	return t, nil
}

// tableForWrite 取得写用表并拒绝只读事务。
func (tx *Tx) tableForWrite(name string) (*tableState, error) {
	if tx.closed {
		return nil, ErrTxClosed
	}
	if tx.readOnly {
		return nil, ErrReadOnly
	}
	t, ok := tx.priv[name]
	if !ok {
		return nil, ErrTableNotFound
	}
	return t, nil
}

// AllowDestructive 显式授权本事务执行删列/改名等破坏性 DDL。
// 迁移协议要求只有声明 Destructive=true 的版本才会调用它。
func (tx *Tx) AllowDestructive() { tx.destructiveAllowed = true }

// writtenRowIDs 汇总本事务对某表插入或更新过（且未删除）的行 id。
func (tx *Tx) writtenRowIDs(table string) map[int64]struct{} {
	ids := make(map[int64]struct{})
	for id := range tx.writeRows[table] {
		if _, removed := tx.deleteRows[table][id]; removed {
			continue
		}
		ids[id] = struct{}{}
	}
	for id := range tx.insertRows[table] {
		ids[id] = struct{}{}
	}
	return ids
}
