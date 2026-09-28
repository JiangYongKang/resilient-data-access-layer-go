package store

// Open 创建一个空数据库实例。
func Open() *Store {
	return &Store{tables: make(map[string]*tableState), nextID: make(map[string]int64)}
}

// Begin 开启读写事务：立即复制当时全部已提交表结构与数据。
func (s *Store) Begin() (*Tx, error) {
	return s.begin(false)
}

// begin 复制快照。readOnly 事务拒绝任何写入。
func (s *Store) begin(readOnly bool) (*Tx, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tx := &Tx{
		store:      s,
		readOnly:   readOnly,
		priv:       make(map[string]*tableState, len(s.tables)),
		baseVer:    make(map[string]int64, len(s.tables)),
		fullRead:   make(map[string]struct{}),
		rowRead:    make(map[string]map[int64]int64),
		writeRows:  make(map[string]map[int64]int64),
		insertRows: make(map[string]map[int64]struct{}),
		deleteRows: make(map[string]map[int64]struct{}),
		newTables:  make(map[string]struct{}),
		ddlTables:  make(map[string]struct{}),
	}
	for name, t := range s.tables {
		tx.priv[name] = copyTableState(t)
		tx.baseVer[name] = t.tableV
		// 对齐全局自增水位：后续 allocateID 取全局“下一待分配”与快照 nextID 的较大者。
		if g := s.nextID[name]; g > t.nextID {
			tx.priv[name].nextID = g
		}
	}
	return tx, nil
}

// Exec 在自动提交事务里执行一次写入操作：fn 返回错误则回滚，否则提交。
func (s *Store) Exec(fn func(tx *Tx) error) error {
	tx, err := s.Begin()
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

// View 在只读事务里执行一次读取操作。
func (s *Store) View(fn func(tx *Tx) error) error {
	tx, err := s.begin(true)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	return fn(tx)
}

func copyTableState(t *tableState) *tableState {
	cp := &tableState{
		name:    t.name,
		columns: append([]Column(nil), t.columns...),
		rows:    make(map[int64]Row, len(t.rows)),
		order:   append([]int64(nil), t.order...),
		nextID:  t.nextID,
		tableV:  t.tableV,
		rowV:    make(map[int64]int64, len(t.rowV)),
	}
	for id, row := range t.rows {
		nr := make(Row, len(row))
		for k, v := range row {
			nr[k] = v
		}
		cp.rows[id] = nr
	}
	for id, v := range t.rowV {
		cp.rowV[id] = v
	}
	return cp
}

// uniqueColumnNames 返回带唯一约束的用户列名。
func (t *tableState) uniqueColumnNames() []string {
	var out []string
	for _, c := range t.columns {
		if c.Unique {
			out = append(out, c.Name)
		}
	}
	return out
}
