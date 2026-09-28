package store

import "fmt"

// Filter 是等值过滤条件。字段合法性与值类型由上层按列元数据保证；
// 引擎层再做一次类型比对，任何不匹配都作为错误而非“查不到”处理。
type Filter struct {
	Field string
	Value any
}

// ScanOptions 控制扫描：等值过滤、id 升序、偏移/限量与游标。
// 所有查询入口（分页/游标/全量）最终都构造同一套选项，保证结果一致。
type ScanOptions struct {
	Filters []Filter
	Offset  int
	Limit   int   // <=0 表示不限制（实际由上层给出有界上限）
	AfterID int64 // 游标：仅返回 id 严格大于该值的行
}

func (t *tableState) column(name string) (Column, bool) {
	for _, c := range t.columns {
		if c.Name == name {
			return c, true
		}
	}
	return Column{}, false
}

// project 把内部行投影给读取方：用户列 + 隐式 id/version。
func (t *tableState) project(id int64, row Row) Row {
	out := make(Row, len(row)+2)
	for k, v := range row {
		out[k] = v
	}
	out[SystemColumnID] = id
	out[SystemColumnVersion] = t.rowV[id]
	return out
}

// match 对一行应用等值过滤；类型不符直接返回错误，绝不静默当成不匹配。
func (t *tableState) match(row Row, id int64, filters []Filter) (bool, error) {
	for _, f := range filters {
		var want any
		switch f.Field {
		case SystemColumnID:
			want = id
		case SystemColumnVersion:
			want = t.rowV[id]
		default:
			if _, ok := t.column(f.Field); !ok {
				return false, fmt.Errorf("%w: unknown filter field %q", ErrColumnNotFound, f.Field)
			}
			want = row[f.Field]
		}
		if !strictEqual(want, f.Value) {
			return false, nil
		}
	}
	return true, nil
}

func strictEqual(a, b any) bool {
	// 两值均按其动态类型比较；nil 只与 nil 相等。
	return a == b
}

// Insert 插入一行。
// 缺列按“确定的默认语义”补值；NOT NULL 且无默认或缺值时拒绝整行。
// 返回自增 id；新行初始版本为 1。
func (tx *Tx) Insert(table string, values Row) (int64, error) {
	t, err := tx.tableForWrite(table)
	if err != nil {
		return 0, err
	}
	row := make(Row, len(t.columns))
	for _, col := range t.columns {
		v, provided := values[col.Name]
		if provided {
			cv, err := checkValue(col, v)
			if err != nil {
				return 0, err
			}
			row[col.Name] = cv
		} else {
			def, err := defaultFor(col)
			if err != nil {
				return 0, err
			}
			row[col.Name] = def
		}
	}
	// 拒绝写入未知列，避免调用方把值写到不存在的字段上而不自知。
	for k := range values {
		if _, ok := t.column(k); !ok {
			return 0, fmt.Errorf("%w: unknown column %q on table %q", ErrColumnNotFound, k, table)
		}
	}
	// 本事务视角内的唯一约束校验（并发复核见 Tx.Commit）。
	for _, col := range t.columns {
		if !col.Unique {
			continue
		}
		nv, provided := row[col.Name]
		if !provided || nv == nil {
			continue
		}
		for _, rid := range t.order {
			if t.rows[rid][col.Name] == nv {
				return 0, fmt.Errorf("%w: column %q duplicate value %v", ErrUniqueViolation, col.Name, nv)
			}
		}
	}
	// 新建表（本事务刚创建，他人不可见）用本地自增；既有表用全局分配，
	// 保证并发事务插入不撞 id。
	var id int64
	if _, isNew := tx.newTables[table]; isNew {
		id = t.nextID
		t.nextID++
	} else {
		// t.nextID 是“下一个待分配 id”水位；分配后把事务内水位推进到全局水位之后。
		id = tx.store.allocateID(table, t.nextID)
		t.nextID = tx.store.peekNextID(table)
	}
	t.rows[id] = row
	// order 保持 id 升序（全局分配的 id 通常最大，正常追加即可；兼容空洞回填）。
	pos := len(t.order)
	for i, rid := range t.order {
		if id < rid {
			pos = i
			break
		}
	}
	t.order = append(t.order, 0)
	copy(t.order[pos+1:], t.order[pos:len(t.order)-1])
	t.order[pos] = id
	t.rowV[id] = 1
	if tx.insertRows[table] == nil {
		tx.insertRows[table] = make(map[int64]struct{})
	}
	tx.insertRows[table][id] = struct{}{}
	return id, nil
}

// Update 按主键与期望行版本更新。
// expectRowVersion<0 表示不检查版本；为 0 或正数时必须与当前行版本一致，
// 否则 affected=0（上层翻译为 VERSION_CONFLICT / NOT_FOUND）。
// 成功后行版本 +1。
func (tx *Tx) Update(table string, id int64, expectRowVersion int64, values Row) (int64, error) {
	t, err := tx.tableForWrite(table)
	if err != nil {
		return 0, err
	}
	row, ok := t.rows[id]
	if !ok {
		return 0, nil
	}
	if expectRowVersion >= 0 && t.rowV[id] != expectRowVersion {
		// 版本不匹配属于应用层乐观锁拒绝；仍登记行读依赖。
		tx.markRowRead(table, id, t.rowV[id])
		return 0, nil
	}
	tx.markRowWrite(table, id, t.rowV[id])
	tx.markRowRead(table, id, t.rowV[id])
	for k, v := range values {
		col, exists := t.column(k)
		if !exists {
			return 0, fmt.Errorf("%w: unknown column %q on table %q", ErrColumnNotFound, k, table)
		}
		if v == nil {
			if !col.Nullable {
				return 0, fmt.Errorf("%w: column %q is not nullable", ErrConstraint, col.Name)
			}
			row[k] = nil
			continue
		}
		cv, err := checkValue(col, v)
		if err != nil {
			return 0, err
		}
		row[k] = cv
	}
	t.rowV[id]++
	return 1, nil
}

// Delete 按主键删除，返回受影响行数（0 即不存在）。
func (tx *Tx) Delete(table string, id int64) (int64, error) {
	t, err := tx.tableForWrite(table)
	if err != nil {
		return 0, err
	}
	row, ok := t.rows[id]
	if !ok {
		return 0, nil
	}
	_ = row
	tx.markRowWrite(table, id, t.rowV[id])
	if tx.deleteRows[table] == nil {
		tx.deleteRows[table] = make(map[int64]struct{})
	}
	tx.deleteRows[table][id] = struct{}{}
	delete(t.rows, id)
	delete(t.rowV, id)
	for i, rid := range t.order {
		if rid == id {
			t.order = append(t.order[:i], t.order[i+1:]...)
			break
		}
	}
	return 1, nil
}

// Get 按主键取一行投影；ok=false 表示不存在。
func (tx *Tx) Get(table string, id int64) (Row, bool, error) {
	t, err := tx.tableForRead(table)
	if err != nil {
		return nil, false, err
	}
	row, ok := t.rows[id]
	if !ok {
		tx.markFullRead(table) // “存在性”依赖按整表读处理，避免他人插入同 id 幻影
		return nil, false, nil
	}
	tx.markRowRead(table, id, t.rowV[id])
	return t.project(id, row), true, nil
}

// List 按条件扫描，结果按 id 升序，供偏移分页与游标分页共用。
func (tx *Tx) List(table string, opts ScanOptions) ([]Row, error) {
	t, err := tx.tableForRead(table)
	if err != nil {
		return nil, err
	}
	tx.markFullRead(table)
	// 先校验全部过滤字段与类型，畸形条件必须整体报错而非返回空。
	for _, f := range opts.Filters {
		switch f.Field {
		case SystemColumnID, SystemColumnVersion:
			if _, ok := f.Value.(int64); !ok {
				return nil, fmt.Errorf("%w: system field %q expects int64, got %T", ErrTypeMismatch, f.Field, f.Value)
			}
		default:
			col, ok := t.column(f.Field)
			if !ok {
				return nil, fmt.Errorf("%w: unknown filter field %q on table %q", ErrColumnNotFound, f.Field, table)
			}
			if f.Value == nil {
				if !col.Nullable {
					return nil, fmt.Errorf("%w: filter on non-nullable column %q with nil", ErrTypeMismatch, col.Name)
				}
				continue
			}
			if _, err := checkValue(col, f.Value); err != nil {
				return nil, err
			}
		}
	}

	var matched []Row
	for _, id := range t.order {
		if id <= opts.AfterID {
			continue
		}
		ok, err := t.match(t.rows[id], id, opts.Filters)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		matched = append(matched, t.project(id, t.rows[id]))
	}
	if opts.Offset > 0 {
		if opts.Offset >= len(matched) {
			matched = nil
		} else {
			matched = matched[opts.Offset:]
		}
	}
	if opts.Limit > 0 && len(matched) > opts.Limit {
		matched = matched[:opts.Limit]
	}
	return matched, nil
}

// Count 返回过滤后的总行数（分页 total 与扫描共用同一过滤条件）。
func (tx *Tx) Count(table string, filters []Filter) (int64, error) {
	t, err := tx.tableForRead(table)
	if err != nil {
		return 0, err
	}
	tx.markFullRead(table)
	for _, f := range filters {
		switch f.Field {
		case SystemColumnID, SystemColumnVersion:
			if _, ok := f.Value.(int64); !ok {
				return 0, fmt.Errorf("%w: system field %q expects int64, got %T", ErrTypeMismatch, f.Field, f.Value)
			}
		default:
			col, ok := t.column(f.Field)
			if !ok {
				return 0, fmt.Errorf("%w: unknown filter field %q on table %q", ErrColumnNotFound, f.Field, table)
			}
			if _, err := checkValue(col, f.Value); err != nil {
				return 0, err
			}
		}
	}
	var n int64
	for _, id := range t.order {
		ok, err := t.match(t.rows[id], id, filters)
		if err != nil {
			return 0, err
		}
		if ok {
			n++
		}
	}
	return n, nil
}

// FindOne 按等值条件点查最多一行（用于唯一键回放查询）。
// 与 List 不同，它逐行登记行读依赖而不登记整表依赖，因此不同键的并发操作
// 不会彼此误判冲突；未命中时不登记“不存在”依赖，唯一性由提交复核兜底。
func (tx *Tx) FindOne(table string, filters []Filter) (Row, bool, error) {
	t, err := tx.tableForRead(table)
	if err != nil {
		return nil, false, err
	}
	for _, f := range filters {
		if f.Field == SystemColumnID || f.Field == SystemColumnVersion {
			if _, ok := f.Value.(int64); !ok {
				return nil, false, fmt.Errorf("%w: system field %q expects int64, got %T", ErrTypeMismatch, f.Field, f.Value)
			}
			continue
		}
		col, ok := t.column(f.Field)
		if !ok {
			return nil, false, fmt.Errorf("%w: unknown filter field %q on table %q", ErrColumnNotFound, f.Field, table)
		}
		if f.Value == nil {
			if !col.Nullable {
				return nil, false, fmt.Errorf("%w: filter on non-nullable column %q with nil", ErrTypeMismatch, col.Name)
			}
			continue
		}
		if _, err := checkValue(col, f.Value); err != nil {
			return nil, false, err
		}
	}
	for _, id := range t.order {
		ok, err := t.match(t.rows[id], id, filters)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			continue
		}
		tx.markRowRead(table, id, t.rowV[id])
		return t.project(id, t.rows[id]), true, nil
	}
	return nil, false, nil
}
