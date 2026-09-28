package engine

import (
	"context"
	"fmt"
)

// Tx 是一个事务句柄。写事务在引擎内部串行化，因此天然可串行化：
// 不会出现后写覆盖前写、部分提交或重复扣减。
type Tx struct {
	ctx      context.Context
	db       *DB
	readOnly bool
	// readOnly 时指向 db.tables（共享已提交快照）；读写时为工作副本。
	tables map[string]*Table
	done   bool
}

// Context 返回事务上下文。
func (tx *Tx) Context() context.Context { return tx.ctx }

func (tx *Tx) checkWritable(op string) error {
	if tx.done {
		return fmt.Errorf("engine: transaction already finished (%s)", op)
	}
	if tx.readOnly {
		return fmt.Errorf("engine: %s is not allowed in a read-only transaction", op)
	}
	return nil
}

func (tx *Tx) table(name string) (*Table, error) {
	t, ok := tx.tables[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrTableNotFound, name)
	}
	return t, nil
}

// ---- DDL ----

// CreateTable 幂等建表：同名同结构直接成功；同名不同结构返回 ErrTableExists。
func (tx *Tx) CreateTable(s *Schema) error {
	if err := tx.checkWritable("CreateTable"); err != nil {
		return err
	}
	if s == nil {
		return fmt.Errorf("%w: nil schema", ErrInvalidValue)
	}
	if err := s.Validate(); err != nil {
		return err
	}
	if existing, ok := tx.tables[s.Table]; ok {
		if !schemaEqual(existing.schema, s) {
			return fmt.Errorf("%w: table %q exists with a different definition", ErrTableExists, s.Table)
		}
		return nil
	}
	tx.tables[s.Table] = newTable(s)
	return nil
}

// TableSchema 返回表结构的深拷贝（避免调用方绕过 DDL 改结构）。
func (tx *Tx) TableSchema(name string) (*Schema, error) {
	t, err := tx.table(name)
	if err != nil {
		return nil, err
	}
	return t.schema.Clone(), nil
}

// AddColumn 幂等加列：列已存在且定义相同则跳过；定义不同则拒绝。
func (tx *Tx) AddColumn(tableName string, c Column) error {
	if err := tx.checkWritable("AddColumn"); err != nil {
		return err
	}
	t, err := tx.table(tableName)
	if err != nil {
		return err
	}
	if existing, _, err := t.schema.ColumnByName(c.Name); err == nil {
		if !columnEqual(existing, c) {
			return fmt.Errorf("%w: column %q already exists with a different definition", ErrTableExists, c.Name)
		}
		return nil
	}
	_, err = t.addColumn(c, false)
	return err
}

// RenameColumn 只改名，不动数据槽位。
func (tx *Tx) RenameColumn(tableName, oldName, newName string) error {
	if err := tx.checkWritable("RenameColumn"); err != nil {
		return err
	}
	t, err := tx.table(tableName)
	if err != nil {
		return err
	}
	return t.renameColumn(oldName, newName)
}

// DropColumn 删除列与对应槽位。
func (tx *Tx) DropColumn(tableName, name string) error {
	if err := tx.checkWritable("DropColumn"); err != nil {
		return err
	}
	t, err := tx.table(tableName)
	if err != nil {
		return err
	}
	return t.dropColumn(name)
}

// ---- DML ----

// Get 按主键读取；不存在返回 ErrNotFound。
func (tx *Tx) Get(tableName string, id int64) (Row, error) {
	t, err := tx.table(tableName)
	if err != nil {
		return Row{}, err
	}
	r, ok := t.rows[id]
	if !ok {
		return Row{}, fmt.Errorf("%w: %q id=%d", ErrNotFound, tableName, id)
	}
	return r, nil
}

// List 统一查询入口（offset/cursor 共用同一查询计划）。
func (tx *Tx) List(tableName string, filters []Filter, page PageRequest) (PageResult, error) {
	t, err := tx.table(tableName)
	if err != nil {
		return PageResult{}, err
	}
	return t.query(filters, page)
}

// Insert 插入一行；不允许显式提供 id；带 version 列而未提供时初始化为 0。
func (tx *Tx) Insert(tableName string, values map[string]Value) (int64, error) {
	if err := tx.checkWritable("Insert"); err != nil {
		return 0, err
	}
	t, err := tx.table(tableName)
	if err != nil {
		return 0, err
	}
	if _, _, verErr := t.schema.ColumnByName("version"); verErr == nil {
		if v, ok := values["version"]; ok {
			if !v.IsNull() {
				n, kindOK := v.AsInt()
				if !kindOK {
					return 0, fmt.Errorf("column %q: %w", "version", ErrTypeMismatch)
				}
				if n != 0 {
					return 0, fmt.Errorf("%w: version must start at 0 on insert", ErrImmutableColumn)
				}
			} else {
				return 0, fmt.Errorf("%w: version cannot be null", ErrColumnRequired)
			}
		} else {
			values = cloneValuesWith(values, "version", NewInt(0))
		}
	}
	return t.insert(values)
}

// Update 按列名更新部分字段；id 与 version 由引擎托管，禁止出现在 patch 中。
// expectVersion>=0 时做乐观并发检查，版本不符返回 ErrVersionConflict。
// 更新成功后 version 列（若存在）自动 +1。
func (tx *Tx) Update(tableName string, id int64, patch map[string]Value, expectVersion int64) error {
	if err := tx.checkWritable("Update"); err != nil {
		return err
	}
	t, err := tx.table(tableName)
	if err != nil {
		return err
	}
	r, ok := t.rows[id]
	if !ok {
		return fmt.Errorf("%w: %q id=%d", ErrNotFound, tableName, id)
	}
	if _, conflict := patch["id"]; conflict {
		return fmt.Errorf("%w: id is managed by the engine", ErrImmutableColumn)
	}
	if _, conflict := patch["version"]; conflict {
		return fmt.Errorf("%w: version is managed by the engine", ErrImmutableColumn)
	}

	verIdx := -1
	if _, i, verErr := t.schema.ColumnByName("version"); verErr == nil {
		verIdx = i + 1
	}
	if verIdx >= 0 && expectVersion >= 0 {
		cur, _ := r.cells[verIdx].AsInt()
		if cur != expectVersion {
			return fmt.Errorf("%w: table %q id=%d expected version=%d but stored=%d (rejected, caller may retry)",
				ErrVersionConflict, tableName, id, expectVersion, cur)
		}
	}

	next := r.cellsCopy()
	for name, v := range patch {
		col, i, err := t.schema.ColumnByName(name)
		if err != nil {
			return err
		}
		if err := CheckColumnValue(col, v); err != nil {
			return fmt.Errorf("column %q: %w", name, err)
		}
		next[i+1] = v
	}
	if verIdx >= 0 {
		cur, _ := next[verIdx].AsInt()
		next[verIdx] = NewInt(cur + 1)
	}
	t.rows[id] = newRow(next)
	return nil
}

// Delete 删除一行；不存在返回 ErrNotFound。
func (tx *Tx) Delete(tableName string, id int64) error {
	if err := tx.checkWritable("Delete"); err != nil {
		return err
	}
	t, err := tx.table(tableName)
	if err != nil {
		return err
	}
	if _, ok := t.rows[id]; !ok {
		return fmt.Errorf("%w: %q id=%d", ErrNotFound, tableName, id)
	}
	delete(t.rows, id)
	return nil
}

// ---- 提交 / 回滚（由 db.Update 的事务边界调用） ----

func (tx *Tx) commitLocked() error {
	if tx.done {
		return fmt.Errorf("engine: transaction already finished")
	}
	tx.done = true
	if tx.readOnly {
		return nil
	}
	// 工作副本转正并整体原子落盘。
	tx.db.tables = tx.tables
	return tx.db.persistLocked()
}

func (tx *Tx) rollbackLocked() error {
	tx.done = true
	return nil // 工作副本直接丢弃，已提交快照保持不变
}

func schemaEqual(a, b *Schema) bool {
	if a.Table != b.Table || len(a.Columns) != len(b.Columns) {
		return false
	}
	for i := range a.Columns {
		if !columnEqual(a.Columns[i], b.Columns[i]) {
			return false
		}
	}
	return true
}

func columnEqual(a, b Column) bool {
	if a.Name != b.Name || a.Type != b.Type || a.Nullable != b.Nullable || a.HasDefault != b.HasDefault {
		return false
	}
	return a.Default.kind == b.Default.kind && a.Default.raw == b.Default.raw
}

func cloneValuesWith(in map[string]Value, k string, v Value) map[string]Value {
	out := make(map[string]Value, len(in)+1)
	for kk, vv := range in {
		out[kk] = vv
	}
	out[k] = v
	return out
}
