package engine

import "fmt"

// Row 是一行的位置化表示：cells[0] 永远是 id，
// cells[i+1] 对齐 Schema.Columns[i]。读取按槽位取值，
// 所以列改名不影响数据，删列同步收缩槽位，读取方不会静默错位。
type Row struct {
	cells []Value
}

func newRow(cells []Value) Row { return Row{cells: cells} }

// ID 返回主键。
func (r Row) ID() int64 { v, _ := r.cells[0].AsInt(); return v }

// Get 按列名读取；列不存在返回 ErrColumnNotFound。
func (r Row) Get(s *Schema, name string) (Value, error) {
	if name == "id" {
		return r.cells[0], nil
	}
	_, i, err := s.ColumnByName(name)
	if err != nil {
		return Value{}, err
	}
	return r.GetByIndex(i + 1)
}

// GetByIndex 按槽位读取（id 槽位为 0，用户列从 1 开始）。
func (r Row) GetByIndex(i int) (Value, error) {
	if i < 0 || i >= len(r.cells) {
		return Value{}, fmt.Errorf("%w: cell index %d out of range [0,%d)", ErrColumnNotFound, i, len(r.cells))
	}
	return r.cells[i], nil
}

// cells 返回内部槽位（引擎内部使用）。
func (r Row) cellsCopy() []Value {
	out := make([]Value, len(r.cells))
	copy(out, r.cells)
	return out
}

// Table 是内存中的一张表：结构 + 行 + 自增序列。
type Table struct {
	schema *Schema
	seq    int64
	rows   map[int64]Row
}

func newTable(s *Schema) *Table {
	return &Table{schema: s.Clone(), rows: map[int64]Row{}}
}

// clone 深拷贝表（结构、序列、行槽位全部独立），供事务工作副本使用。
func (t *Table) clone() *Table {
	cp := &Table{
		schema: t.schema.Clone(),
		seq:    t.seq,
		rows:   make(map[int64]Row, len(t.rows)),
	}
	for id, r := range t.rows {
		cp.rows[id] = Row{cells: r.cellsCopy()}
	}
	return cp
}

// insert 校验并插入一行。values 不允许包含 id；
// 缺列依次用“显式提供值 -> 列默认值 -> NULL”补齐，仍不满足非空则拒绝。
func (t *Table) insert(values map[string]Value) (int64, error) {
	if _, ok := values["id"]; ok {
		return 0, fmt.Errorf("%w: id is managed by the engine", ErrImmutableColumn)
	}
	cells := make([]Value, len(t.schema.Columns)+1)
	for i, col := range t.schema.Columns {
		// 缺列依次用“列默认值 -> NULL”补齐，仍不满足非空则拒绝。
		if v, provided := values[col.Name]; provided {
			if err := CheckColumnValue(col, v); err != nil {
				return 0, fmt.Errorf("column %q: %w", col.Name, err)
			}
			cells[i+1] = v
		} else if col.HasDefault {
			cells[i+1] = col.Default
		} else if col.Nullable {
			cells[i+1] = Null()
		} else {
			return 0, fmt.Errorf("%w: column %q has no value and no default", ErrColumnRequired, col.Name)
		}
	}
	for name, v := range values {
		col, i, err := t.schema.ColumnByName(name)
		if err != nil {
			// 未知列直接拒绝，避免把拼错的字段静默丢弃。
			return 0, err
		}
		if err := CheckColumnValue(col, v); err != nil {
			return 0, fmt.Errorf("column %q: %w", name, err)
		}
		cells[i+1] = v
	}
	t.seq++
	id := t.seq
	cells[0] = NewInt(id)
	t.rows[id] = newRow(cells)
	return id, nil
}

// addColumn 在所有已有行尾部追加槽位。新列必须有确定语义：
// 有默认则每行填默认；否则必须 Nullable，每行填 NULL。
// 列已存在时（重复迁移）按幂等处理：直接返回现有列定义。
func (t *Table) addColumn(c Column, ignoreExisting bool) (Column, error) {
	if existing, _, err := t.schema.ColumnByName(c.Name); err == nil {
		if ignoreExisting {
			return existing, nil
		}
		return Column{}, fmt.Errorf("%w: column %q already exists", ErrTableExists, c.Name)
	}
	tmp := &Schema{Table: t.schema.Table, Columns: append(append([]Column(nil), t.schema.Columns...), c)}
	if err := tmp.Validate(); err != nil {
		return Column{}, err
	}
	if !c.HasDefault && !c.Nullable {
		return Column{}, fmt.Errorf("%w: adding non-nullable column %q requires a default", ErrInvalidValue, c.Name)
	}
	fill := Null()
	if c.HasDefault {
		fill = c.Default
	}
	for id, r := range t.rows {
		r.cells = append(r.cells, fill)
		t.rows[id] = r
	}
	t.schema.Columns = append(t.schema.Columns, c)
	return c, nil
}

// renameColumn 只改列名，槽位与数据不动（读取方不会错位列）。
// 目标名已存在则拒绝；源列不存在返回 ErrColumnNotFound。
func (t *Table) renameColumn(oldName, newName string) error {
	_, i, err := t.schema.ColumnByName(oldName)
	if err != nil {
		return err
	}
	if oldName == newName {
		return nil
	}
	if _, _, err := t.schema.ColumnByName(newName); err == nil {
		return fmt.Errorf("%w: cannot rename %q to %q: target exists", ErrTableExists, oldName, newName)
	}
	t.schema.Columns[i].Name = newName
	return nil
}

// dropColumn 移除列及其在每一行的槽位（收缩后后续列前移，
// 因为 Schema 与行同步收缩，读取仍按槽位对齐，不会错位）。
func (t *Table) dropColumn(name string) error {
	_, i, err := t.schema.ColumnByName(name)
	if err != nil {
		return err
	}
	for id, r := range t.rows {
		r.cells = append(append([]Value(nil), r.cells[:i+1]...), r.cells[i+2:]...)
		t.rows[id] = r
	}
	t.schema.Columns = append(append([]Column(nil), t.schema.Columns[:i]...), t.schema.Columns[i+1:]...)
	return nil
}
