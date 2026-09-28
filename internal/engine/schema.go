package engine

import "fmt"

// Column 描述一列。列按定义顺序固定位置（positional storage），
// 重命名只改名字不动位置；删列移除对应槽位；读取按槽位对齐列，
// 因此列的增删改名不会让读取方静默错位。
type Column struct {
	Name string `json:"name"`
	Type Kind   `json:"type"`
	// Nullable=true 允许写入 NULL。
	Nullable bool `json:"nullable,omitempty"`
	// HasDefault 为 true 时，写入未提供该列使用 Default；
	// 为 false 且 Nullable=false 时缺列即拒绝。
	// 加列迁移必须给出确定默认：要么 HasDefault=true，要么 Nullable=true。
	HasDefault bool  `json:"has_default,omitempty"`
	Default    Value `json:"default"`
}

// Schema 是一张表的结构定义。用户列存放在 Columns；
// 隐式主键 id 永远占据行的第 0 槽位。
type Schema struct {
	Table   string   `json:"table"`
	Columns []Column `json:"columns"`
}

// NewSchema 构造结构并做静态校验（重名列、非法默认值等立即拒绝）。
func NewSchema(table string, cols []Column) (*Schema, error) {
	s := &Schema{Table: table, Columns: cols}
	if err := s.Validate(); err != nil {
		return nil, err
	}
	return s, nil
}

// Validate 校验结构自身合法性。
func (s *Schema) Validate() error {
	if s.Table == "" {
		return fmt.Errorf("%w: empty table name", ErrInvalidValue)
	}
	seen := map[string]bool{}
	for i, c := range s.Columns {
		if c.Name == "" {
			return fmt.Errorf("%w: table %q column #%d has empty name", ErrInvalidValue, s.Table, i)
		}
		if c.Name == "id" {
			return fmt.Errorf("%w: column name %q is reserved", ErrInvalidValue, c.Name)
		}
		if seen[c.Name] {
			return fmt.Errorf("%w: duplicate column %q in table %q", ErrInvalidValue, c.Name, s.Table)
		}
		seen[c.Name] = true
		if c.HasDefault {
			if err := CheckColumnValue(c, c.Default); err != nil {
				return fmt.Errorf("invalid default for column %q: %w", c.Name, err)
			}
		}
	}
	return nil
}

// ColumnByName 找不到时返回 ErrColumnNotFound。
func (s *Schema) ColumnByName(name string) (Column, int, error) {
	for i, c := range s.Columns {
		if c.Name == name {
			return c, i, nil
		}
	}
	return Column{}, 0, fmt.Errorf("%w: column %q", ErrColumnNotFound, name)
}

// HasVersion 报告表是否带乐观版本列 version。
func (s *Schema) HasVersion() bool {
	_, _, err := s.ColumnByName("version")
	return err == nil
}

// Clone 深拷贝结构定义。
func (s *Schema) Clone() *Schema {
	if s == nil {
		return nil
	}
	cp := &Schema{Table: s.Table, Columns: make([]Column, len(s.Columns))}
	copy(cp.Columns, s.Columns)
	return cp
}
