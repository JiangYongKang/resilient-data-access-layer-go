package store

import "fmt"

// validateColumn 校验列定义本身合法（列名、类型、默认值类型）。
func validateColumn(cols []Column, col Column) error {
	if col.Name == "" || col.Name == SystemColumnID || col.Name == SystemColumnVersion {
		return fmt.Errorf("%w: reserved or empty column name %q", ErrInvalidArgument, col.Name)
	}
	for _, c := range cols {
		if c.Name == col.Name {
			return fmt.Errorf("%w: column %q", ErrColumnExists, col.Name)
		}
	}
	if col.HasDefault {
		if _, err := checkValue(col, col.Default); err != nil {
			return err
		}
	}
	return nil
}

// CreateTable 新建一张表；列名重复或含保留名则拒绝。新表不登记读集（尚未被他人可见）。
func (tx *Tx) CreateTable(name string, columns []Column) error {
	if tx.closed {
		return ErrTxClosed
	}
	if tx.readOnly {
		return ErrReadOnly
	}
	if name == "" {
		return fmt.Errorf("%w: empty table name", ErrInvalidArgument)
	}
	if _, ok := tx.priv[name]; ok {
		return fmt.Errorf("%w: %q", ErrTableExists, name)
	}
	seen := make(map[string]struct{}, len(columns))
	for _, col := range columns {
		if col.Name == "" || col.Name == SystemColumnID || col.Name == SystemColumnVersion {
			return fmt.Errorf("%w: reserved or empty column name %q", ErrInvalidArgument, col.Name)
		}
		if _, dup := seen[col.Name]; dup {
			return fmt.Errorf("%w: column %q", ErrColumnExists, col.Name)
		}
		seen[col.Name] = struct{}{}
		if col.HasDefault {
			if _, err := checkValue(col, col.Default); err != nil {
				return err
			}
		}
	}
	tx.priv[name] = &tableState{
		name:    name,
		columns: append([]Column(nil), columns...),
		rows:    make(map[int64]Row),
		rowV:    make(map[int64]int64),
		nextID:  1,
	}
	tx.newTables[name] = struct{}{}
	tx.baseVer[name] = 0 // 新表在快照视角的初始版本为 0
	// 新表全局自增水位初始为 1；提交失败会留下空洞水位，无害（新表随回滚消失，
	// 再次建表为同名新事务，水位从 1 重新开始）。
	tx.store.initNextID(name, 1)
	return nil
}

// HasTable 判断表是否存在（登记读集，供提交冲突仲裁）。
func (tx *Tx) HasTable(name string) bool {
	if tx.closed {
		return false
	}
	_, ok := tx.priv[name]
	if ok {
		tx.markFullRead(name)
	}
	return ok
}

// Columns 返回某张表的当前用户列（事务视角），供上层做字段校验。
func (tx *Tx) Columns(table string) ([]Column, error) {
	t, err := tx.tableForRead(table)
	if err != nil {
		return nil, err
	}
	tx.markFullRead(table)
	return append([]Column(nil), t.columns...), nil
}

// AddColumn 给已有表追加列。
// 确定的默认语义：既有行立即获得该列默认值（行本身不被“修改”，故行版本不变），
// 之后插入的新行若未提供该列，也取同一默认值。重复加同一列返回 ErrColumnExists。
func (tx *Tx) AddColumn(table string, col Column) error {
	t, err := tx.tableForWrite(table)
	if err != nil {
		return err
	}
	tx.ddlTables[table] = struct{}{}
	tx.markFullRead(table)
	if err := validateColumn(t.columns, col); err != nil {
		return err
	}
	def, err := defaultFor(col)
	if err != nil {
		return err
	}
	for _, id := range t.order {
		t.rows[id][col.Name] = def
	}
	t.columns = append(t.columns, col)
	return nil
}

// DropColumn 删除用户列及其在每行中的数据。
// 注意：这是破坏性结构变更，迁移协议默认不允许，需显式授权（见 migrate 包）。
func (tx *Tx) DropColumn(table, column string) error {
	t, err := tx.tableForWrite(table)
	if err != nil {
		return err
	}
	tx.ddlTables[table] = struct{}{}
	tx.markFullRead(table)
	idx := -1
	for i, c := range t.columns {
		if c.Name == column {
			idx = i
			break
		}
	}
	if idx < 0 {
		return fmt.Errorf("%w: column %q on table %q", ErrColumnNotFound, column, table)
	}
	if !tx.destructiveAllowed {
		return fmt.Errorf("%w: DropColumn(%q.%q)", ErrDestructiveDenied, table, column)
	}
	t.columns = append(t.columns[:idx], t.columns[idx+1:]...)
	for _, row := range t.rows {
		delete(row, column)
	}
	return nil
}

// RenameColumn 重命名用户列，逐行搬运数据，不留“同位置列”假象。
// 破坏性结构变更，迁移协议默认不允许，需显式授权。
func (tx *Tx) RenameColumn(table, oldName, newName string) error {
	t, err := tx.tableForWrite(table)
	if err != nil {
		return err
	}
	tx.ddlTables[table] = struct{}{}
	tx.markFullRead(table)
	if newName == "" || newName == SystemColumnID || newName == SystemColumnVersion {
		return fmt.Errorf("%w: reserved or empty column name %q", ErrInvalidArgument, newName)
	}
	for _, c := range t.columns {
		if c.Name == newName {
			return fmt.Errorf("%w: column %q", ErrColumnExists, newName)
		}
	}
	idx := -1
	for i, c := range t.columns {
		if c.Name == oldName {
			idx = i
			break
		}
	}
	if idx < 0 {
		return fmt.Errorf("%w: column %q on table %q", ErrColumnNotFound, oldName, table)
	}
	if !tx.destructiveAllowed {
		return fmt.Errorf("%w: RenameColumn(%q.%q->%q)", ErrDestructiveDenied, table, oldName, newName)
	}
	t.columns[idx].Name = newName
	for _, row := range t.rows {
		if v, ok := row[oldName]; ok {
			row[newName] = v
			delete(row, oldName)
		}
	}
	return nil
}
