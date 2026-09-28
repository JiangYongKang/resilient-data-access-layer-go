// Package store 是仅依赖标准库的进程内事务型存储引擎。
//
// 隔离与并发语义：
//   - Begin 取得当时已提交数据的深拷贝快照，事务内全部读写都在私有副本上进行；
//   - 采用第一提交者胜出（first-committer-wins）：提交时校验事务读过的每张表
//     自快照后未被其他已提交事务修改，否则返回 ErrConflict，本事务不留下任何写入；
//   - DDL 与 DML 一样参与事务，提交失败时结构变更与数据写入一并回滚；
//   - Store 自身通过 RWMutex 串行化提交，模拟连接池下多事务的仲裁点。
package store

import (
	"errors"
	"sync"
)

// 引擎级哨兵错误。上层在边界处翻译为带原因码的 DataError。
var (
	ErrConflict          = errors.New("store: commit conflict, read table changed by another committed transaction")
	ErrTableExists       = errors.New("store: table already exists")
	ErrTableNotFound     = errors.New("store: table not found")
	ErrColumnExists      = errors.New("store: column already exists")
	ErrColumnNotFound    = errors.New("store: column not found")
	ErrTypeMismatch      = errors.New("store: value type does not match column type")
	ErrConstraint        = errors.New("store: constraint violation")
	ErrReadOnly          = errors.New("store: transaction is read-only")
	ErrTxClosed          = errors.New("store: transaction already committed or rolled back")
	ErrInvalidArgument   = errors.New("store: invalid argument")
	ErrDestructiveDenied = errors.New("store: destructive DDL denied: drop/rename column requires explicit approval")
	ErrUniqueViolation   = errors.New("store: unique constraint violation")
)

// ColumnType 描述列的标量类型，过滤与写入时据此做严格类型校验。
type ColumnType int

const (
	TypeInteger ColumnType = iota
	TypeText
	TypeReal
	TypeBool
)

// Column 是一张表的列元数据。
type Column struct {
	Name       string
	Type       ColumnType
	Nullable   bool
	HasDefault bool
	Default    any
	Unique     bool // 该列（含隐式 id）具有唯一约束，写入与提交时双重校验
}

// Row 是一行数据：用户列名 -> 值（读接口会额外投影 id 与 version）。
type Row map[string]any

// Store 是数据库实例。
type Store struct {
	mu     sync.RWMutex
	tables map[string]*tableState

	// nextID 为每张表保存“下一个待分配 id”水位，在 Insert 时推进（而非快照时），
	// 因此并发事务即使基于同一快照也不会拿到相同 id；回滚事务只留下空洞，无害。
	nextID map[string]int64
}

// allocateID 推进某表的全局自增计数器并返回新 id。
// 计数器在“分配时”即持久推进：未提交事务回滚只会产生空洞，绝不会让后来者
// 重新分配到同一 id（与数据库自增列的通常语义一致）。
// allocateID 返回一个新的自增 id。入参 minNext 是调用事务视角里的
// “下一个待分配 id”下界；全局水位取二者较大者，分配后水位 +1。
func (s *Store) allocateID(table string, minNext int64) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.nextID == nil {
		s.nextID = make(map[string]int64)
	}
	n := s.nextID[table]
	if n < minNext {
		n = minNext
	}
	s.nextID[table] = n + 1
	return n
}

// initNextID 仅在全局尚无该表水位时设定初始值。
func (s *Store) initNextID(table string, n int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.nextID[table]; !ok {
		s.nextID[table] = n
	}
}

// peekNextID 返回某表当前的“下一个待分配 id”水位。
func (s *Store) peekNextID(table string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n, ok := s.nextID[table]; ok {
		return n
	}
	return 1
}

// Tx 是事务的私有工作副本。
type Tx struct {
	store    *Store
	closed   bool
	readOnly bool

	priv    map[string]*tableState // 快照副本 + 本事务新建表
	baseVer map[string]int64       // 快照时每张已存在表的表版本

	// 提交仲裁所需的依赖跟踪：
	fullRead   map[string]struct{}           // 整表读（List/Count/HasTable/Columns/DDL 既有表）
	rowRead    map[string]map[int64]int64    // 行读：表 -> 行id -> 读取时的行版本
	writeRows  map[string]map[int64]int64    // 行写：表 -> 行id -> 快照行版本（首次写时记录）
	insertRows map[string]map[int64]struct{} // 表 -> 插入的新行 id 集合
	deleteRows map[string]map[int64]struct{} // 表 -> 删除的行 id 集合
	newTables  map[string]struct{}           // 本事务新建的表
	ddlTables  map[string]struct{}           // 在既有表上做过 DDL

	destructiveAllowed bool // 是否允许删列/改名
}

// tableState 是一张表的内存状态。
type tableState struct {
	name    string
	columns []Column      // 用户列（id/version 为隐式系统字段）
	rows    map[int64]Row // 深拷贝后的可变行
	order   []int64       // 按 id 升序
	nextID  int64
	tableV  int64           // 表级版本：任一次提交写入/结构变更后 +1
	rowV    map[int64]int64 // 行级乐观版本
}

// SystemColumnID / SystemColumnVersion 是所有表隐式携带的系统字段名。
const (
	SystemColumnID      = "id"
	SystemColumnVersion = "version"
)
