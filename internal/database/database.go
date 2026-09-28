// Package database 在存储引擎之上提供并发安全的数据库句柄，
// 模拟连接池：每个入口（请求/事务）拿到独立的工作事务，
// 提交时由引擎做第一提交者胜出的冲突仲裁。
package database

import "github.com/highcumontoa/resilient-data-access-layer-go/internal/store"

// DB 是并发安全的数据库句柄。
type DB struct {
	store *store.Store
}

// Open 创建数据库句柄。
func Open() *DB { return &DB{store: store.Open()} }

// Store 返回底层引擎（迁移器等内部组件使用）。
func (db *DB) Store() *store.Store { return db.store }

// Begin 开启读写事务。
func (db *DB) Begin() (*store.Tx, error) { return db.store.Begin() }

// Exec 在自动提交事务中执行写入。
func (db *DB) Exec(fn func(tx *store.Tx) error) error { return db.store.Exec(fn) }

// View 在只读事务中执行读取。
func (db *DB) View(fn func(tx *store.Tx) error) error { return db.store.View(fn) }
