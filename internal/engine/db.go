package engine

import (
	"context"
	"fmt"
	"sync"
)

// Options 控制本地引擎行为。
type Options struct {
	// Path 为数据文件路径；":memory:" 或空串表示纯内存（不落盘）。
	Path string
}

// DB 是本地数据访问底座：JSON 快照 + 原子 rename 落盘。
//
// 并发模型（单进程内等价于“连接池”）：
//   - 一把 RWMutex 作为唯一写门：任意时刻最多一个读写事务，
//     多个只读事务可并发读取已提交快照；
//   - 读写事务在整库工作副本上执行，提交时一次性原子落盘，
//     因此写事务之间天然“可串行化”，不存在部分提交；
//   - 配合行内 version 列，跨事务的乐观更新可显式检测冲突。
type DB struct {
	path string

	mu     sync.RWMutex
	tables map[string]*Table

	closed bool
}

// Open 打开（不存在则初始化空库）。
func Open(opts Options) (*DB, error) {
	db := &DB{path: opts.Path, tables: map[string]*Table{}}
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.loadLocked(); err != nil {
		return nil, err
	}
	return db, nil
}

// Close 释放资源。
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return fmt.Errorf("engine: db already closed")
	}
	db.closed = true
	return nil
}

// View 在只读事务中执行 fn。只读事务共享当前已提交快照，
// 持用 RLock 期间没有写者能改动数据。
func (db *DB) View(ctx context.Context, fn func(tx *Tx) error) (err error) {
	tx, rl, err := db.begin(ctx, true)
	if err != nil {
		return err
	}
	defer rl()
	return fn(tx)
}

// Update 在读写事务中执行 fn：fn 返回 error 或 panic 则整体回滚，
// 否则一次性原子提交。写事务经写门全局串行化。
func (db *DB) Update(ctx context.Context, fn func(tx *Tx) error) (err error) {
	tx, ul, err := db.begin(ctx, false)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if p := recover(); p != nil {
			_ = tx.rollbackLocked()
			ul()
			panic(p) // 重新抛出，调用方明确知道事务已回滚
		}
		if !committed {
			_ = tx.rollbackLocked()
			ul()
		}
	}()
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.commitLocked(); err != nil {
		return err
	}
	committed = true
	ul()
	return nil
}

// begin 开启事务。readOnly=true 走 RLock（共享快照）；
// false 走 Lock 并克隆整库工作副本。
func (db *DB) begin(ctx context.Context, readOnly bool) (*Tx, func(), error) {
	if readOnly {
		db.mu.RLock()
		if db.closed {
			db.mu.RUnlock()
			return nil, nil, fmt.Errorf("engine: db closed")
		}
		tx := &Tx{ctx: ctx, db: db, readOnly: true, tables: db.tables, done: false}
		return tx, db.mu.RUnlock, nil
	}
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return nil, nil, fmt.Errorf("engine: db closed")
	}
	work := make(map[string]*Table, len(db.tables))
	for name, t := range db.tables {
		work[name] = t.clone()
	}
	tx := &Tx{ctx: ctx, db: db, readOnly: false, tables: work, done: false}
	return tx, db.mu.Unlock, nil
}
