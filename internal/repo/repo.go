// Package repo 在引擎之上提供泛型仓储：统一的条件过滤 + 双模式分页，
// 以及基于版本列的乐观并发更新。
//
// 实例化方式：T 为实体指针类型，例如 repo.New[*domain.Account](db, codec)。
package repo

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/highcumontoa/resilient-data-access-layer-go/internal/engine"
)

// Entity 是仓储托管实体必须实现的契约：主键与版本列。
type Entity interface {
	GetID() int64
	SetID(id int64)
	GetVersion() int64
	SetVersion(v int64)
}

// EntityPtr 约束仓储的类型参数：T 必须是指向实体 E 的指针，
// 实例化形如 repo.New[*domain.Account](db, codec)。
type EntityPtr[T any] interface {
	Entity
	*T
}

// Codec 定义实体 <-> 行 的双向映射与表结构。
// 映射是显式的：未知列/未知字段都在编解码阶段按原因报错。
type Codec[E any, T EntityPtr[E]] struct {
	// Table 返回实体对应的表结构。
	Table func() *engine.Schema
	// ToRow 把实体字段映射为列值（不含 id；version 由仓储托管）。
	ToRow func(e T) (map[string]engine.Value, error)
	// Scan 把行内容填充进已存在的实体（id/version 由仓储统一回填）。
	Scan func(row engine.Row, schema *engine.Schema, dst T) error
}

// Repository 是泛型仓储。所有读路径共用同一个查询计划，
// 因此 offset 分页与 cursor 分页、事务内与事务外结果一致。
type Repository[E any, T EntityPtr[E]] struct {
	db    *engine.DB
	codec Codec[E, T]
	// tx 非空表示仓储处于某个事务边界内，所有方法复用该事务；
	// 提交/回滚由最外层 InTx 统一决定，保证“整批生效或整批不生效”。
	tx *engine.Tx
}

// New 构造仓储。
func New[E any, T EntityPtr[E]](db *engine.DB, codec Codec[E, T]) *Repository[E, T] {
	if codec.Table == nil || codec.ToRow == nil || codec.Scan == nil {
		panic("repo: codec requires Table/ToRow/Scan")
	}
	return &Repository[E, T]{db: db, codec: codec}
}

func (r *Repository[E, T]) withTx(tx *engine.Tx) *Repository[E, T] {
	return &Repository[E, T]{db: r.db, codec: r.codec, tx: tx}
}

func (r *Repository[E, T]) tableName() string { return r.codec.Table().Table }

// view 在已有事务上执行，或新开只读事务。
func (r *Repository[E, T]) view(ctx context.Context, fn func(tx *engine.Tx) error) error {
	if r.tx != nil {
		return fn(r.tx)
	}
	return r.db.View(ctx, fn)
}

// update 在已有事务上执行，或新开读写事务（整批原子提交）。
func (r *Repository[E, T]) update(ctx context.Context, fn func(tx *engine.Tx) error) error {
	if r.tx != nil {
		return fn(r.tx)
	}
	return r.db.Update(ctx, fn)
}

// ListSpec 是一次列表查询的完整描述。
type ListSpec struct {
	Filters []engine.Filter
	Limit   int    // 必填，1..MaxLimit
	Offset  int    // offset 分页：>=0；与非空 Cursor 互斥
	Cursor  string // cursor 分页：非空时 Offset 必须为 0
	Desc    bool
}

// MaxLimit 是单页上限，防止一次拉全量。
const MaxLimit = 500

// ListResult 是统一分页结果。
type ListResult[E any, T EntityPtr[E]] struct {
	Items      []T
	Total      int
	NextCursor string // HasMore=false 时为空
	HasMore    bool
}

// validate 校验分页参数：非法即拒绝，不静默回退为默认页或全量。
func (s ListSpec) validate() error {
	if s.Limit <= 0 {
		return fmt.Errorf("%w: limit must be > 0, got %d", engine.ErrInvalidPagination, s.Limit)
	}
	if s.Limit > MaxLimit {
		return fmt.Errorf("%w: limit %d exceeds max %d", engine.ErrInvalidPagination, s.Limit, MaxLimit)
	}
	if s.Offset < 0 {
		return fmt.Errorf("%w: offset must be >= 0, got %d", engine.ErrInvalidPagination, s.Offset)
	}
	if s.Cursor != "" && s.Offset != 0 {
		return fmt.Errorf("%w: cursor and offset are mutually exclusive (offset=%d)", engine.ErrInvalidPagination, s.Offset)
	}
	return nil
}

// query 是所有列表入口共用的唯一查询计划。
func (r *Repository[E, T]) query(ctx context.Context, spec ListSpec) (ListResult[E, T], error) {
	if err := spec.validate(); err != nil {
		return ListResult[E, T]{}, err
	}
	out := ListResult[E, T]{Items: []T{}}
	err := r.view(ctx, func(tx *engine.Tx) error {
		schema, err := tx.TableSchema(r.tableName())
		if err != nil {
			return err
		}
		page, err := tx.List(r.tableName(), spec.Filters, engine.PageRequest{
			Limit: spec.Limit, Offset: spec.Offset, Cursor: spec.Cursor, Desc: spec.Desc,
		})
		if err != nil {
			return err
		}
		out.Total = page.Total
		out.HasMore = page.HasMore
		if page.HasMore {
			out.NextCursor = engine.EncodeCursor(page.NextFrom, spec.Desc)
		}
		for _, row := range page.Rows {
			e := T(new(E))
			if err := r.codec.Scan(row, schema, e); err != nil {
				return err
			}
			r.fillManaged(row, schema, e)
			out.Items = append(out.Items, e)
		}
		return nil
	})
	if err != nil {
		return ListResult[E, T]{}, err
	}
	return out, nil
}

// fillManaged 统一回填引擎托管字段 id/version，保证不同入口结果一致。
func (r *Repository[E, T]) fillManaged(row engine.Row, schema *engine.Schema, e T) {
	e.SetID(row.ID())
	if _, _, err := schema.ColumnByName("version"); err == nil {
		if v, gErr := row.Get(schema, "version"); gErr == nil {
			if n, ok := v.AsInt(); ok {
				e.SetVersion(n)
			}
		}
	}
}

// List 是统一查询入口：同时支持 offset 与 cursor 两种分页。
func (r *Repository[E, T]) List(ctx context.Context, spec ListSpec) (ListResult[E, T], error) {
	return r.query(ctx, spec)
}

// ListPage 是 offset 分页便捷入口，与 List 走同一查询计划。
func (r *Repository[E, T]) ListPage(ctx context.Context, filters []engine.Filter, offset, limit int) (ListResult[E, T], error) {
	return r.query(ctx, ListSpec{Filters: filters, Offset: offset, Limit: limit})
}

// ListCursor 是 cursor 分页便捷入口，与 List 走同一查询计划。
func (r *Repository[E, T]) ListCursor(ctx context.Context, filters []engine.Filter, cursor string, limit int) (ListResult[E, T], error) {
	return r.query(ctx, ListSpec{Filters: filters, Cursor: cursor, Limit: limit})
}

// Create 插入实体并回填 id；version 由仓储初始化为 0。
func (r *Repository[E, T]) Create(ctx context.Context, e T) error {
	if isNilEntity(e) {
		return fmt.Errorf("%w: nil entity", engine.ErrInvalidValue)
	}
	values, err := r.codec.ToRow(e)
	if err != nil {
		return err
	}
	return r.update(ctx, func(tx *engine.Tx) error {
		id, err := tx.Insert(r.tableName(), values)
		if err != nil {
			return err
		}
		return r.reload(tx, id, e)
	})
}

// BatchCreate 在单个事务里批量插入：全部成功或全部回滚，不会部分提交。
func (r *Repository[E, T]) BatchCreate(ctx context.Context, entities []T) error {
	if len(entities) == 0 {
		return nil
	}
	return r.update(ctx, func(tx *engine.Tx) error {
		for _, e := range entities {
			if isNilEntity(e) {
				return fmt.Errorf("%w: nil entity in batch", engine.ErrInvalidValue)
			}
			values, err := r.codec.ToRow(e)
			if err != nil {
				return err
			}
			id, err := tx.Insert(r.tableName(), values)
			if err != nil {
				return err
			}
			if err := r.reload(tx, id, e); err != nil {
				return err
			}
		}
		return nil
	})
}

// reload 在事务内回读整行并回填实体，使列默认值与引擎托管字段对调用方可见。
func (r *Repository[E, T]) reload(tx *engine.Tx, id int64, e T) error {
	schema, err := tx.TableSchema(r.tableName())
	if err != nil {
		return err
	}
	row, err := tx.Get(r.tableName(), id)
	if err != nil {
		return err
	}
	if err := r.codec.Scan(row, schema, e); err != nil {
		return err
	}
	r.fillManaged(row, schema, e)
	return nil
}

// Get 按 id 读取；不存在返回 engine.ErrNotFound。
func (r *Repository[E, T]) Get(ctx context.Context, id int64) (T, error) {
	var out T
	err := r.view(ctx, func(tx *engine.Tx) error {
		schema, err := tx.TableSchema(r.tableName())
		if err != nil {
			return err
		}
		row, err := tx.Get(r.tableName(), id)
		if err != nil {
			return err
		}
		out = T(new(E))
		if err := r.codec.Scan(row, schema, out); err != nil {
			return err
		}
		r.fillManaged(row, schema, out)
		return nil
	})
	if err != nil {
		var zero T
		return zero, err
	}
	return out, nil
}

// Update 基于版本做乐观并发更新：实体的 version 必须与库中一致，
// 否则返回 engine.ErrVersionConflict（调用方可选择重试）；
// 成功后库中 version 自动 +1 并回填到实体。
func (r *Repository[E, T]) Update(ctx context.Context, e T) error {
	if isNilEntity(e) {
		return fmt.Errorf("%w: nil entity", engine.ErrInvalidValue)
	}
	if e.GetID() <= 0 {
		return fmt.Errorf("%w: entity must be persisted before update", engine.ErrInvalidValue)
	}
	values, err := r.codec.ToRow(e)
	if err != nil {
		return err
	}
	expected := e.GetVersion()
	return r.update(ctx, func(tx *engine.Tx) error {
		schema, err := tx.TableSchema(r.tableName())
		if err != nil {
			return err
		}
		if _, _, verErr := schema.ColumnByName("version"); verErr != nil {
			return fmt.Errorf("%w: table %q has no version column", engine.ErrInvalidValue, r.tableName())
		}
		if err := tx.Update(r.tableName(), e.GetID(), values, expected); err != nil {
			return err
		}
		row, err := tx.Get(r.tableName(), e.GetID())
		if err != nil {
			return err
		}
		v, _ := row.Get(schema, "version")
		n, _ := v.AsInt()
		e.SetVersion(n)
		return nil
	})
}

// Delete 删除实体；不存在返回 engine.ErrNotFound。
func (r *Repository[E, T]) Delete(ctx context.Context, id int64) error {
	return r.update(ctx, func(tx *engine.Tx) error {
		return tx.Delete(r.tableName(), id)
	})
}

// InTx 在同一事务边界内执行 fn；fn 返回 error 则整批回滚（全有或全无）。
// 嵌套调用会加入外层事务，由最外层统一提交，避免内层提前半提交。
func (r *Repository[E, T]) InTx(ctx context.Context, fn func(txRepo *Repository[E, T]) error) error {
	if r.tx != nil {
		return fn(r)
	}
	return r.db.Update(ctx, func(tx *engine.Tx) error {
		return fn(r.withTx(tx))
	})
}

// RetryOnConflict 在乐观锁冲突时按 attempts 上限重试 fn；
// 非冲突错误立即返回。是否重试由调用方显式决定。
func RetryOnConflict(attempts int, fn func() error) error {
	if attempts < 1 {
		return fmt.Errorf("%w: attempts must be >= 1, got %d", engine.ErrInvalidValue, attempts)
	}
	var err error
	for i := 0; i < attempts; i++ {
		err = fn()
		if err == nil {
			return nil
		}
		if !errors.Is(err, engine.ErrVersionConflict) {
			return err
		}
	}
	return fmt.Errorf("repo: exhausted %d conflict retries: %w", attempts, err)
}

// isNilEntity 识别“带类型的 nil 指针”，直接比较 any(e)==nil 会漏判。
func isNilEntity[E any, T EntityPtr[E]](e T) bool {
	v := reflect.ValueOf(e)
	return v.Kind() == reflect.Ptr && v.IsNil()
}
