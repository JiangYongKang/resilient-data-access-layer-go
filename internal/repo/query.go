package repo

import (
	derrors "github.com/highcumontoa/resilient-data-access-layer-go/internal/errors"
	"github.com/highcumontoa/resilient-data-access-layer-go/internal/store"
)

// scanPlan 是过滤/排序/分页归一化后的内部执行计划。
// 三个查询入口各自构造计划，但过滤集合与“id 升序”的基础顺序完全一致，
// 因此同一查询条件下：各分页页面对全量结果做不重不漏的划分。
type scanPlan struct {
	filters []store.Filter
	offset  int
	limit   int
	afterID int64
}

// FindByPage 偏移分页入口。
// 非法分页参数 -> INVALID_PAGINATION；畸形过滤 -> 对应过滤错误，均不返回数据。
func (r *Repository[T]) FindByPage(filters []Filter, page PageRequest) (PageResult[T], error) {
	if err := page.validate(); err != nil {
		return PageResult[T]{}, err
	}
	size := page.normalizedSize()
	var out PageResult[T]
	err := r.db.View(func(tx *store.Tx) error {
		if err := r.validateFilters(tx, filters); err != nil {
			return err
		}
		sf := toStoreFilters(filters)
		total, err := tx.Count(r.table, sf)
		if err != nil {
			return err
		}
		plan := scanPlan{
			filters: sf,
			offset:  (page.Page - 1) * size,
			limit:   size,
		}
		rows, err := tx.List(r.table, store.ScanOptions{
			Filters: plan.filters,
			Offset:  plan.offset,
			Limit:   plan.limit,
		})
		if err != nil {
			return err
		}
		items, err := r.mapAll(rows)
		if err != nil {
			return err
		}
		out = PageResult[T]{
			Items:      items,
			Total:      total,
			Page:       page.Page,
			PageSize:   size,
			TotalPages: int((total + int64(size) - 1) / int64(size)),
		}
		return nil
	})
	if err != nil {
		return PageResult[T]{}, translateStoreError(err)
	}
	return out, nil
}

// FindByCursor 游标分页入口。游标是上一页最后一条的 id，按 id 严格翻页。
// 数据集与偏移入口相同；HasMore 由“多取一条”判定，NextID 为本页最后一条 id。
func (r *Repository[T]) FindByCursor(filters []Filter, cur CursorRequest) (CursorResult[T], error) {
	if err := cur.validate(); err != nil {
		return CursorResult[T]{}, err
	}
	limit := cur.normalizedLimit()
	var out CursorResult[T]
	err := r.db.View(func(tx *store.Tx) error {
		if err := r.validateFilters(tx, filters); err != nil {
			return err
		}
		sf := toStoreFilters(filters)
		rows, err := tx.List(r.table, store.ScanOptions{
			Filters: sf,
			AfterID: cur.AfterID,
			Limit:   limit + 1, // 多取一条以判定是否还有下一页
		})
		if err != nil {
			return err
		}
		hasMore := len(rows) > limit
		if hasMore {
			rows = rows[:limit]
		}
		items, err := r.mapAll(rows)
		if err != nil {
			return err
		}
		var nextID int64
		if hasMore && len(rows) > 0 {
			nextID = rows[len(rows)-1][store.SystemColumnID].(int64)
		}
		out = CursorResult[T]{Items: items, NextID: nextID, HasMore: hasMore}
		return nil
	})
	if err != nil {
		return CursorResult[T]{}, translateStoreError(err)
	}
	return out, nil
}

// FindAll 无分页入口：内部同样走过滤校验与 id 升序扫描，但施加有界上限，
// 既保证与分页入口同源一致，又避免无界读取。
func (r *Repository[T]) FindAll(filters []Filter) ([]T, error) {
	var out []T
	err := r.db.View(func(tx *store.Tx) error {
		if err := r.validateFilters(tx, filters); err != nil {
			return err
		}
		sf := toStoreFilters(filters)
		rows, err := tx.List(r.table, store.ScanOptions{Filters: sf, Limit: MaxPageSize})
		if err != nil {
			return err
		}
		items, err := r.mapAll(rows)
		if err != nil {
			return err
		}
		out = items
		return nil
	})
	if err != nil {
		return nil, translateStoreError(err)
	}
	return out, nil
}

// DrainByCursor 以游标入口翻完整个过滤结果集，用于与偏移分页/全量入口对账。
func (r *Repository[T]) DrainByCursor(filters []Filter, pageSize int) ([]T, error) {
	if pageSize <= 0 {
		pageSize = DefaultPageSize
	}
	var all []T
	var cursor int64
	for {
		page, err := r.FindByCursor(filters, CursorRequest{AfterID: cursor, Limit: pageSize})
		if err != nil {
			return nil, err
		}
		all = append(all, page.Items...)
		if !page.HasMore {
			break
		}
		cursor = page.NextID
		if cursor == 0 {
			return nil, derrors.New(derrors.ReasonInvalidPagination,
				"cursor pagination reported HasMore without a NextID")
		}
	}
	return all, nil
}

func (r *Repository[T]) mapAll(rows []store.Row) ([]T, error) {
	items := make([]T, 0, len(rows))
	for _, row := range rows {
		item, err := r.mapper(row)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, nil
}

// translateStoreError 把引擎哨兵错误翻译为可区分原因码。
func translateStoreError(err error) error {
	if err == nil {
		return nil
	}
	if de, ok := err.(*derrors.DataError); ok {
		return de
	}
	return derrors.Wrap(derrors.ReasonUnknown, err, "repository query failed")
}
