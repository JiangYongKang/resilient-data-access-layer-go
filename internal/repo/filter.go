// Package repo 提供统一仓储读写：条件过滤、偏移分页与游标分页。
//
// 一致性原则：偏移分页、游标分页与全量列表三个入口共享同一份“过滤校验 +
// 执行计划”逻辑，结果都来自 store.List 的同一套（id 升序、等值过滤）语义，
// 因此同一查询条件在任何入口下得到的数据集与顺序完全一致，只是切片方式不同。
package repo

import (
	derrors "github.com/highcumontoa/resilient-data-access-layer-go/internal/errors"
	"github.com/highcumontoa/resilient-data-access-layer-go/internal/store"
)

// Filter 是一个等值过滤条件。字段合法性与值类型由仓储按表元数据严格校验：
// 字段不存在 -> UNKNOWN_FILTER_FIELD；值缺失 -> MISSING_FILTER_VALUE；
// 类型不匹配 -> FILTER_TYPE_MISMATCH。绝不静默当成“查不到”。
type Filter struct {
	Field string
	Value any
}

// PageRequest 是偏移分页参数；Page 从 1 开始。
type PageRequest struct {
	Page     int
	PageSize int
}

// CursorRequest 是游标分页参数；首页 AfterID=0。
type CursorRequest struct {
	AfterID int64
	Limit   int
}

// PageResult 是偏移分页结果。
type PageResult[T any] struct {
	Items      []T
	Total      int64
	Page       int
	PageSize   int
	TotalPages int
}

// CursorResult 是游标分页结果。
type CursorResult[T any] struct {
	Items   []T
	NextID  int64
	HasMore bool
}

// 分页参数的确定上下界，避免无界全表扫描与“给个超大 limit 等于全量”。
const (
	DefaultPageSize = 20
	MaxPageSize     = 100
)

// validate 严格校验分页参数。
func (p PageRequest) validate() error {
	if p.Page < 1 {
		return derrors.New(derrors.ReasonInvalidPagination,
			"page must be >= 1, got %d (malformed paging must be rejected, not yield an empty/full list)", p.Page)
	}
	if p.PageSize != 0 && p.PageSize < 1 {
		return derrors.New(derrors.ReasonInvalidPagination,
			"page_size must be >= 1 when provided, got %d", p.PageSize)
	}
	if p.PageSize > MaxPageSize {
		return derrors.New(derrors.ReasonInvalidPagination,
			"page_size %d exceeds max %d", p.PageSize, MaxPageSize)
	}
	return nil
}

func (p PageRequest) normalizedSize() int {
	if p.PageSize == 0 {
		return DefaultPageSize
	}
	return p.PageSize
}

func (c CursorRequest) validate() error {
	if c.AfterID < 0 {
		return derrors.New(derrors.ReasonInvalidPagination,
			"after_id must be >= 0, got %d", c.AfterID)
	}
	if c.Limit != 0 && c.Limit < 1 {
		return derrors.New(derrors.ReasonInvalidPagination,
			"limit must be >= 1 when provided, got %d", c.Limit)
	}
	if c.Limit > MaxPageSize {
		return derrors.New(derrors.ReasonInvalidPagination,
			"limit %d exceeds max %d", c.Limit, MaxPageSize)
	}
	return nil
}

func (c CursorRequest) normalizedLimit() int {
	if c.Limit == 0 {
		return DefaultPageSize
	}
	return c.Limit
}

// rowMapper 把存储行映射为领域对象。
type rowMapper[T any] func(store.Row) (T, error)
