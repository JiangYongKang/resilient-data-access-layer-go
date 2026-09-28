package repo

import (
	"github.com/highcumontoa/resilient-data-access-layer-go/internal/database"
	derrors "github.com/highcumontoa/resilient-data-access-layer-go/internal/errors"
	"github.com/highcumontoa/resilient-data-access-layer-go/internal/store"
)

// Repository 是基于表元数据的通用仓储，承载过滤与分页的统一校验逻辑。
type Repository[T any] struct {
	db     *database.DB
	table  string
	mapper rowMapper[T]
}

// NewRepository 创建通用仓储。
func NewRepository[T any](db *database.DB, table string, mapper rowMapper[T]) *Repository[T] {
	return &Repository[T]{db: db, table: table, mapper: mapper}
}

// metadata 读取当前表结构（含系统字段），用于过滤字段与类型校验。
func (r *Repository[T]) metadata(tx *store.Tx) ([]store.Column, error) {
	cols, err := tx.Columns(r.table)
	if err != nil {
		return nil, derrors.Wrap(derrors.ReasonRecordNotFound, err, "table %q not available", r.table)
	}
	all := []store.Column{
		{Name: store.SystemColumnID, Type: store.TypeInteger},
		{Name: store.SystemColumnVersion, Type: store.TypeInteger},
	}
	return append(all, cols...), nil
}

// validateFilters 按表元数据严格校验过滤字段存在性与值类型。
func (r *Repository[T]) validateFilters(tx *store.Tx, filters []Filter) error {
	cols, err := r.metadata(tx)
	if err != nil {
		return err
	}
	index := make(map[string]store.Column, len(cols))
	for _, c := range cols {
		index[c.Name] = c
	}
	for _, f := range filters {
		if f.Field == "" {
			return derrors.New(derrors.ReasonMissingFilterValue,
				"empty filter field is not allowed")
		}
		col, ok := index[f.Field]
		if !ok {
			return derrors.New(derrors.ReasonUnknownFilterField,
				"filter field %q does not exist on table %q", f.Field, r.table)
		}
		// 显式区分“值缺失”与“值类型不对”：
		// 未提供值（nil 且列不可空）视为缺少过滤值；可空列允许用 nil 查空值。
		if f.Value == nil {
			if !col.Nullable {
				return derrors.New(derrors.ReasonMissingFilterValue,
					"filter on %q requires a non-nil value", f.Field)
			}
			continue
		}
		if err := matchColumnType(col, f.Value); err != nil {
			return derrors.New(derrors.ReasonFilterTypeMismatch,
				"filter %q rejected: %v", f.Field, err)
		}
	}
	return nil
}

// matchColumnType 只按列声明的标量类型比对，不做跨类型隐式转换。
func matchColumnType(col store.Column, v any) error {
	switch col.Type {
	case store.TypeInteger:
		if _, ok := v.(int64); !ok {
			return derrors.New(derrors.ReasonFilterTypeMismatch,
				"column %q expects int64, got %T", col.Name, v)
		}
	case store.TypeText:
		if _, ok := v.(string); !ok {
			return derrors.New(derrors.ReasonFilterTypeMismatch,
				"column %q expects string, got %T", col.Name, v)
		}
	case store.TypeReal:
		if _, ok := v.(float64); !ok {
			return derrors.New(derrors.ReasonFilterTypeMismatch,
				"column %q expects float64, got %T", col.Name, v)
		}
	case store.TypeBool:
		if _, ok := v.(bool); !ok {
			return derrors.New(derrors.ReasonFilterTypeMismatch,
				"column %q expects bool, got %T", col.Name, v)
		}
	}
	return nil
}

// toStoreFilters 做类型转换；调用前已通过 validateFilters。
func toStoreFilters(filters []Filter) []store.Filter {
	out := make([]store.Filter, len(filters))
	for i, f := range filters {
		out[i] = store.Filter{Field: f.Field, Value: f.Value}
	}
	return out
}
