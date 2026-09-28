package store

import (
	"fmt"
)

// checkValue 校验 Go 值是否匹配列类型并返回规整后的值。
// 严格区分类型：不做字符串/数字之间的隐式转换，避免过滤方静默错位。
func checkValue(col Column, v any) (any, error) {
	if v == nil {
		if col.Nullable {
			return nil, nil
		}
		return nil, fmt.Errorf("%w: column %q is not nullable", ErrConstraint, col.Name)
	}
	switch col.Type {
	case TypeInteger:
		n, ok := v.(int64)
		if !ok {
			return nil, fmt.Errorf("%w: column %q expects int64, got %T", ErrTypeMismatch, col.Name, v)
		}
		return n, nil
	case TypeText:
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("%w: column %q expects string, got %T", ErrTypeMismatch, col.Name, v)
		}
		return s, nil
	case TypeReal:
		f, ok := v.(float64)
		if !ok {
			return nil, fmt.Errorf("%w: column %q expects float64, got %T", ErrTypeMismatch, col.Name, v)
		}
		return f, nil
	case TypeBool:
		b, ok := v.(bool)
		if !ok {
			return nil, fmt.Errorf("%w: column %q expects bool, got %T", ErrTypeMismatch, col.Name, v)
		}
		return b, nil
	default:
		return nil, fmt.Errorf("%w: unknown column type %d", ErrInvalidArgument, col.Type)
	}
}

// defaultFor 返回列的默认值；无默认且可空为 nil，无默认且不可空返回错误。
func defaultFor(col Column) (any, error) {
	if col.HasDefault {
		return checkValue(col, col.Default)
	}
	if col.Nullable {
		return nil, nil
	}
	return nil, fmt.Errorf("%w: column %q needs a value and has no default", ErrConstraint, col.Name)
}
