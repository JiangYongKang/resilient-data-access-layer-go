package engine

import "errors"

// 可区分的失败原因：调用方用 errors.Is 判定，错误文本给出具体输入与判定依据。
var (
	ErrTableNotFound       = errors.New("engine: table not found")
	ErrTableExists         = errors.New("engine: table already exists with a different definition")
	ErrColumnNotFound      = errors.New("engine: column not found")                      // 字段缺失（过滤/更新/读取引用了不存在的列）
	ErrTypeMismatch        = errors.New("engine: value type does not match column type") // 类型不匹配
	ErrInvalidValue        = errors.New("engine: invalid value")
	ErrColumnRequired      = errors.New("engine: non-nullable column requires a value or a default")
	ErrImmutableColumn     = errors.New("engine: column is immutable")
	ErrInvalidFilter       = errors.New("engine: invalid filter")
	ErrInvalidPagination   = errors.New("engine: invalid pagination parameter")
	ErrInvalidCursor       = errors.New("engine: invalid cursor")
	ErrNotFound            = errors.New("engine: row not found")
	ErrVersionConflict     = errors.New("engine: optimistic version conflict")
	ErrInsufficientBalance = errors.New("engine: insufficient balance")
	ErrConflict            = errors.New("engine: conflicting concurrent change")
	ErrSerialization       = errors.New("engine: serialization failure")
)
