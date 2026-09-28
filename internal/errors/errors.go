// Package errors 定义数据访问层所有可区分的拒绝原因。
// 调用方可用 errors.Is 精确判定，不能仅依赖错误文本。
package errors

import (
	"fmt"
	"strings"
)

// ReasonCode 是机器可判别的拒绝原因码。
type ReasonCode string

const (
	ReasonUnknown              ReasonCode = "UNKNOWN"
	ReasonInvalidArgument      ReasonCode = "INVALID_ARGUMENT"
	ReasonInvalidPagination    ReasonCode = "INVALID_PAGINATION"
	ReasonUnknownFilterField   ReasonCode = "UNKNOWN_FILTER_FIELD"
	ReasonFilterTypeMismatch   ReasonCode = "FILTER_TYPE_MISMATCH"
	ReasonMissingFilterValue   ReasonCode = "MISSING_FILTER_VALUE"
	ReasonVersionConflict      ReasonCode = "VERSION_CONFLICT"
	ReasonRecordNotFound       ReasonCode = "RECORD_NOT_FOUND"
	ReasonIdempotentReplay     ReasonCode = "IDEMPOTENT_REPLAY"
	ReasonInsufficientBalance  ReasonCode = "INSUFFICIENT_BALANCE"
	ReasonMigrationChecksum    ReasonCode = "MIGRATION_CHECKSUM_CHANGED"
	ReasonMigrationFailed      ReasonCode = "MIGRATION_FAILED"
	ReasonMigrationBadOrdering ReasonCode = "MIGRATION_BAD_ORDERING"
	ReasonDestructiveChange    ReasonCode = "DESTRUCTIVE_CHANGE_DENIED"
)

// DataError 携带稳定原因码与可读说明，调用方据此区分拒绝原因。
type DataError struct {
	Reason  ReasonCode
	Message string
	Cause   error
}

func (e *DataError) Error() string {
	var b strings.Builder
	b.WriteString(string(e.Reason))
	b.WriteString(": ")
	b.WriteString(e.Message)
	if e.Cause != nil {
		b.WriteString(" (")
		b.WriteString(e.Cause.Error())
		b.WriteByte(')')
	}
	return b.String()
}

func (e *DataError) Unwrap() error { return e.Cause }

// New 构造指定原因的错误。
func New(reason ReasonCode, format string, args ...any) *DataError {
	return &DataError{Reason: reason, Message: fmt.Sprintf(format, args...)}
}

// Wrap 包裹底层错误并保留原因码。
func Wrap(reason ReasonCode, cause error, format string, args ...any) *DataError {
	return &DataError{Reason: reason, Message: fmt.Sprintf(format, args...), Cause: cause}
}
