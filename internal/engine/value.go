package engine

import (
	"encoding/json"
	"fmt"
)

// Kind 是列与值的显式类型枚举。所有写入和过滤都按 Kind 校验，
// 拒绝类型不匹配，避免隐式转换造成的静默错位。
type Kind int

const (
	KindNull Kind = iota
	KindInt
	KindFloat
	KindString
	KindBool
)

// String 仅供日志与错误信息使用。
func (k Kind) String() string {
	switch k {
	case KindNull:
		return "null"
	case KindInt:
		return "int"
	case KindFloat:
		return "float"
	case KindString:
		return "string"
	case KindBool:
		return "bool"
	default:
		return fmt.Sprintf("kind(%d)", int(k))
	}
}

// Value 是引擎边界上的标量值，Go 侧具体类型固定：
// int64 / float64 / string / bool / nil。
type Value struct {
	kind Kind
	raw  any
}

// NewInt / NewFloat / NewString / NewBool / Null 构造强类型值。
func NewInt(v int64) Value     { return Value{kind: KindInt, raw: v} }
func NewFloat(v float64) Value { return Value{kind: KindFloat, raw: v} }
func NewString(v string) Value { return Value{kind: KindString, raw: v} }
func NewBool(v bool) Value     { return Value{kind: KindBool, raw: v} }
func Null() Value              { return Value{kind: KindNull, raw: nil} }

func (v Value) Kind() Kind { return v.kind }
func (v Value) Raw() any   { return v.raw }

// AsInt 等访问器在 Kind 不符时返回 ok=false（不做隐式转换）。
func (v Value) AsInt() (int64, bool)     { x, ok := v.raw.(int64); return x, ok }
func (v Value) AsFloat() (float64, bool) { x, ok := v.raw.(float64); return x, ok }
func (v Value) AsString() (string, bool) { x, ok := v.raw.(string); return x, ok }
func (v Value) AsBool() (bool, bool)     { x, ok := v.raw.(bool); return x, ok }
func (v Value) IsNull() bool             { return v.kind == KindNull }

// CheckKind 校验值是否能写入指定 Kind 的列。Null 可以对任意列通过
// （是否允许 Null 由列的 Nullable 单独判定，见 CheckColumnValue）。
func (v Value) CheckKind(want Kind) error {
	if v.kind == KindNull || v.kind == want {
		return nil
	}
	return fmt.Errorf("%w: got %s, column expects %s", ErrTypeMismatch, v.kind, want)
}

// CheckColumnValue 综合校验：类型匹配 + 非空约束。
func CheckColumnValue(c Column, v Value) error {
	if v.IsNull() {
		if !c.Nullable {
			return fmt.Errorf("%w: column %q is not nullable", ErrColumnRequired, c.Name)
		}
		return nil
	}
	return v.CheckKind(c.Type)
}

// 持久化格式：{"k":<kind>,"v":<raw>}。按 kind 决定数字解析方式，
// 避免 JSON 默认 float64 吃掉 int64。
type valueWire struct {
	K int `json:"k"`
	V any `json:"v"`
}

// MarshalJSON 实现 json.Marshaler。
func (v Value) MarshalJSON() ([]byte, error) {
	return json.Marshal(valueWire{K: int(v.kind), V: v.raw})
}

// UnmarshalJSON 实现 json.Unmarshaler：按声明 Kind 还原底层 Go 类型。
func (v *Value) UnmarshalJSON(data []byte) error {
	var w valueWire
	if err := json.Unmarshal(data, &w); err != nil {
		return fmt.Errorf("%w: value payload: %v", ErrInvalidValue, err)
	}
	*v = Value{kind: Kind(w.K)}
	switch Kind(w.K) {
	case KindNull:
		v.raw = nil
	case KindInt:
		n, err := wireInt64(w.V)
		if err != nil {
			return err
		}
		v.raw = n
	case KindFloat:
		f, ok := w.V.(float64)
		if !ok {
			return fmt.Errorf("%w: expected float payload", ErrInvalidValue)
		}
		v.raw = f
	case KindString:
		s, ok := w.V.(string)
		if !ok {
			return fmt.Errorf("%w: expected string payload", ErrInvalidValue)
		}
		v.raw = s
	case KindBool:
		b, ok := w.V.(bool)
		if !ok {
			return fmt.Errorf("%w: expected bool payload", ErrInvalidValue)
		}
		v.raw = b
	default:
		return fmt.Errorf("%w: unknown kind %d", ErrInvalidValue, w.K)
	}
	return nil
}

func wireInt64(x any) (int64, error) {
	switch n := x.(type) {
	case float64:
		return int64(n), nil
	case int64:
		return n, nil
	case nil:
		return 0, nil
	default:
		return 0, fmt.Errorf("%w: expected int payload", ErrInvalidValue)
	}
}

// String 用于日志打印输入值。
func (v Value) String() string {
	return fmt.Sprintf("%s(%v)", v.kind, v.raw)
}
