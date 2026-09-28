package engine

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
)

// Op 是过滤条件的比较符。
type Op int

const (
	OpEq Op = iota
	OpNeq
	OpGt
	OpGte
	OpLt
	OpLte
)

func (op Op) String() string {
	switch op {
	case OpEq:
		return "="
	case OpNeq:
		return "!="
	case OpGt:
		return ">"
	case OpGte:
		return ">="
	case OpLt:
		return "<"
	case OpLte:
		return "<="
	default:
		return "?"
	}
}

// Filter 是一个 AND 条件。字段缺失（列不存在）或值类型与列不符时
// 查询必须按原因拒绝，不能静默返回空集或全量。
type Filter struct {
	Field string `json:"field"`
	Op    Op     `json:"op"`
	Value Value  `json:"value"`
}

// PageRequest 同时表达两种分页：
//   - offset 分页：Offset>=0 且 Limit>0
//   - cursor 分页：Cursor 非空（base64(JSON)）
//
// 两者互斥；参数非法返回 ErrInvalidPagination / ErrInvalidCursor。
type PageRequest struct {
	Limit  int
	Offset int
	Cursor string
	Desc   bool
}

// PageResult 是统一的分页结果，两种入口共用同一结果结构。
type PageResult struct {
	Rows     []Row
	Total    int
	Limit    int
	NextFrom int64
	HasMore  bool
}

type cursorWire struct {
	V   int   `json:"v"` // 游标格式版本，篡改/不支持即拒绝
	ID  int64 `json:"id"`
	Dir int   `json:"dir"` // 1=asc, -1=desc；方向不一致即拒绝
}

const cursorVersion = 1

// encodeCursor 生成自描述的不透明游标（base64(JSON)）。
func encodeCursor(id int64, desc bool) string {
	dir := 1
	if desc {
		dir = -1
	}
	raw, _ := json.Marshal(cursorWire{V: cursorVersion, ID: id, Dir: dir})
	return base64.RawURLEncoding.EncodeToString(raw)
}

// decodeCursor 校验并解析游标：非法 base64/JSON、版本不符、方向不符
// 都返回 ErrInvalidCursor，绝不回退成全量扫描。
func decodeCursor(token string, desc bool) (int64, error) {
	wantDir := 1
	if desc {
		wantDir = -1
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return 0, fmt.Errorf("%w: not base64: %v", ErrInvalidCursor, err)
	}
	var w cursorWire
	if err := json.Unmarshal(raw, &w); err != nil {
		return 0, fmt.Errorf("%w: malformed payload: %v", ErrInvalidCursor, err)
	}
	if w.V != cursorVersion {
		return 0, fmt.Errorf("%w: unsupported cursor version %d", ErrInvalidCursor, w.V)
	}
	if w.Dir != wantDir {
		return 0, fmt.Errorf("%w: cursor direction does not match query", ErrInvalidCursor)
	}
	if w.ID <= 0 {
		return 0, fmt.Errorf("%w: cursor id must be positive, got %d", ErrInvalidCursor, w.ID)
	}
	return w.ID, nil
}

// validateFilters 对每个过滤条件做字段存在性与类型校验。
func validateFilters(s *Schema, filters []Filter) error {
	for _, f := range filters {
		if f.Field == "" {
			return fmt.Errorf("%w: empty filter field", ErrInvalidFilter)
		}
		col, _, err := s.ColumnByName(f.Field)
		if err != nil {
			// 字段缺失：明确拒绝，不允许静默退化为“无过滤”。
			return fmt.Errorf("filter %s?: %w", f.Op, err)
		}
		// 游标列之外，id 也是可过滤的合法列。
		if f.Field == "id" {
			col = Column{Name: "id", Type: KindInt}
		}
		if !f.Value.IsNull() {
			if err := f.Value.CheckKind(col.Type); err != nil {
				return fmt.Errorf("filter on %q: %w", f.Field, err)
			}
		}
	}
	return nil
}

func rowMatches(r Row, s *Schema, filters []Filter) (bool, error) {
	for _, f := range filters {
		var got Value
		if f.Field == "id" {
			got = r.cells[0]
		} else {
			_, i, err := s.ColumnByName(f.Field)
			if err != nil {
				return false, err
			}
			got = r.cells[i+1]
		}
		ok, err := compareValues(got, f.Op, f.Value)
		if err != nil {
			return false, fmt.Errorf("filter on %q: %w", f.Field, err)
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}

// compareValues 只在相同 Kind 间比较；NULL 的比较规则：
// Eq NULL 用“is null”语义；其余比较符遇 NULL 一律不命中且不报错。
func compareValues(got Value, op Op, want Value) (bool, error) {
	if want.IsNull() {
		isNull := got.IsNull()
		switch op {
		case OpEq:
			return isNull, nil
		case OpNeq:
			return !isNull, nil
		default:
			return false, nil
		}
	}
	if got.IsNull() {
		return false, nil
	}
	if got.Kind() != want.Kind() {
		return false, fmt.Errorf("%w: cannot compare %s with %s", ErrTypeMismatch, got.Kind(), want.Kind())
	}
	cmp := 0
	switch got.Kind() {
	case KindInt:
		a, _ := got.AsInt()
		b, _ := want.AsInt()
		cmp = compareOrdered(a, b)
	case KindFloat:
		a, _ := got.AsFloat()
		b, _ := want.AsFloat()
		switch {
		case a < b:
			cmp = -1
		case a > b:
			cmp = 1
		}
	case KindString:
		a, _ := got.AsString()
		b, _ := want.AsString()
		switch {
		case a < b:
			cmp = -1
		case a > b:
			cmp = 1
		}
	case KindBool:
		a, _ := got.AsBool()
		b, _ := want.AsBool()
		// bool 仅支持等值。
		if op != OpEq && op != OpNeq {
			return false, fmt.Errorf("%w: ordered comparison on bool is not supported", ErrInvalidFilter)
		}
		if a == b {
			cmp = 0
		} else if !a {
			cmp = -1
		} else {
			cmp = 1
		}
	}
	switch op {
	case OpEq:
		return cmp == 0, nil
	case OpNeq:
		return cmp != 0, nil
	case OpGt:
		return cmp > 0, nil
	case OpGte:
		return cmp >= 0, nil
	case OpLt:
		return cmp < 0, nil
	case OpLte:
		return cmp <= 0, nil
	default:
		return false, fmt.Errorf("%w: unknown op %d", ErrInvalidFilter, op)
	}
}

func compareOrdered[T ~int64 | ~string](a, b T) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

// query 执行统一查询计划：
//  1. 校验过滤字段与类型（非法即拒绝）；
//  2. 过滤并按 id 稳定排序；
//  3. 应用 cursor（键集）或 offset 分页；
//  4. 多取 1 行判断 HasMore，并生成下一页游标锚点。
func (t *Table) query(filters []Filter, page PageRequest) (PageResult, error) {
	if err := validateFilters(t.schema, filters); err != nil {
		return PageResult{}, err
	}
	if page.Limit <= 0 {
		return PageResult{}, fmt.Errorf("%w: limit must be > 0, got %d", ErrInvalidPagination, page.Limit)
	}
	if page.Offset < 0 {
		return PageResult{}, fmt.Errorf("%w: offset must be >= 0, got %d", ErrInvalidPagination, page.Offset)
	}

	matched := make([]Row, 0, len(t.rows))
	for _, r := range t.rows {
		ok, err := rowMatches(r, t.schema, filters)
		if err != nil {
			return PageResult{}, err
		}
		if ok {
			matched = append(matched, r)
		}
	}
	sort.Slice(matched, func(i, j int) bool {
		if page.Desc {
			return matched[i].ID() > matched[j].ID()
		}
		return matched[i].ID() < matched[j].ID()
	})
	total := len(matched)

	// cursor 分页：以 id 做键集条件（desc/asc 严格对应）。
	if page.Cursor != "" {
		anchor, err := decodeCursor(page.Cursor, page.Desc)
		if err != nil {
			return PageResult{}, err
		}
		idx := sort.Search(len(matched), func(i int) bool {
			if page.Desc {
				// 已按 id 降序：游标锚点是上一页最后一条，下一页必须严格小于它。
				return matched[i].ID() < anchor
			}
			// 升序：下一页必须严格大于锚点。
			return matched[i].ID() > anchor
		})
		matched = matched[idx:]
	} else {
		if page.Offset > len(matched) {
			page.Offset = len(matched)
		}
		matched = matched[page.Offset:]
	}

	res := PageResult{Total: total, Limit: page.Limit}
	if len(matched) > page.Limit {
		res.HasMore = true
		res.NextFrom = matched[page.Limit-1].ID()
		matched = matched[:page.Limit]
	} else {
		res.HasMore = false
	}
	res.Rows = matched
	return res, nil
}

// EncodeCursor 导出游标编码，供仓储层生成下一页游标。
func EncodeCursor(id int64, desc bool) string { return encodeCursor(id, desc) }
