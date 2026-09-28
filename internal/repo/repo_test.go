package repo

import (
	"errors"
	"testing"

	"github.com/highcumontoa/resilient-data-access-layer-go/internal/database"
	derrors "github.com/highcumontoa/resilient-data-access-layer-go/internal/errors"
	"github.com/highcumontoa/resilient-data-access-layer-go/internal/store"
)

type thing struct {
	ID     int64
	Kind   string
	Active bool
}

func mapThing(row store.Row) (thing, error) {
	return thing{
		ID:     row[store.SystemColumnID].(int64),
		Kind:   row["kind"].(string),
		Active: row["active"].(bool),
	}, nil
}

// seedThings 在独立事务里批量插入：验证同事务批量写入要么全部生效。
func seedThings(t *testing.T, db *database.DB, kinds []string) {
	t.Helper()
	err := db.Exec(func(tx *store.Tx) error {
		for i, k := range kinds {
			if _, err := tx.Insert("things", store.Row{
				"kind":   k,
				"active": i%2 == 0,
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
}

func newThingRepo(t *testing.T) (*database.DB, *Repository[thing]) {
	t.Helper()
	db := database.Open()
	if err := db.Exec(func(tx *store.Tx) error {
		return tx.CreateTable("things", []store.Column{
			{Name: "kind", Type: store.TypeText},
			{Name: "active", Type: store.TypeBool},
		})
	}); err != nil {
		t.Fatal(err)
	}
	return db, NewRepository[thing](db, "things", mapThing)
}

func assertReason(t *testing.T, where string, err error, want derrors.ReasonCode) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: expected reason %s, got nil", where, want)
	}
	var de *derrors.DataError
	if !errors.As(err, &de) {
		t.Fatalf("%s: expected *DataError got %T", where, err)
	}
	t.Logf("%s: input-error=%q => reason=%s (want %s)", where, err.Error(), de.Reason, want)
	if de.Reason != want {
		t.Fatalf("%s: reason=%s want %s", where, de.Reason, want)
	}
}

// TestPagination_MalformedParamsRejected 覆盖各类畸形分页参数，
// 必须以 INVALID_PAGINATION 拒绝，不能静默返回空页或全量。
func TestPagination_MalformedParamsRejected(t *testing.T) {
	db, r := newThingRepo(t)
	seedThings(t, db, []string{"a", "b", "c", "d", "e"})

	badPages := []PageRequest{
		{Page: 0, PageSize: 2},
		{Page: -3, PageSize: 2},
		{Page: 1, PageSize: -1},
		{Page: 1, PageSize: MaxPageSize + 1},
	}
	for _, in := range badPages {
		_, err := r.FindByPage(nil, in)
		assertReason(t, "FindByPage "+pageDesc(in), err, derrors.ReasonInvalidPagination)
	}

	badCursors := []CursorRequest{
		{AfterID: -1, Limit: 2},
		{AfterID: 0, Limit: 0}, // 0 会归一化为默认值，合法——故不放入
		{AfterID: 1, Limit: -5},
		{AfterID: 1, Limit: MaxPageSize + 1},
	}
	badCursors = badCursors[:0]
	badCursors = append(badCursors,
		CursorRequest{AfterID: -1, Limit: 2},
		CursorRequest{AfterID: 1, Limit: -5},
		CursorRequest{AfterID: 1, Limit: MaxPageSize + 1},
	)
	for _, in := range badCursors {
		_, err := r.FindByCursor(nil, in)
		assertReason(t, "FindByCursor "+cursorDesc(in), err, derrors.ReasonInvalidPagination)
	}

	// 判定依据：拒绝时绝不返回数据；合法请求仍能取回全部 5 行。
	p, err := r.FindByPage(nil, PageRequest{Page: 1, PageSize: 50})
	if err != nil {
		t.Fatal(err)
	}
	if p.Total != 5 || len(p.Items) != 5 {
		t.Fatalf("valid query must still return 5 rows, got total=%d items=%d", p.Total, len(p.Items))
	}
	t.Log("判定依据: 所有畸形参数均被 INVALID_PAGINATION 拒绝且无数据；合法查询 total=5")
}

// TestFilter_UnknownAndTypeMismatch 覆盖过滤字段缺失与类型不匹配，
// 二者必须给出不同原因码，且不能静默返回空结果。
func TestFilter_UnknownAndTypeMismatch(t *testing.T) {
	db, r := newThingRepo(t)
	seedThings(t, db, []string{"a", "b"})

	cases := []struct {
		name   string
		f      []Filter
		reason derrors.ReasonCode
	}{
		{"unknown field", []Filter{{Field: "no_such_col", Value: "a"}}, derrors.ReasonUnknownFilterField},
		{"int passed to text", []Filter{{Field: "kind", Value: int64(7)}}, derrors.ReasonFilterTypeMismatch},
		{"string passed to bool", []Filter{{Field: "active", Value: "true"}}, derrors.ReasonFilterTypeMismatch},
		{"missing value on required column", []Filter{{Field: "kind", Value: nil}}, derrors.ReasonMissingFilterValue},
		{"empty field name", []Filter{{Field: "", Value: "x"}}, derrors.ReasonMissingFilterValue},
	}
	for _, c := range cases {
		// 三个入口都必须拒绝且原因一致
		if _, err := r.FindAll(c.f); err == nil {
			t.Fatalf("%s: FindAll should reject", c.name)
		} else {
			assertReason(t, "FindAll/"+c.name, err, c.reason)
		}
		if _, err := r.FindByPage(c.f, PageRequest{Page: 1, PageSize: 2}); err == nil {
			t.Fatalf("%s: FindByPage should reject", c.name)
		} else {
			assertReason(t, "FindByPage/"+c.name, err, c.reason)
		}
		if _, err := r.FindByCursor(c.f, CursorRequest{AfterID: 0, Limit: 2}); err == nil {
			t.Fatalf("%s: FindByCursor should reject", c.name)
		} else {
			assertReason(t, "FindByCursor/"+c.name, err, c.reason)
		}
	}
	t.Log("判定依据: 未知字段/类型不符/缺值分别命中不同原因码，三个入口一致拒绝")
}

// TestQuery_EntryPointsAgree 验证同一查询经全量/偏移分页/游标分页得到
// 完全一致的数据集与顺序，且翻页不重不漏。
func TestQuery_EntryPointsAgree(t *testing.T) {
	db, r := newThingRepo(t)
	kinds := []string{"x", "y", "x", "z", "x", "y", "x"}
	seedThings(t, db, kinds)

	filters := []Filter{{Field: "kind", Value: "x"}}

	all, err := r.FindAll(filters)
	if err != nil {
		t.Fatal(err)
	}
	var viaPages []thing
	var page int = 1
	for {
		res, err := r.FindByPage(filters, PageRequest{Page: page, PageSize: 2})
		if err != nil {
			t.Fatal(err)
		}
		if page == 1 {
			if res.Total != int64(len(all)) || res.TotalPages != (len(all)+1)/2 {
				t.Fatalf("total=%d totalPages=%d want %d/%d", res.Total, res.TotalPages, len(all), (len(all)+1)/2)
			}
		}
		if len(res.Items) == 0 {
			break
		}
		viaPages = append(viaPages, res.Items...)
		if page > 10 {
			t.Fatal("pagination did not terminate")
		}
		page++
	}

	viaCursor, err := r.DrainByCursor(filters, 2)
	if err != nil {
		t.Fatal(err)
	}

	// 逐页游标也验证 HasMore/NextID 的稳定性
	var manual []thing
	var cursor int64
	for {
		res, err := r.FindByCursor(filters, CursorRequest{AfterID: cursor, Limit: 2})
		if err != nil {
			t.Fatal(err)
		}
		manual = append(manual, res.Items...)
		if !res.HasMore {
			if res.NextID != 0 {
				t.Fatal("last page must report NextID=0")
			}
			break
		}
		cursor = res.NextID
	}

	t.Logf("input: filter kind=x over %d rows => all=%d pages=%d cursor=%d manual-cursor=%d",
		len(kinds), len(all), len(viaPages), len(viaCursor), len(manual))
	if len(all) != 4 || len(viaPages) != 4 || len(viaCursor) != 4 || len(manual) != 4 {
		t.Fatalf("all=%d pages=%d cursor=%d manual=%d, want 4 each", len(all), len(viaPages), len(viaCursor), len(manual))
	}
	for i := range all {
		if all[i].ID != viaPages[i].ID || all[i].ID != viaCursor[i].ID || all[i].ID != manual[i].ID {
			t.Fatalf("row %d diverges across entry points: %d %d %d %d", i, all[i].ID, viaPages[i].ID, viaCursor[i].ID, manual[i].ID)
		}
	}
	// 不重不漏：ID 集合唯一
	seen := map[int64]int{}
	for _, th := range viaCursor {
		seen[th.ID]++
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("id %d appeared %d times across cursor pages", id, n)
		}
	}
	t.Log("判定依据: 三种入口逐行 ID 相同且顺序一致，游标翻页无重复无遗漏")
}

// TestQuery_CursorAfterID 验证游标严格落在 AfterID 之后。
func TestQuery_CursorAfterID(t *testing.T) {
	db, r := newThingRepo(t)
	seedThings(t, db, []string{"a", "a", "a"})

	p1, err := r.FindByCursor(nil, CursorRequest{AfterID: 0, Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(p1.Items) != 1 || p1.Items[0].ID != 1 || !p1.HasMore || p1.NextID != 1 {
		t.Fatalf("unexpected first cursor page: %+v", p1)
	}
	p2, err := r.FindByCursor(nil, CursorRequest{AfterID: p1.NextID, Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(p2.Items) != 1 || p2.Items[0].ID != 2 {
		t.Fatalf("second page must start strictly after id=1, got %+v", p2)
	}
	t.Logf("input: AfterID=0 then AfterID=1 => ids=%d,%d 判定依据: 游标严格向后", p1.Items[0].ID, p2.Items[0].ID)
}

// TestBatch_AtomicInOneTransaction 验证同一事务批量写入要么全成功要么全不生效。
func TestBatch_AtomicInOneTransaction(t *testing.T) {
	db, r := newThingRepo(t)

	err := db.Exec(func(tx *store.Tx) error {
		if _, err := tx.Insert("things", store.Row{"kind": "ok1", "active": true}); err != nil {
			return err
		}
		if _, err := tx.Insert("things", store.Row{"kind": "ok2", "active": true}); err != nil {
			return err
		}
		// 第三行写入未知列：整批必须回滚，前两行也不能留下。
		if _, err := tx.Insert("things", store.Row{"kind": "bad", "active": true, "bogus": 1}); err != nil {
			return err
		}
		return nil
	})
	if err == nil {
		t.Fatal("expected batch failure")
	}
	t.Logf("input: batch with one bad insert => error=%q", err.Error())

	all, qerr := r.FindAll(nil)
	if qerr != nil {
		t.Fatal(qerr)
	}
	if len(all) != 0 {
		t.Fatalf("partial commit detected: %d rows survived", len(all))
	}
	t.Log("判定依据: 批内第三行失败后，前两行均未提交 => 无部分提交")
}

func pageDesc(p PageRequest) string {
	return "page=" + itoa(int64(p.Page)) + ",size=" + itoa(int64(p.PageSize))
}

func cursorDesc(c CursorRequest) string {
	return "after=" + itoa(c.AfterID) + ",limit=" + itoa(int64(c.Limit))
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [24]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
