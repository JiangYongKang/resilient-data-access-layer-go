package engine_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/highcumontoa/resilient-data-access-layer-go/internal/engine"
)

// verdict 统一打印输入与判定依据，便于在 -v 日志中解释每个断言。
func verdict(t *testing.T, input string, want, got any) {
	t.Helper()
	t.Logf("INPUT(%s) | WANT=%v | GOT=%v", input, want, got)
}

func itemSchema(t *testing.T) *engine.Schema {
	t.Helper()
	s, err := engine.NewSchema("items", []engine.Column{
		{Name: "code", Type: engine.KindString},
		{Name: "qty", Type: engine.KindInt},
		{Name: "version", Type: engine.KindInt},
	})
	if err != nil {
		t.Fatalf("schema: %v", err)
	}
	return s
}

func seedItems(t *testing.T, db *engine.DB, n int) {
	t.Helper()
	ctx := context.Background()
	err := db.Update(ctx, func(tx *engine.Tx) error {
		if err := tx.CreateTable(itemSchema(t)); err != nil {
			return err
		}
		for i := 1; i <= n; i++ {
			if _, err := tx.Insert("items", map[string]engine.Value{
				"code": engine.NewString(codeFor(i)),
				"qty":  engine.NewInt(int64(i * 10)),
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

func codeFor(i int) string {
	return "c-" + itoa(i)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	if neg {
		b = append([]byte{'-'}, b...)
	}
	return string(b)
}

// ---- 过滤：字段缺失/类型不匹配必须拒绝，不允许静默空或全量 ----

func TestListFilterRejectsUnknownColumn(t *testing.T) {
	db, err := engine.Open(engine.Options{Path: ":memory:"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	seedItems(t, db, 3)

	err = db.View(context.Background(), func(tx *engine.Tx) error {
		_, qErr := tx.List("items", []engine.Filter{
			{Field: "missing_col", Op: engine.OpEq, Value: engine.NewString("x")},
		}, engine.PageRequest{Limit: 10})
		return qErr
	})
	verdict(t, "filter field=missing_col value=string:x", "ErrColumnNotFound", err)
	if !errors.Is(err, engine.ErrColumnNotFound) {
		t.Fatalf("expected ErrColumnNotFound, got %v", err)
	}
}

func TestListFilterRejectsTypeMismatch(t *testing.T) {
	db, _ := engine.Open(engine.Options{Path: ":memory:"})
	defer db.Close()
	seedItems(t, db, 3)

	err := db.View(context.Background(), func(tx *engine.Tx) error {
		_, qErr := tx.List("items", []engine.Filter{
			{Field: "qty", Op: engine.OpEq, Value: engine.NewString("not-an-int")},
		}, engine.PageRequest{Limit: 10})
		return qErr
	})
	verdict(t, "filter field=qty value=string", "ErrTypeMismatch", err)
	if !errors.Is(err, engine.ErrTypeMismatch) {
		t.Fatalf("expected ErrTypeMismatch, got %v", err)
	}
}

func TestListFilterEqWorksAndDoesNotReturnFull(t *testing.T) {
	db, _ := engine.Open(engine.Options{Path: ":memory:"})
	defer db.Close()
	seedItems(t, db, 5)

	var total int
	err := db.View(context.Background(), func(tx *engine.Tx) error {
		res, qErr := tx.List("items", []engine.Filter{
			{Field: "qty", Op: engine.OpEq, Value: engine.NewInt(20)},
		}, engine.PageRequest{Limit: 10})
		if qErr != nil {
			return qErr
		}
		total = len(res.Rows)
		return nil
	})
	verdict(t, "filter qty=20 over 5 rows", "exactly 1 row", total)
	if err != nil || total != 1 {
		t.Fatalf("want 1 matched row, got %d (err=%v)", total, err)
	}
}

// ---- 分页参数：非法即拒绝 ----

func TestPaginationMalformedParams(t *testing.T) {
	db, _ := engine.Open(engine.Options{Path: ":memory:"})
	defer db.Close()
	seedItems(t, db, 4)

	ctx := context.Background()
	cases := []struct {
		name string
		page engine.PageRequest
		want error
	}{
		{"limit zero", engine.PageRequest{Limit: 0}, engine.ErrInvalidPagination},
		{"limit negative", engine.PageRequest{Limit: -1}, engine.ErrInvalidPagination},
		{"offset negative", engine.PageRequest{Limit: 2, Offset: -5}, engine.ErrInvalidPagination},
		{"cursor garbage", engine.PageRequest{Limit: 2, Cursor: "%%%not-base64"}, engine.ErrInvalidCursor},
		{"cursor wrong json", engine.PageRequest{Limit: 2, Cursor: "eyJhIjoxfQ"}, engine.ErrInvalidCursor},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := db.View(ctx, func(tx *engine.Tx) error {
				_, qErr := tx.List("items", nil, tc.page)
				return qErr
			})
			verdict(t, tc.name, tc.want, err)
			if !errors.Is(err, tc.want) {
				t.Fatalf("want %v, got %v", tc.want, err)
			}
		})
	}
}

func TestCursorTamperingRejected(t *testing.T) {
	db, _ := engine.Open(engine.Options{Path: ":memory:"})
	defer db.Close()
	seedItems(t, db, 4)
	ctx := context.Background()

	var firstPage engine.PageResult
	if err := db.View(ctx, func(tx *engine.Tx) error {
		res, err := tx.List("items", nil, engine.PageRequest{Limit: 2})
		if err != nil {
			return err
		}
		firstPage = res
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !firstPage.HasMore {
		t.Fatal("expected more pages")
	}

	// 篡改游标：把锚点 id 改成负数（解出的 JSON 仍合法，但语义非法）。
	bad := engine.EncodeCursor(2, false)
	if bad == "" {
		t.Fatal("encode cursor failed")
	}
	tampered := engine.EncodeCursor(-99, false)
	err := db.View(ctx, func(tx *engine.Tx) error {
		_, qErr := tx.List("items", nil, engine.PageRequest{Limit: 2, Cursor: tampered})
		return qErr
	})
	verdict(t, "tampered cursor id=-99 (valid token="+bad+")", "ErrInvalidCursor", err)
	if !errors.Is(err, engine.ErrInvalidCursor) {
		t.Fatalf("tampered cursor must be rejected, got %v", err)
	}
}

// ---- offset 与 cursor 两种翻页必须拼出完全一致的集合与顺序 ----

func TestOffsetAndCursorPaginationMatch(t *testing.T) {
	db, _ := engine.Open(engine.Options{Path: ":memory:"})
	defer db.Close()
	seedItems(t, db, 7)
	ctx := context.Background()

	collect := func(useCursor bool) []int64 {
		var ids []int64
		if !useCursor {
			for off := 0; ; off += 3 {
				if err := db.View(ctx, func(tx *engine.Tx) error {
					res, err := tx.List("items", nil, engine.PageRequest{Limit: 3, Offset: off})
					if err != nil {
						return err
					}
					for _, r := range res.Rows {
						ids = append(ids, r.ID())
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				if len(ids) >= 7 {
					break
				}
			}
			return ids
		}
		cursor := ""
		for {
			done := false
			if err := db.View(ctx, func(tx *engine.Tx) error {
				res, err := tx.List("items", nil, engine.PageRequest{Limit: 3, Cursor: cursor})
				if err != nil {
					return err
				}
				for _, r := range res.Rows {
					ids = append(ids, r.ID())
				}
				if res.HasMore {
					cursor = engine.EncodeCursor(res.NextFrom, false)
				} else {
					done = true
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if done {
				break
			}
		}
		return ids
	}

	offIDs := collect(false)
	curIDs := collect(true)
	verdict(t, "7 rows, page size 3", offIDs, curIDs)
	if len(offIDs) != 7 || len(curIDs) != 7 {
		t.Fatalf("expected 7 ids each, got offset=%v cursor=%v", offIDs, curIDs)
	}
	for i := range offIDs {
		if offIDs[i] != curIDs[i] {
			t.Fatalf("mismatch at %d: offset=%v cursor=%v", i, offIDs, curIDs)
		}
	}
}

// ---- 乐观版本：旧版本更新必须被拒绝 ----

func TestOptimisticVersionConflict(t *testing.T) {
	db, _ := engine.Open(engine.Options{Path: ":memory:"})
	defer db.Close()
	seedItems(t, db, 1)
	ctx := context.Background()

	// 第一次用版本 0 更新成功，版本变 1。
	if err := db.Update(ctx, func(tx *engine.Tx) error {
		return tx.Update("items", 1, map[string]engine.Value{"qty": engine.NewInt(11)}, 0)
	}); err != nil {
		t.Fatalf("first update: %v", err)
	}
	// 持有旧版本 0 的写者必须冲突，不能覆盖新值 11。
	err := db.Update(ctx, func(tx *engine.Tx) error {
		return tx.Update("items", 1, map[string]engine.Value{"qty": engine.NewInt(99)}, 0)
	})
	verdict(t, "update id=1 expectVersion=0 storedVersion=1", "ErrVersionConflict", err)
	if !errors.Is(err, engine.ErrVersionConflict) {
		t.Fatalf("expected conflict, got %v", err)
	}
	var qty int64
	if err := db.View(ctx, func(tx *engine.Tx) error {
		r, err := tx.Get("items", 1)
		if err != nil {
			return err
		}
		v, _ := r.Get(itemSchema(t), "qty")
		qty, _ = v.AsInt()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if qty != 11 {
		t.Fatalf("stale writer must not overwrite; qty=%d want 11", qty)
	}
}

// ---- 事务原子性：fn 失败必须整体回滚 ----

func TestUpdateRollbackAtomicity(t *testing.T) {
	db, _ := engine.Open(engine.Options{Path: ":memory:"})
	defer db.Close()
	seedItems(t, db, 2)
	ctx := context.Background()

	wantErr := errors.New("boom-after-first-write")
	err := db.Update(ctx, func(tx *engine.Tx) error {
		if err := tx.Update("items", 1, map[string]engine.Value{"qty": engine.NewInt(111)}, 0); err != nil {
			return err
		}
		// 同一事务内第二条操作失败：第一条也必须回滚。
		return wantErr
	})
	verdict(t, "update row1 then fail before commit", "rollback, qty1=10 qty2=20", err)
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected wrapped fn error, got %v", err)
	}
	err = db.View(ctx, func(tx *engine.Tx) error {
		r1, _ := tx.Get("items", 1)
		r2, _ := tx.Get("items", 2)
		s := itemSchema(t)
		v1, _ := r1.Get(s, "qty")
		v2, _ := r2.Get(s, "qty")
		q1, _ := v1.AsInt()
		q2, _ := v2.AsInt()
		if q1 != 10 || q2 != 20 {
			t.Fatalf("partial commit detected: qty1=%d qty2=%d", q1, q2)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// ---- DDL：加列默认语义稳定；改名不动槽位；删列收缩后读取不错位 ----

func TestAddColumnDefaultAndRenameDrop(t *testing.T) {
	db, _ := engine.Open(engine.Options{Path: ":memory:"})
	defer db.Close()
	seedItems(t, db, 2)
	ctx := context.Background()

	err := db.Update(ctx, func(tx *engine.Tx) error {
		// 加列必须带确定默认：给所有已有行回填 5。
		if err := tx.AddColumn("items", engine.Column{
			Name: "level", Type: engine.KindInt, HasDefault: true, Default: engine.NewInt(5),
		}); err != nil {
			return err
		}
		// 改名 qty -> quantity：槽位不动，数据必须仍可读。
		if err := tx.RenameColumn("items", "qty", "quantity"); err != nil {
			return err
		}
		// 删除 code 列：其后的 level 槽位前移，读取必须对齐新结构。
		if err := tx.DropColumn("items", "code"); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ddl: %v", err)
	}

	err = db.View(ctx, func(tx *engine.Tx) error {
		s, err := tx.TableSchema("items")
		if err != nil {
			return err
		}
		if _, _, err := s.ColumnByName("code"); !errors.Is(err, engine.ErrColumnNotFound) {
			t.Fatalf("code must be dropped, got err=%v", err)
		}
		page, err := tx.List("items", nil, engine.PageRequest{Limit: 10})
		if err != nil {
			return err
		}
		for i, r := range page.Rows {
			qv, _ := r.Get(s, "quantity")
			lv, _ := r.Get(s, "level")
			qty, _ := qv.AsInt()
			level, _ := lv.AsInt()
			verdict(t, "row after rename+drop", "quantity preserved, level=5",
				"id="+itoa(int(r.ID()))+" quantity="+itoa(int(qty))+" level="+itoa(int(level)))
			if level != 5 {
				t.Fatalf("default backfill failed: level=%d", level)
			}
			if qty != int64((i+1)*10) {
				t.Fatalf("rename shifted data: quantity=%d want %d", qty, (i+1)*10)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestAddNonNullableColumnWithoutDefaultRejected(t *testing.T) {
	db, _ := engine.Open(engine.Options{Path: ":memory:"})
	defer db.Close()
	seedItems(t, db, 1)
	err := db.Update(context.Background(), func(tx *engine.Tx) error {
		return tx.AddColumn("items", engine.Column{Name: "mandatory", Type: engine.KindString})
	})
	verdict(t, "add non-nullable column without default", "rejection", err)
	if err == nil {
		t.Fatal("adding non-nullable column without a default must be rejected")
	}
}

// ---- 落盘：提交后重开数据仍在，且文件为整体快照 ----

func TestPersistenceRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snap.json")

	db, err := engine.Open(engine.Options{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	seedItems(t, db, 3)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := engine.Open(engine.Options{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	var total int
	if err := db2.View(context.Background(), func(tx *engine.Tx) error {
		res, err := tx.List("items", nil, engine.PageRequest{Limit: 10})
		if err != nil {
			return err
		}
		total = res.Total
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	verdict(t, "reopen snapshot file", 3, total)
	if total != 3 {
		t.Fatalf("expected 3 rows after reopen, got %d", total)
	}
}
