package store

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

func mustTable(t *testing.T, s *Store) {
	t.Helper()
	if err := s.Exec(func(tx *Tx) error {
		if tx.HasTable("items") {
			return nil
		}
		return tx.CreateTable("items", storeColumn())
	}); err != nil {
		t.Fatal(err)
	}
}

func storeColumn() []Column {
	return []Column{
		{Name: "name", Type: TypeText},
		{Name: "qty", Type: TypeInteger},
	}
}

// TestStore_ConcurrentInsertsMergeNoLostRows 验证并发插入提交时行级合并不丢行、不撞 id。
func TestStore_ConcurrentInsertsMergeNoLostRows(t *testing.T) {
	s := Open()
	mustTable(t, s)

	const n = 50
	var wg sync.WaitGroup
	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := s.Exec(func(tx *Tx) error {
				_, err := tx.Insert("items", Row{"name": fmt.Sprintf("n-%d", i), "qty": int64(i)})
				return err
			})
			errCh <- err
		}(i)
	}
	wg.Wait()
	close(errCh)
	for e := range errCh {
		if e != nil {
			t.Fatalf("unrelated inserts must all commit: %v", e)
		}
	}
	var rows []Row
	if err := s.View(func(tx *Tx) error {
		var err error
		rows, err = tx.List("items", ScanOptions{})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if len(rows) != n {
		t.Fatalf("merged commits lost rows: got %d want %d", len(rows), n)
	}
	ids := map[int64]int{}
	for _, r := range rows {
		ids[r[SystemColumnID].(int64)]++
	}
	for id, c := range ids {
		if c != 1 {
			t.Fatalf("id %d duplicated %d times", id, c)
		}
	}
	t.Logf("input: %d concurrent inserts => committed rows=%d, unique ids=%d 判定依据: 无丢行无重号",
		n, len(rows), len(ids))
}

// TestStore_ConcurrentUpdateAndInsert 验证更新已有行与插入新行并发时，
// 行级合并让二者共存：更新生效、新行不丢。
func TestStore_ConcurrentUpdateAndInsert(t *testing.T) {
	s := Open()
	mustTable(t, s)
	var id1 int64
	if err := s.Exec(func(tx *Tx) error {
		var err error
		id1, err = tx.Insert("items", Row{"name": "seed", "qty": int64(1)})
		return err
	}); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	var updErr, insErr error
	go func() {
		defer wg.Done()
		updErr = s.Exec(func(tx *Tx) error {
			_, err := tx.Update("items", id1, 1, Row{"qty": int64(99)})
			return err
		})
	}()
	go func() {
		defer wg.Done()
		insErr = s.Exec(func(tx *Tx) error {
			_, err := tx.Insert("items", Row{"name": "new", "qty": int64(2)})
			return err
		})
	}()
	wg.Wait()
	if updErr != nil || insErr != nil {
		t.Fatalf("update=%v insert=%v, both should coexist", updErr, insErr)
	}

	row, ok, err := func() (Row, bool, error) {
		var r Row
		var found bool
		err := s.View(func(tx *Tx) error {
			var e error
			r, found, e = tx.Get("items", id1)
			return e
		})
		return r, found, err
	}()
	if err != nil || !ok || row["qty"].(int64) != 99 {
		t.Fatalf("updated row state wrong: %v ok=%v row=%v", err, ok, row)
	}
	var n int
	_ = s.View(func(tx *Tx) error {
		rows, e := tx.List("items", ScanOptions{})
		n = len(rows)
		return e
	})
	if n != 2 {
		t.Fatalf("inserted row lost during merge: count=%d want 2", n)
	}
	t.Log("判定依据: 更新 qty=99 生效，新行保留，共 2 行 => 更新/插入合并不互相覆盖")
}

// TestStore_LostUpdateRejected 验证两个并发事务基于同一行版本更新，只有一个提交。
func TestStore_LostUpdateRejected(t *testing.T) {
	s := Open()
	mustTable(t, s)
	var id int64
	if err := s.Exec(func(tx *Tx) error {
		var e error
		id, e = tx.Insert("items", Row{"name": "x", "qty": int64(0)})
		return e
	}); err != nil {
		t.Fatal(err)
	}

	tx1, _ := s.Begin()
	tx2, _ := s.Begin()
	if n, err := tx1.Update("items", id, 1, Row{"qty": int64(1)}); err != nil || n != 1 {
		t.Fatalf("tx1 update: n=%d err=%v", n, err)
	}
	if n, err := tx2.Update("items", id, 1, Row{"qty": int64(2)}); err != nil || n != 1 {
		t.Fatalf("tx2 update: n=%d err=%v", n, err)
	}
	if err := tx1.Commit(); err != nil {
		t.Fatalf("first commit should win: %v", err)
	}
	err := tx2.Commit()
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("second commit must lose with ErrConflict, got %v", err)
	}
	_ = s.View(func(tx *Tx) error {
		row, _, _ := tx.Get("items", id)
		if row["qty"].(int64) != 1 {
			t.Fatalf("first writer value must survive, got %v", row["qty"])
		}
		t.Logf("判定依据: 第一提交者 qty=1 保留，后写 ErrConflict 被拒 => 无丢失更新")
		return nil
	})
}

// TestStore_DDLAndDMLAtomicInOneTx 验证同一事务内 DDL+DML 一起回滚。
func TestStore_DDLAndDMLAtomicInOneTx(t *testing.T) {
	s := Open()
	mustTable(t, s)
	var seedID int64
	_ = s.Exec(func(tx *Tx) error {
		seedID, _ = tx.Insert("items", Row{"name": "seed", "qty": int64(1)})
		return nil
	})

	err := s.Exec(func(tx *Tx) error {
		if err := tx.AddColumn("items", Column{Name: "tag", Type: TypeText, Nullable: true}); err != nil {
			return err
		}
		if _, err := tx.Update("items", seedID, 1, Row{"tag": "temp"}); err != nil {
			return err
		}
		// 再做一个非法写入触发回滚：AddColumn 与 Update 必须一起撤销。
		if _, err := tx.Insert("items", Row{"name": "bad", "qty": int64(9), "unknown_col": 1}); err != nil {
			return err
		}
		return nil
	})
	if err == nil {
		t.Fatal("expected failure")
	}
	t.Logf("input: DDL+DML then failing insert => %v", err)
	_ = s.View(func(tx *Tx) error {
		cols, _ := tx.Columns("items")
		for _, c := range cols {
			if c.Name == "tag" {
				t.Fatal("AddColumn must be rolled back with the tx")
			}
		}
		rows, _ := tx.List("items", ScanOptions{})
		if len(rows) != 1 {
			t.Fatalf("neither update nor failed insert may leave data, rows=%d", len(rows))
		}
		return nil
	})
	t.Log("判定依据: tag 列不存在、仍只有 1 行 => DDL 与 DML 同生共死")
}

// TestStore_DefaultColumnSemantics 验证新增列对既有行立即生效且默认值稳定。
func TestStore_DefaultColumnSemantics(t *testing.T) {
	s := Open()
	mustTable(t, s)
	_ = s.Exec(func(tx *Tx) error {
		_, _ = tx.Insert("items", Row{"name": "a", "qty": int64(1)})
		return nil
	})
	if err := s.Exec(func(tx *Tx) error {
		return tx.AddColumn("items", Column{
			Name: "level", Type: TypeText, Nullable: false,
			HasDefault: true, Default: "std",
		})
	}); err != nil {
		t.Fatal(err)
	}
	_ = s.View(func(tx *Tx) error {
		rows, _ := tx.List("items", ScanOptions{})
		if rows[0]["level"] != "std" {
			t.Fatalf("existing row must get stable default, got %v", rows[0]["level"])
		}
		return nil
	})
	// 新插入行未给 level 也取同一默认值
	var id int64
	_ = s.Exec(func(tx *Tx) error {
		id, _ = tx.Insert("items", Row{"name": "b", "qty": int64(2)})
		return nil
	})
	_ = s.View(func(tx *Tx) error {
		row, _, _ := tx.Get("items", id)
		if row["level"] != "std" {
			t.Fatalf("new row default mismatch: %v", row["level"])
		}
		t.Log("判定依据: 既有行与新行均取 level=std => 默认语义确定稳定")
		return nil
	})
}

// TestStore_UniqueViolationOnConcurrentInsert 验证同唯一键并发插入有一个被拒。
func TestStore_UniqueViolationOnConcurrentInsert(t *testing.T) {
	s := Open()
	if err := s.Exec(func(tx *Tx) error {
		return tx.CreateTable("keys", []Column{
			{Name: "k", Type: TypeText, Unique: true},
		})
	}); err != nil {
		t.Fatal(err)
	}
	tx1, _ := s.Begin()
	tx2, _ := s.Begin()
	if _, err := tx1.Insert("keys", Row{"k": "dup"}); err != nil {
		t.Fatal(err)
	}
	if _, err := tx2.Insert("keys", Row{"k": "dup"}); err != nil {
		t.Fatal(err)
	}
	if err := tx1.Commit(); err != nil {
		t.Fatalf("first unique insert commits: %v", err)
	}
	if err := tx2.Commit(); !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("second unique insert must fail ErrUniqueViolation, got %v", err)
	}
	t.Log("判定依据: 同键并发插入第一笔提交、第二笔 ErrUniqueViolation => 唯一约束有效")
}
