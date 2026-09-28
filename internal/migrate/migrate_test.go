package migrate

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/highcumontoa/resilient-data-access-layer-go/internal/database"
	derrors "github.com/highcumontoa/resilient-data-access-layer-go/internal/errors"
	"github.com/highcumontoa/resilient-data-access-layer-go/internal/store"
)

// assertReason 判定错误必须携带预期原因码，并打印输入与判定依据。
func assertReason(t *testing.T, where string, err error, want derrors.ReasonCode) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: expected error with reason %s, got nil", where, want)
	}
	var de *derrors.DataError
	if !errors.As(err, &de) {
		t.Fatalf("%s: expected *DataError, got %T: %v", where, err, err)
	}
	t.Logf("%s: input-error=%q => reason=%s (want %s)", where, err.Error(), de.Reason, want)
	if de.Reason != want {
		t.Fatalf("%s: reason=%s want %s", where, de.Reason, want)
	}
}

// TestMigrate_FailureRollsBackWholeVersion 验证脚本中途失败时：
// 失败版本内已执行的建表/加列全部回滚，登记表不写入该版本，
// 已应用版本保持原状，不存在“半套结构”。
func TestMigrate_FailureRollsBackWholeVersion(t *testing.T) {
	db := database.Open()
	r := NewRunner(db)

	okMigrations := []Migration{
		{Version: 1, Name: "base", Up: func(tx *store.Tx) error {
			if tx.HasTable("users") {
				return nil
			}
			return tx.CreateTable("users", []store.Column{
				{Name: "name", Type: store.TypeText},
			})
		}},
	}
	sum, err := r.Migrate(okMigrations, 0)
	if err != nil {
		t.Fatalf("baseline migrate: %v", err)
	}
	t.Logf("input: apply v1 => summary applied=%v skipped=%v", sum.Applied, sum.Skipped)

	boom := errors.New("boom: simulated DDL failure midway")
	badMigrations := append(append([]Migration{}, okMigrations...),
		Migration{Version: 2, Name: "partial_then_fail", Up: func(tx *store.Tx) error {
			// 先建表，再回填数据，随后人为失败：前两步必须随事务一并回滚。
			if err := tx.CreateTable("half_table", []store.Column{
				{Name: "note", Type: store.TypeText},
			}); err != nil {
				return err
			}
			if _, err := tx.Insert("half_table", store.Row{"note": "should-not-survive"}); err != nil {
				return err
			}
			if err := tx.AddColumn("users", store.Column{
				Name: "ghost_col", Type: store.TypeText, Nullable: true,
			}); err != nil {
				return err
			}
			return boom
		}})

	sum, err = r.Migrate(badMigrations, 0)
	assertReason(t, "apply failing v2", err, derrors.ReasonMigrationFailed)
	if !errors.Is(err, boom) {
		t.Fatalf("failure cause should be preserved, got %v", err)
	}
	t.Logf("input: apply failing v2 => summary applied=%v, error wraps boom=%v", sum.Applied, errors.Is(err, boom))

	// 判定依据：失败事务不应留下 half_table / ghost_col / 登记记录。
	err = db.View(func(tx *store.Tx) error {
		if tx.HasTable("half_table") {
			return fmt.Errorf("half_table survived rollback")
		}
		cols, cerr := tx.Columns("users")
		if cerr != nil {
			return cerr
		}
		for _, c := range cols {
			if c.Name == "ghost_col" {
				return fmt.Errorf("ghost_col survived rollback")
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("state after failed migration inconsistent: %v", err)
	}
	recs, err := r.State()
	if err != nil {
		t.Fatalf("state: %v", err)
	}
	if len(recs) != 1 || recs[0].Version != 1 {
		t.Fatalf("registry must stay at v1 only, got %+v", recs)
	}
	t.Logf("判定依据: half_table absent, ghost_col absent, registry=%+v => 整批回滚一致", recs)
}

// TestMigrate_IdempotentRepeat 验证重复执行不改变任何已应用版本的数据与结构。
func TestMigrate_IdempotentRepeat(t *testing.T) {
	db := database.Open()
	r := NewRunner(db)

	executed := 0
	ms := []Migration{
		{Version: 1, Name: "accounts", Up: func(tx *store.Tx) error {
			if tx.HasTable("accounts") {
				return nil
			}
			return tx.CreateTable("accounts", []store.Column{
				{Name: "balance", Type: store.TypeInteger, HasDefault: true, Default: int64(0)},
			})
		}},
		{Version: 2, Name: "seed", Up: func(tx *store.Tx) error {
			executed++
			// 即使脚本被重跑，也必须幂等：存在则不重复插入。
			rows, err := tx.List("accounts", store.ScanOptions{})
			if err != nil {
				return err
			}
			if len(rows) > 0 {
				return nil
			}
			_, err = tx.Insert("accounts", store.Row{"balance": int64(42)})
			return err
		}},
	}

	s1, err := r.Migrate(ms, 0)
	if err != nil {
		t.Fatalf("first run: %v", err)
	}
	rows1 := countRows(t, db, "accounts")
	t.Logf("input: run#1 summary=%+v rows(accounts)=%d", s1, rows1)

	for i := 2; i <= 3; i++ {
		s, err := r.Migrate(ms, 0)
		if err != nil {
			t.Fatalf("repeat run #%d: %v", i, err)
		}
		rows := countRows(t, db, "accounts")
		t.Logf("input: run#%d summary=%+v rows=%d (expect skipped=[1 2], rows=1)", i, s, rows)
		if len(s.Applied) != 0 || fmt.Sprint(s.Skipped) != "[1 2]" {
			t.Fatalf("repeat #%d must skip both versions, got %+v", i, s)
		}
		if rows != 1 {
			t.Fatalf("repeat migration changed data: rows=%d want 1", rows)
		}
	}
	if executed != 1 {
		t.Fatalf("v2 script body should not re-run applied version; executed=%d", executed)
	}
	t.Log("判定依据: 三次执行后行数恒为 1，登记版本恒为 [1 2] => 幂等且不改数据")
}

// TestMigrate_ChecksumDriftRejected 验证同版本号脚本身份变化会被拒绝，且不执行脚本。
func TestMigrate_ChecksumDriftRejected(t *testing.T) {
	db := database.Open()
	r := NewRunner(db)
	v1 := []Migration{{Version: 1, Name: "t", Up: func(tx *store.Tx) error {
		if tx.HasTable("t_a") {
			return nil
		}
		return tx.CreateTable("t_a", []store.Column{{Name: "x", Type: store.TypeText}})
	}}}
	if _, err := r.Migrate(v1, 0); err != nil {
		t.Fatal(err)
	}
	v1Changed := []Migration{{Version: 1, Name: "t-renamed-identity", Up: func(tx *store.Tx) error {
		return tx.CreateTable("t_b", nil)
	}}}
	_, err := r.Migrate(v1Changed, 0)
	assertReason(t, "same version different identity", err, derrors.ReasonMigrationChecksum)
	if hasTable(t, db, "t_b") {
		t.Fatal("changed script must not be executed")
	}
	t.Log("判定依据: 校验和不一致 => 拒绝，t_b 未创建")
}

// TestMigrate_DestructiveRequiresDeclaration 验证未声明 Destructive 的删列/改名被拒绝并回滚。
func TestMigrate_DestructiveRequiresDeclaration(t *testing.T) {
	db := database.Open()
	r := NewRunner(db)
	ms := []Migration{
		{Version: 1, Name: "base", Up: func(tx *store.Tx) error {
			if tx.HasTable("things") {
				return nil
			}
			return tx.CreateTable("things", []store.Column{
				{Name: "color", Type: store.TypeText, HasDefault: true, Default: "blue"},
			})
		}},
		{Version: 2, Name: "undeclared_drop", Up: func(tx *store.Tx) error {
			return tx.DropColumn("things", "color")
		}},
	}
	_, err := r.Migrate(ms, 0)
	assertReason(t, "drop without declaration", err, derrors.ReasonDestructiveChange)
	if !strings.Contains(err.Error(), "rolled back") {
		t.Fatalf("error should explain rollback: %v", err)
	}

	// 声明 Destructive=true 后允许，且审计登记随版本提交。
	ms[1].Destructive = true
	ms[1].Name = "declared_drop"
	// 名称改变会产生不同版本集（v2 尚未应用），可正常应用。
	sum, err := r.Migrate(ms, 0)
	if err != nil {
		t.Fatalf("declared destructive should apply: %v", err)
	}
	t.Logf("input: declared destructive v2 => applied=%v", sum.Applied)
	if countRows(t, db, "schema_destructive_approvals") != 1 {
		t.Fatal("destructive approval must be recorded in version transaction")
	}
	t.Log("判定依据: 未声明 => DESTRUCTIVE_CHANGE_DENIED 且回滚；声明后生效并留审计")
}

// TestMigrate_BadOrdering 验证重复版本号与非递增版本号被拒绝。
func TestMigrate_BadOrdering(t *testing.T) {
	db := database.Open()
	r := NewRunner(db)
	ms := []Migration{
		{Version: 2, Name: "two", Up: func(tx *store.Tx) error { return nil }},
		{Version: 2, Name: "dup", Up: func(tx *store.Tx) error { return nil }},
	}
	_, err := r.Migrate(ms, 0)
	assertReason(t, "duplicate versions", err, derrors.ReasonMigrationBadOrdering)
	t.Log("判定依据: 静态校验拒绝重复版本，未执行任何脚本")
}

func countRows(t *testing.T, db *database.DB, table string) int {
	t.Helper()
	var n int
	if err := db.View(func(tx *store.Tx) error {
		rows, err := tx.List(table, store.ScanOptions{})
		if err != nil {
			return err
		}
		n = len(rows)
		return nil
	}); err != nil {
		t.Fatalf("countRows(%s): %v", table, err)
	}
	return n
}

func hasTable(t *testing.T, db *database.DB, name string) bool {
	t.Helper()
	var ok bool
	if err := db.View(func(tx *store.Tx) error { ok = tx.HasTable(name); return nil }); err != nil {
		t.Fatal(err)
	}
	return ok
}
