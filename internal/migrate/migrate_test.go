package migrate_test

import (
	"context"
	"errors"
	"testing"

	"github.com/highcumontoa/resilient-data-access-layer-go/internal/engine"
	"github.com/highcumontoa/resilient-data-access-layer-go/internal/migrate"
)

func verdict(t *testing.T, input string, want, got any) {
	t.Helper()
	t.Logf("INPUT(%s) | WANT=%v | GOT=%v", input, want, got)
}

func baseSchema() *engine.Schema {
	s, _ := engine.NewSchema("things", []engine.Column{
		{Name: "name", Type: engine.KindString},
	})
	return s
}

// 失败发生在一个含两步的版本里：建表成功后下一步强制失败。
// 期望：整批中止，连同一事务里的建表也必须回滚。
func TestMigrationFailureRollsBackEntireBatch(t *testing.T) {
	db, _ := engine.Open(engine.Options{Path: ":memory:"})
	defer db.Close()
	ctx := context.Background()

	mg := migrate.New(db)
	err := mg.Register(
		migrate.Migration{Version: 1, Name: "v1", Steps: []migrate.Step{
			{Kind: "create_table", Schema: baseSchema()},
			{Kind: "data", FailForTest: true},
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = mg.Up(ctx)
	verdict(t, "v1: create_table then forced failing data step", "ErrStepFailed", err)
	if !errors.Is(err, migrate.ErrStepFailed) {
		t.Fatalf("expected ErrStepFailed, got %v", err)
	}

	// 判定依据一：版本表必须不存在（它在同一事务里创建并随回滚消失）。
	var tableErr error
	if vErr := db.View(ctx, func(tx *engine.Tx) error {
		_, tableErr = tx.TableSchema("things")
		return nil
	}); vErr != nil {
		t.Fatal(vErr)
	}
	verdict(t, "inspect table things after failed migration", "ErrTableNotFound", tableErr)
	if !errors.Is(tableErr, engine.ErrTableNotFound) {
		t.Fatalf("things table must not survive rollback, got %v", tableErr)
	}

	// 判定依据二：修复迁移后可以从干净状态重新执行（没有半套结构挡住重跑）。
	mg2 := migrate.New(db)
	if err := mg2.Register(migrate.Migration{Version: 1, Name: "v1", Steps: []migrate.Step{
		{Kind: "create_table", Schema: baseSchema()},
	}}); err != nil {
		t.Fatal(err)
	}
	applied, err := mg2.Up(ctx)
	if err != nil {
		t.Fatalf("re-run after fix must succeed, got %v", err)
	}
	verdict(t, "fixed v1 re-run", "applied=[1]", versions(applied))
	if len(applied) != 1 || applied[0].Version != 1 {
		t.Fatalf("unexpected applied: %+v", applied)
	}
}

// 同一批里第二个版本失败：第一版本也必须一起回滚（整批，而非逐版本提交）。
func TestMigrationMultiVersionBatchAtomic(t *testing.T) {
	db, _ := engine.Open(engine.Options{Path: ":memory:"})
	defer db.Close()
	ctx := context.Background()

	v1 := migrate.Migration{Version: 1, Name: "v1", Steps: []migrate.Step{
		{Kind: "create_table", Schema: baseSchema()},
	}}
	v2 := migrate.Migration{Version: 2, Name: "v2", Steps: []migrate.Step{
		{Kind: "data", FailForTest: true},
	}}
	mg := migrate.New(db)
	if err := mg.Register(v1, v2); err != nil {
		t.Fatal(err)
	}
	_, err := mg.Up(ctx)
	verdict(t, "v1 ok, v2 fails in same batch", "ErrStepFailed + no table", err)
	if !errors.Is(err, migrate.ErrStepFailed) {
		t.Fatalf("expected ErrStepFailed, got %v", err)
	}
	var nf error
	_ = db.View(ctx, func(tx *engine.Tx) error {
		_, nf = tx.TableSchema("things")
		return nil
	})
	if !errors.Is(nf, engine.ErrTableNotFound) {
		t.Fatalf("whole batch must roll back incl. v1 table, got %v", nf)
	}
}

// 重复执行同一版本：第二次 Up 无任何应用，且已有数据逐字节语义不变。
func TestMigrationIdempotentRepeatedUp(t *testing.T) {
	db, _ := engine.Open(engine.Options{Path: ":memory:"})
	defer db.Close()
	ctx := context.Background()

	mg := migrate.New(db)
	migs := []migrate.Migration{
		{Version: 1, Name: "init", Steps: []migrate.Step{
			{Kind: "create_table", Schema: baseSchema()},
			// 幂等回填：只在不存在时插入，重复 Up 不能产生重复行。
			{Kind: "data", Data: func(ctx context.Context, tx *engine.Tx) error {
				page, err := tx.List("things", []engine.Filter{
					{Field: "name", Op: engine.OpEq, Value: engine.NewString("seed")},
				}, engine.PageRequest{Limit: 1})
				if err != nil {
					return err
				}
				if len(page.Rows) > 0 {
					return nil
				}
				_, err = tx.Insert("things", map[string]engine.Value{"name": engine.NewString("seed")})
				return err
			}},
		}},
		{Version: 2, Name: "add_flag", Steps: []migrate.Step{
			{Kind: "add_column", Table: "things", Column: engine.Column{
				Name: "flag", Type: engine.KindInt, HasDefault: true, Default: engine.NewInt(7),
			}},
		}},
	}
	if err := mg.Register(migs...); err != nil {
		t.Fatal(err)
	}
	first, err := mg.Up(ctx)
	if err != nil {
		t.Fatal(err)
	}
	verdict(t, "first up", "applied=[1 2]", versions(first))
	if len(first) != 2 {
		t.Fatalf("want 2 applied, got %d", len(first))
	}

	second, err := mg.Up(ctx)
	if err != nil {
		t.Fatalf("repeated up must not error: %v", err)
	}
	third, err := mg.Up(ctx)
	if err != nil {
		t.Fatalf("third up: %v", err)
	}
	verdict(t, "second/third up", "applied=[]", []int{len(second), len(third)})
	if len(second) != 0 || len(third) != 0 {
		t.Fatalf("repeated up must apply nothing, got %d/%d", len(second), len(third))
	}

	var rows int
	var flag int64
	if err := db.View(ctx, func(tx *engine.Tx) error {
		page, err := tx.List("things", nil, engine.PageRequest{Limit: 100})
		if err != nil {
			return err
		}
		rows = len(page.Rows)
		if rows > 0 {
			s, _ := tx.TableSchema("things")
			v, _ := page.Rows[0].Get(s, "flag")
			flag, _ = v.AsInt()
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	verdict(t, "rows after 3x up; seeded flag default", "rows=1 flag=7", []int64{int64(rows), flag})
	if rows != 1 || flag != 7 {
		t.Fatalf("data changed on repeated migration: rows=%d flag=%d", rows, flag)
	}
}

// 已应用版本被事后修改：校验和变化，Up 必须按原因拒绝（默认语义不得被悄悄改变）。
func TestChecksumChangedRejected(t *testing.T) {
	db, _ := engine.Open(engine.Options{Path: ":memory:"})
	defer db.Close()
	ctx := context.Background()

	mg := migrate.New(db)
	migs := []migrate.Migration{
		{Version: 1, Name: "init", Steps: []migrate.Step{
			{Kind: "create_table", Schema: baseSchema()},
		}},
		{Version: 2, Name: "add_flag", Steps: []migrate.Step{
			{Kind: "add_column", Table: "things", Column: engine.Column{
				Name: "flag", Type: engine.KindInt, HasDefault: true, Default: engine.NewInt(7),
			}},
		}},
	}
	if err := mg.Register(migs...); err != nil {
		t.Fatal(err)
	}
	if _, err := mg.Up(ctx); err != nil {
		t.Fatal(err)
	}

	// 用“默认值从 7 改成 9”的同名同版本脚本重新登记。
	mg2 := migrate.New(db)
	tampered := []migrate.Migration{
		migs[0],
		{Version: 2, Name: "add_flag", Steps: []migrate.Step{
			{Kind: "add_column", Table: "things", Column: engine.Column{
				Name: "flag", Type: engine.KindInt, HasDefault: true, Default: engine.NewInt(9),
			}},
		}},
	}
	if err := mg2.Register(tampered...); err != nil {
		t.Fatal(err)
	}
	_, err := mg2.Up(ctx)
	verdict(t, "registered v2 default 9 vs applied default 7", "ErrChecksumChanged", err)
	if !errors.Is(err, migrate.ErrChecksumChanged) {
		t.Fatalf("expected ErrChecksumChanged, got %v", err)
	}
}

// 登记非法：号段不连续直接拒绝，防止跳过版本造成不可解释的结构状态。
func TestRegisterRejectsNonConsecutiveVersions(t *testing.T) {
	mg := migrate.New(nil)
	err := mg.Register(
		migrate.Migration{Version: 1, Name: "a"},
		migrate.Migration{Version: 3, Name: "c"},
	)
	verdict(t, "register versions 1,3", "ErrVersionOutOfOrder", err)
	if !errors.Is(err, migrate.ErrVersionOutOfOrder) {
		t.Fatalf("expected ErrVersionOutOfOrder, got %v", err)
	}
}

// add_column 不给默认且不可空：登记阶段就拒绝（默认语义必须显式确定）。
func TestRegisterRejectsUnsafeAddColumn(t *testing.T) {
	mg := migrate.New(nil)
	err := mg.Register(migrate.Migration{Version: 1, Name: "a", Steps: []migrate.Step{
		{Kind: "add_column", Table: "things", Column: engine.Column{Name: "x", Type: engine.KindString}},
	}})
	verdict(t, "add non-nullable column without default", "ErrInvalidStep", err)
	if !errors.Is(err, migrate.ErrInvalidStep) {
		t.Fatalf("expected ErrInvalidStep, got %v", err)
	}
}

func versions(ms []migrate.AppliedMigration) []int {
	out := make([]int, len(ms))
	for i, m := range ms {
		out[i] = m.Version
	}
	return out
}
