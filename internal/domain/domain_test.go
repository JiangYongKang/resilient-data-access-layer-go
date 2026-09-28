package domain_test

import (
	"context"
	"errors"
	"testing"

	"github.com/highcumontoa/resilient-data-access-layer-go/internal/domain"
	"github.com/highcumontoa/resilient-data-access-layer-go/internal/engine"
	"github.com/highcumontoa/resilient-data-access-layer-go/internal/migrate"
	"github.com/highcumontoa/resilient-data-access-layer-go/internal/repo"
)

func verdict(t *testing.T, input string, want, got any) {
	t.Helper()
	t.Logf("INPUT(%s) | WANT=%v | GOT=%v", input, want, got)
}

func migrateToLatest(t *testing.T) *engine.DB {
	t.Helper()
	db, err := engine.Open(engine.Options{Path: ":memory:"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mg := migrate.New(db)
	if err := mg.Register(domain.Migrations()...); err != nil {
		t.Fatal(err)
	}
	if _, err := mg.Up(context.Background()); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestEvolutionDefaultsAndRename(t *testing.T) {
	db := migrateToLatest(t)
	ctx := context.Background()

	// V2 新增的 products.status 对新行有确定默认 1（指针非 nil）。
	products := repo.New(db, domain.ProductCodec())
	p := &domain.Product{SKU: "EV1", Name: "evolved", Price: 42}
	if err := products.Create(ctx, p); err != nil {
		t.Fatal(err)
	}
	verdict(t, "insert product after V2", "status=1 defaulted", p.Status)
	if p.Status == nil || *p.Status != 1 {
		t.Fatalf("status default must be 1 after evolution, got %v", p.Status)
	}

	// V3 把 title 改名为 name：旧列名必须明确报“找不到”，不能静默读到错位值。
	err := db.View(ctx, func(tx *engine.Tx) error {
		s, err := tx.TableSchema("products")
		if err != nil {
			return err
		}
		if _, _, ok := s.ColumnByName("name"); ok != nil {
			t.Errorf("name column must exist: %v", ok)
		}
		page, err := tx.List("products", nil, engine.PageRequest{Limit: 1})
		if err != nil {
			return err
		}
		_, gErr := page.Rows[0].Get(s, "title")
		verdict(t, "read legacy column title after rename", "ErrColumnNotFound", gErr)
		if !errors.Is(gErr, engine.ErrColumnNotFound) {
			t.Errorf("legacy name must not silently resolve: %v", gErr)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// V2 新增的 accounts.note 可空；读回零字符串，不报错。
	accounts := repo.New(db, domain.AccountCodec())
	a := &domain.Account{Code: "cust", Balance: 9}
	if err := accounts.Create(ctx, a); err != nil {
		t.Fatal(err)
	}
	got, err := accounts.Get(ctx, a.ID)
	if err != nil {
		t.Fatal(err)
	}
	verdict(t, "account note after nullable add", `note=""`, got.Note)
	if got.Note != "" {
		t.Fatalf("nullable note should read empty, got %q", got.Note)
	}
}

func TestSeedSystemAccountIdempotent(t *testing.T) {
	db := migrateToLatest(t)
	ctx := context.Background()
	accounts := repo.New(db, domain.AccountCodec())
	res, err := accounts.ListPage(ctx, []engine.Filter{
		{Field: "code", Op: engine.OpEq, Value: engine.NewString("system")},
	}, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	verdict(t, "system accounts seeded by V1 data step", 1, len(res.Items))
	if len(res.Items) != 1 {
		t.Fatalf("exactly one system account expected, got %d", len(res.Items))
	}
}
