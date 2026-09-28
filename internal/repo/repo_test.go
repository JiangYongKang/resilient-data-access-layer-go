package repo_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
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

func setupDB(t *testing.T) *engine.DB {
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

// 同一查询必须在三个入口给出一致结果：List(offset) / ListPage / ListCursor。
func TestEntriesReturnIdenticalResults(t *testing.T) {
	db := setupDB(t)
	ctx := context.Background()
	products := repo.New(db, domain.ProductCodec())
	for i := 1; i <= 5; i++ {
		if err := products.Create(ctx, &domain.Product{
			SKU: fmt.Sprintf("SKU-%d", i), Name: fmt.Sprintf("name-%d", i), Price: int64(i * 100),
		}); err != nil {
			t.Fatal(err)
		}
	}
	filters := []engine.Filter{{Field: "price", Op: engine.OpGte, Value: engine.NewInt(200)}}

	a, err := products.List(ctx, repo.ListSpec{Filters: filters, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	b, err := products.ListPage(ctx, filters, 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	c, err := products.ListCursor(ctx, filters, "", 2)
	if err != nil {
		t.Fatal(err)
	}
	verdict(t, "price>=200 limit=2 across List/ListPage/ListCursor",
		"identical id sequences", []any{idsOf(a.Items), idsOf(b.Items), idsOf(c.Items)})
	if !sameIDs(a.Items, b.Items) || !sameIDs(a.Items, c.Items) {
		t.Fatalf("entries disagree: list=%v page=%v cursor=%v", idsOf(a.Items), idsOf(b.Items), idsOf(c.Items))
	}
	if a.Total != b.Total || a.Total != 4 {
		t.Fatalf("total must be consistent, got %d/%d", a.Total, b.Total)
	}
}

func idsOf(ps []*domain.Product) []int64 {
	out := make([]int64, len(ps))
	for i, p := range ps {
		out[i] = p.ID
	}
	return out
}

func sameIDs(a, b []*domain.Product) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].ID != b[i].ID || a[i].Version != b[i].Version {
			return false
		}
	}
	return true
}

// 畸形分页参数：每种非法输入都要落到可区分原因。
func TestMalformedPaginationRejected(t *testing.T) {
	db := setupDB(t)
	ctx := context.Background()
	products := repo.New(db, domain.ProductCodec())

	cases := []struct {
		name string
		spec repo.ListSpec
		want error
	}{
		{"limit=0", repo.ListSpec{Limit: 0}, engine.ErrInvalidPagination},
		{"limit<0", repo.ListSpec{Limit: -3}, engine.ErrInvalidPagination},
		{"limit over max", repo.ListSpec{Limit: repo.MaxLimit + 1}, engine.ErrInvalidPagination},
		{"offset<0", repo.ListSpec{Limit: 10, Offset: -1}, engine.ErrInvalidPagination},
		{"cursor+offset mixed", repo.ListSpec{Limit: 10, Offset: 5, Cursor: "x"}, engine.ErrInvalidPagination},
		{"garbage cursor", repo.ListSpec{Limit: 10, Cursor: "!!!garbage"}, engine.ErrInvalidCursor},
		{"unknown filter field", repo.ListSpec{Limit: 10, Filters: []engine.Filter{
			{Field: "ghost", Op: engine.OpEq, Value: engine.NewString("v")},
		}}, engine.ErrColumnNotFound},
		{"filter type mismatch", repo.ListSpec{Limit: 10, Filters: []engine.Filter{
			{Field: "price", Op: engine.OpEq, Value: engine.NewBool(true)},
		}}, engine.ErrTypeMismatch},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := products.List(ctx, tc.spec)
			verdict(t, tc.name, tc.want, err)
			if !errors.Is(err, tc.want) {
				t.Fatalf("want %v got %v", tc.want, err)
			}
		})
	}
}

// 并发更新：两个过期副本并发写同一行，恰好一个成功，另一个 ErrVersionConflict，
// 库中最终值只反映成功者（后写不覆盖前写）。
func TestConcurrentOptimisticUpdateConflict(t *testing.T) {
	db := setupDB(t)
	ctx := context.Background()
	products := repo.New(db, domain.ProductCodec())
	original := &domain.Product{SKU: "LOCK", Name: "orig", Price: 1}
	if err := products.Create(ctx, original); err != nil {
		t.Fatal(err)
	}

	const writers = 8
	snapshots := make([]*domain.Product, writers)
	for i := range snapshots {
		p, err := products.Get(ctx, original.ID)
		if err != nil {
			t.Fatal(err)
		}
		snapshots[i] = p
	}

	var wg sync.WaitGroup
	results := make([]error, writers)
	start := make(chan struct{})
	for i := 0; i < writers; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			snapshots[i].Name = fmt.Sprintf("writer-%d", i)
			results[i] = products.Update(ctx, snapshots[i])
		}()
	}
	close(start)
	wg.Wait()

	committed, conflicts, others := 0, 0, 0
	for i, err := range results {
		switch {
		case err == nil:
			committed++
			verdict(t, fmt.Sprintf("writer %d", i), "commit", "commit")
		case errors.Is(err, engine.ErrVersionConflict):
			conflicts++
		default:
			others++
			t.Errorf("writer %d unexpected error: %v", i, err)
		}
	}
	verdict(t, "8 concurrent stale writers", "committed=1 conflicts=7 others=0",
		fmt.Sprintf("committed=%d conflicts=%d others=%d", committed, conflicts, others))
	if others != 0 || committed != 1 || conflicts != writers-1 {
		t.Fatalf("exactly one commit expected, got committed=%d conflicts=%d others=%d", committed, conflicts, others)
	}

	final, err := products.Get(ctx, original.ID)
	if err != nil {
		t.Fatal(err)
	}
	verdict(t, "final row", "version=1, name of single winner",
		fmt.Sprintf("version=%d name=%s", final.Version, final.Name))
	if final.Version != 1 {
		t.Fatalf("version must advance exactly once, got %d", final.Version)
	}
}

// RetryOnConflict：冲突时由调用方决定重载重试，最终成功，非冲突错误不重试。
func TestRetryOnConflict(t *testing.T) {
	db := setupDB(t)
	ctx := context.Background()
	products := repo.New(db, domain.ProductCodec())
	p := &domain.Product{SKU: "RET", Name: "n0", Price: 1}
	if err := products.Create(ctx, p); err != nil {
		t.Fatal(err)
	}

	// 先在外部把版本推进到 1。
	holder, _ := products.Get(ctx, p.ID)
	holder.Name = "external"
	if err := products.Update(ctx, holder); err != nil {
		t.Fatal(err)
	}

	attempts := 0
	err := repo.RetryOnConflict(3, func() error {
		attempts++
		fresh, gErr := products.Get(ctx, p.ID) // 每次重试重新加载，拿到最新版本
		if gErr != nil {
			return gErr
		}
		fresh.Name = fmt.Sprintf("retry-%d", attempts)
		return products.Update(ctx, fresh)
	})
	verdict(t, "retry loop with reload after external bump", "success on attempt 2",
		fmt.Sprintf("err=%v attempts=%d", err, attempts))
	if err != nil || attempts != 1 {
		t.Fatalf("reload inside fn should win immediately: err=%v attempts=%d", err, attempts)
	}

	// 直接用过期副本重试：第一次冲突，第二次仍冲突（不重载），耗尽后返回冲突。
	stale := p
	stale.Version = 0
	hardErr := repo.RetryOnConflict(2, func() error { return products.Update(ctx, stale) })
	verdict(t, "stale retries without reload", "ErrVersionConflict exhausted", hardErr)
	if !errors.Is(hardErr, engine.ErrVersionConflict) {
		t.Fatalf("want exhausted conflict, got %v", hardErr)
	}
}

// 批量写入原子性：批次中途失败必须整批回滚（连前几条已插入的也不留）。
func TestBatchCreateAtomicOnFailure(t *testing.T) {
	db := setupDB(t)
	ctx := context.Background()
	accounts := repo.New(db, domain.AccountCodec())

	bad := []*domain.Account{
		{Code: "a1", Balance: 10},
		{Code: "a2", Balance: 20},
		nil, // 批次中间出现非法实体
	}
	err := accounts.BatchCreate(ctx, bad)
	verdict(t, "batch [ok, ok, nil]", "rejection + zero accounts inserted", err)
	if err == nil {
		t.Fatal("batch with nil entity must fail")
	}
	res, lErr := accounts.ListPage(ctx, []engine.Filter{
		{Field: "code", Op: engine.OpNeq, Value: engine.NewString("system")},
	}, 0, 100)
	if lErr != nil {
		t.Fatal(lErr)
	}
	verdict(t, "non-system accounts after failed batch", 0, len(res.Items))
	if len(res.Items) != 0 {
		t.Fatalf("partial batch commit detected: %d rows survived", len(res.Items))
	}
}

// 同一事务边界内的扣款不能重复扣减；失败整体回滚，余额与版本不变。
func TestInTxNoDoubleDeductionAndAtomicTransfer(t *testing.T) {
	db := setupDB(t)
	ctx := context.Background()
	accounts := repo.New(db, domain.AccountCodec())
	alice := &domain.Account{Code: "alice", Balance: 50}
	bob := &domain.Account{Code: "bob", Balance: 0}
	if err := accounts.BatchCreate(ctx, []*domain.Account{alice, bob}); err != nil {
		t.Fatal(err)
	}

	// 余额不足：一个事务里先扣 alice 再加 bob，触发业务错误后必须整笔回滚。
	overdraft := accounts.InTx(ctx, func(tx *repo.Repository[domain.Account, *domain.Account]) error {
		a, err := tx.Get(ctx, alice.ID)
		if err != nil {
			return err
		}
		b, err := tx.Get(ctx, bob.ID)
		if err != nil {
			return err
		}
		if a.Balance < 80 {
			return fmt.Errorf("%w: balance=%d amount=80", engine.ErrInsufficientBalance, a.Balance)
		}
		a.Balance -= 80
		b.Balance += 80
		if err := tx.Update(ctx, a); err != nil {
			return err
		}
		return tx.Update(ctx, b)
	})
	verdict(t, "transfer 80 from balance 50", "ErrInsufficientBalance + rollback", overdraft)
	if !errors.Is(overdraft, engine.ErrInsufficientBalance) {
		t.Fatalf("want insufficient balance, got %v", overdraft)
	}
	aAfter, _ := accounts.Get(ctx, alice.ID)
	bAfter, _ := accounts.Get(ctx, bob.ID)
	verdict(t, "balances after rejected transfer", "alice=50 bob=0 ver=0,0",
		fmt.Sprintf("alice=%d(v%d) bob=%d(v%d)", aAfter.Balance, aAfter.Version, bAfter.Balance, bAfter.Version))
	if aAfter.Balance != 50 || bAfter.Balance != 0 || aAfter.Version != 0 || bAfter.Version != 0 {
		t.Fatalf("rejected transfer changed state: %+v %+v", aAfter, bAfter)
	}

	// 合法转账：同一事务里两次更新一起提交，版本各自恰好 +1（没有重复扣减）。
	if err := accounts.InTx(ctx, func(tx *repo.Repository[domain.Account, *domain.Account]) error {
		a, _ := tx.Get(ctx, alice.ID)
		b, _ := tx.Get(ctx, bob.ID)
		a.Balance -= 30
		b.Balance += 30
		if err := tx.Update(ctx, a); err != nil {
			return err
		}
		return tx.Update(ctx, b)
	}); err != nil {
		t.Fatal(err)
	}
	a2, _ := accounts.Get(ctx, alice.ID)
	b2, _ := accounts.Get(ctx, bob.ID)
	verdict(t, "valid transfer 30", "alice=20(v1) bob=30(v1)",
		fmt.Sprintf("alice=%d(v%d) bob=%d(v%d)", a2.Balance, a2.Version, b2.Balance, b2.Version))
	if a2.Balance != 20 || b2.Balance != 30 || a2.Version != 1 || b2.Version != 1 {
		t.Fatalf("atomic transfer produced wrong state: %+v %+v", a2, b2)
	}
}

// 并发事务下的“读-检查-扣减”必须串行化：不能两个并发请求都扣同一笔额度。
func TestConcurrentDeductionSerializable(t *testing.T) {
	db := setupDB(t)
	ctx := context.Background()
	accounts := repo.New(db, domain.AccountCodec())
	acct := &domain.Account{Code: "wallet", Balance: 1}
	if err := accounts.Create(ctx, acct); err != nil {
		t.Fatal(err)
	}

	const n = 5
	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			// 每个请求都在自己的事务里读取最新余额，余额为 1 才扣 1。
			errs[i] = accounts.InTx(ctx, func(tx *repo.Repository[domain.Account, *domain.Account]) error {
				a, err := tx.Get(ctx, acct.ID)
				if err != nil {
					return err
				}
				if a.Balance < 1 {
					return fmt.Errorf("%w: balance=%d", engine.ErrInsufficientBalance, a.Balance)
				}
				a.Balance -= 1
				return tx.Update(ctx, a)
			})
		}()
	}
	close(start)
	wg.Wait()

	succeeded, insufficient := 0, 0
	for _, err := range errs {
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, engine.ErrInsufficientBalance):
			insufficient++
		default:
			t.Fatalf("unexpected err: %v", err)
		}
	}
	final, _ := accounts.Get(ctx, acct.ID)
	verdict(t, "5 concurrent deductions against balance=1",
		"succeeded=1 insufficient=4 finalBalance=0 version=1",
		fmt.Sprintf("succeeded=%d insufficient=%d finalBalance=%d version=%d",
			succeeded, insufficient, final.Balance, final.Version))
	if succeeded != 1 || insufficient != 4 || final.Balance != 0 || final.Version != 1 {
		t.Fatal("double deduction detected under concurrency")
	}
}
