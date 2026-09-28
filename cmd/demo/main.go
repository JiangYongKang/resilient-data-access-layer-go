// Command demo 在本地（内存库）演示：迁移幂等、双模式分页、
// 并发更新冲突与重试、事务原子转账。
package main

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/highcumontoa/resilient-data-access-layer-go/internal/domain"
	"github.com/highcumontoa/resilient-data-access-layer-go/internal/engine"
	"github.com/highcumontoa/resilient-data-access-layer-go/internal/migrate"
	"github.com/highcumontoa/resilient-data-access-layer-go/internal/repo"
)

func main() {
	ctx := context.Background()
	db, err := engine.Open(engine.Options{Path: ":memory:"})
	must(err)
	defer db.Close()

	// 1) 迁移：第一次执行全部版本，第二次重复执行必须无操作、不改数据。
	mg := migrate.New(db)
	must(mg.Register(domain.Migrations()...))
	a1, err := mg.Up(ctx)
	must(err)
	fmt.Printf("[migrate] first  up applied=%v\n", versions(a1))
	a2, err := mg.Up(ctx)
	must(err)
	fmt.Printf("[migrate] second up applied=%v (idempotent: %v)\n", versions(a2), len(a2) == 0)

	accounts := repo.New(db, domain.AccountCodec())
	products := repo.New(db, domain.ProductCodec())

	// 2) 批量写入（单事务，全有或全无）。
	batch := []*domain.Product{
		{SKU: "A1", Name: "alpha", Price: 100},
		{SKU: "A2", Name: "beta", Price: 200},
		{SKU: "A3", Name: "gamma", Price: 300},
		{SKU: "A4", Name: "delta", Price: 400},
		{SKU: "A5", Name: "epsilon", Price: 500},
	}
	must(products.BatchCreate(ctx, batch))
	fmt.Printf("[batch] created %d products; status defaults to 1 (sample id=%d status=%d)\n",
		len(batch), batch[0].ID, *batch[0].Status)

	// 3) 同一查询的两个入口结果一致；offset 翻页 + cursor 翻页拼出相同集合。
	off, err := products.ListPage(ctx, nil, 0, 2)
	must(err)
	cur, err := products.ListCursor(ctx, nil, "", 2)
	must(err)
	fmt.Printf("[list] offset page1=%s | cursor page1=%s | equal=%v\n",
		skus(off.Items), skus(cur.Items), sameSKUs(off.Items, cur.Items))
	off2, err := products.ListPage(ctx, nil, 2, 2)
	must(err)
	cur2, err := products.ListCursor(ctx, nil, cur.NextCursor, 2)
	must(err)
	fmt.Printf("[list] offset page2=%s | cursor page2=%s | equal=%v\n",
		skus(off2.Items), skus(cur2.Items), sameSKUs(off2.Items, cur2.Items))

	// 4) 畸形参数与非法过滤按原因拒绝（不静默给空/全量）。
	_, err = products.ListPage(ctx, nil, 0, 0)
	fmt.Printf("[guard] limit=0 rejected: %v\n", errors.Is(err, engine.ErrInvalidPagination))
	_, err = products.List(ctx, repo.ListSpec{Limit: 2, Cursor: "not-a-cursor"})
	fmt.Printf("[guard] bad cursor rejected: %v\n", errors.Is(err, engine.ErrInvalidCursor))
	_, err = products.ListPage(ctx, []engine.Filter{{Field: "ghost", Op: engine.OpEq, Value: engine.NewString("x")}}, 0, 10)
	fmt.Printf("[guard] missing filter column rejected: %v\n", errors.Is(err, engine.ErrColumnNotFound))
	_, err = products.ListPage(ctx, []engine.Filter{{Field: "price", Op: engine.OpEq, Value: engine.NewString("oops")}}, 0, 10)
	fmt.Printf("[guard] filter type mismatch rejected: %v\n", errors.Is(err, engine.ErrTypeMismatch))

	// 5) 并发乐观更新：两个过期副本同时写，只允许一个成功，另一个冲突可重试。
	p0, err := products.Get(ctx, batch[0].ID)
	must(err)
	copy1, err := products.Get(ctx, batch[0].ID)
	must(err)
	copy2, err := products.Get(ctx, batch[0].ID)
	must(err)
	var wg sync.WaitGroup
	results := make([]string, 2)
	for i, cp := range []*domain.Product{copy1, copy2} {
		i, cp := i, cp
		wg.Add(1)
		go func() {
			defer wg.Done()
			cp.Name = fmt.Sprintf("writer-%d", i+1)
			err := products.Update(ctx, cp)
			switch {
			case err == nil:
				results[i] = "committed"
			case errors.Is(err, engine.ErrVersionConflict):
				results[i] = "conflict"
			default:
				results[i] = "error:" + err.Error()
			}
		}()
	}
	wg.Wait()
	got, err := products.Get(ctx, batch[0].ID)
	must(err)
	fmt.Printf("[concurrency] stale writers: %v,%v; winner name=%q version=%d (initial=%d)\n",
		results[0], results[1], got.Name, got.Version, p0.Version)

	// 6) 冲突重试：重新加载再写，最终成功。
	err = repo.RetryOnConflict(5, func() error {
		fresh, gErr := products.Get(ctx, batch[0].ID)
		if gErr != nil {
			return gErr
		}
		fresh.Name = "retried-final"
		return products.Update(ctx, fresh)
	})
	must(err)
	got2, _ := products.Get(ctx, batch[0].ID)
	fmt.Printf("[retry] after conflict retry name=%q version=%d\n", got2.Name, got2.Version)

	// 7) 事务原子转账：余额不足整批回滚，不会出现一边扣款一边没加款。
	from := &domain.Account{Code: "alice", Balance: 50}
	to := &domain.Account{Code: "bob", Balance: 0}
	must(accounts.BatchCreate(ctx, []*domain.Account{from, to}))
	transfer := func(amount int64) error {
		return accounts.InTx(ctx, func(tx *repo.Repository[domain.Account, *domain.Account]) error {
			a, gErr := tx.Get(ctx, from.ID)
			if gErr != nil {
				return gErr
			}
			b, gErr := tx.Get(ctx, to.ID)
			if gErr != nil {
				return gErr
			}
			if a.Balance < amount {
				return fmt.Errorf("%w: account=%d balance=%d amount=%d",
					engine.ErrInsufficientBalance, a.ID, a.Balance, amount)
			}
			a.Balance -= amount
			b.Balance += amount
			if err := tx.Update(ctx, a); err != nil {
				return err
			}
			return tx.Update(ctx, b)
		})
	}
	err = transfer(80)
	fmt.Printf("[tx] overdraw rejected: %v\n", errors.Is(err, engine.ErrInsufficientBalance))
	aAfter, _ := accounts.Get(ctx, from.ID)
	bAfter, _ := accounts.Get(ctx, to.ID)
	fmt.Printf("[tx] balances unchanged after rollback: alice=%d bob=%d\n", aAfter.Balance, bAfter.Balance)
	must(transfer(30))
	aAfter2, _ := accounts.Get(ctx, from.ID)
	bAfter2, _ := accounts.Get(ctx, to.ID)
	fmt.Printf("[tx] valid transfer committed atomically: alice=%d bob=%d (versions=%d,%d)\n",
		aAfter2.Balance, bAfter2.Balance, aAfter2.Version, bAfter2.Version)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func versions(ms []migrate.AppliedMigration) []int {
	out := make([]int, len(ms))
	for i, m := range ms {
		out[i] = m.Version
	}
	return out
}

func skus(ps []*domain.Product) []string {
	out := make([]string, len(ps))
	for i, p := range ps {
		out[i] = p.SKU
	}
	return out
}

func sameSKUs(a, b []*domain.Product) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].SKU != b[i].SKU || a[i].ID != b[i].ID {
			return false
		}
	}
	return true
}
