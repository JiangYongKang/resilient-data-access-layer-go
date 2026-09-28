// 本地可运行的最小演示：迁移幂等、分页入口一致性、乐观并发与幂等扣减。
//
// 运行：go run ./cmd/demo
package main

import (
	"errors"
	"fmt"
	"sync"

	"github.com/highcumontoa/resilient-data-access-layer-go/internal/account"
	"github.com/highcumontoa/resilient-data-access-layer-go/internal/database"
	derrors "github.com/highcumontoa/resilient-data-access-layer-go/internal/errors"
	"github.com/highcumontoa/resilient-data-access-layer-go/internal/migrate"
	"github.com/highcumontoa/resilient-data-access-layer-go/internal/repo"
)

func main() {
	db := database.Open()
	runner := migrate.NewRunner(db)

	// 第一次迁移：应用 v1..v3
	sum, err := runner.Migrate(account.Migrations(), 0)
	mustOK(err)
	fmt.Printf("migrate#1 applied=%v skipped=%v current=%d\n", sum.Applied, sum.Skipped, sum.Current)

	// 第二次迁移：全部跳过，脚本不重跑，数据不变
	sum, err = runner.Migrate(account.Migrations(), 0)
	mustOK(err)
	fmt.Printf("migrate#2 applied=%v skipped=%v current=%d (idempotent)\n", sum.Applied, sum.Skipped, sum.Current)

	acctRepo := account.NewRepository(db)
	for i := 0; i < 6; i++ {
		_, err := acctRepo.Create(fmt.Sprintf("acct-%d", i+1), 1000)
		mustOK(err)
	}

	// 同一查询的三个入口：过滤条件相同，结果集必须一致
	filters := []repo.Filter{{Field: "status", Value: "active"}}
	all, err := acctRepo.List(filters)
	mustOK(err)
	page1, err := acctRepo.ListPage(filters, repo.PageRequest{Page: 1, PageSize: 3})
	mustOK(err)
	page2, err := acctRepo.ListPage(filters, repo.PageRequest{Page: 2, PageSize: 3})
	mustOK(err)
	cursorDrain, err := acctRepo.DrainByCursor(filters, 2)
	mustOK(err)
	fmt.Printf("query consistency: all=%d pages=%d+%d cursor=%d total=%d\n",
		len(all), len(page1.Items), len(page2.Items), len(cursorDrain), page1.Total)

	// 并发乐观更新：两个请求基于同一版本更新同一账户，只允许一个成功
	a, err := acctRepo.Get(1)
	mustOK(err)
	var wg sync.WaitGroup
	results := make(chan string, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			v := a.Version
			_, uerr := acctRepo.Update(account.UpdateAccountInput{
				ID: 1, Name: fmt.Sprintf("renamed-by-%d", n), Status: "active", Tier: "standard", Version: v,
			})
			if uerr != nil {
				var de *derrors.DataError
				if errors.As(uerr, &de) {
					results <- "rejected:" + string(de.Reason)
					return
				}
				results <- "error:" + uerr.Error()
				return
			}
			results <- "committed"
		}(i)
	}
	wg.Wait()
	close(results)
	for r := range results {
		fmt.Println("concurrent update ->", r)
	}

	// 幂等扣减：同一业务请求重试两次，只扣一次
	res1, err := acctRepo.Debit(account.DebitRequest{AccountID: 2, Amount: 300, IdempotentID: "biz-001", Version: 1})
	mustOK(err)
	res2, err := acctRepo.Debit(account.DebitRequest{AccountID: 2, Amount: 300, IdempotentID: "biz-001", Version: res1.Account.Version})
	mustOK(err)
	fmt.Printf("idempotent debit: first=%d replayed=%v finalBalance=%d (expected 700)\n",
		res1.Account.Balance, res2.Replayed, res2.Account.Balance)
}

func mustOK(err error) {
	if err != nil {
		panic(err)
	}
}
