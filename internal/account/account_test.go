package account

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/highcumontoa/resilient-data-access-layer-go/internal/database"
	derrors "github.com/highcumontoa/resilient-data-access-layer-go/internal/errors"
	"github.com/highcumontoa/resilient-data-access-layer-go/internal/repo"
)

func bootRepo(t *testing.T) (*database.DB, *Repository) {
	t.Helper()
	db := database.Open()
	r, err := Bootstrap(db)
	if err != nil {
		t.Fatal(err)
	}
	return db, r
}

func reasonOf(err error) derrors.ReasonCode {
	var de *derrors.DataError
	if errors.As(err, &de) {
		return de.Reason
	}
	return derrors.ReasonUnknown
}

// TestUpdate_ConcurrentConflict 验证：两个请求基于同一版本更新同一账户时，
// 只有一个提交成功，另一个以 VERSION_CONFLICT 被拒绝；后写不覆盖前写。
func TestUpdate_ConcurrentConflict(t *testing.T) {
	_, r := bootRepo(t)
	a, err := r.Create("cash", 1000)
	if err != nil {
		t.Fatal(err)
	}
	base := a.Version

	var wg sync.WaitGroup
	outcomes := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			_, uerr := r.Update(UpdateAccountInput{
				ID: a.ID, Name: fmt.Sprintf("writer-%d", n), Status: "active",
				Tier: "standard", Version: base,
			})
			outcomes <- uerr
		}(i)
	}
	wg.Wait()
	close(outcomes)

	var committed, conflict int
	for e := range outcomes {
		switch {
		case e == nil:
			committed++
		case reasonOf(e) == derrors.ReasonVersionConflict:
			conflict++
		default:
			t.Fatalf("unexpected error: %v", e)
		}
	}
	t.Logf("input: 2 updates both at version=%d => committed=%d conflict=%d", base, committed, conflict)
	if committed != 1 || conflict != 1 {
		t.Fatalf("want exactly 1 committed and 1 conflict, got committed=%d conflict=%d", committed, conflict)
	}

	final, err := r.Get(a.ID)
	if err != nil {
		t.Fatal(err)
	}
	if final.Version != base+1 {
		t.Fatalf("version must advance exactly once, got %d want %d", final.Version, base+1)
	}
	t.Logf("判定依据: 最终版本=%d（仅 +1），最终名称=%q（第一提交者胜出，后写未覆盖）",
		final.Version, final.Name)
}

// TestUpdate_RetryAfterConflict 验证冲突交给调用方决定：重读-重试后第二次更新可成功，
// 且不会把第一次的字段错误回滚掉。
func TestUpdate_RetryAfterConflict(t *testing.T) {
	_, r := bootRepo(t)
	a, _ := r.Create("cash", 1000)

	// 第一个请求先提交。
	if _, err := r.Update(UpdateAccountInput{ID: a.ID, Name: "first", Status: "active", Tier: "standard", Version: a.Version}); err != nil {
		t.Fatal(err)
	}
	// 第二个请求拿着旧版本更新 -> 冲突。
	_, err := r.Update(UpdateAccountInput{ID: a.ID, Name: "stale", Status: "frozen", Tier: "gold", Version: a.Version})
	if reasonOf(err) != derrors.ReasonVersionConflict {
		t.Fatalf("want VERSION_CONFLICT, got %v", err)
	}
	t.Logf("input: stale update at version=%d => rejected reason=VERSION_CONFLICT", a.Version)

	// 调用方重读后重试，基于新版本成功。
	cur, _ := r.Get(a.ID)
	updated, err := r.Update(UpdateAccountInput{ID: cur.ID, Name: cur.Name, Status: "frozen", Tier: "gold", Version: cur.Version})
	if err != nil {
		t.Fatalf("retry after re-read should succeed: %v", err)
	}
	if updated.Status != "frozen" || updated.Tier != "gold" || updated.Name != "first" {
		t.Fatalf("retry preserved first writer's name and applied own fields, got %+v", updated)
	}
	t.Logf("判定依据: 重读后基于版本=%d 重试成功，name 保留第一提交者值 first，status/tier 更新", cur.Version)
}

// TestUpdate_NotFoundAndBadVersion 覆盖记录不存在与非法版本。
func TestUpdate_NotFoundAndBadVersion(t *testing.T) {
	_, r := bootRepo(t)
	_, err := r.Update(UpdateAccountInput{ID: 999, Name: "x", Status: "active", Tier: "standard", Version: 1})
	if reasonOf(err) != derrors.ReasonRecordNotFound {
		t.Fatalf("want RECORD_NOT_FOUND, got %v", err)
	}
	a, _ := r.Create("cash", 10)
	_, err = r.Update(UpdateAccountInput{ID: a.ID, Name: "x", Status: "active", Tier: "standard", Version: 0})
	if reasonOf(err) != derrors.ReasonInvalidArgument {
		t.Fatalf("want INVALID_ARGUMENT for version=0, got %v", err)
	}
	t.Log("判定依据: 不存在 => RECORD_NOT_FOUND；version=0 => INVALID_ARGUMENT")
}

// TestDebit_ConcurrentSameIdempotencyKey 验证同一业务键在并发/重试下只扣一次。
// 该断言与调度无关：
//   - 若两个事务真正重叠：恰有一笔成功，另一笔以 VERSION_CONFLICT 或唯一键竞争
//     (IDEMPOTENT_REPLAY) 被拒绝；
//   - 若运行时把两笔串行化：第二笔直接命中幂等回放（成功返回但 Replayed=true）。
//
// 两种调度下，真正发生扣减的次数都必须恰为一次，失败方可读重试且不再扣减。
func TestDebit_ConcurrentSameIdempotencyKey(t *testing.T) {
	_, r := bootRepo(t)
	a, _ := r.Create("cash", 1000)

	type result struct {
		err      error
		replayed bool
	}
	var wg sync.WaitGroup
	results := make(chan result, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, e := r.Debit(DebitRequest{AccountID: a.ID, Amount: 300, IdempotentID: "biz-x", Version: a.Version})
			if e != nil {
				results <- result{err: e}
				return
			}
			results <- result{replayed: res.Replayed}
		}()
	}
	wg.Wait()
	close(results)

	var realDeductions, replays, conflicts int
	for res := range results {
		if res.err != nil {
			rc := reasonOf(res.err)
			if rc != derrors.ReasonVersionConflict && rc != derrors.ReasonIdempotentReplay {
				t.Fatalf("unexpected reason %s: %v", rc, res.err)
			}
			conflicts++
			continue
		}
		if res.replayed {
			replays++
		} else {
			realDeductions++
		}
	}
	t.Logf("input: 2 concurrent debits same key => realDeductions=%d replays=%d conflicts=%d",
		realDeductions, replays, conflicts)
	if realDeductions != 1 {
		t.Fatalf("exactly one real deduction required, got %d (replays=%d conflicts=%d)",
			realDeductions, replays, conflicts)
	}
	if replays+conflicts != 1 {
		t.Fatalf("the other request must be either replayed or rejected, replays=%d conflicts=%d",
			replays, conflicts)
	}

	// 任意一方随后以“最新版本 + 同一业务键”再重试：必须回放，余额不再变化。
	cur, _ := r.Get(a.ID)
	retry, err := r.Debit(DebitRequest{AccountID: a.ID, Amount: 300, IdempotentID: "biz-x", Version: cur.Version})
	if err != nil {
		t.Fatalf("caller re-read and retry should replay, not error: %v", err)
	}
	if !retry.Replayed {
		t.Fatal("retry with same idempotent key must be a replay")
	}
	final, _ := r.Get(a.ID)
	if final.Balance != 700 {
		t.Fatalf("balance must be deducted exactly once, got %d want 700", final.Balance)
	}
	t.Logf("判定依据: 实际扣减=1 回放/冲突=1，读重试 Replayed=true，最终余额=%d（只扣一次）", final.Balance)
}

// TestDebit_ConcurrentDifferentAccounts 验证不同账户的并发扣减互不阻塞：
// 行级依赖跟踪下全部成功，不允许表级误冲突。
func TestDebit_ConcurrentDifferentAccounts(t *testing.T) {
	_, r := bootRepo(t)
	const n = 20
	var ids []int64
	for i := 0; i < n; i++ {
		a, err := r.Create(fmt.Sprintf("acct-%d", i), 100)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, a.ID)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, n)
	for i, id := range ids {
		wg.Add(1)
		go func(idx int, accountID int64) {
			defer wg.Done()
			_, e := r.Debit(DebitRequest{
				AccountID: accountID, Amount: 10,
				IdempotentID: fmt.Sprintf("biz-%d", idx), Version: 1,
			})
			errCh <- e
		}(i, id)
	}
	wg.Wait()
	close(errCh)
	for e := range errCh {
		if e != nil {
			t.Fatalf("unrelated accounts must not conflict: %v", e)
		}
	}
	for _, id := range ids {
		a, _ := r.Get(id)
		if a.Balance != 90 {
			t.Fatalf("account %d balance=%d want 90", id, a.Balance)
		}
	}
	t.Logf("判定依据: %d 个不同账户并发扣减全部成功，余额均为 90，无表级误冲突", n)
}

// TestDebit_InsufficientBalance 验证余额不足以独立原因拒绝，且余额不变。
func TestDebit_InsufficientBalance(t *testing.T) {
	_, r := bootRepo(t)
	a, _ := r.Create("cash", 50)
	_, err := r.Debit(DebitRequest{AccountID: a.ID, Amount: 80, IdempotentID: "biz-low", Version: a.Version})
	if reasonOf(err) != derrors.ReasonInsufficientBalance {
		t.Fatalf("want INSUFFICIENT_BALANCE, got %v", err)
	}
	cur, _ := r.Get(a.ID)
	if cur.Balance != 50 || cur.Version != a.Version {
		t.Fatalf("rejected debit must not change account: balance=%d version=%d", cur.Balance, cur.Version)
	}
	t.Logf("input: balance=50 amount=80 => INSUFFICIENT_BALANCE; 判定依据: 余额与版本均未变")
}

// TestDebit_StaleVersionRejected 验证基于旧版本扣减被拒，余额不变。
func TestDebit_StaleVersionRejected(t *testing.T) {
	_, r := bootRepo(t)
	a, _ := r.Create("cash", 1000)
	_, _ = r.Update(UpdateAccountInput{ID: a.ID, Name: "cash", Status: "active", Tier: "gold", Version: a.Version})

	_, err := r.Debit(DebitRequest{AccountID: a.ID, Amount: 10, IdempotentID: "biz-stale", Version: a.Version})
	if reasonOf(err) != derrors.ReasonVersionConflict {
		t.Fatalf("want VERSION_CONFLICT, got %v", err)
	}
	cur, _ := r.Get(a.ID)
	if cur.Balance != 1000 {
		t.Fatalf("stale debit must not deduct, balance=%d", cur.Balance)
	}
	t.Logf("input: expected version=%d current=%d => VERSION_CONFLICT; 判定依据: 余额未扣", a.Version, cur.Version)
}

// TestDebit_KeyReusedWithDifferentParams 验证同键不同参数被明确拒绝。
func TestDebit_KeyReusedWithDifferentParams(t *testing.T) {
	_, r := bootRepo(t)
	a, _ := r.Create("cash", 1000)
	if _, err := r.Debit(DebitRequest{AccountID: a.ID, Amount: 10, IdempotentID: "k", Version: a.Version}); err != nil {
		t.Fatal(err)
	}
	cur, _ := r.Get(a.ID)
	_, err := r.Debit(DebitRequest{AccountID: a.ID, Amount: 20, IdempotentID: "k", Version: cur.Version})
	if reasonOf(err) != derrors.ReasonIdempotentReplay {
		t.Fatalf("want IDEMPOTENT_REPLAY for mismatched params, got %v", err)
	}
	final, _ := r.Get(a.ID)
	if final.Balance != 990 {
		t.Fatalf("mismatched reuse must not deduct, balance=%d", final.Balance)
	}
	t.Log("判定依据: 同键不同金额 => IDEMPOTENT_REPLAY 拒绝，余额保持 990")
}

// TestAccount_QueryEntryPointsConsistent 验证账户仓储三个入口结果一致。
func TestAccount_QueryEntryPointsConsistent(t *testing.T) {
	_, r := bootRepo(t)
	for i := 0; i < 5; i++ {
		if _, err := r.Create(fmt.Sprintf("acct-%d", i), 100); err != nil {
			t.Fatal(err)
		}
	}
	// 冻结其中两个
	all, _ := r.List(nil)
	for i, a := range all {
		if i < 2 {
			if _, err := r.Update(UpdateAccountInput{ID: a.ID, Name: a.Name, Status: "frozen", Tier: a.Tier, Version: a.Version}); err != nil {
				t.Fatal(err)
			}
		}
	}
	filters := []repo.Filter{{Field: "status", Value: "active"}}
	active, err := r.List(filters)
	if err != nil {
		t.Fatal(err)
	}
	drained, err := r.DrainByCursor(filters, 2)
	if err != nil {
		t.Fatal(err)
	}
	page1, err := r.ListPage(filters, repo.PageRequest{Page: 1, PageSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	page2, err := r.ListPage(filters, repo.PageRequest{Page: 2, PageSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("input: filter status=active => all=%d page1=%d page2=%d cursor=%d total=%d",
		len(active), len(page1.Items), len(page2.Items), len(drained), page1.Total)
	if len(active) != 3 || len(drained) != 3 || len(page1.Items)+len(page2.Items) != 3 || page1.Total != 3 {
		t.Fatal("entry points disagree on active accounts")
	}
	for i := range active {
		if active[i].ID != drained[i].ID {
			t.Fatalf("row %d diverges: %d vs %d", i, active[i].ID, drained[i].ID)
		}
	}
	t.Log("判定依据: 全量/偏移/游标三入口对 status=active 的结果逐行一致")
}
