package account

import (
	"errors"

	derrors "github.com/highcumontoa/resilient-data-access-layer-go/internal/errors"
	"github.com/highcumontoa/resilient-data-access-layer-go/internal/store"
)

// UpdateAccountInput 是基于版本的更新指令；Version 为调用方读到的版本。
type UpdateAccountInput struct {
	ID      int64
	Name    string
	Status  string
	Tier    string
	Version int64
}

// Update 基于版本更新。后写不覆盖前写：
//   - 记录不存在 -> RECORD_NOT_FOUND
//   - 版本与当前不一致 -> VERSION_CONFLICT（携带当前版本，调用方据此重读重试）
//   - 提交时与其他事务撞车（第一提交者胜出）-> VERSION_CONFLICT，可重试
//
// 成功返回更新后的聚合（版本已 +1）。
func (r *Repository) Update(in UpdateAccountInput) (*Account, error) {
	if in.Version < 1 {
		return nil, derrors.New(derrors.ReasonInvalidArgument,
			"optimistic update requires a positive expected version, got %d", in.Version)
	}
	if in.Name == "" {
		return nil, derrors.New(derrors.ReasonInvalidArgument, "account name must not be empty")
	}
	err := r.db.Exec(func(tx *store.Tx) error {
		if _, ok, err := tx.Get(TableAccounts, in.ID); err != nil {
			return err
		} else if !ok {
			return derrors.New(derrors.ReasonRecordNotFound, "account %d not found", in.ID)
		}
		affected, err := tx.Update(TableAccounts, in.ID, in.Version, store.Row{
			"name":   in.Name,
			"status": in.Status,
			"tier":   in.Tier,
		})
		if err != nil {
			return err
		}
		if affected == 0 {
			return derrors.New(derrors.ReasonVersionConflict,
				"account %d expected version %d was already updated (lost-update rejected)", in.ID, in.Version)
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, store.ErrConflict) {
			return nil, derrors.Wrap(derrors.ReasonVersionConflict, err,
				"account %d commit conflict under concurrency, caller may re-read and retry", in.ID)
		}
		if de, ok := err.(*derrors.DataError); ok {
			return nil, de
		}
		return nil, derrors.Wrap(derrors.ReasonUnknown, err, "update account %d failed", in.ID)
	}
	return r.Get(in.ID)
}

// DebitRequest 是一笔扣减请求。
type DebitRequest struct {
	AccountID    int64
	Amount       int64
	IdempotentID string // 同一业务请求重试时保持不变
	Version      int64  // 调用方读到的账户版本
}

// DebitResult 汇报扣减判定，便于调用方解释。
type DebitResult struct {
	Account  *Account
	Replayed bool // 命中幂等键：直接回放，未重复扣减
}

// Debit 在单一事务边界内完成“幂等检查 -> 余额/版本校验 -> 扣减 -> 登记”，
// 要么全部生效要么全部不生效，杜绝部分提交；同一 IdempotentID 不会二次扣减。
func (r *Repository) Debit(req DebitRequest) (*DebitResult, error) {
	if req.Amount <= 0 {
		return nil, derrors.New(derrors.ReasonInvalidArgument,
			"debit amount must be positive, got %d", req.Amount)
	}
	if req.Version < 1 {
		return nil, derrors.New(derrors.ReasonInvalidArgument,
			"debit requires a positive expected version, got %d", req.Version)
	}
	if req.IdempotentID == "" {
		return nil, derrors.New(derrors.ReasonInvalidArgument,
			"idempotent_id is required for debit")
	}

	var resultBalance int64
	var replayed bool
	err := r.db.Exec(func(tx *store.Tx) error {
		// 1) 幂等键先查：命中则整笔操作退化为只读回放，事务不做任何扣减。
		rec, found, err := tx.FindOne(TableIdempotency,
			[]store.Filter{{Field: "idempotent_id", Value: req.IdempotentID}})
		if err != nil {
			return err
		}
		if found {
			if rec["account_id"].(int64) != req.AccountID || rec["amount"].(int64) != req.Amount {
				// 同一键对应不同请求参数：明确拒绝，防止幂等键误用掩盖真实歧义。
				return derrors.New(derrors.ReasonIdempotentReplay,
					"idempotent_id %q was already used with a different account/amount", req.IdempotentID)
			}
			replayed = true
			resultBalance = rec["result_balance"].(int64)
			return nil
		}

		// 2) 读当前账户，登记读集：并发提交由引擎第一提交者胜出仲裁。
		row, ok, err := tx.Get(TableAccounts, req.AccountID)
		if err != nil {
			return err
		}
		if !ok {
			return derrors.New(derrors.ReasonRecordNotFound, "account %d not found", req.AccountID)
		}
		cur, err := mapRow(row)
		if err != nil {
			return err
		}
		if cur.Version != req.Version {
			return derrors.New(derrors.ReasonVersionConflict,
				"account %d expected version %d but current is %d", req.AccountID, req.Version, cur.Version)
		}
		if cur.Balance < req.Amount {
			return derrors.New(derrors.ReasonInsufficientBalance,
				"account %d balance %d is less than debit amount %d",
				req.AccountID, cur.Balance, req.Amount)
		}

		// 3) 带行版本条件扣减（SQL 语义类似 ... WHERE id=? AND version=?）。
		newBalance := cur.Balance - req.Amount
		affected, err := tx.Update(TableAccounts, req.AccountID, req.Version, store.Row{
			"balance": newBalance,
		})
		if err != nil {
			return err
		}
		if affected == 0 {
			return derrors.New(derrors.ReasonVersionConflict,
				"account %d version %d changed during debit", req.AccountID, req.Version)
		}

		// 4) 登记幂等键，与扣减同事务提交。唯一约束由首次插入 + 提交冲突共同保证。
		if _, err := tx.Insert(TableIdempotency, store.Row{
			"idempotent_id":  req.IdempotentID,
			"account_id":     req.AccountID,
			"amount":         req.Amount,
			"result_balance": newBalance,
		}); err != nil {
			return err
		}
		resultBalance = newBalance
		return nil
	})
	if err != nil {
		if errors.Is(err, store.ErrConflict) || errors.Is(err, store.ErrUniqueViolation) {
			reason := derrors.ReasonVersionConflict
			hint := "debit on account %d lost commit race, caller may re-read and retry"
			if errors.Is(err, store.ErrUniqueViolation) {
				reason = derrors.ReasonIdempotentReplay
				hint = "idempotent_id %q raced with an in-flight request; retry to replay"
				return nil, derrors.Wrap(reason, err, hint, req.IdempotentID)
			}
			return nil, derrors.Wrap(reason, err, hint, req.AccountID)
		}
		if de, ok := err.(*derrors.DataError); ok {
			return nil, de
		}
		return nil, derrors.Wrap(derrors.ReasonUnknown, err, "debit failed")
	}

	acc, err := r.Get(req.AccountID)
	if err != nil {
		return nil, err
	}
	// 回放时以登记的结果余额为准做解释（当前余额理论上等于该值）。
	if replayed {
		clone := *acc
		clone.Balance = resultBalance
		return &DebitResult{Account: &clone, Replayed: true}, nil
	}
	return &DebitResult{Account: acc, Replayed: false}, nil
}
