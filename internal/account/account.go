// Package account 是账户领域：余额扣减的乐观并发与幂等控制。
package account

import (
	"fmt"

	"github.com/highcumontoa/resilient-data-access-layer-go/internal/database"
	derrors "github.com/highcumontoa/resilient-data-access-layer-go/internal/errors"
	"github.com/highcumontoa/resilient-data-access-layer-go/internal/repo"
	"github.com/highcumontoa/resilient-data-access-layer-go/internal/store"
)

// Account 是账户聚合。Version 为乐观锁版本，每次成功更新 +1。
type Account struct {
	ID      int64  `json:"id"`
	Name    string `json:"name"`
	Status  string `json:"status"`
	Tier    string `json:"tier"`
	Balance int64  `json:"balance"`
	Version int64  `json:"version"`
}

// Repository 提供账户读写与领域操作；读路径复用通用仓储保证入口一致。
type Repository struct {
	db   *database.DB
	list *repo.Repository[Account]
}

// NewRepository 创建账户仓储（调用前应已完成迁移）。
func NewRepository(db *database.DB) *Repository {
	return &Repository{
		db:   db,
		list: repo.NewRepository[Account](db, TableAccounts, mapRow),
	}
}

// Create 开户：余额不得为负。
func (r *Repository) Create(name string, initialBalance int64) (*Account, error) {
	if name == "" {
		return nil, derrors.New(derrors.ReasonInvalidArgument, "account name must not be empty")
	}
	if initialBalance < 0 {
		return nil, derrors.New(derrors.ReasonInvalidArgument, "initial balance must not be negative, got %d", initialBalance)
	}
	var id int64
	err := r.db.Exec(func(tx *store.Tx) error {
		var e error
		id, e = tx.Insert(TableAccounts, store.Row{
			"name":    name,
			"balance": initialBalance,
			"status":  "active",
			"tier":    "standard",
		})
		return e
	})
	if err != nil {
		return nil, derrors.Wrap(derrors.ReasonUnknown, err, "create account failed")
	}
	return r.Get(id)
}

// Get 按主键读取；不存在返回 RECORD_NOT_FOUND。
func (r *Repository) Get(id int64) (*Account, error) {
	var out *Account
	err := r.db.View(func(tx *store.Tx) error {
		row, ok, err := tx.Get(TableAccounts, id)
		if err != nil {
			return err
		}
		if !ok {
			return derrors.New(derrors.ReasonRecordNotFound, "account %d not found", id)
		}
		a, err := mapRow(row)
		if err != nil {
			return err
		}
		out = &a
		return nil
	})
	if err != nil {
		if de, ok := err.(*derrors.DataError); ok {
			return nil, de
		}
		return nil, derrors.Wrap(derrors.ReasonUnknown, err, "get account %d failed", id)
	}
	return out, nil
}

// List / ListPage / ListCursor 直接暴露统一仓储的三个入口，过滤与分页语义一致。
func (r *Repository) List(filters []repo.Filter) ([]Account, error) {
	return r.list.FindAll(filters)
}

func (r *Repository) ListPage(filters []repo.Filter, page repo.PageRequest) (repo.PageResult[Account], error) {
	return r.list.FindByPage(filters, page)
}

func (r *Repository) ListCursor(filters []repo.Filter, cur repo.CursorRequest) (repo.CursorResult[Account], error) {
	return r.list.FindByCursor(filters, cur)
}

// DrainByCursor 翻完游标分页，供跨入口对账。
func (r *Repository) DrainByCursor(filters []repo.Filter, pageSize int) ([]Account, error) {
	return r.list.DrainByCursor(filters, pageSize)
}

// mapRow 按列名投影，结构演进新增列不会导致位置错位；
// 兼容旧行缺少后加列的情况（v2 前不会出现，但读取保持防御式）。
func mapRow(row store.Row) (Account, error) {
	getText := func(key string) (string, error) {
		v, ok := row[key]
		if !ok || v == nil {
			return "", nil
		}
		s, ok := v.(string)
		if !ok {
			return "", fmt.Errorf("%w: column %q expects string, got %T", store.ErrTypeMismatch, key, v)
		}
		return s, nil
	}
	getInt := func(key string) (int64, error) {
		v, ok := row[key]
		if !ok || v == nil {
			return 0, nil
		}
		n, ok := v.(int64)
		if !ok {
			return 0, fmt.Errorf("%w: column %q expects int64, got %T", store.ErrTypeMismatch, key, v)
		}
		return n, nil
	}
	id, err := getInt(store.SystemColumnID)
	if err != nil {
		return Account{}, err
	}
	ver, err := getInt(store.SystemColumnVersion)
	if err != nil {
		return Account{}, err
	}
	balance, err := getInt("balance")
	if err != nil {
		return Account{}, err
	}
	name, err := getText("name")
	if err != nil {
		return Account{}, err
	}
	status, err := getText("status")
	if err != nil {
		return Account{}, err
	}
	tier, err := getText("tier")
	if err != nil {
		return Account{}, err
	}
	if tier == "" {
		tier = "standard" // 稳定默认语义与迁移 AddColumn 保持一致
	}
	if status == "" {
		status = "active"
	}
	return Account{
		ID:      id,
		Name:    name,
		Status:  status,
		Tier:    tier,
		Balance: balance,
		Version: ver,
	}, nil
}
