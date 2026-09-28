package account

import (
	"github.com/highcumontoa/resilient-data-access-layer-go/internal/database"
	"github.com/highcumontoa/resilient-data-access-layer-go/internal/migrate"
	"github.com/highcumontoa/resilient-data-access-layer-go/internal/store"
)

// 表名常量，供领域仓储与迁移脚本共享，避免字符串漂移。
const (
	TableAccounts    = "accounts"
	TableIdempotency = "account_debit_keys"
)

// Migrations 返回账户域的全部版本脚本。
//
// 兼容规则（详见 docs/MIGRATION.md）：
//   - 只做扩展式变更：加表/加列必须带稳定默认值或允许空；
//   - 脚本可重复执行：所有结构操作先判断存在性（IF-NOT-EXISTS 语义）；
//   - 不删列、不改名；读取方按列名投影，加列不会造成位置错位。
func Migrations() []migrate.Migration {
	return []migrate.Migration{
		{
			Version: 1,
			Name:    "create_accounts",
			Up: func(tx *store.Tx) error {
				if tx.HasTable(TableAccounts) {
					return nil // 幂等：重复执行同一版本不再创建
				}
				return tx.CreateTable(TableAccounts, []store.Column{
					{Name: "name", Type: store.TypeText, Nullable: false},
					{Name: "balance", Type: store.TypeInteger, Nullable: false, HasDefault: true, Default: int64(0)},
					// status 在 v1 即存在，避免“后加状态列”对旧读取方造成歧义
					{Name: "status", Type: store.TypeText, Nullable: false, HasDefault: true, Default: "active"},
				})
			},
		},
		{
			Version: 2,
			Name:    "add_account_tier",
			Up: func(tx *store.Tx) error {
				cols, err := tx.Columns(TableAccounts)
				if err != nil {
					return err
				}
				for _, c := range cols {
					if c.Name == "tier" {
						return nil // 幂等：列已存在则跳过
					}
				}
				// 确定且稳定的默认语义：既有账户与未来新账户都取 "standard"。
				return tx.AddColumn(TableAccounts, store.Column{
					Name: "tier", Type: store.TypeText, Nullable: false,
					HasDefault: true, Default: "standard",
				})
			},
		},
		{
			Version: 3,
			Name:    "create_idempotency_keys",
			Up: func(tx *store.Tx) error {
				if tx.HasTable(TableIdempotency) {
					return nil
				}
				return tx.CreateTable(TableIdempotency, []store.Column{
					{Name: "idempotent_id", Type: store.TypeText, Nullable: false, Unique: true},
					{Name: "account_id", Type: store.TypeInteger, Nullable: false},
					{Name: "amount", Type: store.TypeInteger, Nullable: false},
					{Name: "result_balance", Type: store.TypeInteger, Nullable: false},
				})
			},
		},
	}
}

// Bootstrap 创建数据库、完成迁移，返回可直接使用的账户仓储句柄。
func Bootstrap(db *database.DB) (*Repository, error) {
	runner := migrate.NewRunner(db)
	if _, err := runner.Migrate(Migrations(), 0); err != nil {
		return nil, err
	}
	return NewRepository(db), nil
}
