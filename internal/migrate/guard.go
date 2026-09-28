package migrate

import (
	"time"

	"github.com/highcumontoa/resilient-data-access-layer-go/internal/store"
)

func migrationClock() int64 { return time.Now().UnixNano() }

// authorizeDestructive 在版本事务内：
//  1. 打开引擎的破坏性 DDL 授权（否则 DropColumn/RenameColumn 直接报错回滚）；
//  2. 在审计表幂等登记一条授权记录（随版本事务一起提交/回滚）。
func authorizeDestructive(tx *store.Tx, m Migration) error {
	tx.AllowDestructive()
	rows, err := tx.List("schema_destructive_approvals", store.ScanOptions{
		Filters: []store.Filter{
			{Field: "schema_version", Value: m.Version},
			{Field: "scope", Value: m.Name},
		},
	})
	if err != nil {
		return err
	}
	if len(rows) > 0 {
		return nil
	}
	_, err = tx.Insert("schema_destructive_approvals", store.Row{
		"schema_version": m.Version,
		"scope":          m.Name,
	})
	return err
}
