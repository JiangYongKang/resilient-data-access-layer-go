// Package domain 是示例业务域：账户与商品，
// 承载迁移演进、乐观并发与事务原子性的演示与验证。
package domain

import (
	"context"
	"fmt"

	"github.com/highcumontoa/resilient-data-access-layer-go/internal/engine"
	"github.com/highcumontoa/resilient-data-access-layer-go/internal/migrate"
	"github.com/highcumontoa/resilient-data-access-layer-go/internal/repo"
)

// Account 账户；balance 使用整数最小货币单位，避免浮点语义。
type Account struct {
	ID      int64
	Code    string
	Balance int64
	Note    string // V2 新增（可空）
	Version int64
}

func (a *Account) GetID() int64       { return a.ID }
func (a *Account) SetID(id int64)     { a.ID = id }
func (a *Account) GetVersion() int64  { return a.Version }
func (a *Account) SetVersion(v int64) { a.Version = v }

// Product 商品；列名 title 在 V3 更名为 name（槽位不动，读取不错位）。
type Product struct {
	ID      int64
	SKU     string
	Name    string
	Price   int64
	Status  *int64 // V2 新增，确定默认 1（创建时为 nil 表示交给列默认；读取永远非 nil）
	Version int64
}

func (p *Product) GetID() int64       { return p.ID }
func (p *Product) SetID(id int64)     { p.ID = id }
func (p *Product) GetVersion() int64  { return p.Version }
func (p *Product) SetVersion(v int64) { p.Version = v }

// AccountSchema 是账户表的最新结构。
func AccountSchema() *engine.Schema {
	s, _ := engine.NewSchema("accounts", []engine.Column{
		{Name: "code", Type: engine.KindString},
		{Name: "balance", Type: engine.KindInt},
		{Name: "note", Type: engine.KindString, Nullable: true},
		{Name: "version", Type: engine.KindInt},
	})
	return s
}

// ProductSchema 是商品表的最新结构。
func ProductSchema() *engine.Schema {
	s, _ := engine.NewSchema("products", []engine.Column{
		{Name: "sku", Type: engine.KindString},
		{Name: "name", Type: engine.KindString},
		{Name: "price", Type: engine.KindInt},
		{Name: "status", Type: engine.KindInt, HasDefault: true, Default: engine.NewInt(1)},
		{Name: "version", Type: engine.KindInt},
	})
	return s
}

// AccountCodec 返回账户仓储编解码（显式字段映射，未知列立即报错）。
func AccountCodec() repo.Codec[Account, *Account] {
	return repo.Codec[Account, *Account]{
		Table: AccountSchema,
		ToRow: func(a *Account) (map[string]engine.Value, error) {
			return map[string]engine.Value{
				"code":    engine.NewString(a.Code),
				"balance": engine.NewInt(a.Balance),
				"note":    engine.NewString(a.Note),
			}, nil
		},
		Scan: func(row engine.Row, s *engine.Schema, a *Account) error {
			if err := readString(row, s, "code", &a.Code); err != nil {
				return err
			}
			if err := readInt(row, s, "balance", &a.Balance); err != nil {
				return err
			}
			return readNullableString(row, s, "note", &a.Note)
		},
	}
}

// ProductCodec 返回商品仓储编解码。
func ProductCodec() repo.Codec[Product, *Product] {
	return repo.Codec[Product, *Product]{
		Table: ProductSchema,
		ToRow: func(p *Product) (map[string]engine.Value, error) {
			row := map[string]engine.Value{
				"sku":   engine.NewString(p.SKU),
				"name":  engine.NewString(p.Name),
				"price": engine.NewInt(p.Price),
			}
			// Status==nil 表示创建时不提供，由列默认 1 兜底；
			// 需要显式取值（如下架为 0）时用 productstatus.Status(0) 构造指针。
			if p.Status != nil {
				row["status"] = engine.NewInt(*p.Status)
			}
			return row, nil
		},
		Scan: func(row engine.Row, s *engine.Schema, p *Product) error {
			if err := readString(row, s, "sku", &p.SKU); err != nil {
				return err
			}
			if err := readString(row, s, "name", &p.Name); err != nil {
				return err
			}
			if err := readInt(row, s, "price", &p.Price); err != nil {
				return err
			}
			var status int64
			if err := readInt(row, s, "status", &status); err != nil {
				return err
			}
			p.Status = &status
			return nil
		},
	}
}

func readString(row engine.Row, s *engine.Schema, col string, dst *string) error {
	v, err := row.Get(s, col)
	if err != nil {
		return err
	}
	x, ok := v.AsString()
	if !ok {
		return fmt.Errorf("%w: column %q expects string", engine.ErrTypeMismatch, col)
	}
	*dst = x
	return nil
}

func readNullableString(row engine.Row, s *engine.Schema, col string, dst *string) error {
	v, err := row.Get(s, col)
	if err != nil {
		return err
	}
	if v.IsNull() {
		*dst = ""
		return nil
	}
	return readString(row, s, col, dst)
}

// Status 便于显式指定 status，例如 Status(0) 表示下架。
func Status(v int64) *int64 { return &v }

func readInt[T ~int64](row engine.Row, s *engine.Schema, col string, dst *T) error {
	v, err := row.Get(s, col)
	if err != nil {
		return err
	}
	x, ok := v.AsInt()
	if !ok {
		return fmt.Errorf("%w: column %q expects int", engine.ErrTypeMismatch, col)
	}
	*dst = T(x)
	return nil
}

// Migrations 返回示例迁移序列（版本必须连续升序）。
func Migrations() []migrate.Migration {
	v1Accounts, _ := engine.NewSchema("accounts", []engine.Column{
		{Name: "code", Type: engine.KindString},
		{Name: "balance", Type: engine.KindInt},
		{Name: "version", Type: engine.KindInt},
	})
	v1Products, _ := engine.NewSchema("products", []engine.Column{
		{Name: "sku", Type: engine.KindString},
		{Name: "title", Type: engine.KindString},
		{Name: "price", Type: engine.KindInt},
		{Name: "version", Type: engine.KindInt},
	})
	return []migrate.Migration{
		{
			Version: 1,
			Name:    "init",
			Steps: []migrate.Step{
				{Kind: "create_table", Schema: v1Accounts},
				{Kind: "create_table", Schema: v1Products},
				// 幂等数据回填：系统账户只在缺失时插入，重复迁移不会产生重复行。
				{Kind: "data", Data: seedSystemAccount},
			},
		},
		{
			Version: 2,
			Name:    "note_and_status",
			Steps: []migrate.Step{
				{Kind: "add_column", Table: "accounts", Column: engine.Column{Name: "note", Type: engine.KindString, Nullable: true}},
				{Kind: "add_column", Table: "products", Column: engine.Column{Name: "status", Type: engine.KindInt, HasDefault: true, Default: engine.NewInt(1)}},
			},
		},
		{
			Version: 3,
			Name:    "rename_product_title",
			Steps: []migrate.Step{
				{Kind: "rename_column", Table: "products", OldName: "title", NewName: "name"},
			},
		},
	}
}

// seedSystemAccount 演示幂等数据步骤：存在则不动，不存在才插入。
func seedSystemAccount(ctx context.Context, tx *engine.Tx) error {
	page, err := tx.List("accounts", []engine.Filter{
		{Field: "code", Op: engine.OpEq, Value: engine.NewString("system")},
	}, engine.PageRequest{Limit: 1})
	if err != nil {
		return err
	}
	if len(page.Rows) > 0 {
		return nil
	}
	_, err = tx.Insert("accounts", map[string]engine.Value{
		"code":    engine.NewString("system"),
		"balance": engine.NewInt(0),
	})
	return err
}
