// Package migrate 实现版本化、可重复执行、整批原子的表结构演进协议。
package migrate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/highcumontoa/resilient-data-access-layer-go/internal/engine"
)

// 迁移协议层面的可区分错误。
var (
	// ErrNoMigrations 未登记任何迁移。
	ErrNoMigrations = errors.New("migrate: no migrations registered")
	// ErrVersionOutOfOrder 版本号不连续或不严格升序。
	ErrVersionOutOfOrder = errors.New("migrate: versions must be strictly increasing and consecutive")
	// ErrDuplicateVersion 同一版本重复登记。
	ErrDuplicateVersion = errors.New("migrate: duplicate version")
	// ErrInvalidStep 步骤本身不合法（未知 Kind、参数缺失）。
	ErrInvalidStep = errors.New("migrate: invalid step")
	// ErrChecksumChanged 已应用版本的内容与登记内容不一致（脚本被事后修改）。
	ErrChecksumChanged = errors.New("migrate: applied migration checksum changed")
	// ErrMissingMigration 库里已应用的版本在登记列表中缺失。
	ErrMissingMigration = errors.New("migrate: applied version missing from registry")
	// ErrStepFailed 某个步骤执行失败（整批已回滚，错误里带版本与步骤）。
	ErrStepFailed = errors.New("migrate: step failed")
)

// Step 是单条结构/数据变更。DDL 步骤都走引擎的幂等接口，
// data 步骤必须由编写者保证幂等，这样同版本重复执行不改变已有数据。
type Step struct {
	// Kind: "create_table" | "add_column" | "rename_column" | "drop_column" | "data"
	Kind    string
	Table   string
	Schema  *engine.Schema // create_table
	Column  engine.Column  // add_column（必须带确定默认或可空）
	OldName string         // rename_column
	NewName string         // rename_column
	// Data 用于幂等的数据回填；返回 error 会导致整批迁移中止回滚。
	Data func(ctx context.Context, tx *engine.Tx) error
	// FailForTest 为 true 时该步骤固定返回错误，仅供测试回滚路径。
	FailForTest bool
}

// Migration 是一个版本：严格升序、号段连续、注册后校验和固定。
type Migration struct {
	Version int
	Name    string
	Steps   []Step
}

// AppliedMigration 记录已执行版本与校验和。
type AppliedMigration struct {
	Version  int    `json:"version"`
	Name     string `json:"name"`
	Checksum string `json:"checksum"`
}

const bookkeepingTable = "schema_migrations"

// Migrator 负责登记与执行迁移。
type Migrator struct {
	db  *engine.DB
	reg []Migration
}

// New 构造迁移器。
func New(db *engine.DB) *Migrator {
	return &Migrator{db: db}
}

// Register 登记一组迁移（登记时即做静态校验：版本唯一、严格升序、号段连续）。
func (m *Migrator) Register(migs ...Migration) error {
	reg := append(append([]Migration(nil), m.reg...), migs...)
	sort.SliceStable(reg, func(i, j int) bool { return reg[i].Version < reg[j].Version })
	for i, mig := range reg {
		if mig.Version <= 0 {
			return fmt.Errorf("%w: version must be positive, got %d", ErrInvalidStep, mig.Version)
		}
		if i > 0 {
			if reg[i-1].Version == mig.Version {
				return fmt.Errorf("%w: version %d", ErrDuplicateVersion, mig.Version)
			}
			if reg[i-1].Version+1 != mig.Version {
				return fmt.Errorf("%w: versions %d -> %d are not consecutive",
					ErrVersionOutOfOrder, reg[i-1].Version, mig.Version)
			}
		}
		for si, st := range mig.Steps {
			if err := validateStep(st); err != nil {
				return fmt.Errorf("version %d step #%d: %w", mig.Version, si, err)
			}
		}
	}
	m.reg = reg
	return nil
}

func validateStep(st Step) error {
	switch st.Kind {
	case "create_table":
		if st.Schema == nil {
			return fmt.Errorf("%w: create_table requires Schema", ErrInvalidStep)
		}
	case "add_column":
		if st.Table == "" {
			return fmt.Errorf("%w: add_column requires Table", ErrInvalidStep)
		}
		if st.Column.Name == "" {
			return fmt.Errorf("%w: add_column requires Column.Name", ErrInvalidStep)
		}
		if !st.Column.HasDefault && !st.Column.Nullable {
			return fmt.Errorf("%w: add_column %q needs a default or nullable=true (deterministic fill semantics)",
				ErrInvalidStep, st.Column.Name)
		}
	case "rename_column":
		if st.Table == "" || st.OldName == "" || st.NewName == "" {
			return fmt.Errorf("%w: rename_column requires Table/OldName/NewName", ErrInvalidStep)
		}
	case "drop_column":
		if st.Table == "" || st.OldName == "" {
			return fmt.Errorf("%w: drop_column requires Table and OldName", ErrInvalidStep)
		}
	case "data":
		if st.Data == nil && !st.FailForTest {
			return fmt.Errorf("%w: data step requires Data func", ErrInvalidStep)
		}
	default:
		return fmt.Errorf("%w: unknown kind %q", ErrInvalidStep, st.Kind)
	}
	return nil
}

// checksum 对单个迁移的“结构内容”做稳定哈希。
// Data 是函数字节不可比较，刻意不纳入校验和（回填逻辑不改变结构契约）；
// 所有 DDL 参数都纳入，事后改脚本必然改变校验和而被拒绝。
func checksum(mig Migration) string {
	type wireStep struct {
		Kind     string         `json:"kind"`
		Table    string         `json:"table,omitempty"`
		Schema   *engine.Schema `json:"schema,omitempty"`
		Column   engine.Column  `json:"column,omitempty"`
		OldName  string         `json:"old_name,omitempty"`
		NewName  string         `json:"new_name,omitempty"`
		HasData  bool           `json:"has_data"`
		FailTest bool           `json:"fail_for_test,omitempty"`
	}
	ws := make([]wireStep, 0, len(mig.Steps))
	for _, st := range mig.Steps {
		ws = append(ws, wireStep{
			Kind: st.Kind, Table: st.Table, Schema: st.Schema, Column: st.Column,
			OldName: st.OldName, NewName: st.NewName, HasData: st.Data != nil, FailTest: st.FailForTest,
		})
	}
	payload := struct {
		Version int        `json:"version"`
		Name    string     `json:"name"`
		Steps   []wireStep `json:"steps"`
	}{mig.Version, mig.Name, ws}
	raw, _ := json.Marshal(payload)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:16])
}

// ensureBookkeeping 幂等创建版本记录表（带 version 列仅为兼容实体约定，
// 该表由迁移器直接管理，不经过乐观锁流程）。
func (m *Migrator) ensureBookkeeping(tx *engine.Tx) error {
	s, err := engine.NewSchema(bookkeepingTable, []engine.Column{
		{Name: "version_num", Type: engine.KindInt},
		{Name: "name", Type: engine.KindString},
		{Name: "checksum", Type: engine.KindString},
	})
	if err != nil {
		return err
	}
	return tx.CreateTable(s)
}

// Up 在同一个写事务里执行所有未应用版本：
//   - 任一步骤失败 -> 整批回滚，不留下半套结构；
//   - 重复执行已应用版本 -> 不改变数据（幂等）；
//   - 已应用版本校验和变化 -> ErrChecksumChanged 拒绝。
func (m *Migrator) Up(ctx context.Context) (applied []AppliedMigration, err error) {
	if len(m.reg) == 0 {
		return nil, ErrNoMigrations
	}

	// 先在只读事务里读取已应用版本并对账，决定待执行列表。
	pending := make([]Migration, 0, len(m.reg))
	appliedByVersion := map[int]AppliedMigration{}
	readErr := m.db.View(ctx, func(tx *engine.Tx) error {
		if _, nfErr := tx.TableSchema(bookkeepingTable); nfErr != nil {
			return nil // 全新库：全部待执行
		}
		page, err := tx.List(bookkeepingTable, nil, engine.PageRequest{Limit: 10000})
		if err != nil {
			return err
		}
		for _, row := range page.Rows {
			v, _ := row.GetByIndex(1)
			n, _ := row.GetByIndex(2)
			c, _ := row.GetByIndex(3)
			vn, _ := v.AsInt()
			nm, _ := n.AsString()
			cs, _ := c.AsString()
			am := AppliedMigration{Version: int(vn), Name: nm, Checksum: cs}
			appliedByVersion[am.Version] = am
		}
		return nil
	})
	if readErr != nil {
		return nil, readErr
	}

	for _, mig := range m.reg {
		if am, ok := appliedByVersion[mig.Version]; ok {
			// 已应用版本：校验和必须与登记一致，否则拒绝继续（防止脚本被事后修改）。
			if want := checksum(mig); am.Checksum != want {
				return nil, fmt.Errorf("%w: version %d stored=%s registered=%s",
					ErrChecksumChanged, mig.Version, am.Checksum, want)
			}
			continue
		}
		pending = append(pending, mig)
	}
	// 库里有登记列表中不存在的版本：拒绝，避免在不认识的历史上继续演进。
	for v := range appliedByVersion {
		found := false
		for _, mig := range m.reg {
			if mig.Version == v {
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("%w: version %d", ErrMissingMigration, v)
		}
	}
	if len(pending) == 0 {
		// 全部已应用：重复 Up 到此直接返回，不触碰业务数据。
		return nil, nil
	}

	// 所有待执行版本放进同一个写事务：一损全损，一成全成。
	txErr := m.db.Update(ctx, func(tx *engine.Tx) error {
		if err := m.ensureBookkeeping(tx); err != nil {
			return err
		}
		// 进入写事务后再次复核记录表（防止外部并发迁移导致重复应用）。
		page, err := tx.List(bookkeepingTable, nil, engine.PageRequest{Limit: 10000})
		if err != nil {
			return err
		}
		exists := map[int]bool{}
		for _, row := range page.Rows {
			v, _ := row.GetByIndex(1)
			vn, _ := v.AsInt()
			exists[int(vn)] = true
		}
		for _, mig := range pending {
			if exists[mig.Version] {
				// 已被并发迁移应用：跳过（DDL 本身也幂等，双保险）。
				continue
			}
			for si, st := range mig.Steps {
				if st.FailForTest {
					return fmt.Errorf("%w: version %d step #%d kind=%s: forced failure (rollback entire batch)",
						ErrStepFailed, mig.Version, si, st.Kind)
				}
				if err := runStep(ctx, tx, st); err != nil {
					return fmt.Errorf("%w: version %d step #%d kind=%s: %v",
						ErrStepFailed, mig.Version, si, st.Kind, err)
				}
			}
			cs := checksum(mig)
			if _, err := tx.Insert(bookkeepingTable, map[string]engine.Value{
				"version_num": engine.NewInt(int64(mig.Version)),
				"name":        engine.NewString(mig.Name),
				"checksum":    engine.NewString(cs),
			}); err != nil {
				return fmt.Errorf("%w: record version %d: %v", ErrStepFailed, mig.Version, err)
			}
			applied = append(applied, AppliedMigration{Version: mig.Version, Name: mig.Name, Checksum: cs})
		}
		return nil
	})
	if txErr != nil {
		return nil, txErr
	}
	sort.Slice(applied, func(i, j int) bool { return applied[i].Version < applied[j].Version })
	return applied, nil
}

func runStep(ctx context.Context, tx *engine.Tx, st Step) error {
	switch st.Kind {
	case "create_table":
		return tx.CreateTable(st.Schema)
	case "add_column":
		return tx.AddColumn(st.Table, st.Column)
	case "rename_column":
		return tx.RenameColumn(st.Table, st.OldName, st.NewName)
	case "drop_column":
		return tx.DropColumn(st.Table, st.OldName)
	case "data":
		if st.Data != nil {
			return st.Data(ctx, tx)
		}
		return nil
	default:
		return fmt.Errorf("%w: unknown kind %q", ErrInvalidStep, st.Kind)
	}
}

// Applied 返回当前已应用版本（按版本升序）。
func (m *Migrator) Applied(ctx context.Context) ([]AppliedMigration, error) {
	var out []AppliedMigration
	err := m.db.View(ctx, func(tx *engine.Tx) error {
		if _, nfErr := tx.TableSchema(bookkeepingTable); nfErr != nil {
			return nil
		}
		page, err := tx.List(bookkeepingTable, nil, engine.PageRequest{Limit: 10000})
		if err != nil {
			return err
		}
		for _, row := range page.Rows {
			v, _ := row.GetByIndex(1)
			n, _ := row.GetByIndex(2)
			c, _ := row.GetByIndex(3)
			vn, _ := v.AsInt()
			nm, _ := n.AsString()
			cs, _ := c.AsString()
			out = append(out, AppliedMigration{Version: int(vn), Name: nm, Checksum: cs})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	return out, nil
}

func (m *Migrator) verifyChecksums(applied map[int]AppliedMigration) error {
	registered := map[int]Migration{}
	for _, mig := range m.reg {
		registered[mig.Version] = mig
	}
	for v, am := range applied {
		mig, ok := registered[v]
		if !ok {
			return fmt.Errorf("%w: version %d", ErrMissingMigration, v)
		}
		if want := checksum(mig); am.Checksum != want {
			return fmt.Errorf("%w: version %d stored=%s registered=%s", ErrChecksumChanged, v, am.Checksum, want)
		}
	}
	return nil
}
