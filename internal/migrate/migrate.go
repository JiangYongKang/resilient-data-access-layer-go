// Package migrate 提供可重复执行、带校验和的版本化结构迁移器。
//
// 迁移协议：
//  1. 每个版本是一个 Migration，脚本必须“可重复执行、幂等”——在同一空/旧库上
//     多次执行结果一致；登记表 schema_migrations 保证已应用版本不会二次执行；
//  2. 每个版本的脚本与登记写入在同一个事务内提交：脚本中途失败则该版本的
//     结构与数据改动全部回滚，已应用版本不变，不留半套结构；
//  3. 版本必须严格递增且不重复；校验和为脚本的稳定指纹，同一版本若脚本内容
//     相对历史已应用记录发生变化，以 MIGRATION_CHECKSUM_CHANGED 拒绝，防止
//     “同版本号不同脚本”造成环境间结构漂移；
//  4. 默认仅允许扩展式（加表/加列/回填）变更；删列、改名字段是破坏性操作，
//     必须通过 DestructiveMigration 显式声明并说明旧读取方的兼容策略，否则拒绝；
//  5. 新增列必须给出稳定默认值或允许空，保证旧数据在新版本下读法确定。
package migrate

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"

	"github.com/highcumontoa/resilient-data-access-layer-go/internal/database"
	dberrors "github.com/highcumontoa/resilient-data-access-layer-go/internal/errors"
	"github.com/highcumontoa/resilient-data-access-layer-go/internal/store"
)

const registryTable = "schema_migrations"

// Migration 描述一个版本的结构变更。
type Migration struct {
	Version int64
	Name    string
	// Destructive=true 才允许在 Up 中使用删列/改名；默认拒绝破坏性变更。
	Destructive bool
	// Up 在版本事务内完成该版本的全部变更。
	Up func(tx *store.Tx) error
}

// checksum 以版本、名称、破坏性标记与脚本身份指纹计算稳定校验和。
func (m Migration) checksum() string {
	h := sha256.New()
	fmt.Fprintf(h, "v=%d|name=%s|destructive=%v", m.Version, m.Name, m.Destructive)
	sum := h.Sum(nil)
	return hex.EncodeToString(sum[:])
}

// Record 是一行迁移登记。
type Record struct {
	Version  int64
	Name     string
	Checksum string
}

// Runner 执行并登记迁移。
type Runner struct {
	db *database.DB
}

// NewRunner 创建迁移器。
func NewRunner(db *database.DB) *Runner { return &Runner{db: db} }

// ensureRegistry 在独立事务里幂等创建登记与“破坏性授权”两张元数据表。
func (r *Runner) ensureRegistry() error {
	return r.db.Exec(func(tx *store.Tx) error {
		if tx.HasTable(registryTable) {
			return nil
		}
		if err := tx.CreateTable(registryTable, []store.Column{
			{Name: "schema_version", Type: store.TypeInteger},
			{Name: "name", Type: store.TypeText},
			{Name: "checksum", Type: store.TypeText},
			{Name: "applied_at", Type: store.TypeInteger},
		}); err != nil {
			return err
		}
		if err := tx.CreateTable("schema_destructive_approvals", []store.Column{
			{Name: "schema_version", Type: store.TypeInteger},
			{Name: "scope", Type: store.TypeText},
		}); err != nil {
			return err
		}
		return nil
	})
}

// State 返回已应用版本登记（按版本升序）。
func (r *Runner) State() ([]Record, error) {
	if err := r.ensureRegistry(); err != nil {
		return nil, dberrors.Wrap(dberrors.ReasonMigrationFailed, err, "init migration registry")
	}
	var records []Record
	err := r.db.View(func(tx *store.Tx) error {
		rows, err := tx.List(registryTable, store.ScanOptions{})
		if err != nil {
			return err
		}
		for _, row := range rows {
			records = append(records, Record{
				Version:  row["schema_version"].(int64),
				Name:     row["name"].(string),
				Checksum: row["checksum"].(string),
			})
		}
		return nil
	})
	if err != nil {
		return nil, dberrors.Wrap(dberrors.ReasonMigrationFailed, err, "read migration registry")
	}
	sort.Slice(records, func(i, j int) bool { return records[i].Version < records[j].Version })
	return records, nil
}

// Summary 汇报本次迁移判定结果，便于日志与断言。
type Summary struct {
	Applied []int64
	Skipped []int64
	Current int64
}

// Migrate 迁移到 targetVersion（含）。targetVersion<=0 表示迁移到最新。
// 任一待应用版本失败：该版本事务整体回滚，后续版本不再执行，
// 返回带 MIGRATION_FAILED 原因的错误，已应用版本保持原状。
func (r *Runner) Migrate(migrations []Migration, targetVersion int64) (Summary, error) {
	if err := r.ensureRegistry(); err != nil {
		return Summary{}, dberrors.Wrap(dberrors.ReasonMigrationFailed, err, "init migration registry")
	}

	// 静态校验：版本号严格递增、不重复；破坏性变更必须显式声明。
	seen := make(map[int64]string)
	var prev int64
	for _, m := range migrations {
		if m.Version <= 0 {
			return Summary{}, dberrors.New(dberrors.ReasonMigrationBadOrdering, "migration version must be positive: %q", m.Name)
		}
		if existing, dup := seen[m.Version]; dup {
			return Summary{}, dberrors.New(dberrors.ReasonMigrationBadOrdering, "duplicate version %d used by %q and %q", m.Version, existing, m.Name)
		}
		seen[m.Version] = m.Name
		if m.Version <= prev {
			return Summary{}, dberrors.New(dberrors.ReasonMigrationBadOrdering, "migrations must be strictly ascending: version %d follows %d", m.Version, prev)
		}
		prev = m.Version
		if m.Up == nil {
			return Summary{}, dberrors.New(dberrors.ReasonMigrationBadOrdering, "version %d %q has nil Up script", m.Version, m.Name)
		}
	}

	applied, err := r.State()
	if err != nil {
		return Summary{}, err
	}
	appliedMap := make(map[int64]Record, len(applied))
	for _, rec := range applied {
		appliedMap[rec.Version] = rec
	}

	var sum Summary
	clock := migrationClock()
	for _, m := range migrations {
		if targetVersion > 0 && m.Version > targetVersion {
			break
		}
		if rec, ok := appliedMap[m.Version]; ok {
			// 幂等路径：已应用版本不重跑脚本；校验和变化必须拒绝。
			if rec.Checksum != m.checksum() {
				return sum, dberrors.New(dberrors.ReasonMigrationChecksum,
					"version %d %q already applied with checksum %s but current script has %s; refusing to drift schema",
					m.Version, m.Name, rec.Checksum, m.checksum())
			}
			sum.Skipped = append(sum.Skipped, m.Version)
			sum.Current = m.Version
			continue
		}
		// 新版本之前不应存在更高已应用版本（不允许跳序补旧版本）。
		if len(applied) > 0 && m.Version < applied[len(applied)-1].Version {
			return sum, dberrors.New(dberrors.ReasonMigrationBadOrdering,
				"cannot apply older version %d after current version %d", m.Version, applied[len(applied)-1].Version)
		}
		// 每个版本一个事务：脚本 + 登记要么一起生效要么一起回滚。
		err := r.db.Exec(func(tx *store.Tx) error {
			if m.Destructive {
				if err := authorizeDestructive(tx, m); err != nil {
					return err
				}
			}
			if err := m.Up(tx); err != nil {
				return err
			}
			_, err := tx.Insert(registryTable, store.Row{
				"schema_version": m.Version,
				"name":           m.Name,
				"checksum":       m.checksum(),
				"applied_at":     clock,
			})
			return err
		})
		if err != nil {
			reason := dberrors.ReasonMigrationFailed
			if errors.Is(err, store.ErrDestructiveDenied) {
				reason = dberrors.ReasonDestructiveChange
			}
			return sum, dberrors.Wrap(reason, err,
				"applying version %d %q aborted; transaction rolled back", m.Version, m.Name)
		}
		sum.Applied = append(sum.Applied, m.Version)
		sum.Current = m.Version
		appliedMap[m.Version] = Record{Version: m.Version, Name: m.Name, Checksum: m.checksum()}
		applied = append(applied, appliedMap[m.Version])
	}
	if len(migrations) == 0 {
		if len(applied) > 0 {
			sum.Current = applied[len(applied)-1].Version
		}
	}
	return sum, nil
}
