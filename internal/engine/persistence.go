package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// snapshotVersion 是落盘格式版本（结构演进只动业务表，文件格式另立版本号）。
const snapshotVersion = 1

// snapshot 是整库的持久化快照：一次性整体写入、整体替换，
// 不会出现跨文件的部分提交状态。
type snapshot struct {
	FormatVersion int                  `json:"format_version"`
	Tables        map[string]tableWire `json:"tables"`
}

type tableWire struct {
	Schema *Schema   `json:"schema"`
	Seq    int64     `json:"seq"`
	Rows   [][]Value `json:"rows"` // 每行是按槽位排列的值数组（首元素为 id）
}

func (db *DB) loadLocked() error {
	if db.path == "" || db.path == ":memory:" {
		db.tables = map[string]*Table{}
		return nil
	}
	data, err := os.ReadFile(db.path)
	if os.IsNotExist(err) {
		db.tables = map[string]*Table{}
		return nil
	}
	if err != nil {
		return fmt.Errorf("engine: read snapshot: %w", err)
	}
	if len(data) == 0 {
		db.tables = map[string]*Table{}
		return nil
	}
	var snap snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return fmt.Errorf("engine: corrupt snapshot: %w", err)
	}
	if snap.FormatVersion != snapshotVersion {
		return fmt.Errorf("engine: unsupported snapshot format version %d", snap.FormatVersion)
	}
	db.tables = make(map[string]*Table, len(snap.Tables))
	for name, tw := range snap.Tables {
		if tw.Schema == nil {
			return fmt.Errorf("engine: corrupt snapshot: table %q missing schema", name)
		}
		if err := tw.Schema.Validate(); err != nil {
			return fmt.Errorf("engine: corrupt snapshot table %q: %w", name, err)
		}
		t := &Table{schema: tw.Schema, seq: tw.Seq, rows: map[int64]Row{}}
		for _, cells := range tw.Rows {
			if len(cells) != len(tw.Schema.Columns)+1 {
				return fmt.Errorf("engine: corrupt snapshot: row width %d != schema width %d in table %q (schema change without migration?)",
					len(cells), len(tw.Schema.Columns)+1, name)
			}
			id, ok := cells[0].AsInt()
			if !ok {
				return fmt.Errorf("engine: corrupt snapshot: non-int id in table %q", name)
			}
			t.rows[id] = newRow(cells)
		}
		db.tables[name] = t
	}
	return nil
}

func (db *DB) toSnapshotLocked() snapshot {
	snap := snapshot{FormatVersion: snapshotVersion, Tables: map[string]tableWire{}}
	for name, t := range db.tables {
		tw := tableWire{Schema: t.schema.Clone(), Seq: t.seq, Rows: make([][]Value, 0, len(t.rows))}
		ids := make([]int64, 0, len(t.rows))
		for id := range t.rows {
			ids = append(ids, id)
		}
		// 稳定顺序，方便重复迁移时文件内容可复现（幂等的外部表现之一）。
		sortIDs(ids)
		for _, id := range ids {
			tw.Rows = append(tw.Rows, t.rows[id].cellsCopy())
		}
		snap.Tables[name] = tw
	}
	return snap
}

// persistLocked 原子落盘：写临时文件 -> Sync -> rename 替换。
// 任何一步失败都保留上一份完整快照，不会留下半截数据文件。
func (db *DB) persistLocked() error {
	if db.path == "" || db.path == ":memory:" {
		return nil
	}
	snap := db.toSnapshotLocked()
	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return fmt.Errorf("engine: marshal snapshot: %w", err)
	}
	dir := filepath.Dir(db.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("engine: ensure data dir: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".snap-*.tmp")
	if err != nil {
		return fmt.Errorf("engine: create temp file: %w", err)
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("engine: write snapshot: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("engine: sync snapshot: %w", err)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return fmt.Errorf("engine: close snapshot: %w", err)
	}
	if err := os.Rename(tmpName, db.path); err != nil {
		cleanup()
		return fmt.Errorf("engine: commit snapshot: %w", err)
	}
	// 尽力 fsync 目录，确保 rename 在崩溃后仍可见；失败不影响已替换的数据文件。
	if d, err := os.Open(dir); err == nil {
		_ = syncDir(d)
		_ = d.Close()
	}
	return nil
}

func sortIDs(ids []int64) {
	// 避免引入 slices 包（保持 stdlib 依赖最小）。
	for i := 1; i < len(ids); i++ {
		for j := i; j > 0 && ids[j-1] > ids[j]; j-- {
			ids[j-1], ids[j] = ids[j], ids[j-1]
		}
	}
}
