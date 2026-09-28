# Resilient Data Access Layer（Go，仅标准库）

一套可在本地直接运行的数据访问与结构变更底座，目标是让**仓储读写、表结构演进、并发更新**在不同入口下给出一致、可解释的结果。

- 无任何第三方依赖（离线环境可构建）：内嵌本地存储引擎，JSON 整体快照 + 原子 rename 落盘。
- 统一查询计划：条件过滤 + 两种分页（offset 翻页 / cursor 游标），不同入口结果逐行一致。
- 版本化迁移：可重复执行、幂等、整批原子（失败回到迁移前状态）。
- 乐观并发：行级 `version` 列检测后写覆盖前写；事务全局串行化，写事务天然可串行化。

## 目录结构

```
internal/engine   本地存储引擎：强类型值、位置化行存储、事务、DDL、过滤/分页、原子落盘
internal/migrate  迁移器：版本登记、校验和、整批事务执行
internal/repo     泛型仓储：统一查询入口、乐观更新、事务边界、冲突重试
internal/domain   示例业务域（账户/商品）与三版本迁移样例
cmd/demo          本地端到端演示
```

## 快速验证

```bash
go build ./...
go vet ./...
go test ./... -count=1 -v      # 查看每个用例打印的“输入与判定依据”
go test -race ./... -count=1   # 并发场景竞态检查
go run ./cmd/demo              # 端到端演示（内存库）
```

`go run ./cmd/demo` 的输出依次展示：迁移幂等、双模式分页逐行相等、非法参数按原因拒绝、
8 路并发更新只有一路成功、冲突重试、余额不足整笔回滚、合法转账原子提交。

## 一、仓储读写统一规则

### 强类型与显式拒绝

- 每个值都带显式类型（`KindInt/Float/String/Bool/Null`），写入与过滤**不做隐式类型转换**。
- 查询过滤字段在表中不存在 → `engine.ErrColumnNotFound`；
  过滤值类型与列不符 → `engine.ErrTypeMismatch`。
  两类情况都直接失败，**不会静默返回空结果，也不会退化为无条件全量查询**。
- 写入未知列、非空列缺值且无默认、篡改不可变的 `id`/`version`，同样按可区分错误拒绝。

### 分页：一个查询计划，两个入口

`Repository.List` / `ListPage`（offset）/ `ListCursor`（cursor）最终都走同一个
`engine` 查询计划：过滤 → 按 `id` 稳定排序 → 分页切片 → 多取 1 行判断 `HasMore`。

- offset 分页：`offset >= 0`、`1 <= limit <= MaxLimit(500)`。
- cursor 分页：游标为自描述不透明串 `base64(JSON{ "v":1, "id":<锚点>, "dir":±1 })`。
  - 游标锚点是“上一页最后一条 id”，下一页用**严格**大于/小于锚点继续翻页（不重不漏）。
  - base64/JSON 非法、版本号不符、排序方向不符、id 非正 → `engine.ErrInvalidCursor`。
  - 游标与 offset 同时出现 → `engine.ErrInvalidPagination`。
- 非法 `limit/offset` → `engine.ErrInvalidPagination`，**不回退默认页、不返回全量**。
- `ListResult.Total` 为过滤后的总数；`NextCursor` 仅在 `HasMore=true` 时非空。

判定入口一致性的测试：`TestOffsetAndCursorPaginationMatch`（引擎层）、
`TestEntriesReturnIdenticalResults`（仓储层，三个入口 id/version 序列相等）。

### 列默认与 Go 零值

Go 结构体无法区分“没给字段”和“给了零值”。约定：**想让新增列默认生效，`ToRow` 中省略该列**；
示例中 `Product.Status` 用 `*int64` 表达，创建时为 `nil` 即交给列默认（V2 起默认 1），
读取回填后永远非 nil；要显式写 0 用 `domain.Status(0)`。插入后仓储会回读整行，
保证默认值与 `id/version` 对调用方可见。

## 二、迁移协议与兼容规则

### 协议

1. 迁移按 **version 严格升序且号段连续**登记；重复版本、跳号（如 1→3）在 `Register` 阶段拒绝
   （`ErrDuplicateVersion` / `ErrVersionOutOfOrder`）。
2. 每个版本由若干幂等步骤组成：
   `create_table | add_column | rename_column | drop_column | data`。
   DDL 全部走引擎幂等接口（同名同定义重复执行是 no-op）；`data` 步骤由编写者保证幂等
   （示例：系统账户只在不存在时插入）。
3. `Migrator.Up` 把**当前所有待执行版本放进同一个写事务**：
   - 任一步骤失败（含 `Step.FailForTest`）→ 整批回滚，连同批前面版本已建的表也不留
     （`ErrStepFailed`，错误带版本号、步骤序号与原因）。
   - 全部成功才记录到 `schema_migrations` 表（列 `version_num/name/checksum`）并一次性提交。
4. 重复 `Up`：已应用版本直接跳过，**不触碰业务数据**；幂等 data 步骤不会产生重复行。
5. 每个登记版本有稳定 **checksum**（覆盖全部 DDL 参数；回填函数为代码不参与哈希）。
   已应用版本的脚本被事后修改（例如把列默认 7 改成 9）→ `ErrChecksumChanged` 拒绝，
   防止“同版本不同结构”的不可解释状态。
6. 库里存在登记列表中缺失的版本 → `ErrMissingMigration` 拒绝继续演进。

### 兼容规则（加列 / 改名 / 删列）

- 行按**位置化槽位**存储：槽位 0 永远是自增 `id`，其后严格对齐 Schema 列序。
- **加列**：必须满足“`HasDefault=true` 给确定默认”或“`Nullable=true`”，
  否则在登记期即拒绝（`ErrInvalidStep`）。已有行在尾部追加槽位并统一回填默认/NULL，
  新老行默认语义一致且稳定。
- **改名字段**：只改列名、槽位与数据不动；旧名字立即返回 `ErrColumnNotFound`，
  读取方不会静默读到别的列（“不错位”）。
- **删列**：列与每一行对应槽位同步收缩，Schema 与行始终按槽位对齐；
  快照加载时还会校验行宽 = 列宽+1，绕过迁移的结构/数据不一致会被直接判为文件损坏。
- 落盘是**整库一份快照**：写临时文件 → `fsync` → 原子 `rename` 替换，
  任何一步失败都保留上一份完整文件，不会留下半截数据。

## 三、并发与隔离语义

- `DB` 内部一把 `sync.RWMutex` 作为写门（单进程内等价连接池模型）：
  - 任意时刻最多一个读写事务；多个只读事务可并发，读的是持锁期间的已提交快照。
  - 写事务在整库**工作副本**上执行，提交时一次性原子落盘；fn 返回 error 或 panic
    都会回滚（panic 回滚后原样重抛），因此写事务之间**可串行化、无部分提交**。
- **乐观版本控制**：托管实体必须有 `version` 列（插入时初始化为 0）。
  `Repository.Update` 以实体当前版本作为期望版本；库中版本不一致 →
  `engine.ErrVersionConflict`（错误中带期望值与实际值），**后写不能覆盖前写**；
  成功后版本自动 +1。`id` 与 `version` 由引擎托管，出现在更新 patch 中会被拒绝。
- 是否重试交给调用方：`repo.RetryOnConflict(n, fn)` 仅在 `ErrVersionConflict` 时重试，
  典型用法是在 fn 内重新 `Get` 最新版本再改；非冲突错误立即返回；重试耗尽返回原冲突。
- **批量与事务边界**：`BatchCreate` 与 `Repository.InTx` 都在单事务内执行，
  全有或全无；`InTx` 嵌套时加入外层事务、由最外层统一提交（避免内层半提交）。
  同一事务内“读余额→检查→扣减”即使在并发请求下也不会重复扣减
  （`TestConcurrentDeductionSerializable`：余额 1、5 路并发扣款，恰好 1 成功 4 余额不足，
  最终余额 0、版本恰好 1）。

## 四、错误速查（均可用 `errors.Is` 判定）

| 场景 | 错误 |
|---|---|
| 表不存在 | `engine.ErrTableNotFound` |
| 过滤字段缺失 | `engine.ErrColumnNotFound` |
| 值/过滤类型不匹配 | `engine.ErrTypeMismatch` |
| 非空列缺值、version 为空 | `engine.ErrColumnRequired` |
| 改 id/version | `engine.ErrImmutableColumn` |
| limit/offset 非法、混用两种分页 | `engine.ErrInvalidPagination` |
| 游标非法/被篡改/方向不符 | `engine.ErrInvalidCursor` |
| 按 id 读取/删除不存在行 | `engine.ErrNotFound` |
| 乐观版本冲突 | `engine.ErrVersionConflict` |
| 业务余额不足（示例） | `engine.ErrInsufficientBalance` |
| 迁移步骤失败（已整批回滚） | `migrate.ErrStepFailed` |
| 迁移版本不连续/重复 | `migrate.ErrVersionOutOfOrder` / `ErrDuplicateVersion` |
| 已应用脚本被改动 | `migrate.ErrChecksumChanged` |
| 加列无默认且不可空 | `migrate.ErrInvalidStep` |

## 五、测试场景对照

| 要求场景 | 测试 |
|---|---|
| 迁移失败回滚（单版本 & 多版本整批） | `migrate.TestMigrationFailureRollsBackEntireBatch`、`TestMigrationMultiVersionBatchAtomic` |
| 重复迁移幂等（数据不变、data 不重复） | `migrate.TestMigrationIdempotentRepeatedUp` |
| 脚本事后修改被拒绝 | `migrate.TestChecksumChangedRejected` |
| 并发更新冲突（8 路写仅 1 成功） | `repo.TestConcurrentOptimisticUpdateConflict` |
| 冲突由调用方决定重试 | `repo.TestRetryOnConflict` |
| 畸形分页参数（8 种） | `repo.TestMalformedPaginationRejected`、`engine.TestPaginationMalformedParams` |
| 过滤缺失/类型不匹配 | `engine.TestListFilterRejectsUnknownColumn` / `...TypeMismatch` |
| 两种分页入口一致 | `engine.TestOffsetAndCursorPaginationMatch`、`repo.TestEntriesReturnIdenticalResults` |
| 批量/事务原子、无重复扣减 | `repo.TestBatchCreateAtomicOnFailure`、`TestInTxNoDoubleDeduction...`、`TestConcurrentDeductionSerializable` |
| 加列默认、改名不错位 | `engine.TestAddColumnDefaultAndRenameDrop`、`domain.TestEvolutionDefaultsAndRename` |
| 落盘重开一致 | `engine.TestPersistenceRoundTrip` |

每个用例都通过 `verdict(...)` 打印 **输入 / 期望判定依据 / 实际结果**，用 `go test -v` 可查看。

## 六、实现边界说明

- 存储是单文件 JSON 快照，面向**本地开发/教学/单进程工具**场景；写事务串行化保证正确性，
  不追求高写入吞吐，也不提供跨进程锁（多进程写同一文件不在支持范围内）。
- 排序固定为按主键 `id`（asc/desc），以保证 offset 与 cursor 两种分页的确定性与一致性。
