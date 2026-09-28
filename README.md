# Resilient Data Access Layer (Go)

一套可在本地直接运行的数据访问与结构变更底座，目标是让**仓储读写、表结构演进、
并发更新**在不同入口下给出一致、可解释的结果。仅依赖 Go 标准库（进程内事务型
存储引擎，无需外部数据库），便于离线验证与教学。

## 目录结构

```
internal/
  errors/    稳定原因码：所有拒绝都携带可程序化判别的 ReasonCode
  store/     事务型存储引擎：快照事务、行级合并提交、唯一约束、DDL/DML 同事务
  database/  并发安全句柄（连接池语义入口）
  migrate/   版本化迁移器：校验和、幂等登记、每版本独立事务、破坏性变更授权
  repo/      通用仓储：统一过滤校验 + 偏移分页/游标分页/全量三入口同源
  account/   账户领域：乐观锁更新、余额扣减、幂等键
cmd/demo/    可运行的最小演示
docs/        迁移协议、兼容规则、并发隔离语义
```

## 快速验证（无需联网、无外部依赖）

```bash
go build ./...
go vet ./...
go test ./... -race -count=1          # 全量单测（含并发，打印输入与判定依据）
go test ./... -race -count=5          # 重复运行确认并发用例不 flaky
go run ./cmd/demo                     # 迁移/查询一致性/并发更新/幂等扣减演示
```

## 三类保证一览

### 1. 仓储读写统一、拒绝原因可区分

- 三个查询入口（全量 `FindAll` / 偏移分页 `FindByPage` / 游标分页 `FindByCursor`）
  共享同一份“过滤校验 + id 升序扫描”计划；翻页/游标只是对同一结果集的不同切片，
  同条件下逐行一致、不重不漏。
- 非法输入绝不静默返回空结果或全量数据，而是按原因码拒绝：
  - `INVALID_PAGINATION`：page<1、pageSize/limit 越界（<1 或 >100）、after_id<0；
  - `UNKNOWN_FILTER_FIELD`：过滤列在表元数据中不存在；
  - `FILTER_TYPE_MISMATCH`：值的 Go 类型与列声明类型不符（不做隐式转换）；
  - `MISSING_FILTER_VALUE`：必填列过滤值为 nil 或字段名为空。
- 判定方式：用 `errors.As(err, *errors.DataError)` 读取 `Reason`，不要匹配错误文本。

### 2. 表结构随版本演进：可重复、幂等、失败整批回滚

- 每个版本是一个脚本；已应用版本登记在 `schema_migrations`，重复执行迁移只
  `skip` 而不重跑脚本、不改变任何数据。
- 每个版本的脚本与登记写入在**同一事务**提交：脚本中途失败，该版本的建表/加列/
  回填全部回滚，已应用版本不变，不留半套结构。
- 每个版本有稳定校验和；同版本号的身份（名称/破坏性标记）相对历史记录发生变化时，
  以 `MIGRATION_CHECKSUM_CHANGED` 拒绝，防止环境间结构漂移。
- 新增列必须带稳定默认值或允许空；既有行立即取得默认值，新插入行取同一默认值。
- 删列/改名字段是破坏性变更：默认以 `DESTRUCTIVE_CHANGE_DENIED` 拒绝，必须在迁移
  上显式声明 `Destructive: true`，授权记录随版本事务审计落库。读取方一律按**列名
  投影**而非位置，因此加列不会让读取方静默错位。

### 3. 并发更新可预期：不丢更新、不重复扣减、不部分提交

- 每行带隐式 `version`。基于版本的更新在 WHERE 条件里携带期望版本：
  - 版本落后 -> `VERSION_CONFLICT`，后写不覆盖前写；调用方可重读后自行决定重试；
  - 提交时与并发事务撞车 -> 引擎第一提交者胜出，另一笔收到 `ErrConflict`，
    在领域层同样翻译为 `VERSION_CONFLICT`。
- 隔离语义：快照事务 + 提交时行级合并发布。
  - 点读只依赖具体行（行版本），范围读依赖整表版本（防谓词幻影）；
  - 因此**不同账户的并发扣减互不阻塞**，同账户的并发修改才冲突；
  - 插入在分配时取全局自增 id，提交做行级合并，不会丢先提交事务的行、不重号。
- 扣减 `Debit` 在单一事务内完成“幂等键检查 → 版本/余额校验 → 扣减 → 幂等键登记”：
  - 全部成功才提交，任何一步失败全部回滚，不存在部分提交；
  - 同一 `IdempotentID` 的重试命中回放（`Replayed=true`），不重复扣减；
  - 幂等键有唯一约束，同键并发插入由提交复核兜住；
  - 余额不足 -> `INSUFFICIENT_BALANCE`，账户余额与版本均不变。

## 错误原因码

见 `internal/errors/errors.go`。关键码：

| ReasonCode                     | 触发场景                                   |
|--------------------------------|--------------------------------------------|
| `INVALID_PAGINATION`           | 分页/游标参数非法                          |
| `UNKNOWN_FILTER_FIELD`         | 过滤字段不存在                             |
| `FILTER_TYPE_MISMATCH`         | 过滤值类型与列不符                         |
| `MISSING_FILTER_VALUE`         | 必填过滤值缺失                             |
| `VERSION_CONFLICT`             | 乐观锁版本落后或提交竞争（可重读重试）     |
| `RECORD_NOT_FOUND`             | 目标记录不存在                             |
| `INSUFFICIENT_BALANCE`         | 扣减余额不足                               |
| `IDEMPOTENT_REPLAY`            | 幂等键回放 / 同键不同参数                  |
| `MIGRATION_FAILED`             | 版本脚本失败（事务已回滚）                 |
| `MIGRATION_CHECKSUM_CHANGED`   | 同版本身份漂移                             |
| `MIGRATION_BAD_ORDERING`       | 版本重复、非递增、脚本为空                 |
| `DESTRUCTIVE_CHANGE_DENIED`    | 未声明的删列/改名                          |

更多设计细节见 [`docs/MIGRATION.md`](docs/MIGRATION.md) 与
[`docs/CONCURRENCY.md`](docs/CONCURRENCY.md)。
