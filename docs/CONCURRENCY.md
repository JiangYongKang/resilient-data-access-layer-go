# 并发与隔离语义

本文档说明 `internal/store` 与 `internal/account` 在连接池/并发请求下如何保证
更新不丢、扣减不重、批量不部分提交，以及冲突如何暴露给调用方。

## 1. 事务模型

- `store.Begin()` 立即复制当时全部已提交表结构与数据（深拷贝快照），事务内的
  读写都发生在私有副本上；`View` 是只读事务，`Exec` 是“自动提交”事务；
- `database.DB` 是并发安全句柄，可被任意 goroutine 共享，等价于“每个入口请求
  拿到一个独立事务/连接”；
- `Commit` 在全局写锁内做依赖仲裁并一次性发布；`Rollback` 丢弃全部副本，不留痕。

## 2. 隔离级别：快照隔离 + 第一提交者胜出

事务跟踪四类依赖，提交时逐一校验，任一不满足即整体中止：

| 依赖 | 何时登记 | 提交仲裁 |
|------|----------|----------|
| 整表读 `fullRead` | `List/Count/HasTable/Columns`、在既有表上的 DDL | 表当前版本必须等于快照版本（防谓词幻影、防结构并发变更） |
| 行读 `rowRead` | `Get/FindOne` 命中具体行 | 该行当前版本必须等于读取时版本，行被删即冲突 |
| 行写 `writeRows` | `Update/Delete`（记录快照行版本） | 该行自快照后未被他人提交修改 |
| 插入 `insertRows` | `Insert`（全局自增 id） | id 未被占用 + 唯一列对已提交世界复核 |

含义：

- **第一提交者胜出**：两个事务改同一行，先提交的生效，后提交的收到
  `store.ErrConflict`，自己的全部改动不发布；
- **读不阻塞写、写不阻塞读**：事务都在私有快照上工作，只在提交点仲裁；
- **不同行不互相误冲突**：点读/写只依赖行，范围读才依赖整表。因此 20 个不同
  账户并发扣减可以全部成功（见 `TestDebit_ConcurrentDifferentAccounts`）；
- **范围谓词仍可串行化**：基于过滤/游标的读取登记整表依赖，提交期间若有并发
  插入改变了结果集会让读取方事务冲突，从而避免幻读。

## 3. 提交发布：行级合并不是整表覆盖

纯 DML 事务提交时，以**当前已提交状态**为基准做行级合并：

- 插入的新行并入（id 在分配时全局自增，不依赖快照，故不撞号、不丢别人的行）；
- 更新的行只覆盖被触碰的列，未触碰列保留已提交世界现值，行版本取私副本的新版本；
- 删除的行移除；
- 若事务期间表被 DDL 扩展了列，合并时按当前列结构为新行补默认值。

在既有表上做 DDL 的事务因登记了整表读依赖，提交时当前状态必等于快照，直接
整表发布即可，不会覆盖他人数据。

## 4. 乐观锁：把冲突变成可解释的拒绝

- 所有表隐式带 `id`（自增主键）与 `version`（行版本，首次插入为 1，每次更新 +1）；
- 领域更新必须携带调用方读到的 `Version`：
  - `store.Update(table, id, expectVersion, values)` 在行版本不等时返回
    `affected=0`，领域层翻译为 `VERSION_CONFLICT`；
  - 提交撞车返回的 `ErrConflict` 同样翻译为 `VERSION_CONFLICT`；
- 调用方协议：
  1. 读取聚合（得到 version=V）；
  2. 基于 V 提交修改；
  3. 收到 `VERSION_CONFLICT` 后**重读**得到新版本 V'，合并字段后由调用方决定
     是否重试——系统不会默默后写覆盖前写。

`TestUpdate_ConcurrentConflict` 与 `TestUpdate_RetryAfterConflict` 验证：
两个基于同一版本的更新恰有一个成功、最终版本只 +1、第一提交者的字段保留，
重读重试后第二次更新成功。

## 5. 余额扣减：幂等 + 原子 + 余额前置校验

`Repository.Debit` 在**单一事务**内顺序完成：

1. 用幂等键点查 `account_debit_keys`（唯一列）：
   - 命中且参数一致：退化为只读回放，`Replayed=true`，不做任何扣减；
   - 命中但金额/账户不同：`IDEMPOTENT_REPLAY` 拒绝，防止幂等键误用；
2. 点读账户，校验行版本与余额：
   - 版本不符 -> `VERSION_CONFLICT`；
   - 余额不足 -> `INSUFFICIENT_BALANCE`，账户不变；
3. 带行版本条件更新余额（`balance = balance - amount`）；
4. 插入幂等键登记（与扣减同事务）。

因此：

- **不重复扣减**：同一 `IdempotentID` 的重试要么回放，要么（并发同键且尚未见
  登记行时）在提交点因行版本竞争或唯一约束被拒；无论运行时把并发请求调度成
  “重叠”还是“串行”，真实扣减次数恒为一次（见
  `TestDebit_ConcurrentSameIdempotencyKey` 的调度无关断言）；
- **不部分提交**：扣减与幂等登记同生共死；批量写入放在同一 `Exec` 闭包里，
  任意一行失败整批回滚（见 `TestBatch_AtomicInOneTransaction`）。

## 6. 错误映射

| 引擎错误 | 领域原因码 | 调用方处理 |
|----------|-----------|------------|
| `ErrConflict`（提交竞争） | `VERSION_CONFLICT` | 重读后决定是否重试 |
| 行版本不匹配（affected=0） | `VERSION_CONFLICT` | 同上 |
| `ErrUniqueViolation`（幂等键并发） | `IDEMPOTENT_REPLAY` | 用同键重试以回放结果 |
| 余额不足（应用判定） | `INSUFFICIENT_BALANCE` | 不可重试，提示余额 |
| `ErrTableNotFound/ErrColumnNotFound` | 多为 `RECORD_NOT_FOUND`/查询前置错误 | 视场景处理 |

所有原因码集中在 `internal/errors`，请用 `errors.As` 判别而非字符串匹配。

## 7. 本地验证

```bash
go test ./internal/store/ -race -v      # 引擎：合并不丢行、丢更新拒绝、DDL/DML 原子
go test ./internal/account/ -race -v    # 领域：并发更新、同键只扣一次、异账户不冲突
go test ./... -race -count=5            # 重复运行确认并发用例稳定
go run ./cmd/demo                       # 直观观察各类结果
```
