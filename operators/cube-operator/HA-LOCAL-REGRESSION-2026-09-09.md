# Router HA 本地修补与回归：2026-09-09

## 结论

**本次授权的四处修补已写入，但验收没有全部通过。生产仍为 `NO-GO`。**

Driver 新构建测试存在 TypeScript mock 类型错误；Rust 新增测试存在枚举构造语法错误。这些是本次开发引入的测试源码问题，不能归因于 yarn、Kubernetes 或外部服务，也不能关闭类型检查来绕过。

没有操作 Kubernetes，没有构建或部署镜像，没有 Git 提交或推送。部分临时日志名沿用 `20260908`，本报告记录的是跨日后本轮实际结果。

## 已写入的修补

| 问题 | 修改 | 当前验证 |
| --- | --- | --- |
| 重复 Bind 覆盖原子建表来源标记 | 保留既有 `Binding.created` | Rust 测试未执行 |
| schema 缺失拒绝结果未持久化 | 返回并持久记录 `LEDGER_SCHEMA_NOT_FOUND` 业务拒绝 | Rust 测试未执行 |
| Driver 授权记录身份核对不足 | 显式比较返回的 key/generation 与定位信息 | 新测试套件未能编译，未执行对应新增用例 |
| 故障脚本旧主连接没有认证 | 直接构造 WebSocket 时传入 `driver.recoveryHeaders()` | 脚本纯逻辑及语法检查通过，未建立真实集群连接 |

## 本轮实际结果

| 目标 | 结果 | 说明 |
| --- | --- | --- |
| Operator `controllers` | PASS | 整包回归，0.802 秒 |
| Operator `config` | PASS | 整包回归，2.120 秒 |
| Driver `PreAggregationRecovery` | 44/44 PASS | 使用源码映射，不以旧 dist 代替 |
| Driver `AtomicPreAggregationLedger` | 9/9 PASS | 使用源码映射 |
| Driver `AtomicPreAggregationBuilds` | 编译失败 | 新构建及身份核对用例未执行 |
| Driver `tsc --noEmit` | FAIL，退出码 2 | 三处测试 mock 类型转换错误 |
| Orchestrator 三个相关套件 | 67/67 PASS | PreAggregations 33、PreAggregationRecovery 27、ReplacePreAggregationTableNames 7 |
| Orchestrator `tsc --noEmit` | PASS | 类型检查通过 |
| 故障脚本纯逻辑回归 | 15/15 PASS | 原子证据、旧主拒绝证据和重试策略，不是真实 HA 证明 |
| 故障脚本语法 | PASS | `node --check`、`bash -n` |
| Rust 测试构建 | 主动中止 | 94.8 秒，SIGTERM，退出状态 -15；停止时仍编译 librocksdb-sys |
| Rust 行为测试 | 0 项执行 | 没有本轮 Rust PASS 证据 |
| Kubernetes 故障 E2E | 未运行 | 不在本次授权范围内 |

不要将上述已通过用例相加后描述成“整体验收通过”。**Driver 整体回归失败，Rust 验证未完成。**

## 精确阻断与下一次最小修补

1. `packages/cubejs-cubestore-driver/test/AtomicPreAggregationBuilds.test.ts` 第 222、235、241 行的 `fetch as jest.Mock` 触发 TS2352。需要正确表达 mocked fetch 类型，并重跑失败的 Driver 套件及类型检查，不能禁用诊断。
2. `rust/cubestore/cubestore/src/metastore/pre_aggregation_ledger_tests.rs` 的 `pre_aggregation_ledger_atomic_create_schema_rejection_is_durable` 中，`let old_owner` 对 `LedgerCommand::Create` 枚举变体使用了 `..base` 结构体更新语法。应直接使用已有 match 分支完整构造变体，再编译和执行 Rust 回归。

Rust 语法问题在编译器到达该源码前已被发现，因此主动停止了仅本次启动的 Cargo 进程组，避免继续耗时等待必失败结果。没有广泛终止其他进程，也没有删除编译缓存。日志中的 `USER_REQUESTED_ABORT` 为构建包装器记录标签，实际是父任务依据已知错误主动要求停止，不是用户要求放弃验证。

以上新问题尚未再次修补。来源意图丢失、过期构建接管、诊断读取保护及跨节点引用治理等既有生产缺口仍见 [业务接入续作报告](HA-BUSINESS-INTEGRATION-2026-09-08.md)，本次局部修补不能消除这些边界。

## 原始证据

- [Operator 回归](artifacts/local-regression-20260909/operator.log)
- [Driver/Orchestrator 汇总](artifacts/local-regression-20260909/driver-summary.log)
- [Driver Jest](artifacts/local-regression-20260909/driver-jest.log)
- [Driver 类型检查](artifacts/local-regression-20260909/driver-tsc.log)
- [结构化结果](artifacts/local-regression-20260909/driver-results.json)
- [Rust 构建与中止记录](artifacts/local-regression-20260909/rust.log)
- [故障脚本纯逻辑回归](artifacts/local-regression-20260909/script-tests.log)

## 第二轮：已授权测试源码修补后的实际结果

本节更新上文两项测试源码阻断的状态，不覆盖或伪装前轮失败记录。

- Driver 三处 fetch mock 类型转换已修正，保留 diagnostics 与全部断言；`AtomicPreAggregationBuilds` **15/15 PASS**，包括 key/generation 不匹配用例；Driver `tsc --noEmit` **PASS**。
- 这轮没有重跑前轮已经通过的 Driver 53 项或 Orchestrator 67 项。Driver 分批累计 68 项，不可写成本轮一次运行 68 项通过。
- Rust `old_owner` 枚举构造已修正。
- Rust `cargo +nightly-2025-08-01 test --locked --offline -j1 -p cubestore --lib --no-run` 在 **821.8 秒**后退出 **101**，不是超时。
- Rust 编译错误 **E0046**：`rust/cubestore/cubestore/src/queryplanner/test_utils.rs:28` 的 `MetaStoreMock` 缺少 `acquire_pre_aggregation_query_refs`、`release_pre_aggregation_query_refs`、`mutate_pre_aggregation_ledger`、`get_pre_aggregation_ledger` 四个新增 trait 方法。
- Rust 测试目标未生成，行为测试仍执行 **0 项**；没有进入 ledger/query_refs/HTTP/password_auth/authority 热回归。

下一处明确修补目标是测试 mock 的新增接口契约。不能删掉测试模块、关闭检查或返回虚假成功来制造通过。该漏项尚未修补，生产结论仍为 `NO-GO`。

证据：[Driver 本轮日志](artifacts/local-regression-20260909/driver-testfix.log)、[Driver Jest JSON](artifacts/local-regression-20260909/driver-testfix-jest.json)、[Rust 编译错误日志](artifacts/local-regression-20260909/rust-testfix.log)。本轮未操作 Kubernetes 或 Git。
