# Task 13 Report: Production HA Failure Matrix

日期：2026-08-03  
仓库：`/Users/xujiawei/magic/workbench/cube`  
结论：**NOT EXECUTED；不能作为生产放行证据**

## 1. 范围与判定

本报告依据以下已有材料整理，不新增或修改产品代码：

- `docs/superpowers/plans/2026-08-03-cubestore-router-production-ha-remediation.md` 的 Task 13 及全局 HA 约束。
- `task-6-report.md`、`task-8-report.md`、`task-10-report.md` 的 `NEEDS_CONTEXT` 结论。
- `task-9-review-final.md` 的 Redis/lease loss、UNKNOWN 和真实 Lua integration evidence 缺口。
- `task-7-review-final3.md` 的 Worker identity、lease-store Secret、image 和 CSI durability 缺口。
- `operators/cube-operator/HA-VALIDATION-REPORT.md` 的本地 demo 结果及其 epoch/生产边界限制。
- `docs/superpowers/plans/task-14-report.md` 的生产 gate 判定。

Task 13 计划要求机器可读 JSON、至少 100 次重复 failover、RPO 0、duplicate committed mutations 0、split-brain overlap 0 和实测 RTO。当前没有可归档的 Task 13 `ha-matrix.sh` 输出、JSON、故障注入日志或 100 次循环结果，因此所有矩阵项均为 **NOT EXECUTED**。历史 demo 的单次切主、编译、dry-run 或 `SELECT 1` 结果不升级为生产 PASS。

## 2. Failure matrix

完整的检测信号、预期结果、回滚动作和证据字段见 [`operators/cube-operator/HA-FAILURE-MATRIX.md`](operators/cube-operator/HA-FAILURE-MATRIX.md)。矩阵中：

- `NOT EXECUTED` 表示本任务没有运行该故障注入或收集对应的 machine-readable evidence。
- `HISTORICAL LIMITED` 表示已有记录只覆盖本地 demo/静态检查，不能证明 Task 13 acceptance。
- 任何不能证明旧 holder 已 fencing 的情况都必须进入 `no-leader`，禁止手工改 label、ConfigMap 或 Service selector 继续放量。

## 3. 当前证据与缺口

| 项目 | 当前记录 | Task 13 判定 |
|---|---|---|
| Router graceful/`kill -9`/Pod deletion | `HA-VALIDATION-REPORT.md` 有本地 demo 切主记录，但不是 100 次真实数据循环 | `HISTORICAL LIMITED`；Task 13 `NOT EXECUTED` |
| Lease store outage、stale token | 没有可归档的 Redis/PG 故障注入证据；Task 9 明确真实 Lua integration 未执行 | `NOT EXECUTED` |
| MetaStore restart、CSI reattach | Task 7 review 明确没有 live CSI restart/node replacement、backup/restore 证据 | `NOT EXECUTED` |
| Worker loss/import/compaction | 没有 Task 13 workload JSON 或恢复哈希；Worker identity 仍有已知缺口 | `NOT EXECUTED` |
| Mixed-version Router rollout | 没有旧/新二进制兼容和实际 rollback 输出 | `NOT EXECUTED` |
| Query/write/upload/retry | 历史资料指出 query/demo 有限，写幂等和 Redis integration 仍不完整 | `NOT EXECUTED` |
| RPO/RTO/duplicate/split-brain | 没有 Task 13 归档统计 | `NOT EXECUTED` |

## 4. Release decision

当前不能宣称 G5 或生产 release candidate 通过。Task 14 应继续按 `FAIL`/`NEEDS_CONTEXT` 处理，直到：

1. 运行并归档完整故障矩阵 JSON，至少 100 次循环。
2. 对每个故障收集 epoch、holder/token hash、EndpointSlice、CR status、Router/Worker/MetaStore 日志和 workload hashes；不得记录明文 token、DSN 或 Secret。
3. 证明 committed metadata RPO 0、duplicate committed mutations 0、split-brain overlap 0，并给出 measured RTO。
4. 补齐 Redis/PG、CSI、Worker identity、immutable image 和 rollback-compatible version 的运行前置条件。

未执行项不是失败测试结果，也不是通过结果；它们是生产放行前的阻断项。
