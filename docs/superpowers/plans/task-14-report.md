# Task 14 Report: Production Docs and Release Gate

日期：2026-08-03  
仓库快照：`codex/ha-router-experiment`，`1d1f5c97e8`  
结论：**FAIL，禁止生产上线**

## 1. 判定规则

- `PASS`：有可复核的实际命令、输出或归档 JSON 证据，且证据覆盖该项 acceptance，而不是只证明文件存在或进程能启动。
- `FAIL`：已有代码审查、缺失交付物或实际结果明确表明不满足要求。
- `NEEDS_CONTEXT`：没有足够的可复核证据，不能写成 PASS；在生产 release gate 中按阻断处理。

本报告只汇总当前仓库和既有记录，Task 14 没有重新运行测试、没有连接外部依赖，也没有把本地演示结果扩展为生产结论。`SELECT 1`、单行 `RouterHaProbe` 查询、dry-run、编译成功和文件存在均不能单独满足生产 gate。

## 2. 当前交付状态

| Task | 状态 | 当前真实状态与阻断原因 |
|---|---|---|
| 1. Real data fixture | `NEEDS_CONTEXT` | 已有真实数据检查脚本和断言强化提交；没有当前可归档的 10,000 行数据、table/partition/chunk/row hash 故障切换结果。现有演示仍主要是 `SELECT 1` 或单行 probe。 |
| 2. Network protocol compatibility | `NEEDS_CONTEXT` | 有兼容性修复提交，但本报告没有可复核的旧/新 golden bytes 混合版本运行证据。 |
| 3. Leadership/fencing contract | `NEEDS_CONTEXT` | API/契约实现提交存在；没有在本快照中确认完整的 API-server validation、默认值和负向测试 acceptance。 |
| 4. External lease store | `FAIL` | Redis/PostgreSQL 实现文件存在，但没有真实外部 Redis 和 PostgreSQL 的原子竞争、续租、过期、故障、时钟偏移和恢复证据；不能证明生产选举安全。 |
| 5. Lease agent | `NEEDS_CONTEXT` | lease-agent 代码存在；没有归档 atomic write、backend outage expiry、holder change 和单调 epoch 的运行结果。 |
| 6. Central leadership guard | `FAIL` | 计划要求的 `rust/cubestore/cubestore/src/leadership/mod.rs` 在当前快照缺失；因此不能证明 HTTP、WebSocket、MySQL、upload、RPC、Scheduler 和 background loop 全部被同一 guard fence。 |
| 7. Authoritative MetaStore | `FAIL` | demo/StatefulSet 与 Worker endpoint wiring 已出现，但 review 已指出 runtime identity、Secret/image、CSI durability 和恢复证据缺失；没有证明两个 Router 只读同一个可恢复 MetaStore。 |
| 8. Two-phase promotion | `FAIL` | 计划要求的 `election_controller.go` 未出现；没有候选确认、epoch ack、EndpointSlice exactly-one 和 timeout 回滚证据。 |
| 9. Mutation idempotency | `FAIL` | `task-9-review-final.md` 明确指出 lease/Redis loss 后可能重新执行、`UNKNOWN` reconciliation 无权威边界、大结果引用不可恢复，且真实 Lua/Redis integration 被跳过。 |
| 10. Recoverable jobs/uploads | `NEEDS_CONTEXT` | 最近提交记录为 needs-context；没有覆盖 assignment、mid-upload、post-commit/pre-response 和 pre-aggregation version switch 的可复核故障矩阵。 |
| 11. Refresher HA | `FAIL` | 计划要求的 `cube_refresher_controller.go` 和 refresher failover acceptance 未出现；没有证明 takeover 不产生重复 build。 |
| 12. Production Kubernetes manifests | `FAIL` | 计划要求的 `config/production/` manifests 未出现；没有 digest pinning、PDB/anti-affinity、network policy、Secret refs、preStop fencing 和 rollback-compatible versions 的生产清单。 |
| 13. Failure matrix | `FAIL` | 计划要求的 `operators/cube-operator/tests/e2e/ha-matrix.sh` 未出现；没有机器可读 JSON、100 次 failover、RPO/duplicate/split-brain/RTO 结果。 |
| 14. Release gate | `FAIL` | 本报告建立 gate，但 acceptance 证据、runbook、owner sign-off 尚未齐全。 |
| 15. Raft MetaStore | `NEEDS_CONTEXT` | 可选后续里程碑；不是当前单写 RWO release 的必要项，但不能把它写成已交付。 |

## 3. 已有 PASS 与边界

| 检查 | 状态 | 证据边界 |
|---|---|---|
| 本地 K8s Router role/Service 切换演示 | `PASS`（仅限 demo） | `operators/cube-operator/HA-ROUTER-K8S-DEMO.md` 记录了两个 Router、leader label、`leaderEpoch` 递增和 `cube-router-leader` EndpointSlice 切换。该结果使用 demo 状态介质和有限查询，不能证明生产 external lease、共享 MetaStore 或故障注入安全性。 |
| Cube API -> Driver -> Leader Service -> Router 查询切换 | `PASS`（仅限有限 query E2E） | 同一文档记录了 2026-08-02 的 Cube API HTTP 查询在删除 leader 前后成功，业务 data hash 一致。没有覆盖真实 mutation、10,000 行数据、Redis mutationId、refresh、上传、长事务或外部依赖故障，因此 production Cube API E2E gate 仍为 `FAIL`。 |
| 生产 release readiness | `FAIL` | 没有任何证据证明完整 failure matrix、RPO 0、duplicate committed mutations 0、split-brain overlap 0 以及 agreed RTO。 |

## 4. 外部依赖 release gates

下表是上线前必须单独执行并归档的 gate。示例 DSN、manifest、dry-run 或本地进程不算证据。

| 依赖 / 场景 | 当前状态 | 必须补齐的证据 |
|---|---|---|
| 外部 Redis | `FAIL` | 真实 Redis URL/Secret；Lua 脚本在真实 Redis 上执行；owner-token CAS、TTL/renew、两个 contender、重启、连接中断、AOF/replication 和恢复结果；mutation `UNKNOWN` 不得在 pending TTL 后被重新获取。 |
| 外部 PostgreSQL | `FAIL` | 真实 PostgreSQL Secret/TLS 或明确安全配置；`SELECT ... FOR UPDATE` 竞争测试、server-side time、epoch 迁移、连接故障、主备/恢复和 clock-skew 结果。Redis 与 PostgreSQL 不能只在配置示例中出现。 |
| CSI RWO / MetaStore | `FAIL` | 目标 CSI StorageClass、RWO PVC、节点 drain、volume detach/reattach、MetaStore Pod restart、snapshot、backup/restore、PV reclaim policy 和 clean namespace restore 的输出；证明同一 table ID、chunk、Job metadata 持久存在且不存在并发 RocksDB open。 |
| Object storage | `NEEDS_CONTEXT` | 真实 S3/MinIO endpoint、Secret、checksum、临时 object、publish 和 orphan cleanup 故障证据；不能用本地 filesystem 配置替代生产 durability。 |
| 真实 Cube API E2E | `FAIL` | 两个 API replica 通过生产样式 Service 访问 Router；创建唯一 schema/table，写入至少 10,000 个确定性 rows，切主期间持续读写，校验 table/partition/chunk/row hash，并覆盖 connection loss、mutationId、upload、refresh 和 post-commit/pre-response。 |
| 镜像、CRD、兼容与回滚 | `FAIL` | 所有 image digest、CRD/API version、schema migration version、旧/新二进制兼容矩阵、上一个可回滚 digest，以及在 staging/clean namespace 的实际 rollback 结果。 |

## 5. 禁止生产上线条件

满足任一条件都必须停止发布，进入 no-leader 或 maintenance 状态，不得通过手工改 ConfigMap、Pod label 或 Service selector 绕过 gate：

- 任一必需 Task 或 G0-G5 gate 为 `FAIL` 或 `NEEDS_CONTEXT`。
- 没有真实外部 Redis 或 PostgreSQL 的 lease/故障/恢复证据，或无法确认唯一 unexpired holder 和单调 fencing epoch。
- 不能证明 stale Router 在 HTTP、WebSocket、MySQL、upload、内部 RPC、Scheduler、Job、Snapshot 和 cleanup commit 前均被 fence。
- 两个 Router 读取独立 local MetaStore，或不能证明 RWO PVC 在节点切换期间没有并发打开。
- 只有 `SELECT 1`、单行 probe、健康检查、编译、dry-run 或单次无故障查询，没有真实业务数据 hash 和故障注入结果。
- Cube API 仍通过 Pod IP 或 broad follower Service 发送生产业务流量。
- mutation lease 丢失、Redis 重启、连接断开后可能盲目重放；`UNKNOWN` 没有权威 reconciliation 或永久 tombstone。
- 未提供 CSI backup/restore、object storage recovery、clean namespace restore 或已验证的 previous image rollback。
- image 使用 mutable tag，Secret/凭据进入明文 manifest，或 owner acceptance matrix 未由 Storage、Platform、Cube API、Application owner 签字。

## 6. 强制 release acceptance

上线前必须归档以下材料，并将对应状态从 `FAIL/NEEDS_CONTEXT` 改为 `PASS`，不能由本报告代签：

1. Task 13 machine-readable JSON：至少 100 次重复 failover；RPO 0、duplicate committed mutations 0、split-brain overlap 0，并记录测得 RTO。
2. 外部 Redis 和 PostgreSQL 的真实 integration logs、配置版本和故障恢复时间。
3. CSI RWO MetaStore 的 restart、node detach/reattach、snapshot、restore 和 clean namespace proof。
4. 真实 Cube API read/write/refresh/upload E2E 的 table、chunk、row、job 和 object hash。
5. image digest、CRD/schema 版本、兼容矩阵、备份位置、回滚版本和演练输出。
6. Storage、Platform、Cube API、Application owner 的签字 acceptance matrix。

## 7. 回滚与紧急手工 fencing

回滚的首要目标是避免双写，不是保持可用性。任何强制 promotion 前都必须完成旧 holder fencing；不能以“旧 Pod 可能已经断开”为依据直接 promotion。

1. 立即停止 mutation、upload、refresh 和 Job submission；将 Cube API 置于 maintenance/no-write，保留只读请求也必须经过当前有效 Leader Service。
2. 记录 CR status、当前/最高 epoch、lease holder/token hash、Router Pod/Node、EndpointSlice、MetaStore PVC、Redis/PG lease key 和最近日志。不要公开 token 或 DSN。
3. 从生产业务 Service 移除旧 leader 路由，并通过受控 Pod termination、network policy 或节点隔离完成旧 holder fencing；确认其 leadership file 已过期，且外部 lease store 不再接受旧 token 的 renew/release/commit。
4. 若不能证明旧 holder 已 fence，保持 no-leader，禁止手工改 label、ConfigMap 或 selector 造出新 leader。由 Platform owner 按事故 runbook 执行双人复核。
5. 只有在 lease store 确认旧 epoch 不再有效、候选 Router 已读到同一 MetaStore 且返回相同新 epoch 后，才允许 two-phase promotion；随后确认 EndpointSlice 恰好一个 ready endpoint。
6. 若当前版本或新 leader 不稳定，使用已归档的 previous immutable image digest 和兼容 CRD/schema 版本回滚；禁止回滚到 local-RocksDB 双写模式或绕过 leadership guard 的版本。
7. 若 MetaStore/PVC 损坏或无法恢复，保持 no-leader，使用最近一次一致性 snapshot/backup 恢复到隔离 namespace/PVC，先校验 metadata、row/object hash，再逐步恢复 Service 流量。
8. 回滚后重新执行最小 read/write/refresh smoke、epoch monotonicity、single EndpointSlice 和 mutation duplicate 检查；未完成证据归档前不得解除 maintenance。

## 8. 最终签字

当前没有可记录为已完成的 Storage、Platform、Cube API 或 Application owner 签字。签字前必须引用具体的 archived command output/JSON 和 image/CRD/schema 版本；口头确认、代码 review 结论或本地 demo 不能替代签字证据。

