# Cube Router HA 故障矩阵与可观测性

本文件是 Task 13 的执行与证据约定。它描述应如何检测故障、什么结果才算安全、如何回滚，以及必须归档什么证据。**本轮仅新增文档，以下所有条目均未执行；不得把预期结果当作实际结果。**

## 判定规则

- `PASS` 只能来自真实故障注入、真实数据 workload 和可复核的机器可读证据。
- 失败或证据不完整时优先保持 `no-leader`、停止 mutation/upload/refresh/Job submission，再处理恢复。
- 任何 epoch 下降、两个有效 holder 重叠、旧 token 仍可 renew/release/commit、重复 committed mutation、metadata/hash 漂移均为阻断。
- 证据必须包含时间戳、场景 ID、镜像/CRD/schema 版本、故障窗口、epoch、holder/token hash、EndpointSlice、CR status、关键日志和数据 hashes；禁止明文 token、DSN、Secret。

## 故障矩阵

| 场景 | Detection | Expected result | Rollback / recovery | Evidence / status |
|---|---|---|---|---|
| Leader crash：graceful stop、`kill -9`、Pod deletion | `leader`/`status.leader`、EndpointSlice、lease holder、epoch、`STALE_LEADER` error rate、last commit timestamp | 旧 Router 在 lease expiry 或进程终止后不能继续 commit；新 Router 仅在旧 holder fenced、epoch 严格递增、MetaStore ready 后接流量；单一 ready endpoint | 先撤 mutation 流量并确认旧 Pod/lease/token 已失效；不能证明 fencing 时保持 `no-leader`；确认同一 table/chunk/row hash 后再 promotion | 归档 Pod/Node events、CR status、lease record、EndpointSlice、Router logs、RPO/RTO 和 workload JSON；**NOT EXECUTED**。已有 demo 仅为 `HISTORICAL LIMITED` |
| Lease store unavailable：Redis/PG timeout、read/write error、partition | lease read/renew latency、error/timeout、lease-agent leadership file age/expiry、`LeaseAcquired`/`Degraded` conditions | Router fail closed；过 `renewDeadline` 写入 expired state，停止所有写路径和 background mutation；不能以 CR status/ConfigMap 继续选主 | 进入 `no-leader`，隔离不可确认的旧 holder；恢复后重新 acquire 新 epoch，不复用旧 permit/token；先做 read-only smoke 和 fencing check | Redis/PG health、lease-agent file snapshots（不含 token）、renew errors、conditions、epoch timeline、日志；**NOT EXECUTED** |
| Stale token：旧 holder renew/release/commit，或旧 WebSocket/长写请求在 promotion 后返回 | owner/token CAS rejection、observed/current epoch、`STALE_LEADER` count、commit audit log | 所有旧 token 操作被拒绝；长请求在 durable commit 前再次校验 permit；旧 WebSocket、HTTP、MySQL、RPC、Scheduler、upload、cleanup 不得提交 | 立即 stop mutation；保留拒绝证据；只让新 epoch holder promotion；若拒绝边界不明，维持 `no-leader` 并回滚到上一个已验证 immutable image | CAS responses、old/new epoch、token hash、commit IDs、connection close/reconnect logs；**NOT EXECUTED** |
| MetaStore restart + CSI volume reattachment | MetaStore readiness、RPC errors、PVC/PV events、volume attach/detach、table/partition/chunk/Job hashes | 单写 RWO PVC，无并发 RocksDB open；Router 无 MetaStore 时不写；恢复后 table IDs、chunks、Jobs、committed hashes 保持一致 | 停止流量并保持 `no-leader`；若卷/数据损坏，从隔离 namespace 的 snapshot/backup 恢复，校验 metadata/data/object hashes 后再逐步放量 | PVC/PV/CSI events、MetaStore logs、open/lock evidence、before/after hashes、restore log；**NOT EXECUTED** |
| Worker loss：import、compaction、Job heartbeat/complete 各阶段 | Worker readiness/membership、job owner epoch/attempt、heartbeat age、queue state、object/chunk/hash counts | 任务可被新 owner 安全 takeover；失联 Worker 不能完成旧 epoch commit；已提交数据不重复、未提交任务可重试或进入明确 orphaned 状态 | 停止受影响 mutation；reconcile MetaStore Jobs，再恢复 assignment；对不确定结果禁止盲重放，先做 authoritative reconciliation | Worker Pod/Node events、Job transition log、attempt/epoch、object keys/checksums、duplicate count；**NOT EXECUTED** |
| Router rollout：mixed old/new binaries、old image rollback | rollout status、ready replicas、protocol/version mismatch、epoch continuity、traffic errors、EndpointSlice | 逐步 rollout 时只有一个 write-capable Router；旧/新协议兼容或在处理前显式 fail fast；epoch 不下降，单 endpoint 保持 | 暂停 rollout，fence 新旧异常 holder，切回上一个 immutable digest 和兼容 CRD/schema；禁止回滚到 local-RocksDB 双写版本 | image digests、deployment ReplicaSets、compatibility logs、epoch/EndpointSlice timeline、rollback command/output；**NOT EXECUTED** |
| Redis/PG split brain：双后端、主从分歧、恢复后旧租约可见 | compare lease holder/epoch/token hash across configured authorities、renew/release CAS、clock/replication lag、two-holder alert | 必须有单一 authoritative lease store；分歧期间拒绝 promotion 和 commit；恢复后旧 token 不得在任一 backend 获得有效权利；split-brain overlap = 0 | 立即停止 mutation，隔离/下线不一致 backend；由平台 owner 选择并校验 authoritative store，重新 acquire 新 epoch；无法证明时保持 `no-leader` | 两端脱敏 lease snapshots、replication/health logs、CAS outcomes、holder overlap calculation、incident timeline；**NOT EXECUTED** |

## Workload assertions

| Workload | Detection | Expected result | Rollback / recovery | Evidence / status |
|---|---|---|---|---|
| Query：continuous real-data reads | request status/error class、leader epoch、result row count/hash、query gap/RTO | 切主期间允许短暂明确错误窗口，但恢复后结果 hash、table/partition/chunk metadata 与切主前一致；follower/old epoch 返回 `STALE_LEADER`，不能静默读错数据 | 重连当前 leader；若 hash 漂移或 epoch 不单调，停止流量并恢复到最近一致性 snapshot/backup | request timeline、error windows、epochs、row/table/chunk hashes、measured RTO；**NOT EXECUTED**。历史 `SELECT 1` 不能替代 |
| Write：idempotent mutation and retry | mutation ID、owner epoch、commit marker、response status、duplicate committed count | 同一 mutation 至多一次 committed；commit 前 fencing 失败可安全重试；commit 状态不确定时进入 durable `UNKNOWN`/reconciliation，不得盲重放；RPO 0 | 暂停自动 retry；按 authoritative MetaStore/commit marker reconcile；无法判定时保持 unknown/no-write，不删除 tombstone | mutation IDs、attempts、commit markers、reconciliation proof、duplicate count、RPO；**NOT EXECUTED**。Task 9 记录真实 Redis/Lua evidence 缺失 |
| Upload：mid-upload、post-upload/pre-commit、post-commit/pre-response | temp object key、checksum、publish marker、metadata commit、response/connection outcome | 临时对象不可被查询；只有 checksum 验证且 epoch permit 仍有效时发布 metadata；重连不产生重复 committed object/chunk | 保留并隔离 temp object；按 marker/hash reconcile，清理只允许由有效 leader 执行；不确定时禁止重新 publish | object key/checksum、publish/commit markers、metadata hashes、connection logs、duplicate objects；**NOT EXECUTED** |
| Retry：HTTP/WebSocket/MySQL/client retry after timeout or disconnect | retry classifier、`retryable` flag、mutation ID、old/new epoch、transport replay log | 读请求可按安全策略重试；非幂等写没有 mutation ID 时不得自动重试；有 mutation ID 也必须以 authoritative outcome reconciliation 为前提；旧连接不能跨 epoch 提交 | 关闭旧连接，重连当前 leader；对未知写结果进入 reconcile/no-write；禁止用客户端重试掩盖 lease/MetaStore 不可用 | retry decision logs、request fingerprints、mutation IDs、UNKNOWN records、final outcome and duplicate count；**NOT EXECUTED** |

## Minimum machine-readable result

每个场景应至少产出一条 JSON 记录，字段建议如下：

```json
{
  "scenario": "leader-crash-kill9",
  "status": "NOT_EXECUTED",
  "faultWindow": {"start": null, "end": null},
  "epochs": {"before": null, "after": null, "monotonic": null},
  "holders": {"overlap": null, "oldFenced": null},
  "workload": {"rpo": null, "duplicateCommitted": null, "dataHashesEqual": null},
  "rtoSeconds": null,
  "evidence": []
}
```

Task 13 的整体 acceptance 是至少 100 次重复 failover，并且 `rpo=0`、`duplicateCommitted=0`、`holders.overlap=0`，同时记录 agreed/measured RTO。当前没有任何该格式的归档输出，因此本文件中的结果字段全部保持未执行状态。
