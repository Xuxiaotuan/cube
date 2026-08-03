# ADR-015: Raft Hot-Standby MetaStore

- 日期：2026-08-03
- 状态：Proposed，**实现未开始**
- 范围：CubeStore authoritative MetaStore；不改变首个安全单写 RWO 发布的回滚路径

## 决策摘要

为后续 hot-standby 里程碑采用三节点 Raft 集群，使用 majority quorum 提交 MetaStore mutation；只有提交后的日志 entry 才能 apply 到本地 RocksDB。Raft `term` 与已提交 `index` 组成 authoritative fencing epoch，并由 Router 在每个受保护写入的 commit 边界重新校验。

生产默认使用 3 个跨故障域节点，quorum 为 2。少于 quorum 时 MetaStore 进入 no-write/read-only 状态，不能靠 Kubernetes 状态、Pod label、ConfigMap 或外部 Router lease 伪造 promotion。当前 `replicas: 1` 的 RWO 部署保留为独立的 single-writer rollback mode，不与 Raft 集群并发写。

## 背景和非目标

当前 MetaStore 是单写 RocksDB 服务，部署在 RWO PVC 上；已有 Router lease/fencing 记录可保护 Router，但不是 MetaStore 的复制日志或 Raft term。现有 RocksDB WAL、backup 和 snapshot 只能作为单写恢复材料，不能证明 quorum commit、线性化读写或 hot standby。

本 ADR 不把 CubeStore data files、object storage 或 Worker 执行状态直接复制进 Raft；Raft 只复制 authoritative metadata、任务提交状态、发布 marker、dedup/tombstone 和恢复所需的版本信息。大对象仍由 object storage 保存，MetaStore 只提交其不可变引用和校验和。

## 数据模型和命令边界

所有改变 authoritative MetaStore 的操作必须变成确定性的 `CommandEnvelope`：

```json
{
  "schema_version": 1,
  "cluster_id": "stable-cluster-id",
  "command_id": "client-or-mutation-id",
  "kind": "CreateTable|CommitChunk|AssignJob|PublishObject|...",
  "payload": "canonical-bytes",
  "precondition": {"state_version": 42, "owner_epoch": 17},
  "requested_by": "router-or-worker-id"
}
```

规则如下：

- `payload` 使用 canonical serialization；禁止把本地时间、随机数、线程顺序或未排序 map 直接带入 apply。
- `command_id` 在 committed dedup table 中永久保留到 retention policy 允许安全清理；未知结果必须保留 tombstone，禁止通过 TTL 到期盲目重放。
- state record 至少包含 `state_version`、owner/fencing epoch、mutation status、object/chunk checksum、job attempt 和 schema version。
- apply 只由确定性 state machine 执行；同一 `log_index` 只能成功 apply 一次，并记录 apply result hash。
- migration command 必须显式版本化；不兼容 schema 先通过兼容读写阶段，不能让新二进制直接解释旧 payload。

Raft metadata 还要持久化 `cluster_id`、node membership、last applied index、snapshot marker、current term、voted-for 和 fencing record。任何本地 RocksDB 状态若无法与 committed log/index 对账，节点必须拒绝 ready。

## 日志、提交和快照

1. Leader 先将带 term/index 的 entry durable append，再向 followers replicate；entry 只有在多数节点 durable ack 后才进入 `commit_index`。
2. Leader 和 follower 都只能 apply `index <= commit_index` 的 entry；客户端成功响应必须晚于 state machine apply 和 result marker durable。
3. 每个 entry 保存 schema version、command envelope、term、index、checksum 和 apply-result hash；checksum 不一致时停止该节点并报警，不能静默修复。
4. Snapshot 是 state machine 的一致性点，至少包含 last-included-index/term、cluster membership、dedup/tombstone、fencing record、schema version、state hash 和 checksum。
5. snapshot 生成期间必须固定一致性 index；snapshot install 后先校验 hash，再接收后续 log。旧 log 只能在已有足够 snapshot/backup 且 peer 不需要时 compact。
6. Raft snapshot 与现有 `RocksStore` backup/snapshot 分开命名和保留；后者可以作为底层导入/恢复工具，但不等于 Raft snapshot。
7. 节点替换先以 learner 身份接收 snapshot 和 log，达到 `match_index >= commit_index` 并通过 hash 校验后，再以 joint consensus 加入 voting membership；移除旧节点也必须经过 joint consensus。

## Quorum、读和选主

- 生产集群固定从 3 节点起步，使用 `floor(N/2)+1` quorum；不支持两节点生产模式。
- 写入需要 leader 证明当前 term 的 quorum commit；任何无法达到 quorum 的请求返回明确的 unavailable/no-write，不返回“已提交”。
- Router 读取 authoritative metadata 使用 leader ReadIndex/等价线性化读；follower stale read 只能用于明确标注为非 authoritative 的运维观察。
- 采用持久化 term/vote、pre-vote、随机 election timeout 和 leader completeness；candidate 必须拥有至少最新的 committed log 才能赢得选票。
- 新 leader 在服务业务写入前提交并 apply 当前 term 的 no-op/leadership record，并取得可观测的 ready 条件；Kubernetes readiness 不得单独触发流量切换。
- 成员变更一次只做一个 joint-consensus 变更，禁止同时替换两个 voting node；跨故障域和时钟/网络健康度是部署门禁。

## Fencing 和 Router 接线

将 `(raft_term, committed_index)` 映射为单调的 `FencingEpoch`，并在 permit 中携带 `cluster_id`、leader ID、term、index 和签名/token hash。数值比较必须能识别 term 变化；同 term 下 index 只能前进。

每个 Router/Worker 的受保护 mutation 必须：

1. 在请求开始时获取当前 permit，但不能只依赖开始时的检查。
2. 在进入 MetaStore commit 前执行线性化 leadership check，确认 permit 的 term/index 仍是当前已提交 epoch。
3. 将 `owner_epoch`/`command_id` 写入同一 replicated command，令旧 epoch 无法提交。
4. 在 promotion 或 fencing loss 时关闭长连接、停止 background mutation，并把不确定结果置为 durable `UNKNOWN`，交由 authoritative reconciliation 处理。

MetaStore API 对旧 term、旧 index、错误 cluster ID、错误 token、非 leader 本地写和无法证明 quorum 的请求统一返回可观测的 `STALE_LEADER`/`NO_QUORUM` 类错误。Router 的外部 Redis/PG lease 仍可在 single-writer rollback mode 使用，但在 Raft mode 不得成为第二个独立的写授权源。

## 从 RWO 单写迁移

迁移禁止双写，按以下顺序执行：

1. 宣布 maintenance/no-write，停止 mutation、upload publish、refresh 和 Job submission；保留并记录最后已确认的 source state/log/snapshot ID。
2. 在 source MetaStore 生成一致性 snapshot/backup，记录 metadata、table/partition/chunk/job/object hashes、schema/migration version 和 source fencing epoch。
3. 在隔离 namespace 启动 3 个 Raft node，以一个 bootstrap leader 导入 snapshot；其余节点以 learner 追平并完成 state hash、last-applied index、membership 校验。
4. 执行 process death、snapshot restore、one-node loss 和 stale-source write probe；任何 hash 漂移、旧 source 可写或 epoch 回退都停止迁移。
5. 通过版本化配置把 Router/Worker 的 MetaStore endpoint 切到 Raft service；只有新 leader 的 ready/linearizable probe 和唯一业务 endpoint 通过后才解除 no-write。
6. 保留旧 RWO PVC 为不可写 rollback source，记录切换 index 和 image/CRD/schema 版本；不得让旧 source 重新获得 lease 后接收流量。
7. 迁移证据必须包含命令输出、时间线、脱敏 token hash、EndpointSlice、leader/term/index、RPO/RTO、对象 checksum 和最终对账结果。

## 回滚

- **切换前**：Raft 集群未接收业务写时，可丢弃隔离集群并恢复 source RWO；source 仍需重新 acquire 新的 single-writer epoch。
- **切换后**：不能直接把旧 RWO promotion，也不能回放未经对账的客户端请求。先停止业务写、fence Raft leader/Service，并从最新一致的 Raft snapshot/log 生成新的 RWO restore volume；校验 committed index、state hash 和业务 hashes 后，才允许 single-writer mode 接管。
- **quorum 丢失**：保持 no-leader/no-write；恢复一个可证明的 majority 或从最近一致 snapshot 恢复，禁止手工提高 epoch 绕过 quorum。
- **版本回滚**：只回滚到已验证兼容的 immutable image、command schema、CRD 和 snapshot format；若日志格式不兼容，先按迁移/恢复流程导出到兼容 snapshot，不直接打开旧日志。
- **不确定状态**：保留 command ID、commit marker 和 tombstone；在 authoritative reconciliation 完成前禁止自动 retry、object republish 或 Job duplicate commit。

## 验收门禁

以下每项都要有机器可读记录和可复核日志；缺一项即 `FAIL/NO-GO`：

| 门禁 | 必须证明 |
|---|---|
| Determinism | 三节点对同一 log replay 得到相同 state hash、apply hash 和 metadata versions。 |
| Quorum safety | 3 节点丢 1 节点仍可线性化；丢 2 节点无写入成功；无 quorum 不产生 committed ack。 |
| Election/partition | 网络分区、旧 leader 恢复、重复选举和 process death 不出现双 leader commit、term 回退或 lost committed entry。 |
| Fencing | 旧 term/index/permit 在所有 Router/Worker 入口 commit 前被拒绝；split-brain overlap 为 0。 |
| Idempotency | duplicate command、timeout、断线重试和 durable `UNKNOWN` 不产生重复 committed mutation。 |
| Snapshot/recovery | snapshot install、log truncation、节点替换、kill -9 和 clean namespace restore 保留 committed metadata 和 hashes。 |
| Migration/rollback | 完成一次 RWO -> Raft cutover 和一次失败回滚；旧 RWO 从未与 Raft 并发写。 |
| Compatibility/operations | immutable image、协议/schema 版本、backup retention、alerts、runbook、owner sign-off 齐全。 |
| Release SLO | `rpo=0`、`duplicateCommitted=0`、`holders.overlap=0`，并记录 measured RTO；RTO 不得超过发布前声明的 `RTO_MAX_SECONDS`。 |

验收测试至少覆盖 100 次重复 failover，以及 leader crash、网络分区、Redis/PG 不可用时的 mode separation、snapshot restore 和 node replacement。任何只验证进程存活、`SELECT 1`、manifest dry-run、单次无故障查询或本地 mock 的结果都不能替代上述门禁。

## 分阶段实施顺序

1. 固定命令 schema、状态 hash 和 mutation idempotency contract。
2. 接入受维护 Raft 实现，完成 durable log、quorum apply、snapshot 和 membership。
3. 在隔离环境完成 partition/process-death/restore 测试，再接 Router fencing。
4. 完成 RWO 冻结迁移、single-writer rollback 和 mixed-version 演练。
5. 归档 acceptance JSON 与 owner sign-off 后，才允许把 Raft mode 标为 production capable。

