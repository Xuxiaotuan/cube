# Task 15 Report: Raft Hot MetaStore Standby

日期：2026-08-03  
仓库快照：`codex/ha-router-experiment`  
结论：**FAIL，Raft/hot-standby 尚未交付；仅完成设计边界和执行计划**

## 1. 判定规则

- `PASS` 需要有可复核的实现、故障注入结果和归档证据；代码存在或设计完整不能单独算通过。
- 本任务是首个安全单写 RWO 发布之后的可选后续里程碑。它不是单写版本的必要条件，但它是“无 CSI detach/attach 延迟的 MetaStore hot standby”目标的必要条件。
- 当前报告没有运行测试、没有启动 Raft 集群、没有连接外部依赖，也没有把 demo、静态 manifest 或 RocksDB 备份结果扩展为 Raft 生产结论。

## 2. 当前实现核对

| 能力 | 状态 | 当前证据与准确边界 |
|---|---|---|
| MetaStore 数据引擎 | `PARTIAL` | `rust/cubestore/cubestore/src/metastore/mod.rs` 仍导出既有 RocksDB 表、WAL 和 snapshot 模块；这是当前单写 MetaStore，不是复制状态机。 |
| MetaStore 部署 | `SINGLE_WRITER_ONLY` | `operators/cube-operator/config/metastore/statefulset.yaml` 明确为 `replicas: 1`、`ReadWriteOnce` PVC，且 sole process 独占 authoritative RocksDB。demo 的 `run.sh` 也把单副本作为配置断言。 |
| Raft 模块/依赖 | `NOT_IMPLEMENTED` | 未发现 `rust/cubestore/cubestore/src/metastore/raft/`、Raft 配置入口或受维护 Raft 依赖的接线。 |
| 复制命令格式 | `NOT_IMPLEMENTED` | 现有 MetaStore mutation 尚未统一封装为带 schema version、command ID、precondition 和确定性 payload 的 replicated command。 |
| quorum commit | `NOT_IMPLEMENTED` | 没有三节点 membership、majority commit、commit index 或“仅 quorum commit 后 apply”的实现和测试。 |
| 日志/快照 | `PARTIAL` | 现有 RocksDB WAL/备份/snapshot 能支持单写恢复；没有包含 Raft term、last-included-index、membership、dedup tombstone 的 Raft log/snapshot，也没有 snapshot install 或 node replacement 证据。 |
| 选主 | `NOT_IMPLEMENTED` | operator 的 Router `LeaseRecord.Epoch/Token` 和 `status.leaderEpoch` 是外部 lease/fencing 机制，不是 MetaStore Raft term/index，也不能证明 MetaStore 只有一个 Raft leader。 |
| fencing | `NOT_IMPLEMENTED_FOR_RAFT` | `internal/leadership` 提供了 lease contract，但 MetaStore 没有把 Raft term/index 作为 authoritative Router fencing epoch；没有 stale leader 在 quorum、commit 和长请求边界上的拒绝证据。 |
| 迁移/回滚 | `NOT_EXECUTED` | 没有从单写 RWO 一致性点迁移到三节点集群的演练，也没有证明 Raft 写入后的安全回滚；旧 RWO 只能作为受控 rollback source，不能与新集群并发写。 |
| 验收证据 | `NOT_EXECUTED` | 没有 `metastore_raft_failover.rs`、Jepsen-style partition 结果、process-death 结果、hash 对账或机器可读 Raft acceptance JSON。 |

## 3. 本次文档交付

- [ADR-015 Raft hot-standby MetaStore](./ADR-015-raft-hot-standby-metastore.md) 定义数据模型、日志和快照、quorum、选主、fencing、迁移、回滚和验收门禁。
- 本报告明确当前实现仍停留在 single-writer RWO，避免把现有 Router demo 的 epoch 切换误标为 Raft hot standby。
- `FINAL-HA-ACCEPTANCE.md` 已将 Task 15 从“缺少上下文”修正为“设计已记录但实现和验收失败”，并保留首个单写发布与后续 hot-standby 的边界。

## 4. 剩余阻断

1. **复制状态机未实现**：所有会改变 MetaStore 的 table、partition、chunk、job、upload、WAL、pre-aggregation 和 dedup 状态都必须进入同一确定性命令流。
2. **Raft 运行时未选型和接线**：需要锁定受维护实现、transport、durability API、membership change 和 snapshot install 行为，不能以自制选主替代 Raft。
3. **RocksDB apply 边界未建立**：必须证明 follower 不在本地先写，leader 也只在 majority commit 后 apply；重放必须产生相同 state hash。
4. **Router fencing 未绑定 Raft epoch**：所有业务写入口和 background mutation 必须在提交前确认当前已提交 term/index；旧 leader 只能返回 stale/fenced，不得依赖 Kubernetes label 或 ConfigMap。
5. **迁移与回滚未演练**：必须有冻结写入、源快照校验、三节点追平、切流、旧 RWO 保留和失败回退的带时间戳证据。
6. **故障验收未执行**：至少要覆盖一节点丢失、quorum 丢失、网络分区、旧 leader 恢复、重复 command、snapshot restore、成员替换和 mixed-version rollback。

## 5. 可执行交付切片

| 切片 | 交付物 | 通过条件 |
|---|---|---|
| 15.1 命令与状态模型 | `CommandEnvelope`、确定性序列化、command ID 去重、版本化 migration | 相同日志在三节点产生相同 state hash；重复 command 只产生一次 committed mutation。 |
| 15.2 日志与快照 | durable log、commit/apply pipeline、带校验和的 snapshot、snapshot install、成员恢复 | kill -9、重启和 snapshot restore 不丢 committed entry；落后节点可在不双写的情况下追平。 |
| 15.3 quorum 与选主 | 三节点 membership、pre-vote、term/index、majority commit、joint membership change | 一节点故障仍可线性化读写；少于 quorum 时拒绝写，不生成“本地已提交”假结果。 |
| 15.4 fencing 接线 | Raft epoch 到 Router permit 的映射，所有入口的 commit-time guard，旧连接关闭/拒绝 | 旧 term、旧 index、旧 permit 在 HTTP、WebSocket、MySQL、RPC、upload、Job 和 cleanup commit 前全部被拒绝。 |
| 15.5 迁移与回滚 | 单写 RWO 到 Raft 的 offline/冻结写迁移 runbook，Raft snapshot 回建 RWO 的回滚 runbook | 完成一次 staging cutover 和一次失败回滚；table/chunk/job/object hash 与 committed index 对账一致。 |
| 15.6 生产验收 | machine-readable failure matrix、partition/process-death 测试、镜像/协议/快照版本清单 | 所有硬门禁 PASS，RPO=0、duplicate committed=0、split-brain overlap=0，且 measured RTO 不超过发布前声明的 `RTO_MAX_SECONDS`。 |

## 6. 交付判定

在 15.1-15.6 全部完成、证据归档并由 Storage、Platform、Cube API、Application owner 签字前：

- 不得把三节点 StatefulSet 或三个进程启动写成 hot standby 已交付。
- 不得让旧 RWO MetaStore 与 Raft 集群同时接受写入。
- 不得因 Raft 没有 quorum 而通过手工改 label、selector、ConfigMap 或 Router lease 强行 promotion。
- 生产发布结论保持 `FAIL/NO-GO`；单写 RWO 路线仍按 `FINAL-HA-ACCEPTANCE.md` 的独立 hard gates 判定。

