# Cube Router HA 生产验收清单

> 版本说明：以下第一至第五节是待执行的生产门禁，不是已通过结果。历史六项业务验收与最新控制面源码修改分别见 [演示报告](HA-ROUTER-K8S-DEMO.md)，不得混用不同镜像的证据。

## 一、基础可用性（必须）
- [ ] `operators/cube-operator` 部署成功，CR `status.leader` 与 `status.leaderEpoch` 有持续更新。
- [ ] `CUBESTORE_ROUTER_ROLE_STRICT=true` 已开启。
- [ ] 每个 Router Pod 通过 `CUBESTORE_ROUTER_ROLE_FILE` 读取同一 `route-role.json`，状态源为文件且可被更新。
- [ ] `cube-router-leader` Service 的 Endpoints 在切主后不出现空窗超过 SLO（可在演练脚本中监控）。
- [ ] 任意时刻 `kubectl get pods -l app=cube-router -l cubestore.io/router-role` 最多一个 `leader`。

## 二、数据一致性（必须）
- [ ] 只读查询：切主前后结果哈希一致。
- [ ] 通过 `data-consistency-check.sh` 进行至少 20 次连续切主 + 读一致性验证。
- [ ] 写操作在应用侧必须显式使用 `retryable: false`（或幂等机制）——当前版本不提供全局写幂等协议。

## 三、故障场景（必须）
- [ ] leader Pod 被杀后，`leaderEpoch` 递增且 `status.leader` 切换到新副本。
- [ ] 切主后未出现 `old leader` 持续占用 `leader` 标签。
- [ ] Router 进程异常重启期间，无误入非 leader 的长连接执行 write。

## 四、安全与容量（建议）
- [ ] Operator、CRD、Router、Demo Namespace RBAC 最小化。
- [ ] 添加告警：
  - `/router/status` 无响应
  - `leader` 标签抖动频率异常
  - 切换超时
  - `CubeStoreConnectionError` 比例上升
- [ ] 为切主事件加审计日志（变更链路、leaderEpoch、参与副本）。

## 五、上线门禁（必须）
- [ ] 在 staging 完成：
  - 30 分钟 soak + 5+ 次切主。
  - 30% 读流量 + 10% 写流量（带重放保护）
  - 网络抖动注入（短时 1~3 分钟）
- [ ] 若未接入外部写幂等层，暂时将风险级别标记为“只读主导 / 可用性优先”

## 六、未闭环（必须记录）
- 文件导入型预聚合已有共享 CacheStore 构建身份、不可变清单、阶段记录与远端上传回执；不是所有状态都在 Router 本地。连接上下文、未确认本地上传仍可能丢失。
- 未完成写幂等统一协议（idempotency key + 重试去重）。
- 分布式并发写并发冲突恢复（事务语义）仍需外部补充。
- 对未被文件导入恢复协议覆盖的普通业务写入，应按其存储语义设计：
  - 写幂等键（业务 key 或事务 token）
  - 写入去重或事务保护（按业务选择，不要求为 Router HA 额外引入 Redis/PG）
  - 切主期间客户端关闭旧连接并重新建连（避免跨 leader 重连复用）

## 七、当前实现可交付范围
- 已有候选实现：Router 主备与文件导入型预聚合恢复；历史六项真实场景有分批验收证据，尚非同一冻结发布产物的完整生产验收。
- 不可单点保证目标：切主期间写请求幂等一致性、内存级运行时状态跨实例恢复。
- 未完成门禁：控制面异常恢复测试、Refresher 自身崩溃恢复、权威存储跨节点恢复、恢复记录安全 GC、长期容量与完整故障矩阵。ProductionReady 仍不得标记为 True。
