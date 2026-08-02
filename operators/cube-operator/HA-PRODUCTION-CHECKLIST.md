# Cube Router HA 生产验收清单

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
- 运行时状态仍非共享：缓存、上传 temp、队列上下文仍在 Router 实例内存/本地文件，切主后可出现短时不一致与重放窗口。
- 未完成写幂等统一协议（idempotency key + 重试去重）。
- 分布式并发写并发冲突恢复（事务语义）仍需外部补充。
- 若必须上线生产写工作流，必须同步完成：
  - 写幂等键（业务 key 或事务 token）
  - 写入去重/幂等持久存储（Redis/DB）
  - 切主期间客户端关闭旧连接并重新建连（避免跨 leader 重连复用）

## 七、当前实现可交付范围
- 可交付目标：主备角色可见性、角色一致性切换、/router/status 与 CR 状态链路闭环、自动化演练脚本。
- 不可单点保证目标：切主期间写请求幂等一致性、内存级运行时状态跨实例恢复。
- 上线建议：将写入链路分层治理（应用侧幂等、网关去重、数据库防重放）并与本方案联动。
