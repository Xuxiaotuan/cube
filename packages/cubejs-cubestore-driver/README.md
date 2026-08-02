<p align="center"><a href="https://cube.dev"><img src="https://i.imgur.com/zYHXm4o.png" alt="Cube.js" width="300px"></a></p>

[Website](https://cube.dev) • [Docs](https://docs.cube.dev) • [Blog](https://cube.dev/blog) • [Slack](https://slack.cube.dev) • [Twitter](https://twitter.com/the_cube_dev)

[![npm version](https://badge.fury.io/js/%40cubejs-backend%2Fserver.svg)](https://badge.fury.io/js/%40cubejs-backend%2Fserver)
[![GitHub Actions](https://github.com/cube-js/cube.js/workflows/Build/badge.svg)](https://github.com/cube-js/cube.js/actions?query=workflow%3ABuild+branch%3Amaster)

# Cube Store Driver

MySQL protocol based Cube Store driver.

[Learn more](https://github.com/cube-js/cube.js#getting-started)

## Router HA 配置（多 Router + 主备切换）

当 `cubeStoreHost` 传入多个地址（逗号分隔）时，Driver 会通过 `GET /router/status` 进行主节点探测并做 failover。

可用环境变量：

- `cubeStoreRouterLeaderProbeUseStatus`：是否优先走 status 接口（默认 true）。
- `cubeStoreRouterLeaderStatusProbeTimeoutMs`：status 探测超时，默认 `900`。
- `cubeStoreRouterLeaderStatusMaxAgeSecs`：status 信息过期阈值（秒）。若返回时间戳过旧会视为失效；默认 `90`。设置 `0` 关闭时效检查。
- `cubeStoreRouterLeaderProbeTimeoutMs`：status 失败后 fallback 到 `SELECT 1` 探测的超时时间。
- `cubeStoreRouterLeaderProbeTtlMs`：主节点探测结果缓存 TTL。
- `cubeStoreRouterLeaderEpochFence`：是否开启 leader epoch 防陈旧（默认 true）。开启后会拒绝 `leaderEpoch` 低于当前已观察到最高值的 status 节点。
- `cubeStoreStrictWriteRetryWithoutMutationId`：是否强制约束。默认 `true`。
- `cubeStoreRouterLeaderConnectionResetOnChange`：切换 leader 后是否关闭全部旧连接重建（默认 true）。

写入一致性建议：
- 非幂等写请求建议默认 `retryable: false`。
- 如需允许重试，请通过 `query` 选项传入 `mutationId`（也会透传到 `queryTracingObj` 的 `mutationId`/`mutation_id`），并在上层对该 `mutationId` 做幂等去重（推荐 Redis/DB 记录）。
- 当 `cubeStoreStrictWriteRetryWithoutMutationId=true` 时，未带 `mutationId` 的写请求会被强制转为 `retryable: false`。

改进点：
- `GET /router/status` 返回 `leaderState`，包含 `activeLeader`/`updatedAt`，客户端会校验状态一致性。
- 发现多个 `is_leader=true` 的情况会返回“无唯一 leader”，避免误把异常主副本当作稳定入口。
- `leaderState.activeLeader` 存在时，会校验 `node_name` 与该值一致，避免脏状态误选。
- `leaderEpoch`（来自 operator 统一状态）会作为防陈旧闸门，拒绝落后任期的节点，避免把“旧 leader 回位”当作当前主入口。
- 也可以通过查询选项 `retryable: false` 强制某条 SQL 不做连接级重试（用于高风险非幂等操作）。
- `status` 无效时会回退到 `SELECT 1` probe。
- WebSocket 重连仅会重放 `SELECT/SHOW/DESCRIBE/EXPLAIN/PRAGMA` 等读请求；非幂等 SQL 在连接重置时会直接失败而不重放，从而避免切主期间 `INSERT/WRITE/QUEUE` 类写放大重复执行风险。

## 使用 Redis 做写入幂等闭环（推荐）

当 Router 做主备切换时，客户端侧可按 `mutationId` 做**幂等重放**，避免同一次写在重试/重连时重复落库。  
本地实现会在 `CubeStoreDriver` 中落到 Redis，支持以下行为：

- 首次携带 `mutationId` 的写请求会以 `mutationId` 为幂等键写 `PENDING`，执行成功后写 `COMPLETED`。
- 其他并发携带同一 `mutationId` 的请求会等待结果，直接返回 `COMPLETED` 的历史结果，或复用 `FAILED` 的失败结果。
- 若参数指纹不一致，则返回冲突错误，避免把不同语义复用为同一幂等命令。
- 连接重试/切主导致的异常不会直接标记为 `FAILED`，仅失败分支会回写失败态，默认允许重试。

环境变量配置：

- `CUBE_STORE_IDEMPOTENCY_REDIS_DSN`（推荐）：Redis DSN，如：`redis://:password@100.82.226.63:30078/0`
- `CUBE_STORE_IDEMPOTENCY_REDIS_KEY_PREFIX`：Redis key 前缀（默认 `cubejs:cubestore:mutation`）
- `CUBE_STORE_IDEMPOTENCY_COMPLETED_TTL_SECONDS`：完成记录保留秒数（默认 `86400`）
- `CUBE_STORE_IDEMPOTENCY_FAILED_TTL_SECONDS`：失败记录保留秒数（默认 `86400`）
- `CUBE_STORE_IDEMPOTENCY_PENDING_TTL_SECONDS`：进行中记录保留秒数（默认 `30`）
- `CUBE_STORE_IDEMPOTENCY_POLL_INTERVAL_MS`：并发等待轮询间隔（默认 `250`）
- `CUBE_STORE_IDEMPOTENCY_POLL_MAX_ATTEMPTS`：并发等待重试次数（默认 `120`）

注意：
- 该方案仅对 `query` 中携带 `mutationId` 的写类请求生效（`INSERT/CREATE/ALTER/QUEUE/DELETE/UPDATE/...` 等非只读 SQL）。
- 无 `mutationId` 的写请求依然按现有重试策略执行，无法保证重试幂等。

### License

Cube Store driver is [Apache 2.0 licensed](./LICENSE).
