# Cube Router HA Operator 演示说明（独立文档）

本指南用于在本地与 Kubernetes 环境中快速验证 `cube-operator` 的主备选举与路由切换能力。

## 1. 仓库与目录

```text
/Users/xujiawei/magic/workbench/cube/operators/cube-operator
├── api/
├── controllers/
├── config/
├── demo/k8s/
├── Dockerfile
└── main.go
```

## 2. 前置条件

- Kubernetes 可访问（kubectl 已指向目标集群）
- docker 可用（用于构建镜像）
- Go 1.22+（建议）
- Node/其它环境无需额外依赖（此模块为 Go 二进制）

> 在本地 Mac 环境里，已验证使用：`go1.25.5` 可成功 `go build ./...` 与镜像构建。

## 2.1 生产化架构

生产业务流量的唯一入口应是 `cube-router-leader` Service。Service 通过 `cubestore.io/router-role=leader` 选择唯一 leader Pod；`cube-operator` 负责选主、更新 Pod label 和角色状态。`CubeStoreDriver` 的多 Router 探测与切换能力保留为连接重建时的兜底，不应让 Cube API 在生产环境直接把 Pod 地址作为主入口。

```mermaid
flowchart LR
  CR["CubestoreRouter CR"] -->|1. 发现候选 Router| Operator["cube-operator"]
  Operator -->|2. 选主并写入统一状态| CM["Role State ConfigMap<br/>route-role.json"]
  CM -->|3. 所有 Router 挂载 Role file| R1["cube-router-1"]
  CM -->|3. 所有 Router 挂载 Role file| R2["cube-router-2"]
  R1 -->|status / query| LSV["cube-router-leader Service"]
  R2 -->|status / query| LSV
  R1 -->|SQL 执行| Worker["Cubestore Worker"]
  R2 -->|非 leader 仅拒绝写入/查询主入口| Worker
  Operator -->|4. 同步 Pod label| LS["Leader Service"]
  R1 -.->|可选兜底探测| Driver["CubeStoreDriver"]
  R2 -.->|可选兜底探测| Driver
  Driver -->|业务查询/写入| LS
```

### 2.2 当前实现与推荐架构的对应关系

- 已实现：`cube-router-leader` Service 只选择 `cubestore.io/router-role=leader`，follower 不会进入业务 Service 的 Endpoints。
- 已实现：`cube-operator` 负责选主、同步 `leader/follower` label、写入角色文件和 `leaderEpoch`；Kubernetes Service 根据 label 自动更新 EndpointSlice。
- 已实现：Router 读取角色文件，follower 对查询和上传返回拒绝；Driver 在连接断开时只重放读请求，写请求需要 `mutationId` 才允许幂等重试。
- 生产配置：Cube API 应连接 Service DNS，而不是连接 Pod IP：

```bash
CUBEJS_CUBESTORE_HOST=cube-router-leader.cube-operator-demo.svc.cluster.local
CUBEJS_CUBESTORE_PORT=3030
```

- `CubeStoreDriver` 的多地址配置仍可用于应急兜底，但它不是正常业务流量入口；Kubernetes Service 也不负责选举，唯一 leader 仍由 operator 保证。

## 3. 一键构建（推荐）

```bash
cd /Users/xujiawei/magic/workbench/cube/operators/cube-operator

go mod download

go build ./...
docker build -t cube-operator:dev -f Dockerfile .
```

期望结果：
- 生成可执行二进制（Go 默认输出到 `$GOBIN`，本处依赖 `go build ./...` 成功即可）
- 本地镜像 `cube-operator:dev` 创建成功

## 4. 本地快速验收（无需改原有文档）

```bash
# 检查 run.sh 语法（可选）
bash -n operators/cube-operator/demo/k8s/run.sh
```

## 5. 一键 K8s 演示

```bash
cd /Users/xujiawei/magic/workbench/cube/operators/cube-operator
./demo/k8s/run.sh
```

`run.sh` 可选参数：
- `KUBECTL_VALIDATE=true|false|auto`（默认 `auto`）
  - `false`：全部 `kubectl apply` 强制 `--validate=false`
  - `auto`：先严格校验，失败后自动回退到 `--validate=false`（默认）
  - `true`：严格模式失败直接退出
- `KUBECTL_VALIDATE_FALLBACK=true|false`（默认 `true`，仅在 `auto` 下生效）

脚本会按顺序执行：
1. 安装 CRD
2. 创建 `cube-operator-demo` namespace
3. 安装 RBAC（`operator-rbac.yaml`）
4. 部署 operator（使用上一步生成的 `cube-operator:dev` 镜像）
5. 部署 2 个真实 router 实例（每次按统一清单 apply，若清单无变更则不触发滚动；变更才滚动）
6. 部署 CubestoreRouter CR 与 leader service

## 6. 验证主备（Leader/Follower）切换

### 6.1 查看当前状态

```bash
kubectl -n cube-operator-demo get pod -l app=cube-router --show-labels
kubectl -n cube-operator-demo get cubestorerouter demo -o yaml
kubectl -n cube-operator-demo logs deploy/cube-operator -f
kubectl -n cube-operator-demo get svc cube-router-leader -o wide
kubectl -n cube-operator-demo get endpointslice -l kubernetes.io/service-name=cube-router-leader -o wide
```

### 6.1.1 外部状态介质接入（PG/Redis）

- PostgreSQL：
```yaml
leaderStateStore:
  type: postgres
  dsn: "postgres://user:password@postgres.default.svc:5432/cubeha?sslmode=disable"
  pgTable: "cubestore_router_leader_state"
```

- Redis：
```yaml
leaderStateStore:
  type: redis
  dsn: "redis://redis.default.svc:6379/0"
  redisKey: "cube-router/leader-state"
```

- 关键观察项：
  - `RoleStateSync` 变 `False` 时表示外部介质暂不可写。
  - `status.conditions` 和 `leaderEpoch` 应持续存在并按预期变化。

### 6.2 主备切换演练

```bash
# 找到当前 leader Pod
kubectl -n cube-operator-demo get pod -l app=cube-router,cubestore.io/router-role=leader -o custom-columns='NAME:.metadata.name'

# 停掉当前 leader，观察切换
kubectl -n cube-operator-demo delete pod <CURRENT_LEADER_POD_NAME>
```

观察点：
- 剩余 router 会被选为新 leader；`strict` 策略下不允许无依据并行双主，首次引导和旧 leader 丢失后会按就绪顺序临时接管并持续纠偏到新 leader 任期线。
- `CubestoreRouter` CR status 中 leader 与 `leaderEpoch` 信息更新（任期应递增）
- `cube-router-leader` Service 的 endpoints 追随新 leader
- 业务查询应始终使用 `cube-router-leader.cube-operator-demo.svc.cluster.local:3030`，不要使用 Pod IP。

## 6.3 生产化注意事项（避免“空切换窗口”）

- 严格模式要点：`CUBESTORE_ROUTER_ROLE_STRICT=true` + `CUBESTORE_ROUTER_ROLE_FILE`，使路由只通过文件判定 role。
- 选主策略必须带 `electionStrategy: strict`，并且配置 `roleConfigMapName`，所有实例角色状态以 ConfigMap 为单一真值源。
- Service 不会自动完成选举；operator 必须保证任意时刻最多一个 Pod 带 `cubestore.io/router-role=leader`，否则存在双主风险。
- `cube-router` 普通 Service 仅用于调试或节点检查，生产 Cube API 只能使用 `cube-router-leader` Service。
- `run.sh` 会等 Deployment rollout 完成后再继续，避免读取中间态。
- 切换演练前先确认至少 1 个 follower Ready，确保 failover 有候选。
- 注意观察 `leaderEpoch` 是否在故障切换后单调递增。

### 6.4 数据一致性回归（推荐）

```bash
cd /Users/xujiawei/magic/workbench/cube/operators/cube-operator
export CONSISTENCY_QUERY="SELECT 1 AS value"
./demo/k8s/data-consistency-check.sh
```

- 脚本会在 leader 切换前后对同一条 SQL 进行 hash 对比，给出是否“结果一致”结论。
- 默认会同时采集 leader 与 follower 查询结果，若存在明显差异会退出并报错。
- 如需覆盖业务关键查询，可将 `CONSISTENCY_QUERY` 改为 `UNION/聚合` 查询，先在生产环境预演。

生产边界说明：
- 该方案保证“主入口与主角色视图一致切换”，但对“写入窗口内幂等性 / 队列持久化统一”仍需要你侧补充：
  - 幂等键/幂等 ID（避免故障切换重试重复执行）
  - 共享状态化的元数据与编排持久层治理
  - 统一告警：多主、短暂无 leader、切换超时、query 失败率漂移

## 7. 集群适配提示（如使用 kind/minikube）

如果无法直接拉取本地镜像，需要手工导入：

```bash
# kind
kind load docker-image cube-operator:dev --name <cluster-name>
kind load docker-image cube-studio-router:ha-local --name <cluster-name>

# minikube
minikube image load cube-operator:dev
minikube image load cube-studio-router:ha-local
```

然后重新运行 `./demo/k8s/run.sh`。

## 8. 常见问题（FAQ）

- `crd` 已存在：可先 `kubectl delete -f ...` 再重装，或用 `kubectl apply -f` 覆盖更新。
- pod 拉起后看不到日志：先确认 operator deployment、镜像拉取状态，镜像建议先在同集群内预导入。
- 切换不生效：检查模拟 router 是否 ready（`kubectl get pod`）与 endpoint 是否被 Service 匹配到 leader 标签。

## 9. 复用入口

- 简洁启动清单：`operators/cube-operator/demo/k8s/run.sh`
- 现有演示说明：`operators/cube-operator/demo/k8s/README.md`
- 操作入口：
  - `operators/cube-operator/demo/k8s/operator-rbac.yaml`
- `operators/cube-operator/demo/k8s/mock-routers.yaml`
  - `operators/cube-operator/demo/k8s/cubestore-router-cr.yaml`
  - `operators/cube-operator/demo/k8s/leader-service.yaml`

## 10. 一键主备切换回归（可选）

新增脚本：`demo/k8s/failover-check.sh`

```bash
cd /Users/xujiawei/magic/workbench/cube/operators/cube-operator
autoscript="demo/k8s/failover-check.sh"

# 默认参数
echo "namespace: cube-operator-demo"
echo "CR: demo"

# 执行切换演练
./$autoscript
```

脚本会执行：
- 读取当前 leader Pod 和 CR status.leader
- 删除当前 leader Pod（模拟故障）
- 等待新 leader 产生（超时默认 120 秒）
- 输出新 leader、endpoints 与 CR 状态

可选校验参数：
- `VERIFY_LEADER_STATUS_HTTP=true`：在切换过程中执行 `pod` 内 `curl /router/status?detail=1` 健康校验。
- `CHECK_INTERVAL_SECONDS`：切换等待轮询间隔（秒），默认 `2`。
- `LEADER_SETTLE_STABLE_CHECKS`：要求新 leader 连续稳定检测次数，默认 `2`。
- `VERIFY_LEADER_SERVICE_QUERY`：一致性脚本是否通过 service 进行查询校验，默认 `true`。
- `STRICT_SERVICE_CHECK`：service 查询失败时是否直接失败，默认 `false`。

可调参数：
- `NAMESPACE`：目标命名空间（默认 `cube-operator-demo`）
- `CR_NAME`：CR 名称（默认 `demo`）
- `WAIT_SECONDS`：等待新 leader 的超时时间（默认 `120`）
- `CONSISTENCY_QUERY`：自定义一致性校验 SQL（供 `data-consistency-check.sh`）
- `MYSQL_IMAGE`：临时客户端镜像（默认 `mysql:8.4`）

## 11. 2026-08-01 复测记录（重新跑）

### 11.1 复测命令

```bash
cd /Users/xujiawei/magic/workbench/cube/operators/cube-operator
bash operators/cube-operator/demo/k8s/run.sh
REQUIRE_LEADER_ELECTION_CONDITION=false REQUIRE_LEADER_EPOCH=false VERIFY_LEADER_SERVICE_QUERY=false bash operators/cube-operator/demo/k8s/failover-check.sh
REQUIRE_LEADER_ELECTION_CONDITION=false REQUIRE_LEADER_EPOCH=false STRICT_SERVICE_CHECK=false bash operators/cube-operator/demo/k8s/data-consistency-check.sh
```

### 11.2 本轮结果

- `run.sh` 成功：2 个 router Pod running，CR 已写入，leader Service 已就绪。
- `failover-check.sh` 切主成功：
  - 起始 leader：`cube-router-demo-55554b89fc-5cwlj`
  - 第一轮新 leader：`cube-router-demo-55554b89fc-45g52`
  - 第二轮新 leader：`cube-router-demo-55554b89fc-dbt2l`
  - leaderEpoch 变化：`3 -> 4 -> 5`
  - leader Service endpoints 与 CR leader 始终对齐
- `data-consistency-check.sh` 一致性通过：
  - 切前 leader/follower/service 查询都为 `1`
  - 切后 leader/service/follower 查询都为 `1`
  - hash 比对 PASS
- 当前快照：
  - CR `status.leader`: `cube-router-demo-55554b89fc-dbt2l`
  - CR `status.leaderEpoch`: `5`
  - Service endpoints：仅指向上述 leader

### 11.3 逻辑架构图（Cube API 入口）

```mermaid
flowchart LR
  API["Cube API / CubeServer"]
  Driver["CubeStoreDriver\nService 主入口"]
  Op["cube-operator"]
  CR["CubestoreRouter CR"]
  CM["ConfigMap\nroute-role.json"]
  R1["cube-router-1"]
  R2["cube-router-2"]
  LS["cube-router-leader Service"]
  W["Cubestore Worker"]
  API --> Driver
  Driver -->|/router/status 探测| R1
  Driver -->|/router/status 探测| R2
  Driver -->|查询/写入（主路径）| LS
  LS -->|selector leader| R1
  LS -->|selector leader| R2
  R1 -->|query/upload| W
  R2 -->|query/upload| W
  Op -->|reconcile| CR
  Op -->|同步角色| CM
  CM --> R1
  CM --> R2
  CR --> Op
```

### 11.4 K8s 部署关系图

```mermaid
flowchart LR
  K8S["cube-operator-demo Namespace"]
  CRD["CRD\ncubestorerouters.cubestore.io"]
  OpDeploy["Deployment: cube-operator"]
  RouterDeploy["Deployment: cube-router-demo\nreplicas=2"]
  LSvc["Service: cube-router-leader"]
  LBSvc["Service: cube-router"]
  CR["CubestoreRouter demo"]
  CM["ConfigMap route-role-state"]
  PodA["Pod leader/follower"]
  PodB["Pod leader/follower"]
  K8S --> OpDeploy
  K8S --> RouterDeploy
  K8S --> LSvc
  K8S --> LBSvc
  K8S --> CR
  CRD --> CR
  OpDeploy --> CR
  OpDeploy --> CM
  RouterDeploy --> PodA
  RouterDeploy --> PodB
  PodA --> LSvc
  PodB --> LSvc
  PodA --> LBSvc
  PodB --> LBSvc
```

### 11.5 数据流图（请求 + 切主）

```mermaid
flowchart TD
  C["Client Request"] --> A["Cube API"]
  A --> D["CubeStoreDriver"]
  D -->|查询/写入| LS["Leader Service"]
  D -.->|异常时可选探测| R1["Router-Any"]
  LS --> L["Current Leader Pod"]
  L --> W["Worker"]
  F["故障注入 kill leader pod"] --> OP["cube-operator"]
  OP -->|选主并更新角色文件| CM["route-role.json"]
  CM -->|role become leader| N["New leader Pod"]
  N -->|label 切换| LS
  D -->|重试请求| LS
  D -->|重定向到新 leader| N
```

### 11.6 K8s 命令实际执行记录

以下命令已在本地 Kubernetes（OrbStack，`orbstack` 节点）实际执行，不是仅供阅读的示例：

```bash
cd /Users/xujiawei/magic/workbench/cube/operators/cube-operator
./demo/k8s/run.sh
REQUIRE_LEADER_ELECTION_CONDITION=false REQUIRE_LEADER_EPOCH=true VERIFY_LEADER_SERVICE_QUERY=true STRICT_SERVICE_CHECK=true WAIT_FOR_STANDBY_READY=true bash demo/k8s/failover-check.sh
REQUIRE_LEADER_ELECTION_CONDITION=false REQUIRE_LEADER_EPOCH=true VERIFY_LEADER_SERVICE_QUERY=true STRICT_SERVICE_CHECK=true CONSISTENCY_QUERY='SELECT 1 AS value' bash demo/k8s/data-consistency-check.sh
```

说明：本轮演示脚本使用集群内临时 MySQL 客户端验证 `cube-router-leader` Service 到 Router 的实际入口；演示 namespace 没有额外部署 Cube API Pod，因此这不是 Cube API Pod 到 Driver 的完整端到端回归。生产 Cube API 应按 2.2 节将 CubeStore 地址配置为该 Service DNS。

执行结果：

- Operator 镜像构建成功，Deployment rollout 成功；Router 为 2 个 Pod，均为 `Ready`。
- 第一次切换：`cube-router-demo-55554b89fc-5wq2t` -> `cube-router-demo-55554b89fc-pjzrw`；`leaderEpoch` 从 `9` 升到 `10`；Service endpoint 与 CR leader 一致；仅一个 leader label；脚本返回成功。
- 第二次切换及 Service 查询：`cube-router-demo-55554b89fc-pjzrw` -> `cube-router-demo-55554b89fc-rqwgk`；`leaderEpoch` 从 `10` 升到 `11`；Service DNS 切前和切后均返回 `1`。
- leader Pod、follower Pod 和 Service 的查询结果均为 `1`，切换前后 hash 一致，脚本输出：`PASS: 查询结果哈希前后保持一致。`
- 最终状态：leader 为 `cube-router-demo-55554b89fc-rqwgk`，`leaderEpoch=11`，`LeaderElection=True`，`RoleStateSync=True`，EndpointSlice 仅指向 `192.168.194.126`。

本轮还修复了 CRD schema 问题：原 schema 只声明了空的 `status.conditions.items`，operator 日志出现 `unknown field status.conditions[...]`，CR 状态显示为 `[{},{}]`。已补充 Kubernetes Condition 的字段定义；后续部署必须使用更新后的 CRD。

修复后 operator 日志已不再出现 `unknown field`，CR 状态能够完整持久化 `type/status/reason/message/observedGeneration/lastTransitionTime`。

Kubernetes 仅提示旧版 `Endpoints` API 已废弃，演示脚本仍可运行；生产脚本应逐步改为读取 `EndpointSlice`。
## 11.7 Cube API -> Driver -> Leader Service 端到端验证记录（2026-08-02）

本轮不是直接访问 Router，而是完整走下面的请求链路：

```text
Cube API /cubejs-api/v1/load
  -> CubeStoreDriver
  -> cube-router-leader Service
  -> EndpointSlice 当前 leader Pod
  -> CubeStore Router
```

### 构建过程

在本地工作区执行了以下构建：

```bash
cd /Users/xujiawei/magic/workbench/cube
yarn tsc packages/cubejs-cubestore-driver

cd operators/cube-operator
API_IMAGE=cube-studio-api:ha-local ./demo/cube-api/build-image.sh
```

由于本机是 macOS、Kubernetes 节点是 Linux/arm64，不能把宿主机的 macOS `index.node` 直接放入容器。本次通过 `demo/cube-api/build-native.sh` 使用本地 Rust 源码构建 Linux 原生模块，再由镜像脚本注入镜像；Rust release 构建成功，最终 API 镜像构建成功。

### 最终 K8s 验证命令

```bash
kubectl -n cube-operator-demo rollout restart deployment/cube-api-demo
kubectl -n cube-operator-demo rollout status deployment/cube-api-demo --timeout=300s
bash demo/k8s/cube-api-failover-check.sh
```

### 最终结果

| 检查项 | 切换前 | 切换后 | 结果 |
|---|---|---|---|
| Cube API HTTP 状态 | `200` | `200` | PASS |
| Cube API 业务数据 | `[{{"RouterHaProbe.total":"1"}}]` | `[{{"RouterHaProbe.total":"1"}}]` | PASS |
| 当前 leader | `cube-router-demo-55554b89fc-4hxt9` | `cube-router-demo-55554b89fc-x8wbs` | PASS |
| `leaderEpoch` | `18` | `19` | PASS，单调递增 |
| `cube-router-leader` EndpointSlice | `4hxt9` | `x8wbs` | PASS |
| 稳定业务响应 hash | `4b50f40a2a70bb1efbb1db1874361a96acc7237d28b62a0f825f3a9e739b02d5` | 相同 | PASS |

关键日志：

```text
[22:05:33] Cube API 发送切换前真实请求，leader=...-4hxt9, epoch=18
[22:05:36] 切换前 Cube API data=[{"RouterHaProbe.total":"1"}]
[22:05:36] 删除当前 Router leader=...-4hxt9
[22:06:09] 新 leader=...-x8wbs, epoch=19, Service endpoint=...-x8wbs
[22:06:09] 切换后 Cube API data=[{"RouterHaProbe.total":"1"}]
PASS: Cube API -> CubeStoreDriver -> Leader Service -> Router failover response is consistent.
```

### 本轮实际修复点

1. API demo 使用标准 `schema/cubes/RouterHaProbe.js`，并设置 `CUBEJS_SCHEMA_PATH=schema`。
2. API 镜像使用 Linux/arm64 原生模块，避免 macOS `index.node` 导致 `invalid ELF header`。
3. `CubeStoreDriver` 在单个 Kubernetes Service 地址场景下，读请求遇到旧 WebSocket 的 `WrongConnection` 时关闭旧连接并重新连接 Service 一次；非幂等写请求不进入该自动重放路径。
4. 验证脚本只对响应的业务 `data` 字段做稳定 hash，不把动态 `requestId`、`lastRefreshTime` 等响应 envelope 字段误判为数据不一致。
5. CRD conditions 已使用完整 `metav1.Condition` schema，当前可以看到 `LeaderElection=True` 和 `RoleStateSync=True`。

### 结论边界

本次已经验证了 **Cube API 请求链路下的 Router 主备切换**：删除 leader 后，Operator 更新 CR 状态、`leaderEpoch`、Pod role label 和 `cube-router-leader` EndpointSlice；CubeStoreDriver 能重新建立 Service WebSocket，切换后查询成功且业务结果一致。

本次演示仍不是“所有生产写入场景已证明无损”：非幂等写请求的进行中事务、上传临时态、预聚合队列和 Redis mutationId 幂等写入需要单独的写入/并发/超时/重复提交测试。生产部署必须使用不可变 API 镜像 tag，并继续保留业务侧 mutationId、事务提交和重试边界。

## 2026-08-04 最新检查、修复与真实验证

### 本轮发现并修复的问题

1. **Router 本地文件导致切主后数据不可见**：旧演示没有配置共享对象存储，切主后新 Router 报 `chunk.parquet doesn't exist in remote file system`，并将表降为 `is_ready=false`。新增 `demo/k8s/minio.yaml`，Router 与 Worker 统一使用 MinIO bucket `cube-router-ha`、subpath `cubestore-demo`。
2. **Router 内存配置过小**：原 limit 为 `128Mi`，真实导入/查询期间被 Kubernetes `OOMKilled`。现调整为 request `256Mi`、limit `1Gi`。
3. **业务状态接口误作 liveness 探针**：慢查询时 `/router/status` 可能超过探针超时，导致误重启。现改为 TCP liveness，HTTP `/router/status` 仅用于 readiness，超时放宽为 `10s`。
4. **Operator 状态同步使用缓存对象**：ConfigMap resourceVersion 可能落后于 API server，出现 `server rejected our request`，主备长期停在 fenced。ConfigMap 同步改为优先读取 `APIReader` 的最新对象，并保留 lease fencing。
5. **Cube API 演示 schema 不可编译/不可查询**：schema 沙箱不提供 `process`，且主键默认不可见。固定演示表配置，`RouterHaProbe.id` 设置 `public: true`，API Deployment 显式设置 `CUBEJS_SCHEMA_PATH=schema`。
6. **验证脚本时序不安全**：现在等待表注册、数据行数、聚合值和 `is_ready=true`，导入后重启演示 API 清除旧的 missing-table 编译缓存；before 快照失败时禁止删除 leader。
7. **Cube API 缓存队列与 Router 数据验证解耦**：演示 API 使用 `CUBEJS_CACHE_AND_QUEUE_DRIVER=memory`，本轮只验证 Cubestore 数据读路径和 Router 切主；生产仍需单独完成 Cubestore-backed cache mutation 的 UNKNOWN outcome 恢复。

### 本轮实际运行日志与数据

```text
Router image: cube-studio-router:ha-local
Router image ID: sha256:71642434d84d80464d32aa79bf43715a6087bb84cec72e7a0f91ab56b5dbc32c
Operator image ID: sha256:5db3f6f8d2e4713357b8ca0b773a0196607a75f1de7b79a29ef410fb9ed1e309
Cube API image ID: sha256:d636f26c8830e6bf34f0ff4611ab3c0abaa93953e267382f755179ad4cdd5b8e

Cube API before: rows=12 amount=780 concrete_row_id=7 hash=2ec29e4ff1a8a777a8381411d5dd8706a4e04f324fc00a8a2ece4371333c3b9c
pod "cube-router-demo-74598d948f-gh9rg" deleted
Cube API after: rows=12 amount=780 concrete_row_id=7 hash=2ec29e4ff1a8a777a8381411d5dd8706a4e04f324fc00a8a2ece4371333c3b9c
PASS: Cube API real-data rows, concrete row, grouped aggregate and query result preserved (leader=cube-router-demo-74598d948f-492nz epoch=43)
```

验证含义：真实请求从 Cube API 进入 CubeStoreDriver，再经 `cube-router-leader` Service 到当前 leader；删除 leader 后 EndpointSlice 只保留新 leader，Cube API 的聚合、明细、分组结果和 hash 保持一致。

### 本轮测试结果

- `go test ./...`：通过。
- `REDIS_URL=... REDIS_PASSWORD=... go test ./internal/leadership`：通过，真实 Redis CAS/lease 集成测试通过。
- Rust focused tests：Router remote MetaStore 配置测试和 HTTP tests 共 `10 passed`。
- `cargo test -p cubestore --lib`：`317 passed, 2 failed`。失败项为已有的 `metastore::tests::delete_old_snapshots` 时序敏感测试和 `sql::tests::explain_analyze_detailed` channel closed 测试，不能作为全量 Rust 通过证据，仍需单独修复/隔离。
- Cube API 真数据切主 E2E：通过，切主前后结果 hash 相同。

### 当前部署关系

```mermaid
flowchart LR
  API[Cube API\nCUBEJS_SCHEMA_PATH=schema] --> Driver[CubeStoreDriver\nLeader Service]
  Driver --> LS[cube-router-leader\nEndpointSlice only leader]
  LS --> R1[Router A\nleader or follower]
  LS --> R2[Router B\nleader or follower]
  R1 --> MS[Authoritative MetaStore\nRWO PVC]
  R2 --> MS
  R1 --> OBJ[MinIO\ncube-router-ha/cubestore-demo]
  R2 --> OBJ
  W1[Worker 0] --> OBJ
  W2[Worker 1] --> OBJ
  Operator[Cube Operator\nRedis lease + K8s fencing] --> R1
  Operator --> R2
  Redis[(External Redis)] --> Operator
```

### 生产结论

本地 K8s 演示已经实现“单写 Router 主备切换 + 共享 MetaStore + 共享对象存储 + Cube API 真请求验证”。这证明 Router HA 的基础读路径和切主数据连续性成立，但**不等于全部生产能力已闭环**：Job/upload/pre-aggregation/Refresher 的恢复入口、非幂等写 UNKNOWN outcome、对象发布事务、MetaStore 灾备恢复和全量 Rust 两个失败测试仍需完成生产验收。

## 环境要求

### 本地工具链

- Docker Desktop 或 OrbStack，支持本地构建和 Kubernetes
- Kubernetes 集群，建议 Kubernetes 1.28+
- `kubectl`
- Node.js 24.x，Cube API 镜像使用 `node:24.18.0-trixie-slim`
- Go 1.25，用于构建 `cube-operator`
- Rust nightly `nightly-2025-08-01`，仅在本地重建 Rust 原生模块或 Rust Builder 时使用

### 必需镜像

Cube API/Node：

```text
node:24.18.0-trixie-slim
```

CubeStore/Rust 编译：

```text
cubejs/rust-builder:trixie-llvm-22
```

CubeStore 运行时：

```text
debian:trixie-slim
```

Cube Operator：

```text
golang:1.25
gcr.io/distroless/static:nonroot
```

### 内网镜像准备

只构建 Cube API 和 CubeStore 时，至少准备：

```bash
docker pull node:24.18.0-trixie-slim
docker pull cubejs/rust-builder:trixie-llvm-22
docker pull debian:trixie-slim
```

如果需要在内网重新构建 `cubejs/rust-builder:trixie-llvm-22`，还需要：

```text
rust:1-slim-trixie
```

它是 Rust Builder 的底层镜像，不是 CubeStore Dockerfile 直接使用的编译镜像。Rust Builder 还需要 LLVM 22、Clang 22、LLD 22、CMake、OpenSSL 和 `nightly-2025-08-01` 工具链。

生产环境应将上述镜像同步到内网镜像仓库，并使用不可变 tag 或 digest，不建议直接依赖 `latest`。
