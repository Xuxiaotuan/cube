# cube-operator（实验版）

该目录给出一个最小化的 Go Operator，用于在 Kubernetes 内为 Cubestore Router 提供
`leader/follower` 选举与标签化。

## 目录
- `api/`：CRD 类型定义（`CubestoreRouter`）
- `controllers/`：选主/降级控制器
- `config/crd/`：CRD 清单
- `config/manager/`：operator in-cluster 部署对象
- `config/rbac/`：默认命名空间 RBAC（示例）
- `demo/k8s/`：一套 K8s 演示清单和脚本

## 本地开发（本机运行）

1. 安装依赖
```bash
cd operators/cube-operator
go mod download
```
2. 安装 CRD 与 RBAC（默认命名空间）
```bash
kubectl apply -f config/crd/bases/cubestore.io_cubestorerouters.yaml
kubectl apply -f config/rbac/role.yaml
kubectl apply -f config/rbac/service_account.yaml
kubectl apply -f config/rbac/role_binding.yaml
```
3. 启动控制器（本机）
```bash
go run . --leader-elect=true
```
4. 提交路由实例（示例含 `electionStrategy` 与 `roleConfigMapName`）
```bash
kubectl apply -f examples/cubestorerouter.yaml
```

## K8s 演示（推荐）

我们提供一套可直接跑的演示套件（含 2 个真实 Cubestore Router + Operator + Leader Service）：

```bash
cd operators/cube-operator
./demo/k8s/run.sh
```

脚本会做：
- 安装 CRD
- 创建 demo namespace、operator SA/RBAC
- 本地构建 `cube-operator:dev` 镜像并部署 operator
- 部署 2 个真实 router（默认镜像 `cube-studio-router:ha-local`，可通过 `ROUTER_IMAGE` 覆盖；清单无变更则不触发滚动）
- 创建 `CubestoreRouter` CR 与 `cube-router-leader` Service（按 `cubestore.io/router-role=leader` 选择）
- 统一配置路由角色真值源：`CUBESTORE_ROUTER_ROLE_STRICT` 与 `CUBESTORE_ROUTER_ROLE_FILE`
- 真值文件固定为 `route-role.json`（由 operator 写入同名 ConfigMap key），路由容器读取 `CUBESTORE_ROUTER_ROLE_FILE=/etc/cubestore/router-role/route-role.json`。

演示切换：
```bash
kubectl -n cube-operator-demo get pod -l app=cube-router --show-labels
kubectl -n cube-operator-demo get cubestorerouter demo -o yaml
kubectl -n cube-operator-demo logs deploy/cube-operator -f

# 找到当前 leader pod 并删除
kubectl -n cube-operator-demo get pod -l app=cube-router -l cubestore.io/router-role=leader -o custom-columns='NAME:.metadata.name'
kubectl -n cube-operator-demo delete pod <leader-pod-name>
```

观察现象：
- Operator 会将剩余健康实例改为 `cubestore.io/router-role=leader`
- `CubestoreRouter` CR 中 `.status.leader` 与 `.status.leaderEpoch` 更新（任期应单调递增）
- `cube-router-leader` Service 端点自动切到新 leader 上

数据一致性回归（推荐）：
```bash
cd /Users/xujiawei/magic/workbench/cube/operators/cube-operator
export CONSISTENCY_QUERY="SELECT 1 AS value"
export VERIFY_LEADER_SERVICE_QUERY=false  # 首次演练可先跳过 service 路径校验
./demo/k8s/data-consistency-check.sh
```

脚本会在 leader 切前后执行同一条 SQL 并对比 hash，验证读一致性（读路径通过 3306 SQL 协议）。

可调参数：
- `WAIT_SECONDS`：等待切换完成最长时间（默认 `120`）
- `CONSISTENCY_QUERY`：一致性验证 SQL（默认 `SELECT 1 AS value`）
- `MYSQL_IMAGE`：临时 MySQL 客户端镜像（默认 `mysql:8.4`）
- `VERIFY_LEADER_SERVICE_QUERY`：是否校验 `cube-router-leader` Service 读取（默认 `true`）
- `STRICT_SERVICE_CHECK`：当 service 查询失败时是否直接退出（默认 `false`）

配套架构图与边界说明：
- [HA-ROUTER-ARCHITECTURE.md](./HA-ROUTER-ARCHITECTURE.md)
- 生产验收清单：
  - [HA-PRODUCTION-CHECKLIST.md](./HA-PRODUCTION-CHECKLIST.md)

## 当前行为说明
- 先尝试读取每个 router 的 `GET /router/status?detail=1`。
- `strict`（推荐生产）：优先使用 state 文件内 `activeLeader`；若文件暂不可用且仅有 1 个可用实例，会用该实例临时兜底，否则返回空 leader，避免误主从引导。
- 非 strict：在无 leader 时按创建顺序兜底选主，优先保证可用性；推荐切到 strict 后再逐步放量。
- 引入 `status.leaderEpoch`，用于防止回退到旧 leader：同一时刻应只允许 leader 任期前进。
- 会将 Pod 标签写成 `cubestore.io/router-role=leader|follower`。

### 外部状态持久化（PG/Redis）

你可以在 `CubestoreRouter` CR 中开启外部状态介质，避免仅依赖 ConfigMap 的状态单点：

```yaml
spec:
  leaderStateStore:
    type: redis # 或 postgres
    dsn: "redis://redis.default.svc:6379/0"
    redisKey: "cube-router/leader-state"
```

或

```yaml
spec:
  leaderStateStore:
    type: postgres
    dsn: "postgres://user:password@postgres.default.svc:5432/cubeha?sslmode=disable"
    pgTable: cubestore_router_leader_state
```

若未设置 `leaderStateStore`，系统默认回退到 `ConfigMap`；生产建议启用外部介质。

> 当前实现支持 `CUBESTORE_ROUTER_ROLE_FILE` 主备真值源；生产上建议补充告警、限流与故障演练手册。

### 一致性边界（生产前请确认）
- 现在的切主逻辑解决的是“请求入口主备可用性”和“路由节点一致性主角色视图”。
- Router 运行时缓存、队列、上传临时态在现有实现里仍以节点本地为主；故障切换后可能出现短暂重复/未观测窗口（尤其是并发写入和上传流程）。
- 已加固项：
  - Router HTTP/WS 入口在非 leader 时会直接拒绝请求（含 `/upload-temp-file`）。
  - Driver 的 `/router/status` 探测会校验 leader 识别状态并拒绝多主/过期主节点快照。
  - WebSocket 连接关闭时只重放可幂等读查询，非幂等写类请求不重放，避免切主期间重复写入。
- 建议在生产再补齐：
  - 幂等键/幂等写入策略（避免重复执行）。
  - 外部共享编排状态与队列持久化治理（避免单机内存状态影响切主后行为）。
  - 监控告警：`/router/status` 无 leader、角色抖动、切换超时。

若你的 Kubernetes 不是直接可见镜像仓库（如 kind/minikube），需先把镜像导入集群：
```bash
# kind
kind load docker-image cube-operator:dev --name <cluster-name>
kind load docker-image cube-studio-router:ha-local --name <cluster-name>
# minikube
minikube image load cube-operator:dev
minikube image load cube-studio-router:ha-local
```
再执行 `./demo/k8s/run.sh`。

## 一键验收（本地环境）

在你机器已经准备好 Docker/Kubernetes + Node/Rust/Go 的情况下，执行以下命令跑完整验收：

```bash
cd operators/cube-operator
./ha-validate.sh
```

脚本包含：
- driver workspace 依赖安装与编译
- cubestore-driver `tsc` / `lint` / `build`
- operator `go test`
- cubestore Rust `cargo check`
