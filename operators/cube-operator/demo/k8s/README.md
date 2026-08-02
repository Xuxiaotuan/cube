# cube-operator K8s 演示清单

- 一键脚本：`demo/k8s/run.sh`
- 组件：
  - 2 个真实 Router Pod（`app=cube-router`）
  - cube-operator 1 副本（监听 router Pod）
  - cubestore.io CRD + CR 资源
  - 一个 `cube-router-leader` Service（selector: `cubestore.io/router-role=leader`）
  - `3306`（MySQL 监听端口）与 `3030`（Router HTTP/状态端口）对外服务

说明：
- 默认镜像会返回标准 `role` 信息；若响应未带有效 leader，`strict` 模式下应暂停写入并等待恢复（避免误主引导），非 strict 仅用于演示可用性兜底。
- 角色真值存放在 ConfigMap `route-role.json`，并通过 `CUBESTORE_ROUTER_ROLE_FILE=/etc/cubestore/router-role/route-role.json` 挂载到 router。
- 角色真值会携带 `leaderEpoch`，用于避免故障切换时回落到旧 leader。
- 切主演示方法：删除当前 leader Pod，观察 operator 在 CR 与 Pod label 上切换。
- 数据一致性演练：使用 `demo/k8s/data-consistency-check.sh` 在杀掉 leader 前后执行同一条 `SELECT`，对比结果 hash，验证切换窗口内读一致性。

## 外部状态介质（PG/Redis）

- PostgreSQL
```yaml
leaderStateStore:
  type: postgres
  dsn: "postgres://user:password@postgres.default.svc:5432/cubeha?sslmode=disable"
  pgTable: "cubestore_router_leader_state"
```

- Redis
```yaml
leaderStateStore:
  type: redis
  dsn: "redis://redis.default.svc:6379/0"
  redisKey: "cube-router/leader-state"
```

- 在 `type` 指定为 `postgres` 或 `redis` 时，如果外部存储不可达，`RoleStateSync` 状态会变为 `False`，Operator 不会静默吞错。

可直接执行的校验命令：
```bash
cd /Users/xujiawei/magic/workbench/cube/operators/cube-operator
export CONSISTENCY_QUERY="SELECT 1 AS value"
# 可选：若 service 3306 端口未就绪，先临时关闭服务校验
export VERIFY_LEADER_SERVICE_QUERY=false
./demo/k8s/data-consistency-check.sh
```

脚本会同时打印切前/切后 `leaderEpoch`。
