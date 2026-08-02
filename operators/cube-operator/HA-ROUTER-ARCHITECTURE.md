# Cube Router HA 架构（实验）

```mermaid
flowchart LR
  subgraph k8s["Kubernetes"]
    direction LR
    CR["CubestoreRouter CR status + candidates"]
    OP["cube-operator (controller)\n选主与标签写回"]
    CM["ConfigMap route-role.json\nrole state + leaderEpoch"]
    P1["Router Pod A\n/ws + /router/status"]
    P2["Router Pod B\n/ws + /router/status"]
    S["Service cube-router-leader\nselector cubestore.io/router-role=leader"]
  end

  subgraph Client["Cube 应用侧"]
    D["CubeStoreDriver\n多 Router 地址 + failover"]
  end

  D -->|GET /router/status| P1
  D -->|GET /router/status| P2
  D -->|/ws query| S
  D -->|/ws query| P1
  D -->|/ws query| P2

  OP -->|probe /router/status| P1
  OP -->|probe /router/status| P2
  OP -->|write label| P1
  OP -->|write label| P2
  OP -->|sync role state| CM
  P1 -->|read file| CM
  P2 -->|read file| CM

  CM --> S
  P1 -->|leader/follower role\nstrict mode| OP
  P2 -->|leader/follower role\nstrict mode| OP
```

## 当前实现边界（生产注意）

- 路由层请求入口（`query`/`upload`）通过 leader/follower gate 与 `leaderEpoch` 做高可用路由。
- 运行时内存状态（缓存、上传 temp、队列处理上下文）仍主要在 pod 本地；切主后会有瞬时重复/偏差风险。
- 未实现分布式幂等键与写事务重放协议；非幂等 SQL 的重试由客户端侧 `retryable` 控制。
- 建议把本架构用于主备切换高可用入口层，并配套外部幂等系统（消息 ID、事务日志、写侧去重）后再上生产。

### `/router/status` 字段约定（新增）

- `activeLeader`（顶层）：当前主节点名称（可优先使用，兼容旧版本）。
- `leaderEpoch`（顶层）：当前任期。
- `leaderState`：仍保留兼容结构，包含 `activeLeader`、`leaderEpoch`、`updatedAt`。

示例返回：

```json
{
  "node_name": "cube-router-a",
  "is_leader": true,
  "role": "leader",
  "mode": "primary",
  "activeLeader": "cube-router-a",
  "leaderEpoch": 12,
  "leaderStateSource": "file",
  "timestamp_unix_secs": 1760000000,
  "leaderState": {
    "activeLeader": "cube-router-a",
    "leaderEpoch": 12,
    "updatedAt": "2026-..."
  }
}
```

## 与 Cube 主系统结合架构

```mermaid
flowchart LR
  subgraph UserLayer[User Client]
    A[Client / API Request]
  end

  subgraph CubeLayer[Cube 应用层]
    C[Cube Server]
    D[CubeStoreDriver\n多 Router 地址 + failover]
    Q[查询编排与结果缓存]
  end

  subgraph K8sLayer[Kubernetes]
    subgraph OperatorScope[cube-operator]
      OP[cubestore-router-controller]
      CR[CR: CubestoreRouter]
      CM[route-role ConfigMap]
    end

    subgraph RouterScope[Router 集群]
      R1[Router Pod-1]
      R2[Router Pod-2]
      LS[Service cube-router-leader]
      SVC[Service cube-router]
    end
  end

  subgraph DataLayer[Worker]
    W[Cube Store Worker/Storage]
  end

  A --> C
  C --> Q
  Q --> D

  D -->|GET /router/status| R1
  D -->|GET /router/status| R2
  D -->|/ws query| LS
  D -->|/ws query| SVC

  OP -->|reconcile| CR
  OP -->|read from routers| R1
  OP -->|read from routers| R2
  OP -->|write leader label| R1
  OP -->|write leader label| R2
  OP -->|sync role state| CM
  R1 -->|read CM| CM
  R2 -->|read CM| CM
  CM --> LS

  R1 -->|query/write path| W
  R2 -->|query/write path| W

  CR -->|leader/epoch/status| C
  C -->|monitor| OP
```
