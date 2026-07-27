# Cube Core 本地构建 & 部署指南

## 项目概览

Cube 是一个 Yarn + Lerna monorepo，含约 50 个 JS/TS 包 + 多个 Rust crate：

```
cube/
├── packages/              # JS/TS 源码
│   ├── cubejs-server      # 服务端入口（TypeScript）
│   ├── cubejs-server-core # 核心编排
│   ├── cubejs-api-gateway # REST/GraphQL/SQL API
│   ├── cubejs-schema-compiler # 数据模型编译器
│   ├── cubejs-query-orchestrator # 查询编排
│   ├── cubejs-client-core # JS 客户端 SDK
│   ├── cubejs-playground  # 开发期 Playground UI
│   └── cubejs-*-driver    # 30+ 数据库驱动
├── rust/
│   ├── cubesql/           # PG 协议代理（Rust，已编译）
│   ├── cubestore/         # OLAP 存储引擎（Rust，未编译，用官方镜像）
│   └── cube/              # Tesseract planner、Neon 绑定（Rust，已编译）
└── docker-compose/        # 部署配置
```

---

## 构建流程概览

```
源码（TS + Rust）
  │
  ├── Stage 1: rust-builder ──────────────────────
  │   ├── 安装 Rust toolchain（容器内 rustup）
  │   ├── 编译 cubesql（PG 协议代理，45MB 二进制）
  │   └── 编译 cubejs-backend-native（Neon 绑定，70MB .node）
  │
  ├── Stage 2: js-builder ────────────────────────
  │   ├── yarn install（50+ 包的全部依赖）
  │   ├── tsc --build（编译所有 TypeScript）
  │   ├── rollup 打包客户端 SDK（UMD/CJS/ESM）
  │   └── vite 构建 Playground 前端
  │
  └── Stage 3: runtime ───────────────────────────
      ├── 从 Stage 1 复制 cubesqld + index.node
      └── 从 Stage 2 复制全部编译产物
```

**构建后镜像包含：**

| 组件 | 来源 | 大小 |
|---|---|---|
| Cube API 服务器 | TypeScript → tsc 编译 | — |
| Playground 前端 | React → Vite 构建 | ~3.5MB JS bundle |
| 客户端 SDK | TypeScript → Rollup 打包 | UMD + CJS + ESM |
| 30+ 数据库驱动 | TypeScript 编译 | — |
| **cubesqld**（PG 协议代理） | **Rust → cargo 从源码编译** | **~45MB** |
| **index.node**（Neon 原生绑定） | **Rust → cargo 从源码编译** | **~70MB** |
| CubeStore（OLAP 存储引擎） | **未编译**，用官方镜像 | — |

> CubeStore 未编译的原因：需要 Rust nightly-2025-08-01 + LLVM 22，`node:24.18.0` 基础镜像基于 Debian Bookworm 不提供这些包，且该项目维护了专用构建镜像。部署时拉取 `cubejs/cubestore:arm64v8` 官方镜像。

---

## 构建镜像

### 本地 Dockerfile

项目官方提供了 `packages/cubejs-docker/latest.Dockerfile`（生产镜像，跳过 Rust）和 `dev.Dockerfile`（开发镜像，包含 Rust 编译）。但我们没有直接用它们，而是写了 `local.Dockerfile`，原因是：

| 问题 | 官方 Dockerfile 的做法 | 我们的修改 |
|---|---|---|
| **DNS 解析失败** | 依赖 Docker Hub 基础镜像和 apt mirror | `--network host` 让容器共享宿主机网络 |
| **国内网络慢** | 无代理支持 | Dockerfile 内硬编码 `http_proxy=http://127.0.0.1:7890` |
| **OrbStack + Tailscale** | 未考虑 | `--network host` 绕过容器内 DNS 问题 |
| **cubestore tsconfig 干扰 tsc** | 无此问题（不在本地构建） | `grep -v` 删除 root tsconfig 中的 cubestore 引用 |
| **yarn postinstall 失败** | 无此问题（生产镜像不编译） | `yarn install --ignore-scripts` 跳过 |
| **node-gyp 找不到 headers** | 使用 slim 镜像 | `node:24.18.0` 全量镜像 + symlink 修复 |
| **ARM64 架构** | 官方支持 | `node:24.18.0` 原生 ARM64 |

### 对项目源码的修改

构建过程修改了项目中的几个文件，克隆仓库后直接构建会失败，需要同步这些修改：

**`.dockerignore`** — 官方版本过滤掉了 Rust 源码目录。需要添加：
```
!rust/cubesql/
!rust/cube/
!rust/cubestore/tsconfig.json
```

**`packages/cubejs-docker/local.Dockerfile`** — 我们写的完整 Dockerfile（三个 stage）。

**`~/.config/clash/config.yaml`** — 需要 `allow-lan: true` 让 Docker 容器能访问宿主代理。

### 构建命令

```bash
# 前置：确保 Clash 代理已开启 allow-lan
cd /path/to/cube

docker build \
  --network host \
  -t cubejs/cube:local \
  -f packages/cubejs-docker/local.Dockerfile \
  .
```

### 构建耗时（ARM64 Mac 实测）

| 阶段 | 时间 | 说明 |
|---|---|---|
| apt-get 安装编译依赖 | 30s | python3, gcc, cmake, curl |
| rustup 安装 Rust 稳定版 | 1min | 容器内安装 rustc 1.97+ |
| **cargo build -p cubesql** | **~6min** | 编译 cube 使用的 DataFusion fork + 全部依赖 |
| **cargo build cubejs-backend-native** | **~9min** | 编译 Neon 绑定 + Tesseract planner |
| yarn install (--ignore-scripts) | ~12min | 下载全部 npm 依赖 |
| tsc --build | ~1min | 编译 ~50 个 TS 包 |
| rollup 打包 | 20s | 客户端 SDK UMD/CJS/ESM |
| lerna run build | ~3min | 编译各驱动 + Playground (Vite) |
| **总计** | **~35min** | 后续增量构建会快很多（Docker 缓存层） |

---

## 部署

### 服务架构

```
                         ┌──────────────┐
                         │  PostgreSQL   │ ← 测试数据源
                         │  :5432       │
                         └──────┬───────┘
                                │
              ┌─────────────────┼──────────────────┐
              │                 │                  │
              ▼                 ▼                  ▼
      ┌──────────────┐ ┌──────────────┐   ┌──────────────┐
      │  cube_api    │ │  refresh     │   │ CubeStore    │
      │  :4000       │ │  worker      │   │ Router       │
      │  :15432      │ │  (后台刷新)   │   │ :9999        │
      └──────────────┘ └──────────────┘   └──────┬───────┘
                                                  │
                                          ┌───────┴────────┐
                                          │                │
                                          ▼                ▼
                                   ┌──────────┐    ┌──────────┐
                                   │ Worker 1 │    │ Worker 2 │
                                   │ :10001   │    │ :10002   │
                                   └──────────┘    └──────────┘
```

### 服务列表

| 服务 | 镜像 | 是否本地构建 | 扩缩容 |
|---|---|---|---|
| `cube_api` | `cubejs/cube:local` | ✅ 从源码构建 | ✅ `--scale cube_api=3` |
| `cube_refresh_worker` | `cubejs/cube:local` | ✅ 从源码构建 | ✅ 同上 |
| `cubestore_router` | `cubejs/cubestore:arm64v8` | ❌ Docker Hub 拉取 | — |
| `cubestore_worker_1/2` | `cubejs/cubestore:arm64v8` | ❌ Docker Hub 拉取 | ✅ 可增删 worker |
| `postgres` | `postgres:16` | ❌ Docker Hub 拉取 | — |

### 启动

```bash
cd /path/to/cube

# 启动全部服务
docker compose -f docker-compose/docker-compose.yml up -d

# 查看状态
docker compose -f docker-compose/docker-compose.yml ps

# 日志
docker compose -f docker-compose/docker-compose.yml logs -f cube_api

# 扩缩容
docker compose up -d --scale cube_api=3
docker compose up -d --scale cubestore_worker_1=4
# （注意：worker 扩缩后需同步更新 CUBESTORE_WORKERS 环境变量）

# 停止
docker compose -f docker-compose/docker-compose.yml down
```

---

## 测试验证

### 测试数据

`docker-compose/postgres/init.sql` 在 Postgres 首次启动时自动执行，创建 `orders` 表并插入 20 条测试数据。

### 数据模型

`docker-compose/model/schema/Orders.js` 定义了：

```javascript
cube(`Orders`, {
  sql: `SELECT * FROM public.orders`,
  measures: { count, totalAmount, avgAmount, maxAmount, minAmount },
  dimensions: { id, status, amount, createdAt },
  segments: { completed, cancelled }
});
```

### 查询测试

| 方式 | 地址 |
|---|---|
| Playground | http://localhost:4000 |
| REST API | `curl http://localhost:4000/cubejs-api/v1/load -H "Content-Type: application/json" -d '{"query":{"measures":["Orders.count"]}}'` |
| PG 协议 | `psql -h localhost -p 15432 -U root -d cube_test -c "SELECT status, SUM(amount) FROM orders GROUP BY status"` |

---

## 环境变量参考

| 变量 | 说明 | 默认值 |
|---|---|---|
| `CUBEJS_DEV_MODE` | 开发模式（启用 Playground） | `false` |
| `CUBEJS_API_SECRET` | API 签名密钥 | 必填 |
| `CUBEJS_DB_TYPE` | 数据库类型 | `postgres` |
| `CUBEJS_CUBESTORE_HOST` | CubeStore 地址 | `localhost` |
| `CUBEJS_REFRESH_WORKER` | 是否作为刷新 Worker | `false` |

> ⚠️ Cube Core（开源版）的 `cacheAndQueueDriver` 仅支持 `cubestore` 或 `memory`，**不支持 Redis**。Redis 是 Cube Cloud（商业版）的功能，不要设置 `CUBEJS_REDIS_URL`。

---

## 常见问题

### DNS 解析失败 `getaddrinfo EAI_AGAIN`

原因：OrbStack + Tailscale 导致容器内 DNS 间歇性失效。

解决：使用 `--network host` 让构建容器共享宿主机网络栈，配合 Clash 等代理工具。

### ARM64 架构问题

- `node:24.18.0` 全量镜像已支持 ARM64
- `cubejs/cubestore:latest` 不支持 ARM64，必须用 `cubejs/cubestore:arm64v8`

### node-gyp 编译失败 `gyp: /usr/common.gypi not found`

原因：`node:24.18.0` 的 Node headers 在 `/usr/local/include/node/`，但 node-gyp 找的是 `/usr/include/node/`。

已在 Dockerfile 中修复：
```dockerfile
RUN ln -sf /usr/local/include/node /usr/include/node \
    && ln -sf /usr/local/include/node/common.gypi /usr/common.gypi
```

### tsc 找不到 cubestore 的输入文件

原因：root `tsconfig.json` 引用了 `rust/cubestore/tsconfig.json`，但该目录被 `.dockerignore` 过滤。

已在 Dockerfile 中修复：
```dockerfile
RUN rm -f /cube/rust/cubestore/tsconfig.json \
    && grep -v "rust/cubestore" /cube/tsconfig.json > /tmp/tsconfig.json \
    && mv /tmp/tsconfig.json /cube/tsconfig.json \
    && yarn tsc
```
