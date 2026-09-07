# cube-operator：Cube 编排与 Router 主备实验

> **当前生产结论：NO-GO。** Operator 已支持整套 Cube 编排及 Router 单主/备节点切换；最新授权与恢复修复通过了相关本地测试，但尚未完成新协议的真实部署、业务故障恢复及多节点验收。不能将 Pod Ready、Service 切换或单测通过等同于生产级数据一致性。
>
> 状态更新：2026-09-07。本页优先说明最新结论；详细文档中的失败、修复及历史演练是不同时间的检查点，不可混用。最近运行环境观察不是持续监控，也不是本次文档修改后重新执行的验证。

## 先看哪些文档

| 目的 | 入口 |
| --- | --- |
| 看当前修复结果、未完成项与证据边界 | [HA-CLOSURE-2026-09-07.md](HA-CLOSURE-2026-09-07.md)，以末尾“恢复边界与持久授权整改”小节为最新检查点 |
| 看 K8s 演示过程、架构、部署与数据流说明 | [HA-ROUTER-K8S-DEMO.md](HA-ROUTER-K8S-DEMO.md)，历史日志不代表当前新协议已部署 |
| 看 Kubernetes authority 协议设计 | [HA-PROTOCOL-V2-DESIGN.md](HA-PROTOCOL-V2-DESIGN.md)，设计不等于实现或验收完成 |
| 看生产条件、发布门禁与故障覆盖 | [PRODUCTION-DEPLOYMENT.md](PRODUCTION-DEPLOYMENT.md)、[RELEASE-ACCEPTANCE.md](RELEASE-ACCEPTANCE.md)、[HA-FAILURE-MATRIX.md](HA-FAILURE-MATRIX.md) |
| 看 Lease 丢失及恢复边界 | [LEASE-RECOVERY.md](LEASE-RECOVERY.md) |
| 看最近真实 API/环境预检 | [原始报告](demo/k8s/evidence/2026-09-07-eight-items/refresher-preflight-15EUIf/REPORT.md) |

## 能做什么，不能保证什么

| 层级 | 当前能力 | 不代表什么 |
| --- | --- | --- |
| `CubeCluster` CRD/CR | 编排 Cube API、Router、MetaStore、Worker，可配置单实例 Refresher；管理 Service、PVC、RBAC 等依赖 | 不等于所有组件都具备 HA，尤其不是多副本 MetaStore 或 Refresher 选主证明 |
| `CubestoreRouter` CRD/CR | 管理 Router 主备角色及流量切换 | 不是让 2-3 个独立 MetaStore 的 Router 同时写入 |
| Kubernetes Lease/CAS | 主备选举使用 Kubernetes 协调，不要求新增 Redis/PostgreSQL | Lease 不存业务数据，不能替代 MetaStore、对象存储或写入幂等协议 |
| Router/Worker authority | 基于身份、Lease 和写入上下文约束访问；相关本地测试已通过 | 不代表已在真实 Kubernetes 中通过断网、暂停旧主和后台任务兼容性验收 |
| 预聚合恢复保护 | 保留明确 UNKNOWN 和身份异常保护，正常首次构建及清理回归通过 | 权威代次、发布/引用/回收事务与 UNKNOWN 对账仍未闭环 |
| leader Service | 为业务提供稳定路由入口 | Service 不同步内存、临时上传文件、构建记录或持久化数据 |

目标是单个 active Router 和 standby Router，而非独立状态的多主写入。允许切主期间短暂不可用，可以为恢复和隔离留出时间，但不能替代旧主写入隔离及数据一致性证明。

默认方向为 Kubernetes 原生协调；Redis/PostgreSQL 后端仅为历史兼容选项，不是本方案的必需组件。MetaStore 持久化和业务对象存储仍然需要可靠的数据层。

## 最新验证结果

下表保留上一轮定向修复的本地基线；最新增量结果见随后小节。两者均不是运行集群的新镜像验收：

| 检查 | 结果 | 原始证据 |
| --- | --- | --- |
| Rust lib/tests/bin 编译 | PASS，1 分 05 秒，仍有警告 | [rust-build.log](demo/k8s/evidence/2026-09-07-eight-items/targeted-repairs/rust-build.log) |
| 新授权测试 `authority_` | 2/2 PASS | [rust-authority.log](demo/k8s/evidence/2026-09-07-eight-items/targeted-repairs/rust-authority.log) |
| RPC 回归 `task4_rpc_` | 2/2 PASS | [rust-task4-rpc.log](demo/k8s/evidence/2026-09-07-eight-items/targeted-repairs/rust-task4-rpc.log) |
| Go controllers/agent | PASS；API package 无测试文件 | [go-authority-bootstrap.log](demo/k8s/evidence/2026-09-07-eight-items/targeted-repairs/go-authority-bootstrap.log) |
| 预聚合/队列三套回归 | 76/76 PASS；有延迟退出警告，最终退出 0 | [preaggregation-regression.log](demo/k8s/evidence/2026-09-07-eight-items/targeted-repairs/preaggregation-regression.log) |
| Refresher harness / 预检单测 | 28/28、7/7 PASS | [检查报告](demo/k8s/evidence/2026-09-07-eight-items/refresher-preflight-15EUIf/REPORT.md) |
| 最近真实 Cube API 基线 | 认证 `/meta`、`/load` 为 HTTP 200，保留查询数据 | [api-readonly-baseline.json](demo/k8s/evidence/2026-09-07-eight-items/refresher-preflight-15EUIf/api-readonly-baseline.json) |
| 生产环境预检 | BLOCKED / `productionGo=false` | [production-preflight.json](demo/k8s/evidence/2026-09-07-eight-items/refresher-preflight-15EUIf/production-preflight.json) |

本轮定向修复了三处问题：Worker `create_chunk` 写操作标签错误；首次 Lease 引导使用错误注释键名；预聚合补丁过度阻断首次构建并全面暂停清理。修复没有放宽 Worker 白名单，也没有取消 Lease 丢失时的 fail-closed 保护。

历史 74 项测试通过的预聚合候选补丁仍有首次构建阻塞回退，不能作为成功证据；76 项结果对应其后修复。此后新增 Driver 恢复边界和实际数据库重开验证如下，不能将普通重开提升为快照恢复证明。

### 最新增量：恢复边界与持久授权整改

| 增量 | 实际结果 | 证据 |
| --- | --- | --- |
| Driver 恢复返回值 | false 仅用于已有 selected 记录；缺记录或缺上传源抛出 UNKNOWN，不自动进入重建；源恢复后可继续原 manifest | [44/44 测试通过](demo/k8s/evidence/2026-09-07-production-closure/driver-recovery-tests.log)、[类型检查通过](demo/k8s/evidence/2026-09-07-production-closure/driver-typecheck.log) |
| grant 内存有界性 | 安装持久提交成功后只保留当前 grant；20 次续约、128 次轮换和失败保持均覆盖；未放宽 Lease 校验 | [Rust 编译与 5/5 authority 测试](demo/k8s/evidence/2026-09-07-production-closure/rust-authority-durability.log) |
| 数据库普通重开 | 关闭并释放实际 RocksDB 后同路径打开，校验持久记录、旧 incarnation/epoch 拒绝和新授权写入 | 同上；没有验证磁盘故障、旧快照恢复或授权记录丢失 |
| RPC 回归 | 2/2 PASS | [回归日志](demo/k8s/evidence/2026-09-07-production-closure/rust-task4-rpc.log) |
| 安装入口 | `run-cubecluster.sh` 接入 authority RBAC；1 个接线测试含 2 个命名空间子用例通过，三个 RBAC 资源 server dry-run 通过 | [安装验证日志](demo/k8s/evidence/2026-09-07-production-closure/operator-install-wiring.log) |

没有真实创建新 RBAC 或部署新镜像；dry-run 未验证实际 MetaStore ServiceAccount 的 TokenReview 请求。原子构建代次、发布/引用/回收事务仍未实现，生产仍为 NO-GO。

## 运行环境与源码必须分开看

最近观察的环境为本地 `orbstack`、命名空间 `cube-ha-remediation`、集群 CR `analytics`：API 1、Refresher 1、Router 2、Worker 2、MetaStore 1。它是单节点，PVC 使用 `local-path`，没有命名空间 NetworkPolicy，`ProductionReady=False`。

最新 authority 代码修复尚未构建成部署镜像并在该集群完成验收。旧部署的 API 正常或历史切主成功，不能为新协议背书。版本验收应记录源码提交、镜像 digest、实际 Pod imageID、环境和测试时间，不能只写可变镜像 tag。

只读查看该演示环境：

```bash
kubectl --context orbstack -n cube-ha-remediation get cubecluster analytics -o yaml
kubectl --context orbstack -n cube-ha-remediation get deploy,sts,svc,pvc
kubectl --context orbstack -n cube-ha-remediation get cubestorerouter analytics-router -o yaml
kubectl --context orbstack -n cube-ha-remediation get endpointslices
```

这些命令仅观察资源状态，不能代替业务验收。

## 本地开发与演示入口

从仓库根目录进入：

```bash
cd operators/cube-operator
go mod download
go test ./controllers ./api/... ./internal/agent -count=1 -timeout=90s
```

项目保留两类演示入口，运行前阅读对应清单与 [演示文档](HA-ROUTER-K8S-DEMO.md)：

| 入口 | 用途 | 边界 |
| --- | --- | --- |
| `./demo/k8s/run-cubecluster.sh` | 演示通过顶层 CR 编排 Cube | 会操作资源；不是新 authority 的自动迁移或生产安装证明 |
| `./demo/k8s/run.sh` | 历史 Router 主备演示 | 不可用旧镜像演练结果替代新协议验收 |
| `./ha-validate.sh` | 本地依赖、Driver 静态检查/构建、Go 测试、Rust 检查 | 不是“完整生产验收”，不包含所有真实故障 E2E |

只在隔离的本地测试环境执行会修改资源的脚本，确认脚本实际使用的 kube-context、命名空间、镜像及对象存储，不要直接用于生产。MinIO 演示实例不是生产数据层；kind/minikube 等环境还需按相应工具导入镜像或使用可访问仓库。

### 新 authority 安装特别说明

`spec.authority` 是新安装的显式配置，包含 API 超时、校验窗口、时钟偏差预算；不能把参数随便填写后认为已满足 fencing 条件。

[authority-canary.yaml](config/samples/authority-canary.yaml) 是待填入真实镜像及存储参数的候选示例，不是已验收的一键部署文件。[authority RBAC](config/rbac/authority.yaml) 已接入 `run-cubecluster.sh` 并通过接线测试及 server dry-run；旧 `run.sh` 入口未在本轮扩展。不要只照旧 RBAC 命令安装后就开启 strict，实际 MetaStore SA 权限与运行链路仍需验证。

已有非 strict 集群需要单独批准的停写、隔离旧写者和维护迁移流程，不应直接通过滚动更新开启新协议。`strict` 是安全约束模式，不是生产认证标签。

## 如何验收数据，而不只是验收切换

| 层次 | 可以证明什么 | 不能证明什么 |
| --- | --- | --- |
| Pod Ready、Lease epoch、Service EndpointSlice | 组件就绪及入口角色切换 | 已确认写入不丢、旧主停止写入、业务数据正确 |
| `SELECT 1` | SQL 请求能够返回 | 任何业务表、预聚合或上传数据的一致性 |
| 同一业务 SQL 切前切后 hash 相同 | 本次查询结果相同 | 两次都读旧缓存、未消费新预聚合、并发写丢失等情况已被排除 |
| 真实 Cube API + 已知源数据 + 构建身份 | 经业务入口查询及实际预聚合身份关联 | 未覆盖的故障窗口和多节点场景也安全 |

历史 `data-consistency-check.sh` 的默认 `SELECT 1` 只能用于连通性冒烟。跳过 leader Service 路径不能视为业务入口验收通过。真实验收必须固定输入、预期数据、buildId/generation、物理表身份、故障点和镜像版本，并保存切换前后结果以及旧主被拒绝的证据。

当前 Refresher 重启 E2E 还缺代理 Service，且 ledger 存在物理表 ready 但未终结的记录。不得删除 UNKNOWN、放宽全局静默条件或跳过数据检查来换取 PASS。

## 尚未通过的生产门禁

| 优先级 | 未完成项 | 状态 |
| --- | --- | --- |
| P0 | 新 authority 的真实 TLS/RBAC/TokenReview/晋升与后台任务兼容性 | `evidence_incomplete` |
| P0 | 暂停旧主、网络隔离、Lease 失效时拒绝旧写者 | 新协议真实 K8s 故障验收未完成 |
| P0 | 快照恢复、磁盘故障及授权记录缺失的安全处理 | 普通数据库关闭重开测试已过，其余仍为 `implementation_incomplete / evidence_incomplete` |
| P0 | 构建代次、UNKNOWN 对账及发布/引用/回收原子协调 | `implementation_incomplete`；Driver false 歧义已收紧，不替代权威 claim 或事务化清理 |
| P0 | Refresher 崩溃恢复及真实 Cube API 数据一致性 E2E | 未通过 |
| P1 | 访问 Kubernetes API 的开销与负载优化 | grant 状态有界清理已实现并测试；API 调用开销优化及负载验收未完成 |
| P1 | 完整监控、统一镜像、升级/回滚及多节点故障验收 | 未完成；当前单节点环境不足以验收节点级容灾 |
| 验收前提 | RPO/RTO、保留期、真实测试集群及备份恢复条件 | 尚未完成定标和实证 |

本页不承诺零丢失、exactly-once 或自动恢复所有 Job/upload/preaggregation/Refresher 工作。允许短暂不可用也不改变这些数据保护门禁。满足条件并取得对应运行证据后，才能更新生产结论。

## 目录

- `api/`：`CubeCluster`、`CubestoreRouter` 类型定义。
- `controllers/`：整套 Cube 编排、主备协调及 authority 接入。
- `config/crd/`：CRD 清单。
- `config/manager/`：Operator 部署对象。
- `config/rbac/`：权限清单，安装完整性必须单独核验。
- `demo/k8s/`：演示、检查脚本及分日期原始证据。
