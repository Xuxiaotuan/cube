# Cube Operator 与 CubeStore Router HA

> **当前生产结论：NO-GO。** 已形成 Router 主备切换、MetaStore 服务端写入授权和文件导入型预聚合恢复保护的实现，并取得多项本地验证结果；完整业务恢复事务、新协议真实部署及多节点容灾仍未完成。
>
> 本页功能与证据基线为分支 `codex/ha-router-experiment` 的代码提交 [`78f2d6bb39`](https://github.com/Xuxiaotuan/cube/commit/78f2d6bb39)（2026-09-07）。后续仅修改文档的提交不等于新的构建、部署或运行验收。历史测试不自动升级为当前版本的完整验收。

## 阅读入口

| 目的 | 文档 |
| --- | --- |
| 最新修复、剩余工作和原始证据 | [闭环报告](HA-CLOSURE-2026-09-07.md)，以末尾“恢复边界与持久授权整改”为最新代码检查点 |
| 逻辑架构、K8s 部署关系、请求与切主数据流、演示过程 | [K8s 演示文档](HA-ROUTER-K8S-DEMO.md) |
| 新授权协议及实现边界 | [HA-PROTOCOL-V2-DESIGN.md](HA-PROTOCOL-V2-DESIGN.md) |
| 生产条件、发布门禁、故障覆盖 | [部署条件](PRODUCTION-DEPLOYMENT.md)、[发布验收](RELEASE-ACCEPTANCE.md)、[故障矩阵](HA-FAILURE-MATRIX.md) |
| Lease 丢失及恢复约束 | [LEASE-RECOVERY.md](LEASE-RECOVERY.md) |
| 最近真实 API 基线及环境预检 | [检查报告](demo/k8s/evidence/2026-09-07-eight-items/refresher-preflight-15EUIf/REPORT.md) |

## 一、这套 HA 如何工作

目标是 **一个 Active Router 加备用 Router**，不是多个持有独立状态的 Router 同时写入。主备协调使用 Kubernetes，不强制新增 Redis/PostgreSQL；业务状态仍由 CubeStore 的 MetaStore、CacheStore 和对象存储承载。

| 层次 | 当前职责 | 验证边界 |
| --- | --- | --- |
| Cube Operator / `CubeCluster` CRD、CR | 编排 API、Router、MetaStore、Worker 及可选的单实例 Refresher，管理 Service、PVC、RBAC 和配置 | 整套组件可编排，不等于每个组件都具备 HA |
| `CubestoreRouter` CRD、CR / Kubernetes Lease、CAS | 协调主备身份与任期，由 Router 控制器执行晋升及流量切换 | Lease 保存协调状态，不保存业务数据 |
| Leader Service + Router | 提供稳定业务入口，结合角色核验、拒绝新变更和停机排空 | Service 不同步内存、临时上传、构建状态或数据文件 |
| MetaStore authority | 服务端核验调用者身份、Lease 和授权上下文，约束写入 | 新协议有本地验证，真实 TLS/权限/晋升/后台任务链路未整体验收 |
| Worker Job attempt | 独立管理任务执行所有权；健康 Worker 不因 Router 任期变化自动作废，限制失去所有权的旧执行者发布 | 不等于所有 Job 和故障窗口均已验证 |
| 文件导入型预聚合恢复 | 记录构建身份、不可变上传清单、阶段及目标表身份；查询实际状态后恢复或保持 UNKNOWN | 尚无完整的构建领取、发布、引用及回收原子事务 |

默认方向是 Kubernetes 原生协调，Redis/PostgreSQL 后端仅为历史兼容选项。MetaStore 和对象存储依旧需要可靠的持久化与备份恢复能力，不能用 Lease 或单副本临时 MinIO 替代。

允许切主时短暂不可用，可以为隔离和恢复留出时间，但不免除旧写者隔离、已确认数据保护和结果一致性要求。

## 二、最新实现及验证结果

### 最新提交补齐的四项问题

| 改进 | 已取得的结果 | 不能据此推导的结论 |
| --- | --- | --- |
| 收紧预聚合恢复返回值 | false 只用于已有 selected 记录；缺构建身份或上传源时保持 UNKNOWN，不自动进入重建；Driver 44/44 测试、类型检查通过 | 多执行者构建领取已经原子化 |
| 限制 grant 内存保留 | 持久安装成功后仅保留当前 grant；覆盖 20 次续约、128 次轮换和失败保持，不放宽 Lease 校验 | 长期负载或 Kubernetes API 开销已达标 |
| 真正关闭并重开 RocksDB | 释放数据库引用、关闭原 DB、同路径重开；核对持久记录、旧 incarnation/epoch 拒绝及新授权写入；authority 5/5 通过 | 旧快照、磁盘故障或授权记录丢失已经安全恢复 |
| 接通 authority RBAC 安装入口 | `run-cubecluster.sh` 已接入权限清单；1 个接线测试含 2 个命名空间子用例通过，3 个资源 server dry-run 通过 | 新权限已经部署，实际 MetaStore ServiceAccount 调用链已通过 |

最新 RPC 回归为 **2/2 通过**。此前修复首次预聚合构建阻塞及正常清理的三套回归为 **76/76 通过**，属于上一代码检查点，不冒充最新整套验收。

因此，“恢复返回值混用”“grant 无界保留”“只重建 State，没有真实 DB 重开”“RBAC 未接入安装入口”不能再原样列为尚未修改；其对应的更高层业务和生产门禁仍需完成。

### 原始证据

| 检查 | 归档日志 |
| --- | --- |
| Driver 44 项恢复测试 | [driver-recovery-tests.log](demo/k8s/evidence/2026-09-07-production-closure/driver-recovery-tests.log) |
| TypeScript 类型检查 | [driver-typecheck.log](demo/k8s/evidence/2026-09-07-production-closure/driver-typecheck.log) |
| Rust lib/bins 编译、5 项授权测试 | [rust-authority-durability.log](demo/k8s/evidence/2026-09-07-production-closure/rust-authority-durability.log) |
| 最新 2 项 RPC 回归 | [rust-task4-rpc.log](demo/k8s/evidence/2026-09-07-production-closure/rust-task4-rpc.log) |
| 安装接线及 RBAC server dry-run | [operator-install-wiring.log](demo/k8s/evidence/2026-09-07-production-closure/operator-install-wiring.log) |
| 上一轮 76 项预聚合/队列回归 | [preaggregation-regression.log](demo/k8s/evidence/2026-09-07-eight-items/targeted-repairs/preaggregation-regression.log) |
| 最近认证 API 查询数据 | [api-readonly-baseline.json](demo/k8s/evidence/2026-09-07-eight-items/refresher-preflight-15EUIf/api-readonly-baseline.json) |
| 生产环境预检 BLOCKED | [production-preflight.json](demo/k8s/evidence/2026-09-07-eight-items/refresher-preflight-15EUIf/production-preflight.json) |

测试入口失败日志与通过日志分别保存，没有使用“找不到测试也算通过”。源码测试配置使用本机绝对路径，其他机器需调整 root。Rust 编译仍有警告；上一轮 Jest 有延迟退出警告，最终退出成功，不等于这些告警已经排除。

### 历史切换证据的使用方式

已有历史主备和限定业务演练记录，包括上传、排空、文件导出及独立 Refresher 构建过程中 Router 切主。具体场景、输入、镜像和结果应回到 [演示文档](HA-ROUTER-K8S-DEMO.md) 的对应记录核对。

这些场景分批运行，并非最新 authority 版本的一次完整验收。历史“数十秒接流量”测量不等于业务查询已恢复，更不是当前版本 SLA 或“无感切换”承诺。不能合并历史通过次数来代替统一版本的故障矩阵。

## 三、最核心的代码缺口：原子业务恢复事务

目前 `PreAggregationBuildStore` 仍使用多条独立 `CACHE SET NX` 保存构建身份、阶段、manifest、tableId 和 active 选择。受保护表扫描也不是事务快照。

**状态保存下来了，不等于并发恢复和清理已经安全协调。**

| 待实现的核心事务 | 要解决的问题 |
| --- | --- |
| 权威构建领取与 generation | 哪个执行者有权推进构建，旧执行者何时失效；不能用各进程自己的计数器代替 |
| 校验与发布原子提交 | generation、manifest、tableId 和 ready 结果必须在发布时共同校验 |
| 引用与 retirement 互斥 | 查询/构建仍引用的表不能被删除；退休后旧执行者不能重新发布 |
| UNKNOWN 安全对账 | 一次看到 absent，不等于此前请求不可能稍后产生效果；缺证据不能自动重放 |

这些是 **`implementation_incomplete`**，不是单纯缺少测试环境。自动安全 GC 不能绕过这部分上线，也不能通过删除 UNKNOWN、放宽保护或无条件重试来“闭环”。

下一步需要把关键决定放入 MetaStore 的串行写事务，而不是继续增加客户端状态标记。历史未终态构建的迁移边界尚待确认：保留旧记录和数据、停写维护后迁移，或承担在线兼容迁移的额外设计与验收。尚未据此操作旧数据。

## 四、代码进展与运行部署必须分开汇报

最近记录的环境是单节点 `orbstack`、命名空间 `cube-ha-remediation`、CR `analytics`：API 1、Refresher 1、Router 2、Worker 2、MetaStore 1；PVC 为 `local-path`，没有命名空间 NetworkPolicy，`ProductionReady=False`。

**最新 authority 修复尚未构建成统一部署镜像并在该环境完成验收。** 最近旧部署认证 `/meta`、`/load` 返回 HTTP 200，只证明查询基线；不代表新协议、切主后数据一致性或 Refresher 崩溃恢复通过。

Refresher 自身重启 E2E 还缺代理 Service，并有物理表 ready 但 ledger 未终结的历史记录。未删除这些记录来制造 PASS。单节点 PVC 也不能证明节点级容灾。

| 维度 | 当前状态 |
| --- | --- |
| Router 主备及历史业务切换 | 已实现，有限定场景历史实测 |
| 新服务端授权及恢复边界 | 有实现，多项本地回归通过 |
| 数据库普通关闭重开 | 已实测，不含快照回退和磁盘故障 |
| 预聚合全流程并发一致性 | 核心事务仍需开发 |
| 新协议真实部署与故障恢复 | 未完成整体验收 |
| 整套 Cube 生产级 HA | 无放行依据，NO-GO |

发布记录必须关联源码提交、镜像 digest、实际 Pod imageID、测试环境和时间，不能只写可变镜像 tag。

## 五、剩余生产门禁

| 优先级 | 未完成项 |
| --- | --- |
| P0 | 构建 claim/generation、发布/引用/retirement 原子事务及 UNKNOWN 对账 |
| P0 | 新 strict authority 的真实 TLS、RBAC、TokenReview、晋升及 Scheduler/GC 等后台任务兼容性 |
| P0 | 暂停旧主、网络隔离、Lease 失效时的实际写入拒绝与业务恢复 |
| P0 | Refresher 自身崩溃恢复及真实 Cube API 数据一致性 E2E |
| P0 | 快照回退、磁盘故障、授权记录缺失的安全处理 |
| P1 | Kubernetes API 开销、长期负载、积压/UNKNOWN 年龄/容量及恢复阶段监控 |
| P1 | 统一镜像、升级回滚、备份恢复、多节点故障验收 |
| 验收前提 | RPO/RTO、保留期、迁移方式及生产等价测试资源定标 |

最近本机可用空间约 17 GiB；没有擅自清理缓存、镜像或数据卷来腾空间，也没有据此启动大型镜像构建。构建资源和多节点环境不足属于外部条件，不能掩盖上面的事务代码未完成。

本项目当前不承诺零丢失、exactly-once 或自动恢复所有 Job/upload/preaggregation/Refresher 工作。

## 六、开发与演示入口

从仓库根目录进入：

```bash
cd operators/cube-operator
go mod download
go test ./controllers ./api/... ./internal/agent -count=1 -timeout=90s
```

只读查看最近的本地演示环境：

```bash
kubectl --context orbstack -n cube-ha-remediation get cubecluster analytics -o yaml
kubectl --context orbstack -n cube-ha-remediation get deploy,sts,svc,pvc
kubectl --context orbstack -n cube-ha-remediation get cubestorerouter analytics-router -o yaml
kubectl --context orbstack -n cube-ha-remediation get endpointslices
```

| 入口 | 用途和限制 |
| --- | --- |
| `./demo/k8s/run-cubecluster.sh` | 顶层 CR 编排演示；已接入 authority RBAC，但不是新协议的运行验收或旧集群自动迁移 |
| `./demo/k8s/run.sh` | 历史 Router 主备演示；未在本轮扩展新 authority 安装接线 |
| `./ha-validate.sh` | 本地依赖、静态检查、构建与测试；不是完整故障 E2E 或生产验收 |
| [authority-canary.yaml](config/samples/authority-canary.yaml) | 需填入真实镜像、存储和时序预算的候选配置，不是已验证的一键部署文件 |

会修改资源的脚本只能在确认了 kube-context、命名空间、镜像和存储的隔离测试环境运行。不要直接用于生产。已有非 strict 集群不能通过普通滚动更新直接开启新协议，需要停写、隔离旧写者和维护迁移；strict 是约束模式，不是生产认证。

历史 `data-consistency-check.sh` 默认的 `SELECT 1` 只是连通性冒烟。跳过 leader Service 路径不能算业务入口验收通过；同一 SQL 两次 hash 相同，也可能两次都命中旧缓存。真实验收需要已知源数据、预期结果、构建身份、物理表身份和明确故障点。

## 七、给领导汇报的口径

> Cube Router HA 已形成主备切换、控制面加固和文件导入型预聚合恢复保护，并将写入授权校验推进到 MetaStore 服务端。最新一轮修复了恢复状态判断、授权内存保留、数据库真实重开及权限安装接线问题，相关本地测试通过。
>
> 当前仍有两类工作：一是完成预聚合构建领取、结果发布、引用保护和回收的原子协调；二是将统一版本部署到测试集群，完成新授权协议、故障恢复及多节点验收。项目已有实质性能力和验证进展，但尚不能宣布整套 Cube 达到生产级高可用。

## 目录

- `api/`：CubeCluster、CubestoreRouter 类型定义。
- `controllers/`：Cube 编排、主备协调及 authority 接入。
- `config/crd/`：CRD 清单。
- `config/manager/`：Operator 部署对象。
- `config/rbac/`：权限清单。
- `demo/k8s/`：演示、检查脚本及分日期原始证据。

## 2026-09-08 原子预聚合账本进展

本轮新增 MetaStore 原子账本、受认证 Router HTTP 桥接及显式 Driver 客户端。Rust lib/bins 编译检查通过；Driver 53/53 测试及类型检查通过。Rust 测试构建在 600 秒上限内未完成，未执行用例。

**当前仍为生产 `NO-GO`，不是全部整改完成。** 真实预聚合 CREATE/绑定/发布、查询及流式引用生命周期、Operator 新入口认证接线仍未完成集成；没有部署新镜像或执行 Kubernetes 切主 E2E。新账本入口拒绝默认无密码认证，不会自动替换原预聚合流程。

实现范围、链路图、原始日志与待完成门禁见 [2026-09-08 原子账本进展](HA-ATOMIC-LEDGER-2026-09-08.md)。历史测试不得当作本轮新增协议的验收结果。

## 2026-09-08 业务接入续作（未验证）

已将原子账本接入真实预聚合领取、建表/绑定、发布和回收路径，并新增 Rust 实际查询/流式引用保护、默认 Router 密码认证及 Operator Secret 接线。本批没有运行编译、测试、镜像构建或部署，不能沿用上一阶段的通过结果。

**仍为生产 `NO-GO`，且有明确代码阻断项，不只是缺测试：** 重复 Bind 会覆盖原子建表来源标记；Driver 授权记录缺 key/generation 显式比较；故障脚本直接构造 WebSocket 尚需认证接线。此外，建表拒绝持久化、部分诊断读取、来源丢失和过期构建恢复仍未闭环。

完整范围、配置契约和待修问题见 [业务接入续作报告](HA-BUSINESS-INTEGRATION-2026-09-08.md)。请勿将本批源码的能力声明当作运行验收。

## 2026-09-09 本地修补与回归

四处已授权修补已写入：Bind 保留原子建表标记、schema 缺失拒绝结果持久化、Driver key/generation 比对、故障脚本旧主 WebSocket 认证。Operator 两包回归、Driver 原有 53 项、Orchestrator 67 项及脚本 15 项纯逻辑测试通过。

**第二轮结果：** Driver 三处 mock 类型转换已修正，新增构建套件 15/15 和类型检查通过；Rust 枚举构造已修正，但测试目标因 `MetaStoreMock` 漏实现四个新增方法而编译失败（E0046，821.8 秒，退出码 101），行为测试仍为 0 项。整体未通过，生产仍为 `NO-GO`。没有 Kubernetes 故障注入、镜像部署或 Git 操作。

精确错误、各层结果及原始日志见 [2026-09-09 本地回归报告](HA-LOCAL-REGRESSION-2026-09-09.md)。
