# Router HA: 12 项收尾工作记录

## 结论和版本边界

基线为 `87020e400f`，本报告对应之后的未提交工作区。**尚未全部完成，不是生产 GO，也不是已推送的发布版本。** 用户要求全部修复，以下保留每项的实现和验证差距，不把局部安全阻断、单测或文档当成完整能力。

本轮使用五个并行子任务，分别处理控制面、TS 恢复、Rust 隔离、发布部署，以及独立 Refresher 崩溃脚本。协调任务执行了本地环境检查、完整 Go 回归、Operator 镜像构建/部署和 Cube API 数据核对。当前 Kubernetes 只有一个 OrbStack 节点；生产 context、存储提供方、RPO/RTO 尚未提供。

## 逐项状态

| # | 工作 | 本轮实现或证据 | 剩余事项 / 状态 |
|---|---|---|---|
| 1 | Lease 丢失与初始化恢复 | 保留 bootstrap guard，检查存活 Pod 历史；Release 改为 CAS 过期 tombstone，保留 epoch，拒绝 int32 溢出；新增受控人工恢复手册和模型测试 | 人工恢复要求旧实例真实隔离和完整历史上界 H，未实操灾难恢复；evidence_incomplete |
| 2 | Manager 生命周期与接管 | SetupSignalHandler 取消调谐；续约验证完整身份；新 Operator 实际接管后 leader/epoch 不变 | 尚未完成 SIGKILL、暂停旧控制器后恢复、所有 Manager 故障场景和连续业务中断窗口测量 |
| 3 | 权威读取回归 | 独立缓存/权威 fake、不可达、冲突、缺少 Reader、取消测试；真实 K8s CAS 三轮通过 | 真实 API 网络分区仍属于完整故障矩阵，不由 fake 替代 |
| 4 | Router/Worker 旧执行者隔离 | Router 子任务传播 guard，本地串行写前重检；健康 Job 子任务不绑定 Router epoch；修复完成回执丢失后的幂等返回 | 此阶段新编译程序 23 项通过；随后新增真实 RPC fixture 编译失败，当前 Rust 测试集不能算通过；远程完整数据故障未通过 |
| 5 | Refresher 自身崩溃恢复 | UNKNOWN 保留错误身份并阻止 iterator 将其作为普通源错误跳过；新崩溃 harness 候选代码 | harness 存在语法错误，三处崩溃边界均未跑通；不支持跨上下文遗留构建后台接管 |
| 6 | 安全 GC | 未加入不安全 TTL 或客户端扫描后盲删 | **未实现**。未使用的 compaction 候选方法已撤下，永久身份、清单、阶段及链仍增长 |
| 7 | 扫描性能 | metadata visitor 有界并发，不截断历史；保留未知构建保护 | CACHE KEYS 仍不分页，历史链未压缩，无量化负载预算；未完成 |
| 8 | 发布兼容与回滚 | digest 组合校验、离线 release freeze/check、升级和回滚证据契约；本地全 tag CR 仍支持 | 不等于运行时协议协商；新旧 RPC 混部、升级中断、逆向持久状态回滚尚无实测 |
| 9 | 可观测性 | 有限 condition 指标、完整观察时间戳、过时代际=-1、删除清理；19 项 digest/metric 用例通过 | 未完成 UNKNOWN-since、积压、字节容量、PVC 和阶段 RTO 指标 |
| 10 | 同一产物验收 | 离线检查器 16 项用例通过；Operator 新镜像实际部署，API 既有数据对账通过 | 未冻结并重跑一整套最终镜像；已知失败的新测试不得标为 PASS |
| 11 | 权威状态灾备 | 手册涵盖 MetaStore、CacheStore、对象文件共同一致性点与旧节点隔离 | 单节点环境，无经批准 RPO/RTO 和存储恢复条件；external_blocked |
| 12 | 生产配置 | 示例 SA/Role/Binding 命名空间统一，补 Manager Lease 权限和部署前提说明 | 未部署生产等价多节点拓扑，未证明卷迁移与网络边界；external_blocked |

## 本轮验证记录

| 验证层级 | 结果 | 说明 |
|---|---|---|
| 最终完整 Go 测试 | PASS | `go test ./... -count=1 -timeout=120s`；主程序/API 类型包没有测试，不计数；真实 K8s 测试默认 opt-in SKIP |
| 真实 Kubernetes CAS | 3/3 PASS | `CUBE_HA_LIVE_LEASE_TEST=1 go test ./internal/leadership -run '^TestKubernetesLiveLeaseCAS$' -v -count=3 -timeout=120s`；仅创建唯一测试 Lease，并用 UID 前置条件删除 |
| digest/指标专项 | 19 项 PASS | 6 个顶层 Go 测试，19 个独立叶子用例；包含 15 个子测试，不重复计数 |
| 离线发布检查器 | 16/16 PASS | 临时文件 fixture，不是镜像发布或业务验收 |
| TS 源码限定单测 | Driver 40、Orchestrator 10、Core 4 PASS | 子任务执行；不是各包完整测试总数；早期更高数字包含后续已撤销候选代码的测试，不作为最终数字 |
| TS 编译产物 | 三包成功生成 dist | Driver/Orchestrator/Core `tsc --project ... --noEmitOnError`；未构建部署新 API 镜像 |
| Rust 本轮第一阶段 | 23/23 PASS | 真实增量编译后 job_attempt 8、HA 4、HTTP 11；不是旧缓存程序证据 |
| Rust 后续 RPC fixture | FAIL（编译） | E0277；两项新 RPC 场景未执行。此失败覆盖“当前测试集通过”的结论 |
| 查询观测器纯函数测试 | 5/5 PASS | 不覆盖 HTTP 成功缺少 data 的已知漏判，不能独立认证故障观察脚本 |
| Refresher 新脚本 | FAIL（语法） | 未运行任何真实 Refresher 崩溃场景 |

初始全量 Go 回归曾在健康初始化时触发 30ms fixture 超时。修复将健康同步恢复为生产时限，故障注入仍保留 30ms，增加进入阻塞读取的断言；相关测试重复 50 次通过，最终完整套件通过。初始 TS 编译发现 TS2683；引入该错误的不安全全局扫描/API 候选代码已撤下，最终三包实际类型检查与 emit 通过。保留失败记录，不仅归档绿灯。

## 实际本地部署和数据

- Context `orbstack`，namespace `cube-ha-remediation`；不操作生产集群。
- 新 Operator：`cube-operator:ha-closure-20260907`，Linux ARM64 Image ID `sha256:fe9c58cbe3ea516a5b522cd3b160730646bfc3c45bde743629392cd466efbcb9`。
- Image ID 是本地构建内容标识，不冒充 registry manifest digest。没有发布镜像到远端仓库。
- 仅更新 `deployment/cube-operator` 的 `manager` 镜像，rollout 成功；Router、lease-agent、API、Refresher、MetaStore、Worker 仍使用此前镜像。
- 接管前后 leader 均为 `analytics-router-69457448dc-vm9nn`，epoch 均为 **20**。这是一次升级接管观察，不代表全部重启/分区测试通过。
- 更新后真实 Cube API 查询于 `2026-09-07T03:51:09.350Z` 返回 **16** 组；行数 **4096**、金额 **142653440**、ID 校验和 **22914881536** 均符合独立预期。
- 另有升级前 10 秒、10 次既有预聚合消费成功记录。它可能命中 API/查询缓存，不是每次都穿过 Router 的证明，也不是新预聚合恢复证据。不得将其当作连续零中断或 SLA。

## 当前必须修正的新增验收代码

1. `demo/k8s/control-plane-query-observer.js`：HTTP 成功但没有 data 时可能跳过失败计数。修正应将缺失 data 明确判失败，并加入响应层测试。
2. `demo/k8s/refresher-restart-check.js:276`：`HA_ALLOW_REFRE SHER_DELETE` 拼写导致语法错误。应删除该错误的 undefined 断言，保留下方明确授权等于 `1` 的断言，不能仅去掉空格形成互相矛盾的检查。
3. `rust/cubestore/cubestore/src/metastore/job_attempt_rpc_tests.rs:219`：fixture 将 `Arc<dyn MetaStore>` 注入需要 Sized 具体类型的工厂，应保留 `Arc<MetaStoreRpcClient>` 直到注入，不修改生产 Injector 协议。

已向用户报告新增问题，当前停在修正确认边界；这些失败候选未提交推送。不得删除失败测试后声称场景通过。完整 GC 与容灾前提是另外的未完成事项，不会因修正这三处测试代码自动完成。

## 相关手册

- [Lease 受控恢复](LEASE-RECOVERY.md)
- [固定产物验收](RELEASE-ACCEPTANCE.md)
- [生产部署与灾备前提](PRODUCTION-DEPLOYMENT.md)
- [本次原始证据](demo/k8s/evidence/2026-09-07-closure/README.md)

## 本轮授权修复后的更新：2026-09-07

本节覆盖上文关于三个测试问题“尚未修复”的旧状态，不覆盖其他未完成项，也不代表生产验收通过。

| 项目 | 本轮结果 | 证据边界 |
| --- | --- | --- |
| 查询观测器漏判 | 已修正 HTTP 成功但缺少 data 的漏判；6/6 测试通过 | 含真实本地 HTTP 服务驱动 CLI：缺失、null、空数组均记录偏差，后续恢复不能掩盖失败 |
| Refresher 演示脚本授权变量拼写 | 已移除错误断言；13/13 helper、授权及 wire 测试通过，语法检查通过 | 未执行真实 Refresher 崩溃恢复 |
| Rust RPC 测试类型错误 | remote_store 返回具体 Arc<MetaStoreRpcClient>，编译通过 | 两项真实 RPC 测试中旧 attempt 拒绝通过，CSV 用例在初始化阶段失败 |
| 本地 Operator 重启 | PASS；60 秒观察，59 次成功查询，0 次不可用，0 次数据偏差 | 已有 rollup 经 Cube API 消费，可能命中缓存，不证明每次执行 Router 或新建预聚合恢复 |

### 本地重启的实际数据

- 环境：orbstack / cube-ha-remediation，单节点。
- 操作：滚动重启 deployment/cube-operator，rollout 成功。
- Router leader 前后不变：analytics-router-69457448dc-vm9nn；epoch 前后均为 20。
- API Pod 未替换：analytics-api-8d47d47-52mxd。
- EndpointSlice 唯一就绪后端与 leader 匹配。
- 查询汇总：行数 4096，amount 142653440，checksum 22914881536。
- 规范化响应 hash：2cc18a091c142902cc2af305542fe869977e259e071d155313df90d8bc189784。
- 原始证据：demo/k8s/evidence/2026-09-07-closure/approved-fixes/。

### 新发现的阻塞，尚未修复

1. Rust CSV RPC fixture 在 SchedulerImpl 建立事件接收端前调用 prepare_import，触发 SendError(Insert(Schemas,1))。错误发生在测试初始化，不能计为 CSV 导入/响应丢失验证通过。存储写入可能已经提交，不应通过吞掉生产事件错误修复。建议先注册远端 MetaStore，再解析 SchedulerImpl 保持接收端但不启动处理循环，之后准备数据和领取 Job。
2. Refresher 演示脚本要求所有其他 Pod Ready，但当前命名空间存在正常 Completed 的 MinIO 初始化 Job Pod，会产生误失败。需区分应持续运行的服务 Pod 与已成功完成的 Job Pod。
3. Refresher 新 Pod Ready 与故障屏障释放的先后关系仍需确认；当前不能宣布其真实恢复 E2E 通过。

以上新问题只做了定位，没有继续修改。最新 Rust/API/lease-agent 代码尚未整体构建部署并完成全矩阵。未执行生产、多节点隔离或灾备测试，结论仍为 evidence_incomplete / 非生产 GO。本轮未提交或推送 Git。

## 第二轮授权修复结果：CSV fixture 与 Completed Job Pod

本节覆盖上一节中这两个问题“尚未修复”的状态；不改变其他未完成项。

| 修复 | 修改范围 | 本轮实际验收 |
| --- | --- | --- |
| CSV fixture 初始化顺序 | job_attempt_rpc_tests.rs：先注册远端 MetaStore，再解析 Scheduler 保持事件 receiver，最后 prepare/claim；不改变生产事件处理 | task4_rpc_ 两项真实 RPC 测试 2 PASS / 0 FAIL，退出码 0，含编译共 47.84 秒 |
| Completed Job Pod 误判 | refresher-restart-check.js/test.js/md：仅豁免 phase=Succeeded 且有 Job ownerReference 的 Pod 的 Ready 要求 | 20/20 helper/wire 测试通过，含新增 7 项针对性用例；JS/shell 语法检查通过 |

Rust 覆盖 TCP 暂停后旧 attempt 拒绝，以及真实 HTTP CSV 导入、发布/完成响应丢失后的恢复；核对 chunk 身份不重复、receipt 和 SQL 数据 11、22、33。执行命令：

```bash
cd rust/cubestore
CARGO_TARGET_DIR="$PWD/target" cargo +nightly-2025-08-01 test --locked --offline -p cubestore --lib task4_rpc_ -j 1 -- --test-threads=1 --nocapture
```

Refresher 校验仍拒绝 Failed Job、运行中未 Ready Job、非 Job 的 Succeeded Pod、服务缺失及 UID 替换；镜像、重启次数和删除状态检查保留。

原始日志已归档到 `demo/k8s/evidence/2026-09-07-closure/approved-fixes/rust-rpc-setup-fix.log` 与 `refresher-completed-job-fix.log`。旧失败日志保留，不用后续成功覆盖历史证据。

边界：本轮仅修复并验证上述测试代码。未构建 Linux 镜像、未部署最新全套代码、未执行真实 Refresher 崩溃恢复、未执行完整 K8s 故障矩阵。屏障释放与 Ready 的依赖关系、初始 unfinished ledger 门禁及固定 API Pod IP 限制仍需在真实 E2E 前处理或确认。本轮未执行 Git 提交或推送。整体仍为 evidence_incomplete，非生产 GO。

## 全量剩余任务第一波：代码进展与新增阻塞

基线为已推送的 bad90b9ee6；本节记录后续未提交修改，不覆盖此前历史原始日志。

| 工作 | 本轮结果 | 不能据此声称 |
| --- | --- | --- |
| Durable build 队列超时 | 携带 buildId 的超时保留为 MUTATION_UNKNOWN，并增加构建关联日志；Driver 40/40、Orchestrator 14/14、Core 4/4；三个包 noEmit 类型检查通过 | 未实现独立上下文恢复入口或完整 GC，未更新 dist |
| Refresher 故障 harness | Running 后释放屏障、最终再验证 Ready；稳定 Service DNS；增加恢复后两次 API 消费与身份检查；27/27 helper/wire 和语法检查通过 | 未执行真实 E2E；不存在“27 次真实恢复成功” |
| 升级/回滚发布门禁 | 精确、有方向的批准边及证据哈希绑定；Python 31/31 | 未创建真实兼容边，没有运行时协议协商或真实升级/回滚通过 |
| 组件指标 | desired/ready/updated 副本及 generation lag；相关 Go 26 个独立聚焦用例通过 | 不是应用恢复积压、UNKNOWN 年龄及容量监控全部完成 |
| 隔离 Lease 恢复 | 本地真实 K8s 四个独占 UUID Lease 场景 4/4，通过 UID 前置条件清理 | reservation 为测试模型；未证明生产 epoch 上界发现或旧节点隔离 |
| 生命周期故障测试 | 本地子进程 SIGTERM/SIGINT/SIGKILL 通过；暂停测试 FAIL | 不能宣称全量 Go 测试通过或真实 Manager 故障矩阵完成 |
| 生产预检查 | 新只读工具，7/7 单测；真实 orbstack 运行 BLOCKED、exit 1 | 不是工具执行故障，也不是生产 GO |

### 必须公开的本轮问题

- 新增 lifecycle_acceptance_test.go 的暂停状态识别在 Darwin 上失败。仅定位，尚未再次修补；原始状态数值未记录，不能把推导值冒充实际捕获值。
- 新增 isolated-controlplane-acceptance.sh 写入仓库根 demo/k8s，而非 operators/cube-operator/demo/k8s。尚未移动；移动时还须修正 ROOT 和手册入口，不能只改位置。
- 这两个问题保留为当前失败，未通过跳过测试或放宽超时隐藏。尚未冻结镜像、构建或部署这一波。

### 核心协议缺口，暂停在架构确认边界

A3 不只是欠一次测试：远端 MetaStoreCall 不携带 Router 授权，调用方取消不能撤回已发出的请求。A4 已测试的 JobAttempt 路径是独立保护，不能替代 A3。

完整上下文恢复与 GC 同样缺少可信 scope、单调 generation 和提交时退休检查。不能靠全局扫描或删除 PRE_AGG 身份掩盖。待确认设计见 [HA-PROTOCOL-V2-DESIGN.md](HA-PROTOCOL-V2-DESIGN.md)：K8s 选主，MetaStore 统一最小授权/构建/退休账本，严格拒绝旧协议入口；不增加 Redis、PG 或自研 Raft。新增协议、服务身份与停写升级尚待用户确认，没有实现。

### 当前实际环境阻塞

2026-09-07 的只读预检查：Router 同在一个 orbstack 节点；MetaStore/Worker 挂载 local-path RWO 卷；namespace 无 NetworkPolicy；组件及 Operator 使用 tag 而非 digest 引用；RPO/RTO 未提供。非本地存储、NetworkPolicy 存在或两个节点本身也不等于灾备、安全及物理故障域已经验收。

新工具及说明：demo/k8s/production-preflight.py、production-preflight.test.py、production-preflight.md。没有读取或导出 Secret/容器环境变量，没有修改集群。

### 证据来源

已存在原始文件的结果归档到 demo/k8s/evidence/2026-09-07-closure/wave1/。TS 与发布门禁测试当时未保存磁盘日志；其数字来自对应执行任务的工具输出（TS session 52750、chunk 286173；发布门禁 session 66888），不是可下载的完整原始日志。不得伪造原始日志或把摘要标成原文。

整体仍是 evidence_incomplete；生产环境、目标和新增协议决定属于待输入项。本轮未提交、未推送、未构建新镜像、未改变实际 Cube 工作负载。

## 协议与停写升级获准后的本轮结果

用户已确认“允许 接受”。以下是实际实现与测试状态，不等于剩余任务全部完成。

| 项目 | 当前结果 | 证据层级 |
| --- | --- | --- |
| 上轮暂停状态识别 | 修复后 lifecycle 重复 10 次通过，实测 status=0x117f、PID 匹配 | 本地子进程／隔离模型 |
| 上轮脚本错误目录 | 已移至 operators/cube-operator/demo/k8s，ROOT/手册同步，语法与 synthetic wrapper 通过 | 本地脚本 |
| 修复上述两项后的全量 Go | 当时完整离线 go test ./... 通过 | 仅该时点；不能覆盖之后新增 authority agent 代码 |
| MetaStore PKI | 21 个独立用例通过；immutable Secret、归属、链、SAN、密钥及有效期校验 | helper 未接实际控制器渲染 |
| Kubernetes 身份验证 | 实机正确 audience/Pod 绑定、错误 audience、无效 token 三项通过 | 使用本地管理身份执行 TokenReview，不证明 MetaStore SA RBAC 或 CubeStore RPC |
| 可重复身份脚本 | authority-tokenreview-check.py：3 个 helper 测试通过；实际脚本 exit 0 | 仅 orbstack/cube-ha-remediation，显式 opt-in；token 仅在内存传递 |
| lease-agent 授权接线 | HTTPS install ACK 后复查 Lease/Pod，再发布 active 文件；配置和若干拒绝路径通过 | 新超时夹具挂起，agent 包 90s FAIL，后续用例未完成 |
| Rust A3 | HTTPS、认证、持久 grant/写入检查及客户端/启动接线已写入 | --locked --offline 构建因 Cargo.lock 未同步 exit 101，尚未进入编译 |

### 当前必须修正的三个具体问题

1. 新 Go authority timeout handler 等待请求上下文退出，测试服务器清理挂起。需读完请求体、提供独立清理释放通道并保持 deadline 断言，不能只扩大超时。
2. 新 Rust Worker admission 在写线程恢复 JobAttempt 上下文之前运行，可能误拒绝合法 Worker。已发现但尚未修正；不得启用 strict。
3. Cargo.toml 增加 warp/tls 后 Cargo.lock 未同步。锁定构建被正确拒绝；需要离线同步依赖锁后重新编译及测试，不能把之前的 Rust 2/2 当作本次补丁通过。

上述问题未通过跳过用例或伪造 PASS 隐藏，本轮停在验收关口。没有新镜像、没有部署、没有提交/推送；当前 full-Go 和新 Rust 协议均不能标 PASS。

### 实际 Lease 契约修正与尚未交付项

holder 为 Pod 名称，epoch 来自 leaseTransitions，权威注解前缀为 cubejs.io/lease-*，不能使用最初草案的假定字段。完整协议与已批准状态见 HA-PROTOCOL-V2-DESIGN.md。

PKI helper 仍未接到渲染；TokenReview RBAC、投射 token、严格模式激活/维护状态机及跨组件真实测试仍未完成。scope/generation/retirement 权威账本、迁移、Refresher 完整恢复及 GC 尚未实现。生产环境、RPO/RTO、保留策略输入仍缺失。

### 本轮原始证据

归档目录：demo/k8s/evidence/2026-09-07-closure/authority-checkpoint/。

- controlplane-approved-fix.log：上轮两个测试问题修复、重复测试及该时点全量 Go。
- tls-helper.log：PKI 21 项局部测试。
- tokenreview-live.json、tokenreview-script.json、tokenreview-helper.log：真实身份与脚本证据，不包含 token。
- agent-integration-fail.log：新 authority agent 超时失败与堆栈。
- rust-locked-build-fail.log：新 Rust 锁文件构建失败。

身份脚本使用 CUBE_HA_AUTH_IDENTITY_TEST=1 显式启用，生成十分钟专用 audience、绑定现有 Router Pod 的短期测试 token，不落盘、不输出，不修改权限或工作负载。它不是生产通用授权工具，也不替代业务 HA 验收。

## 三处问题获准修复后的最新验收结果

用户确认继续修复上一节三个问题。本节覆盖其旧状态，但不覆盖整体未完成边界。

| 问题 | 本轮修改 | 实际验证 |
| --- | --- | --- |
| Go timeout 夹具清理挂起 | 先读取请求体，cleanup 独立释放 handler；保留 30ms 并断言 context.DeadlineExceeded | timeout 子用例 0.03s PASS；authority/promotion/sync 专项及全量离线 go test ./... 全部 PASS，exit 0 |
| Rust Worker 上下文顺序 | JOB_ATTEMPT 在 admission 前恢复，Router grant 与 Worker owner 检查保持独立 | 修改已应用；后续编译阻塞，运行验收未完成 |
| Cargo.lock 未同步 | 离线同步 4 个新增 TLS 依赖，未升级已有包 | --locked --offline 已进入实际编译，不再报锁文件未同步 |

新编译阻塞：rocks_store.rs:94 的穷尽匹配未覆盖新增 TableId::RouterAuthority，编译报 E0004，40.9 秒后 exit 101。这是上一轮新增授权代码遗漏的分支，尚未继续修正。没有跳过或吞掉编译错误。

实际 Rust 编译命令：

```bash
cargo +nightly-2025-08-01 metadata --offline --format-version 1
cargo +nightly-2025-08-01 test --locked --offline -p cubestore --lib --bins --no-run -j 1
```

本轮 authority_、task4_rpc_ 的执行数均为 0。先前历史 RPC PASS 不代表当前新增授权代码通过。Rust 仍 NOT_READY_TO_FREEZE，不能继续宣称跨组件验收完成。

最新原始日志归档：

- demo/k8s/evidence/2026-09-07-closure/authority-checkpoint/go-authority-fixed.log
- demo/k8s/evidence/2026-09-07-closure/authority-checkpoint/rust-authority-fixed-build.log

本轮没有 Kubernetes 工作负载变更，没有镜像构建/部署，没有 Git 提交或推送。严格模式仍未启用；PKI 渲染、协议激活、构建生命周期账本、恢复及 GC 等后续任务仍未完成。

## 2026-09-07 RouterAuthority 分支修复及剩余门禁

本节补充此前 E0004 编译失败记录，不覆盖历史失败证据。

- 已在 `rust/cubestore/cubestore/src/metastore/rocks_store.rs` 显式加入 `TableId::RouterAuthority => false`：持久授权记录不参与 TTL 清理，不使用 wildcard。
- 执行 `cargo +nightly-2025-08-01 test --locked --offline -p cubestore --lib --bins --no-run -j 1`，180.02 秒后达到有界执行上限，退出码 124。此次输出未再报告 E0004，但编译未完成，不等于编译通过。
- `authority_`、`task4_rpc_` 测试均未启动，实际执行测试数为 0。不能复用旧版本 A4 的通过记录为新协议背书。
- 原始输出：`demo/k8s/evidence/2026-09-07-closure/authority-checkpoint/rust-authority-match-fix.log`。
- 本轮未构建镜像、未部署、未提交或推送。当前状态：`evidence_incomplete`，Rust `NOT READY_TO_FREEZE`，生产 `NO-GO`。

### 尚未闭环的工作

| 优先级 | 工作 | 当前证据 / 缺口 | 完成标准 |
| --- | --- | --- | --- |
| P0 | Rust 编译与授权回归 | 编译超时，两个测试组未执行 | 完整编译成功，记录实际测试数量及结果 |
| P0 | Operator 到 authority 的部署集成 | TLS helper 有独立测试，但自动证书挂载、专用 SA/RBAC、投射 token、配置及严格模式启用未闭环 | Operator 创建完整依赖，真实 MetaStore 权限及 Go/Rust 联调通过 |
| P0 | 旧主隔离与切主安全 | 缺新协议下真实暂停旧主、断网、Lease 读失败及 Worker 并发验收 | 旧主写入被拒绝，新主及合法 Worker 可工作，拒绝原因可追踪 |
| P0 | 持久授权恢复 | restart fixture 只重建 authority 状态；不是完整数据库关闭重开证明 | 数据库重开、快照恢复、授权记录缺失和旧状态回滚均有明确安全行为及实验 |
| P0 | Pre-aggregation 恢复与回收 | authoritative generation、retirement、reference ledger 未实现；UNKNOWN 对账未闭环 | 构建/发布/回收具备一致的权威身份与状态，失败不误发布、不误删 |
| P0 | Refresher 到 Cube API 的真实 E2E | harness/helper 通过不代表业务切主通过 | 三个故障窗口实测；经真实 Cube API 验证结果和物理预聚合身份 |
| P1 | strict 模式完整运行兼容性 | 后台 MetaStore 写入无请求上下文会被拒绝；Worker 白名单覆盖需实测 | Scheduler、GC、compaction、upload 和健康 Worker 全链路通过 |
| P1 | 授权性能与资源有界性 | 当前每次 Router mutation 安装并验证授权；grant 内存映射未见旧条目清理 | 保持 fencing 语义前提下限制状态增长，并测量 API 压力、吞吐和延迟 |
| P1 | 错误分类与监控 | 客户端非 2xx 归并为远端拒绝；UNKNOWN 年龄、积压、容量及分阶段 RTO 未齐备 | 错误分类可驱动安全恢复，监控和告警可定位阻塞 |
| P1 | 版本发布与升级 | 最新源码未形成统一镜像及 digest；无真实升级/回滚证据 | 固定版本完成兼容性、升级、回滚和整轮业务故障测试 |
| 环境 | 生产等价验收 | 本地单节点、local-path PVC、无 NetworkPolicy；RPO/RTO 尚未定标 | 明确指标，多节点隔离/恢复、MetaStore 与对象存储备份恢复验收 |

运行环境本轮只读快照：`orbstack` / `cube-ha-remediation` / `analytics` 的 `spec.authority` 为 null；API 镜像为 `cube-studio-api:ha-remediation-20260907-refresher`，lease-agent 为 `cube-operator:ha-remediation-20260907-cutover`，MetaStore/Router/Worker 为 `cube-studio-router:ha-remediation-20260907-final`。它们不包含本轮新授权协议的部署证明。`ProductionReady=False`，原因为 `EvidenceIncomplete`。

结论：已有入口选主和切换能力，不等于已完成生产级写一致性及预聚合恢复。上述门禁未通过前，不宣布生产 GO。
