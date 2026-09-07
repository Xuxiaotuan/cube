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
