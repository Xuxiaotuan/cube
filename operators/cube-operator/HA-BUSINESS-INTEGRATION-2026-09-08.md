# Router HA 业务接入续作：2026-09-08

## 当前结论

**本批已修改真实调用路径，不再只有账本接口；但存在明确未修问题，且本轮没有运行编译、测试或部署。生产仍为 `NO-GO`。**

上一阶段的 Driver 53/53 和 Rust 非测试目标编译结果不能作为本批修改的验证结果。本批状态为 `evidence_incomplete`，不只是“缺少一份测试报告”。没有提交或推送代码，没有操作 Kubernetes Pod、Secret、PVC 或现有数据。

## 已写入的业务接入

| 范围 | 本批实现 | 证据层级 |
| --- | --- | --- |
| 预聚合真实流程 | Loader 执行队列接入权威 selection claim、physical claim、来源意图、续租、typed CREATE、publish、权威 ready；严格模式不以 CACHE NX 作为选主或发布判据 | 代码已写，未验证 |
| 建表与绑定 | MetaStore 复用 `create_table_in_batch`，同批创建真实表、索引、绑定和请求回执 | 代码已写，存在下述 provenance 问题 |
| 导入完成与发布 | 原子创建表的 Publish 检查持久导入回执后，同批设置真实 ready 和 Published；未完成导入不能只靠客户端设置 ready | 代码已写，未验证 |
| 回收 | 严格流程使用账本 retire/drop，不直接 SQL DROP；被引用表不能进入回收 | 代码已写，完整读取覆盖仍有缺口 |
| 查询引用 | SQL 结果缓存的实际执行回调获取物理表引用；缓存纯结果命中不登记物理读取；详细执行分析接入相同保护 | 代码已写，未验证 |
| 流式执行 | 底层本地/集群流使用有界生产者排空及执行结束协调，不能因外层 Promise 或消费者取消就立即释放引用 | 代码已写，未验证 |
| 默认 Router 认证 | `CUBESTORE_SQL_PASSWORD` 非空时启用密码；显式空值或不可用值拒绝，不降级无密码 | 代码及测试源码已写，未执行 |
| Operator 配置 | 复用 CR 的 `pod.env` 和 SecretKeyRef，派生 API/Refresher 凭据、严格开关及 Router drain 凭据 | 代码及测试源码已写，未执行 |
| Driver WebSocket | Driver 内部 WebSocket 连接携带与 HTTP 相同的 Basic 认证信息 | 代码已写；故障脚本的直接构造仍需接线 |
| 故障证据 | 原脚本增加原子 ledger、Published、物理 tableId、locations 与版本比对，不能仅以 CACHE ready 判成功 | 断言及纯测试源码已写，未运行真实 E2E |

## 严格模式配置契约

复用现有 CRD 的 `pod.env`，没有增加新的 CRD 字段或外部组件。

- `spec.router.pod.env` 中的 `CUBESTORE_SQL_PASSWORD` 必须引用非 optional 的 SecretKeyRef。
- `spec.api.pod.env` 中的 `CUBEJS_CUBESTORE_USER` 必须配置非空用户名。
- Operator 将同一 SecretKeyRef 派生为 API/Refresher 的 `CUBEJS_CUBESTORE_PASS` 和 Router 的 `CUBESTORE_DRAIN_PASSWORD`。
- Router 的 `CUBESTORE_DRAIN_USER` 使用同一用户名，避免 `--drain` 因缺少认证失败。
- 严格模式下 API/Refresher 配置 `CUBEJS_CUBESTORE_PRE_AGGREGATION_LEDGER_STRICT=true`。
- 默认健康探针使用免认证路径；SQL 密码不注入 lease-agent、MetaStore 或 Worker，也不写入 ConfigMap。

示例见 [authority-canary.yaml](config/samples/authority-canary.yaml)。这只是配置接线，不表示该示例已在集群创建或运行通过。

## 当前明确阻断项

| 项目 | 问题与影响 | 当前处理 |
| --- | --- | --- |
| 原子建表来源标记 | 旧 Bind 分支重复绑定会把 `Binding.created` 写成 false；随后 Publish 可能错误进入旧表路径 | 新发现，未修补，禁止据此部署 |
| 授权记录身份核对 | `AtomicPreAggregationBuilds.authority()` 检查存在及 owner，但缺少返回 key/generation 与请求定位信息的显式比较 | 新发现，未修补 |
| 建表拒绝持久化 | schema 不存在时仍可能返回普通错误，没有统一保存为可重放的业务拒绝回执 | 未闭环 |
| 故障脚本 WebSocket 认证 | 直接构造旧主连接的调用点没有自动继承 Driver 认证；401/403 不能作为旧主隔离证据 | 待补同源认证并验证连接确实建立 |
| 诊断读取保护 | 普通物理 EXPLAIN、DEBUG DUMP 文件读取尚未完整接入引用保护 | 未闭环，不声明全部读取已保护 |
| 来源意图恢复 | 来源缓存完全丢失，或 physical claim 后、来源记录写入前崩溃，仍保持 UNKNOWN | 没有自动恢复入口，不能称生产恢复闭环 |
| 过期构建接管 | 无人续租超过当前一小时期限后不盲目领取新代次接管旧表 | 安全拒绝不等于自动恢复完成 |
| 功能兼容 | 聚合索引、流式 ingestion、无 LOCATION 导入及未明确精度的 decimal/bigdecimal 暂不接受 | 不能称所有预聚合类型均已支持 |
| 测试与运行 | 新建表/多表原子测试矩阵尚未补全；本批无编译、回归、镜像或故障 E2E 结果 | `evidence_incomplete` |

## 引用释放的安全边界

实际查询执行采用独立执行任务持有引用；客户端取消不会直接取消实际执行并立即释放引用。底层流要完成排空并确认结束，成功结果完成后才尝试释放。

遇到执行/传输错误、任务终止不明、进程崩溃、释放 RPC 失败或旧 Router 已失去授权时，引用会保留。没有使用 TTL 或对象 Drop 自动释放来掩盖不确定状态。

这避免了不安全的过早回收，但会留下待对账引用，且客户端取消后后端可能继续消耗资源。**当前没有完整的跨节点取消确认或崩溃引用自动治理，不能宣称 GC 已生产闭环。**

## 后续验收顺序

1. 修复上述明确代码问题及故障脚本认证，补全必要测试源码。
2. 获准后运行 Rust、Driver、Operator 和脚本断言回归，分别记录实际结果。
3. 构建统一版本镜像，在明确授权的测试环境部署完整 Cube API/Refresher 链路。
4. 对已确认身份的测试 Router 注入故障，验证数据、构建代次、旧写入拒绝和引用/回收结果。

故障注入不能直接套用历史默认命名空间并强制删除当前 Router。必须确认本次目标、版本及数据影响范围。
