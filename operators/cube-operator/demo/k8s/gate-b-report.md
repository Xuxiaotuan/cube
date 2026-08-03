# Gate B failover report

- Started: 2026-08-03T16:05:39Z
- Namespace: `cube-operator-demo`

## Preflight
- PASS: Redis Deployment available
- PASS: MetaStore ready
- PASS: Workers ready: 2
- PASS: Redis PING/PONG
- PASS: Redis Secret contains dsn/url/password keys
- Before leader: none (epoch=8, ip=none)
- FAIL: CR leader/epoch preflight invalid
- PASS: promotion ConfigMap matches CR leader/epoch
## Router contracts before deletion
- FAIL: cube-router-demo-6545987484-8rdrj promotion.json contract invalid
- FAIL: cube-router-demo-6545987484-wktg6 promotion.json contract invalid
- FAIL: leader EndpointSlice mismatch or empty: none
NAME                 TYPE        CLUSTER-IP        EXTERNAL-IP   PORT(S)             AGE   SELECTOR
cube-router-leader   ClusterIP   192.168.194.158   <none>        3030/TCP,3306/TCP   4d    app=cube-router,cubestore.io/router-role=leader
apiVersion: v1
items:
- addressType: IPv4
  apiVersion: discovery.k8s.io/v1
  endpoints: null
  kind: EndpointSlice
  metadata:
    creationTimestamp: "2026-07-30T15:35:57Z"
    generateName: cube-router-leader-
    generation: 2410
    labels:
      app: cube-router
      endpointslice.kubernetes.io/managed-by: endpointslice-controller.k8s.io
      kubernetes.io/service-name: cube-router-leader
    name: cube-router-leader-h2qcx
    namespace: cube-operator-demo
    ownerReferences:
    - apiVersion: v1
      blockOwnerDeletion: true
      controller: true
      kind: Service
      name: cube-router-leader
      uid: 039eded6-5b5b-40ad-85b7-77924388428a
    resourceVersion: "1217609"
    uid: ce5d3a10-2034-475a-8020-c212d097ab0e
  ports: null
kind: List
metadata:
  resourceVersion: ""
- FAIL: no active leader Pod; destructive deletion was not executed
## Final resources
NAME                                READY   STATUS    RESTARTS   AGE   IP                NODE       NOMINATED NODE   READINESS GATES   LABELS
cube-router-demo-6545987484-8rdrj   2/2     Running   0          10m   192.168.194.112   orbstack   <none>           <none>            app.kubernetes.io/name=cube-router-demo,app=cube-router,cubestore.io/router-role=follower,pod-template-hash=6545987484
cube-router-demo-6545987484-wktg6   2/2     Running   0          11m   192.168.194.111   orbstack   <none>           <none>            app.kubernetes.io/name=cube-router-demo,app=cube-router,cubestore.io/router-role=follower,pod-template-hash=6545987484
apiVersion: cubestore.io/v1alpha1
kind: CubestoreRouter
metadata:
  annotations:
    cubestore.io/lease-cluster: cube-operator-demo/demo
    cubestore.io/lease-epoch: "8"
    cubestore.io/lease-token: 8f58ed57bfebe6bd0eb43dc9094c39ffac50d394f60349459469d6008a035efd
    cubestore.io/promotion-candidate: cube-router-demo-6545987484-wktg6
    cubestore.io/promotion-epoch: "8"
    cubestore.io/promotion-phase: fenced
    kubectl.kubernetes.io/last-applied-configuration: |
      {"apiVersion":"cubestore.io/v1alpha1","kind":"CubestoreRouter","metadata":{"annotations":{},"name":"demo","namespace":"cube-operator-demo"},"spec":{"electionStrategy":"lease","healthPath":"/router/status","metaStore":{"address":"cubestore-metastore.cube-operator-demo.svc:9999"},"namespace":"cube-operator-demo","roleConfigMapName":"cube-router-demo-router-role-state","routerPort":3030,"selector":{"app":"cube-router"},"stateStore":{"secretRef":{"name":"cube-router-demo-lease-store","namespace":"cube-operator-demo"},"type":"redis"},"storage":{"dataPVC":"worker-data"}}}
  creationTimestamp: "2026-07-30T15:35:57Z"
  generation: 3
  name: demo
  namespace: cube-operator-demo
  resourceVersion: "1219039"
  uid: b5a03658-1869-4286-b27e-fb1780c5fc6c
spec:
  electionStrategy: lease
  healthPath: /router/status
  leaseDurationSeconds: 30
  metaStore:
    address: cubestore-metastore.cube-operator-demo.svc:9999
  namespace: cube-operator-demo
  renewDeadlineSeconds: 20
  retryPeriodSeconds: 5
  roleConfigMapName: cube-router-demo-router-role-state
  routerPort: 3030
  selector:
    app: cube-router
  stateStore:
    secretRef:
      name: cube-router-demo-lease-store
      namespace: cube-operator-demo
    type: redis
  storage:
    dataPVC: worker-data
status:
  candidates:
  - ip: 192.168.194.112
    lastProbeTime: "2026-08-03T16:05:05Z"
    name: cube-router-demo-6545987484-8rdrj
    namespace: cube-operator-demo
    ready: true
    role: follower
  - ip: 192.168.194.111
    lastProbeTime: "2026-08-03T16:05:05Z"
    name: cube-router-demo-6545987484-wktg6
    namespace: cube-operator-demo
    ready: true
    role: follower
  conditions:
  - lastTransitionTime: "2026-08-03T16:05:05Z"
    message: No leader candidate detected from role state; waiting for reconciliation.
    observedGeneration: 3
    reason: NoLeader
    status: "False"
    type: LeaderElection
  - lastTransitionTime: "2026-08-03T16:05:05Z"
    message: 'Failed to sync role state: router promotion is waiting for a readiness
      gate'
    observedGeneration: 3
    reason: StateSyncFailed
    status: "False"
    type: RoleStateSync
  - lastTransitionTime: "2026-08-03T16:05:05Z"
    message: No Rust Router leadership guard is wired into Job assignment, heartbeat,
      or completion commits.
    observedGeneration: 3
    reason: Blocked
    status: "False"
    type: JobRecovery
  - lastTransitionTime: "2026-08-03T16:05:05Z"
    message: No authoritative Job/upload/pre-aggregation mutation reconciliation endpoint
      is available for UNKNOWN outcomes.
    observedGeneration: 3
    reason: NeedsContext
    status: "False"
    type: MutationReconcile
  - lastTransitionTime: "2026-08-03T16:05:05Z"
    message: Router status must expose isLeader, leaderEpoch, leaseEpoch, leaseTokenHash,
      and metaStoreReady before promotion can route traffic.
    observedGeneration: 3
    reason: NeedsContext
    status: "False"
    type: PromotionReady
  - lastTransitionTime: "2026-08-03T16:05:05Z"
    message: No Refresher CRD/controller or durable active/standby refresh ownership
      entry point exists.
    observedGeneration: 3
    reason: NeedsContext
    status: "False"
    type: RefresherReady
  lastSwitchedAt: "2026-08-03T15:28:41Z"
  leaderEpoch: 8
  recovery:
    jobRecovery:
      message: No Rust Router leadership guard is wired into Job assignment, heartbeat,
        or completion commits.
      reason: Blocked
      state: Blocked
    mutationReconcile:
      message: No authoritative Job/upload/pre-aggregation mutation reconciliation
        endpoint is available for UNKNOWN outcomes.
      reason: NeedsContext
      state: NeedsContext
    promotion:
      message: Router status must expose isLeader, leaderEpoch, leaseEpoch, leaseTokenHash,
        and metaStoreReady before promotion can route traffic.
      reason: NeedsContext
      state: NeedsContext
    refresher:
      message: No Refresher CRD/controller or durable active/standby refresh ownership
        entry point exists.
      reason: NeedsContext
      state: NeedsContext
apiVersion: v1
items:
- addressType: IPv4
  apiVersion: discovery.k8s.io/v1
  endpoints: null
  kind: EndpointSlice
  metadata:
    creationTimestamp: "2026-07-30T15:35:57Z"
    generateName: cube-router-leader-
    generation: 2410
    labels:
      app: cube-router
      endpointslice.kubernetes.io/managed-by: endpointslice-controller.k8s.io
      kubernetes.io/service-name: cube-router-leader
    name: cube-router-leader-h2qcx
    namespace: cube-operator-demo
    ownerReferences:
    - apiVersion: v1
      blockOwnerDeletion: true
      controller: true
      kind: Service
      name: cube-router-leader
      uid: 039eded6-5b5b-40ad-85b7-77924388428a
    resourceVersion: "1217609"
    uid: ce5d3a10-2034-475a-8020-c212d097ab0e
  ports: null
kind: List
metadata:
  resourceVersion: ""
2026-08-03T16:05:05Z	INFO	Warning: Reconciler returned both a non-zero result and a non-nil error. The result will always be ignored if the error is non-nil and the non-nil error causes reqeueuing with exponential backoff. For more details, see: https://pkg.go.dev/sigs.k8s.io/controller-runtime/pkg/reconcile#Reconciler	{"controller": "cubestorerouter", "controllerGroup": "cubestore.io", "controllerKind": "CubestoreRouter", "CubestoreRouter": {"name":"demo","namespace":"cube-operator-demo"}, "namespace": "cube-operator-demo", "name": "demo", "reconcileID": "a301f077-1d03-4edf-a26c-dde5049dcd6e"}
2026-08-03T16:05:05Z	ERROR	Reconciler error	{"controller": "cubestorerouter", "controllerGroup": "cubestore.io", "controllerKind": "CubestoreRouter", "CubestoreRouter": {"name":"demo","namespace":"cube-operator-demo"}, "namespace": "cube-operator-demo", "name": "demo", "reconcileID": "a301f077-1d03-4edf-a26c-dde5049dcd6e", "error": "router promotion is waiting for a readiness gate"}
sigs.k8s.io/controller-runtime/pkg/internal/controller.(*Controller).reconcileHandler
	/go/pkg/mod/sigs.k8s.io/controller-runtime@v0.18.4/pkg/internal/controller/controller.go:324
sigs.k8s.io/controller-runtime/pkg/internal/controller.(*Controller).processNextWorkItem
	/go/pkg/mod/sigs.k8s.io/controller-runtime@v0.18.4/pkg/internal/controller/controller.go:261
sigs.k8s.io/controller-runtime/pkg/internal/controller.(*Controller).Start.func2.2
	/go/pkg/mod/sigs.k8s.io/controller-runtime@v0.18.4/pkg/internal/controller/controller.go:222
2026-08-03T16:05:05Z	INFO	Warning: Reconciler returned both a non-zero result and a non-nil error. The result will always be ignored if the error is non-nil and the non-nil error causes reqeueuing with exponential backoff. For more details, see: https://pkg.go.dev/sigs.k8s.io/controller-runtime/pkg/reconcile#Reconciler	{"controller": "cubestorerouter", "controllerGroup": "cubestore.io", "controllerKind": "CubestoreRouter", "CubestoreRouter": {"name":"demo","namespace":"cube-operator-demo"}, "namespace": "cube-operator-demo", "name": "demo", "reconcileID": "97bedf79-df2f-4caa-84f6-44ae736c6fc4"}
2026-08-03T16:05:05Z	ERROR	Reconciler error	{"controller": "cubestorerouter", "controllerGroup": "cubestore.io", "controllerKind": "CubestoreRouter", "CubestoreRouter": {"name":"demo","namespace":"cube-operator-demo"}, "namespace": "cube-operator-demo", "name": "demo", "reconcileID": "97bedf79-df2f-4caa-84f6-44ae736c6fc4", "error": "router promotion is waiting for a readiness gate"}
sigs.k8s.io/controller-runtime/pkg/internal/controller.(*Controller).reconcileHandler
	/go/pkg/mod/sigs.k8s.io/controller-runtime@v0.18.4/pkg/internal/controller/controller.go:324
sigs.k8s.io/controller-runtime/pkg/internal/controller.(*Controller).processNextWorkItem
	/go/pkg/mod/sigs.k8s.io/controller-runtime@v0.18.4/pkg/internal/controller/controller.go:261
sigs.k8s.io/controller-runtime/pkg/internal/controller.(*Controller).Start.func2.2
	/go/pkg/mod/sigs.k8s.io/controller-runtime@v0.18.4/pkg/internal/controller/controller.go:222
2026-08-03T16:05:05Z	INFO	Warning: Reconciler returned both a non-zero result and a non-nil error. The result will always be ignored if the error is non-nil and the non-nil error causes reqeueuing with exponential backoff. For more details, see: https://pkg.go.dev/sigs.k8s.io/controller-runtime/pkg/reconcile#Reconciler	{"controller": "cubestorerouter", "controllerGroup": "cubestore.io", "controllerKind": "CubestoreRouter", "CubestoreRouter": {"name":"demo","namespace":"cube-operator-demo"}, "namespace": "cube-operator-demo", "name": "demo", "reconcileID": "f791f9f5-dbbf-4ad1-b9bd-1c178c798935"}
2026-08-03T16:05:05Z	ERROR	Reconciler error	{"controller": "cubestorerouter", "controllerGroup": "cubestore.io", "controllerKind": "CubestoreRouter", "CubestoreRouter": {"name":"demo","namespace":"cube-operator-demo"}, "namespace": "cube-operator-demo", "name": "demo", "reconcileID": "f791f9f5-dbbf-4ad1-b9bd-1c178c798935", "error": "router promotion is waiting for a readiness gate"}
sigs.k8s.io/controller-runtime/pkg/internal/controller.(*Controller).reconcileHandler
	/go/pkg/mod/sigs.k8s.io/controller-runtime@v0.18.4/pkg/internal/controller/controller.go:324
sigs.k8s.io/controller-runtime/pkg/internal/controller.(*Controller).processNextWorkItem
	/go/pkg/mod/sigs.k8s.io/controller-runtime@v0.18.4/pkg/internal/controller/controller.go:261
sigs.k8s.io/controller-runtime/pkg/internal/controller.(*Controller).Start.func2.2
	/go/pkg/mod/sigs.k8s.io/controller-runtime@v0.18.4/pkg/internal/controller/controller.go:222
2026-08-03T16:05:05Z	INFO	Warning: Reconciler returned both a non-zero result and a non-nil error. The result will always be ignored if the error is non-nil and the non-nil error causes reqeueuing with exponential backoff. For more details, see: https://pkg.go.dev/sigs.k8s.io/controller-runtime/pkg/reconcile#Reconciler	{"controller": "cubestorerouter", "controllerGroup": "cubestore.io", "controllerKind": "CubestoreRouter", "CubestoreRouter": {"name":"demo","namespace":"cube-operator-demo"}, "namespace": "cube-operator-demo", "name": "demo", "reconcileID": "34e88adc-7cc7-4c8c-84da-960a7bb14953"}
2026-08-03T16:05:05Z	ERROR	Reconciler error	{"controller": "cubestorerouter", "controllerGroup": "cubestore.io", "controllerKind": "CubestoreRouter", "CubestoreRouter": {"name":"demo","namespace":"cube-operator-demo"}, "namespace": "cube-operator-demo", "name": "demo", "reconcileID": "34e88adc-7cc7-4c8c-84da-960a7bb14953", "error": "router promotion is waiting for a readiness gate"}
sigs.k8s.io/controller-runtime/pkg/internal/controller.(*Controller).reconcileHandler
	/go/pkg/mod/sigs.k8s.io/controller-runtime@v0.18.4/pkg/internal/controller/controller.go:324
sigs.k8s.io/controller-runtime/pkg/internal/controller.(*Controller).processNextWorkItem
	/go/pkg/mod/sigs.k8s.io/controller-runtime@v0.18.4/pkg/internal/controller/controller.go:261
sigs.k8s.io/controller-runtime/pkg/internal/controller.(*Controller).Start.func2.2
	/go/pkg/mod/sigs.k8s.io/controller-runtime@v0.18.4/pkg/internal/controller/controller.go:222
2026-08-03T16:05:05Z	INFO	Warning: Reconciler returned both a non-zero result and a non-nil error. The result will always be ignored if the error is non-nil and the non-nil error causes reqeueuing with exponential backoff. For more details, see: https://pkg.go.dev/sigs.k8s.io/controller-runtime/pkg/reconcile#Reconciler	{"controller": "cubestorerouter", "controllerGroup": "cubestore.io", "controllerKind": "CubestoreRouter", "CubestoreRouter": {"name":"demo","namespace":"cube-operator-demo"}, "namespace": "cube-operator-demo", "name": "demo", "reconcileID": "25383acc-68ad-4a92-b82a-b19fbcaa3eb7"}
2026-08-03T16:05:05Z	ERROR	Reconciler error	{"controller": "cubestorerouter", "controllerGroup": "cubestore.io", "controllerKind": "CubestoreRouter", "CubestoreRouter": {"name":"demo","namespace":"cube-operator-demo"}, "namespace": "cube-operator-demo", "name": "demo", "reconcileID": "25383acc-68ad-4a92-b82a-b19fbcaa3eb7", "error": "router promotion is waiting for a readiness gate"}
sigs.k8s.io/controller-runtime/pkg/internal/controller.(*Controller).reconcileHandler
	/go/pkg/mod/sigs.k8s.io/controller-runtime@v0.18.4/pkg/internal/controller/controller.go:324
sigs.k8s.io/controller-runtime/pkg/internal/controller.(*Controller).processNextWorkItem
	/go/pkg/mod/sigs.k8s.io/controller-runtime@v0.18.4/pkg/internal/controller/controller.go:261
sigs.k8s.io/controller-runtime/pkg/internal/controller.(*Controller).Start.func2.2
	/go/pkg/mod/sigs.k8s.io/controller-runtime@v0.18.4/pkg/internal/controller/controller.go:222
2026-08-03T16:05:06Z	INFO	Warning: Reconciler returned both a non-zero result and a non-nil error. The result will always be ignored if the error is non-nil and the non-nil error causes reqeueuing with exponential backoff. For more details, see: https://pkg.go.dev/sigs.k8s.io/controller-runtime/pkg/reconcile#Reconciler	{"controller": "cubestorerouter", "controllerGroup": "cubestore.io", "controllerKind": "CubestoreRouter", "CubestoreRouter": {"name":"demo","namespace":"cube-operator-demo"}, "namespace": "cube-operator-demo", "name": "demo", "reconcileID": "6a09d5a9-a38e-470a-b1d3-d6628ed92a0d"}
2026-08-03T16:05:06Z	ERROR	Reconciler error	{"controller": "cubestorerouter", "controllerGroup": "cubestore.io", "controllerKind": "CubestoreRouter", "CubestoreRouter": {"name":"demo","namespace":"cube-operator-demo"}, "namespace": "cube-operator-demo", "name": "demo", "reconcileID": "6a09d5a9-a38e-470a-b1d3-d6628ed92a0d", "error": "router promotion is waiting for a readiness gate"}
sigs.k8s.io/controller-runtime/pkg/internal/controller.(*Controller).reconcileHandler
	/go/pkg/mod/sigs.k8s.io/controller-runtime@v0.18.4/pkg/internal/controller/controller.go:324
sigs.k8s.io/controller-runtime/pkg/internal/controller.(*Controller).processNextWorkItem
	/go/pkg/mod/sigs.k8s.io/controller-runtime@v0.18.4/pkg/internal/controller/controller.go:261
sigs.k8s.io/controller-runtime/pkg/internal/controller.(*Controller).Start.func2.2
	/go/pkg/mod/sigs.k8s.io/controller-runtime@v0.18.4/pkg/internal/controller/controller.go:222
2026-08-03T16:05:07Z	INFO	Warning: Reconciler returned both a non-zero result and a non-nil error. The result will always be ignored if the error is non-nil and the non-nil error causes reqeueuing with exponential backoff. For more details, see: https://pkg.go.dev/sigs.k8s.io/controller-runtime/pkg/reconcile#Reconciler	{"controller": "cubestorerouter", "controllerGroup": "cubestore.io", "controllerKind": "CubestoreRouter", "CubestoreRouter": {"name":"demo","namespace":"cube-operator-demo"}, "namespace": "cube-operator-demo", "name": "demo", "reconcileID": "36bae84a-494f-4b06-9a33-63e0b3f392f5"}
2026-08-03T16:05:07Z	ERROR	Reconciler error	{"controller": "cubestorerouter", "controllerGroup": "cubestore.io", "controllerKind": "CubestoreRouter", "CubestoreRouter": {"name":"demo","namespace":"cube-operator-demo"}, "namespace": "cube-operator-demo", "name": "demo", "reconcileID": "36bae84a-494f-4b06-9a33-63e0b3f392f5", "error": "router promotion is waiting for a readiness gate"}
sigs.k8s.io/controller-runtime/pkg/internal/controller.(*Controller).reconcileHandler
	/go/pkg/mod/sigs.k8s.io/controller-runtime@v0.18.4/pkg/internal/controller/controller.go:324
sigs.k8s.io/controller-runtime/pkg/internal/controller.(*Controller).processNextWorkItem
	/go/pkg/mod/sigs.k8s.io/controller-runtime@v0.18.4/pkg/internal/controller/controller.go:261
sigs.k8s.io/controller-runtime/pkg/internal/controller.(*Controller).Start.func2.2
	/go/pkg/mod/sigs.k8s.io/controller-runtime@v0.18.4/pkg/internal/controller/controller.go:222
2026-08-03T16:05:10Z	INFO	Warning: Reconciler returned both a non-zero result and a non-nil error. The result will always be ignored if the error is non-nil and the non-nil error causes reqeueuing with exponential backoff. For more details, see: https://pkg.go.dev/sigs.k8s.io/controller-runtime/pkg/reconcile#Reconciler	{"controller": "cubestorerouter", "controllerGroup": "cubestore.io", "controllerKind": "CubestoreRouter", "CubestoreRouter": {"name":"demo","namespace":"cube-operator-demo"}, "namespace": "cube-operator-demo", "name": "demo", "reconcileID": "4705eaee-6c51-4387-bf20-c680e0d53e5c"}
2026-08-03T16:05:10Z	ERROR	Reconciler error	{"controller": "cubestorerouter", "controllerGroup": "cubestore.io", "controllerKind": "CubestoreRouter", "CubestoreRouter": {"name":"demo","namespace":"cube-operator-demo"}, "namespace": "cube-operator-demo", "name": "demo", "reconcileID": "4705eaee-6c51-4387-bf20-c680e0d53e5c", "error": "router promotion is waiting for a readiness gate"}
sigs.k8s.io/controller-runtime/pkg/internal/controller.(*Controller).reconcileHandler
	/go/pkg/mod/sigs.k8s.io/controller-runtime@v0.18.4/pkg/internal/controller/controller.go:324
sigs.k8s.io/controller-runtime/pkg/internal/controller.(*Controller).processNextWorkItem
	/go/pkg/mod/sigs.k8s.io/controller-runtime@v0.18.4/pkg/internal/controller/controller.go:261
sigs.k8s.io/controller-runtime/pkg/internal/controller.(*Controller).Start.func2.2
	/go/pkg/mod/sigs.k8s.io/controller-runtime@v0.18.4/pkg/internal/controller/controller.go:222
2026-08-03T16:05:15Z	INFO	Warning: Reconciler returned both a non-zero result and a non-nil error. The result will always be ignored if the error is non-nil and the non-nil error causes reqeueuing with exponential backoff. For more details, see: https://pkg.go.dev/sigs.k8s.io/controller-runtime/pkg/reconcile#Reconciler	{"controller": "cubestorerouter", "controllerGroup": "cubestore.io", "controllerKind": "CubestoreRouter", "CubestoreRouter": {"name":"demo","namespace":"cube-operator-demo"}, "namespace": "cube-operator-demo", "name": "demo", "reconcileID": "7cd580c7-617e-4503-a2e6-836fc04d7a8a"}
2026-08-03T16:05:15Z	ERROR	Reconciler error	{"controller": "cubestorerouter", "controllerGroup": "cubestore.io", "controllerKind": "CubestoreRouter", "CubestoreRouter": {"name":"demo","namespace":"cube-operator-demo"}, "namespace": "cube-operator-demo", "name": "demo", "reconcileID": "7cd580c7-617e-4503-a2e6-836fc04d7a8a", "error": "router promotion is waiting for a readiness gate"}
sigs.k8s.io/controller-runtime/pkg/internal/controller.(*Controller).reconcileHandler
	/go/pkg/mod/sigs.k8s.io/controller-runtime@v0.18.4/pkg/internal/controller/controller.go:324
sigs.k8s.io/controller-runtime/pkg/internal/controller.(*Controller).processNextWorkItem
	/go/pkg/mod/sigs.k8s.io/controller-runtime@v0.18.4/pkg/internal/controller/controller.go:261
sigs.k8s.io/controller-runtime/pkg/internal/controller.(*Controller).Start.func2.2
	/go/pkg/mod/sigs.k8s.io/controller-runtime@v0.18.4/pkg/internal/controller/controller.go:222
2026-08-03T16:05:25Z	INFO	Warning: Reconciler returned both a non-zero result and a non-nil error. The result will always be ignored if the error is non-nil and the non-nil error causes reqeueuing with exponential backoff. For more details, see: https://pkg.go.dev/sigs.k8s.io/controller-runtime/pkg/reconcile#Reconciler	{"controller": "cubestorerouter", "controllerGroup": "cubestore.io", "controllerKind": "CubestoreRouter", "CubestoreRouter": {"name":"demo","namespace":"cube-operator-demo"}, "namespace": "cube-operator-demo", "name": "demo", "reconcileID": "9a411ed8-5af6-438c-a8a9-497353a8c095"}
2026-08-03T16:05:25Z	ERROR	Reconciler error	{"controller": "cubestorerouter", "controllerGroup": "cubestore.io", "controllerKind": "CubestoreRouter", "CubestoreRouter": {"name":"demo","namespace":"cube-operator-demo"}, "namespace": "cube-operator-demo", "name": "demo", "reconcileID": "9a411ed8-5af6-438c-a8a9-497353a8c095", "error": "router promotion is waiting for a readiness gate"}
sigs.k8s.io/controller-runtime/pkg/internal/controller.(*Controller).reconcileHandler
	/go/pkg/mod/sigs.k8s.io/controller-runtime@v0.18.4/pkg/internal/controller/controller.go:324
sigs.k8s.io/controller-runtime/pkg/internal/controller.(*Controller).processNextWorkItem
	/go/pkg/mod/sigs.k8s.io/controller-runtime@v0.18.4/pkg/internal/controller/controller.go:261
sigs.k8s.io/controller-runtime/pkg/internal/controller.(*Controller).Start.func2.2
	/go/pkg/mod/sigs.k8s.io/controller-runtime@v0.18.4/pkg/internal/controller/controller.go:222
- Finished: 2026-08-03T16:05:43Z

## Result
FAIL: no safe leader target; failover was not attempted.
