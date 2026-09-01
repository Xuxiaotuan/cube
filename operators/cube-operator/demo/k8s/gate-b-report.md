# Gate B failover report

- Started: 2026-09-01T15:32:36Z
- Namespace: `cube-operator-demo`

## Preflight
- PASS: Using Kubernetes Lease backend; skipping Redis checks
- PASS: MetaStore ready
- PASS: Workers ready: 2
- PASS: Using Kubernetes Lease backend; Redis secret is not required
- Before leader: cube-router-demo-8c6ccc8dd-sjblw (epoch=8, ip=192.168.194.90)
- PASS: CR leader and epoch present
- PASS: promotion ConfigMap matches CR leader/epoch
## Router contracts before deletion
- FAIL: cube-router-demo-8c6ccc8dd-mg7q8 promotion.json contract invalid
- FAIL: cube-router-demo-8c6ccc8dd-sjblw promotion.json contract invalid
- PASS: leader Service EndpointSlice matches old leader
NAME                 TYPE        CLUSTER-IP        EXTERNAL-IP   PORT(S)             AGE   SELECTOR
cube-router-leader   ClusterIP   192.168.194.158   <none>        3030/TCP,3306/TCP   32d   app=cube-router,cubestore.io/router-role=leader
apiVersion: v1
items:
- addressType: IPv4
  apiVersion: discovery.k8s.io/v1
  endpoints:
  - addresses:
    - 192.168.194.90
    conditions:
      ready: true
      serving: true
      terminating: false
    nodeName: orbstack
    targetRef:
      kind: Pod
      name: cube-router-demo-8c6ccc8dd-sjblw
      namespace: cube-operator-demo
      uid: c9eae115-4044-4427-a522-ea2baf617fc6
  kind: EndpointSlice
  metadata:
    annotations:
      endpoints.kubernetes.io/last-change-trigger-time: "2026-09-01T15:31:21Z"
    creationTimestamp: "2026-07-30T15:35:57Z"
    generateName: cube-router-leader-
    generation: 2563
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
    resourceVersion: "2822032"
    uid: ce5d3a10-2034-475a-8020-c212d097ab0e
  ports:
  - name: http
    port: 3030
    protocol: TCP
  - name: mysql
    port: 3306
    protocol: TCP
kind: List
metadata:
  resourceVersion: ""
## Destructive failover
- PASS: MySQL probe pod ready
- Deleting old leader Pod: cube-router-demo-8c6ccc8dd-sjblw
- PASS: delete accepted at 2026-09-01T15:34:17.3NZ
- FAIL: did not observe old primary write rejection
- PASS: new leader: cube-router-demo-8c6ccc8dd-mg7q8
- PASS: leaderEpoch increased: 8 -> 9
- FAIL: cube-router-demo-8c6ccc8dd-mg7q8 promotion.json contract invalid
- FAIL: no transient API HTTP 503 observed
- FAIL: API did not show a successful post-failover response
## Final resources
NAME                               READY   STATUS    RESTARTS   AGE     IP               NODE       NOMINATED NODE   READINESS GATES   LABELS
cube-router-demo-8c6ccc8dd-mg7q8   2/2     Running   0          3m30s   192.168.194.91   orbstack   <none>           <none>            app.kubernetes.io/name=cube-router-demo,app=cube-router,cubestore.io/router-role=leader,pod-template-hash=8c6ccc8dd
cube-router-demo-8c6ccc8dd-zj8ct   2/2     Running   0          35s     192.168.194.93   orbstack   <none>           <none>            app.kubernetes.io/name=cube-router-demo,app=cube-router,cubestore.io/router-role=follower,pod-template-hash=8c6ccc8dd
apiVersion: cubestore.io/v1alpha1
kind: CubestoreRouter
metadata:
  annotations:
    cubestore.io/lease-cluster: cube-operator-demo/demo
    cubestore.io/lease-epoch: "9"
    cubestore.io/lease-token: cdd6776272551d832bcc9a41df1ed8b8775cd437fb87f3e2bbf36d41c97bda21
    cubestore.io/promotion-candidate: cube-router-demo-8c6ccc8dd-mg7q8
    cubestore.io/promotion-epoch: "9"
    cubestore.io/promotion-phase: serving
    kubectl.kubernetes.io/last-applied-configuration: |
      {"apiVersion":"cubestore.io/v1alpha1","kind":"CubestoreRouter","metadata":{"annotations":{},"name":"demo","namespace":"cube-operator-demo"},"spec":{"electionStrategy":"lease","healthPath":"/router/status","metaStore":{"address":"cubestore-metastore.cube-operator-demo.svc:9999"},"namespace":"cube-operator-demo","roleConfigMapName":"cube-router-demo-router-role-state","routerPort":3030,"selector":{"app":"cube-router"},"stateStore":{"type":"kubernetes"},"storage":{"dataPVC":"worker-data"}}}
    test.t: tmp-1788273916
  creationTimestamp: "2026-07-30T15:35:57Z"
  generation: 4
  name: demo
  namespace: cube-operator-demo
  resourceVersion: "2822268"
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
    type: kubernetes
  storage:
    dataPVC: worker-data
status:
  candidates:
  - ip: 192.168.194.91
    lastProbeTime: "2026-09-01T15:34:50Z"
    name: cube-router-demo-8c6ccc8dd-mg7q8
    namespace: cube-operator-demo
    ready: true
    role: leader
  - ip: 192.168.194.93
    lastProbeTime: "2026-09-01T15:34:50Z"
    name: cube-router-demo-8c6ccc8dd-zj8ct
    namespace: cube-operator-demo
    ready: true
    role: follower
  conditions:
  - lastTransitionTime: "2026-09-01T15:34:50Z"
    message: 'Leader elected: cube-router-demo-8c6ccc8dd-mg7q8 (candidates: 2)'
    observedGeneration: 4
    reason: LeaderSelected
    status: "True"
    type: LeaderElection
  - lastTransitionTime: "2026-09-01T15:34:50Z"
    message: Role state and labels synchronized.
    observedGeneration: 4
    reason: StateSyncSucceeded
    status: "True"
    type: RoleStateSync
  - lastTransitionTime: "2026-09-01T15:34:50Z"
    message: No Rust Router leadership guard is wired into Job assignment, heartbeat,
      or completion commits.
    observedGeneration: 4
    reason: Blocked
    status: "False"
    type: JobRecovery
  - lastTransitionTime: "2026-09-01T15:34:50Z"
    message: No authoritative Job/upload/pre-aggregation mutation reconciliation endpoint
      is available for UNKNOWN outcomes.
    observedGeneration: 4
    reason: NeedsContext
    status: "False"
    type: MutationReconcile
  - lastTransitionTime: "2026-09-01T15:34:50Z"
    message: Router acknowledged the exact lease epoch, token hash, and MetaStore
      readiness.
    observedGeneration: 4
    reason: PromotionAcknowledged
    status: "True"
    type: PromotionReady
  - lastTransitionTime: "2026-09-01T15:34:50Z"
    message: No Refresher CRD/controller or durable active/standby refresh ownership
      entry point exists.
    observedGeneration: 4
    reason: NeedsContext
    status: "False"
    type: RefresherReady
  lastSwitchedAt: "2026-09-01T15:34:50Z"
  leader: cube-router-demo-8c6ccc8dd-mg7q8
  leaderEpoch: 9
  leaderIP: 192.168.194.91
  leaderRole: leader
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
      message: Router acknowledged the exact lease epoch, token hash, and MetaStore
        readiness.
      reason: PromotionAcknowledged
      state: Ready
    refresher:
      message: No Refresher CRD/controller or durable active/standby refresh ownership
        entry point exists.
      reason: NeedsContext
      state: NeedsContext
NAME                 TYPE        CLUSTER-IP        EXTERNAL-IP   PORT(S)             AGE   SELECTOR
cube-router-leader   ClusterIP   192.168.194.158   <none>        3030/TCP,3306/TCP   32d   app=cube-router,cubestore.io/router-role=leader
apiVersion: v1
items:
- addressType: IPv4
  apiVersion: discovery.k8s.io/v1
  endpoints:
  - addresses:
    - 192.168.194.91
    conditions:
      ready: true
      serving: true
      terminating: false
    nodeName: orbstack
    targetRef:
      kind: Pod
      name: cube-router-demo-8c6ccc8dd-mg7q8
      namespace: cube-operator-demo
      uid: 4eb50c34-3a64-4a9e-a898-6c2d093014f2
  kind: EndpointSlice
  metadata:
    annotations:
      endpoints.kubernetes.io/last-change-trigger-time: "2026-09-01T15:31:23Z"
    creationTimestamp: "2026-07-30T15:35:57Z"
    generateName: cube-router-leader-
    generation: 2566
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
    resourceVersion: "2822261"
    uid: ce5d3a10-2034-475a-8020-c212d097ab0e
  ports:
  - name: http
    port: 3030
    protocol: TCP
  - name: mysql
    port: 3306
    protocol: TCP
kind: List
metadata:
  resourceVersion: ""
- API trace: /tmp/gate-b-api-53247.trace
- Finished: 2026-09-01T15:34:52Z

## Result
FAIL: 6 check(s) failed.
