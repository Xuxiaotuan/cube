package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/cube-js/cube-operator/api/v1alpha1"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	labelNamespace                    = "cubestore.io/router-role"
	labelLeader                       = "leader"
	labelFollower                     = "follower"
	defaultPort                 int32 = 3030
	defaultPath                       = "/router/status"
	defaultRoleDataKey                = "route-role.json"
	defaultRoleConfigMap              = "router-role-state"
	defaultPgTable                    = "cubestore_router_leader_state"
	defaultRedisKey                   = "cube-router/leader-state"
	leaderStateBackendConfigMap       = "configmap"
	leaderStateBackendPostgres        = "postgres"
	leaderStateBackendRedis           = "redis"
	leaderConditionType               = "LeaderElection"
	syncConditionType                 = "RoleStateSync"
	leaderStateRecordID               = "leader-state"
	leaderStateRecordIDPrefix         = "route-state"
)

const leaderStateOpTimeout = 4 * time.Second

type CubestoreRouterReconciler struct {
	client.Client
	*runtime.Scheme
}

func (r *CubestoreRouterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.CubestoreRouter{}).
		Owns(&corev1.Pod{}).
		Complete(r)
}

func (r *CubestoreRouterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := ctrl.Log.WithValues("cubestore-router", req.NamespacedName)

	var cr v1alpha1.CubestoreRouter
	if err := r.Get(ctx, req.NamespacedName, &cr); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	targetNS := cr.Spec.Namespace
	if targetNS == "" {
		targetNS = req.Namespace
	}
	roleStateConfigMap := resolveRoleConfigMapName(&cr)

	podList, err := r.listRouters(ctx, targetNS, cr.Spec.Selector)
	if err != nil {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, err
	}

	candidates := r.probeCandidates(ctx, podList, cr)
	leader, leaderCandidates := r.chooseLeader(candidates, cr.Spec.ElectionStrategy, cr.Status.Leader)

	observedStateEpoch, observedErr := r.readLeaderEpochFromState(ctx, targetNS, &cr, roleStateConfigMap)
	if observedErr != nil {
		log.Info("read role state config map failed, using in-memory status epoch as fallback", "error", observedErr.Error())
	}

	nextLeaderEpoch := resolveLeaderEpoch(cr.Status.LeaderEpoch, observedStateEpoch, cr.Status.Leader)
	if leader != nil && cr.Status.Leader != leader.Name {
		if cr.Status.Leader == "" && cr.Status.LeaderEpoch == 0 && observedStateEpoch == 0 {
			nextLeaderEpoch = 1
		} else {
			nextLeaderEpoch += 1
		}
	}
	if leader != nil && nextLeaderEpoch == 0 {
		nextLeaderEpoch = 1
	}

	nextStatus := cr.Status
	nextStatus.Candidates = r.candidatesToStatus(candidates)
	nextStatus.Conditions = nil
	nextStatus.Conditions = r.withLeaderCondition(candidates, leader, leaderCandidates, nextStatus.Conditions, int64(cr.Generation))

	syncStateErr := r.syncRoles(ctx, targetNS, candidates, leader)
	if syncStateErr == nil {
		syncStateErr = r.syncRoleState(ctx, targetNS, &cr, candidates, leader, nextLeaderEpoch, roleStateConfigMap)
	}
	nextStatus.Conditions = r.withSyncCondition(nextStatus.Conditions, syncStateErr, int64(cr.Generation))
	nextStatus.Conditions = r.normalizeConditions(nextStatus.Conditions)

	if leader != nil {
		nextStatus.Leader = leader.Name
		nextStatus.LeaderIP = leader.PodIP
		nextStatus.LeaderRole = labelLeader
		nextStatus.LeaderEpoch = nextLeaderEpoch
		if cr.Status.Leader != leader.Name {
			now := metav1.Now()
			nextStatus.LastSwitchedAt = &now
		}
	} else {
		nextStatus.Leader = ""
		nextStatus.LeaderIP = ""
		nextStatus.LeaderRole = ""
		nextStatus.LeaderEpoch = nextLeaderEpoch
		if nextStatus.LeaderEpoch == 0 {
			nextStatus.LeaderEpoch = 1
		}
		nextStatus.LastSwitchedAt = cr.Status.LastSwitchedAt
	}

	if !statusEqual(cr.Status, nextStatus) {
		cr.Status = nextStatus
		if err := r.Status().Update(ctx, &cr); err != nil {
			log.Error(err, "update status failed")
			return ctrl.Result{RequeueAfter: 5 * time.Second}, err
		}
	}

	if syncStateErr != nil {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, syncStateErr
	}

	return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
}

func resolveRoleConfigMapName(cr *v1alpha1.CubestoreRouter) string {
	if strings.TrimSpace(cr.Spec.RoleConfigMap) != "" {
		return strings.TrimSpace(cr.Spec.RoleConfigMap)
	}
	return fmt.Sprintf("%s-%s", cr.Name, defaultRoleConfigMap)
}

func resolveLeaderEpoch(statusEpoch int64, observedStateEpoch int64, previousLeader string) int64 {
	if statusEpoch > 0 {
		return maxInt64(statusEpoch, observedStateEpoch)
	}
	if observedStateEpoch > 0 {
		return observedStateEpoch
	}
	if strings.TrimSpace(previousLeader) == "" {
		return 1
	}
	return 1
}

func maxInt64(a, b int64) int64 {
	if a >= b {
		return a
	}
	return b
}

type candidate struct {
	Name      string
	Namespace string
	PodIP     string
	Role      string
	Ready     bool
	LastProbe metav1.Time
	CreatedAt time.Time
}

type routerLeaderState struct {
	LeaderEpoch  int64                     `json:"leaderEpoch"`
	ActiveLeader string                    `json:"activeLeader"`
	UpdatedAt    string                    `json:"updatedAt"`
	Candidates   map[string]map[string]any `json:"candidates"`
}

const (
	leaderStateStoreTypeConfigMap leaderStateBackendType = "configmap"
	leaderStateStoreTypePostgres  leaderStateBackendType = "postgres"
	leaderStateStoreTypeRedis     leaderStateBackendType = "redis"
)

type leaderStateBackendType string

var leaderStateStoreTypeRegex = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?$`)

func (r *CubestoreRouterReconciler) listRouters(ctx context.Context, namespace string, selector map[string]string) ([]corev1.Pod, error) {
	var pods corev1.PodList
	opts := []client.ListOption{client.InNamespace(namespace)}
	if len(selector) > 0 {
		opts = append(opts, client.MatchingLabels(selector))
	}
	if err := r.List(ctx, &pods, opts...); err != nil {
		return nil, err
	}
	out := make([]corev1.Pod, 0, len(pods.Items))
	for _, pod := range pods.Items {
		if pod.DeletionTimestamp != nil {
			continue
		}
		out = append(out, pod)
	}
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})
	return out, nil
}

func (r *CubestoreRouterReconciler) probeCandidates(ctx context.Context, pods []corev1.Pod, cr v1alpha1.CubestoreRouter) []candidate {
	port := cr.Spec.RouterPort
	if port == 0 {
		port = defaultPort
	}
	path := strings.TrimSpace(cr.Spec.HealthPath)
	if path == "" {
		path = defaultPath
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	httpClient := &http.Client{Timeout: 2 * time.Second}
	out := make([]candidate, 0, len(pods))

	for _, p := range pods {
		cand := candidate{
			Name:      p.Name,
			Namespace: p.Namespace,
			PodIP:     p.Status.PodIP,
			Ready:     isPodReady(&p),
			LastProbe: metav1.Now(),
			Role:      labelFollower,
			CreatedAt: p.CreationTimestamp.Time,
		}

		if cand.PodIP == "" || !cand.Ready {
			out = append(out, cand)
			continue
		}

		endpoint := fmt.Sprintf("http://%s%s?detail=1", net.JoinHostPort(cand.PodIP, fmt.Sprintf("%d", port)), path)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			out = append(out, cand)
			continue
		}

		resp, err := httpClient.Do(req)
		if err == nil {
		if resp.StatusCode == http.StatusOK {
				var body struct {
					NodeName    string `json:"node_name"`
					Role        string `json:"role"`
					Mode        string `json:"mode"`
					ActiveLeader string `json:"activeLeader"`
					LeaderState struct {
						ActiveLeader string `json:"activeLeader"`
					} `json:"leaderState"`
				}
				if err := json.NewDecoder(resp.Body).Decode(&body); err == nil {
					nodeName := strings.ToLower(strings.TrimSpace(body.NodeName))
					activeLeader := strings.ToLower(strings.TrimSpace(body.ActiveLeader))
					if activeLeader == "" {
						activeLeader = strings.ToLower(strings.TrimSpace(body.LeaderState.ActiveLeader))
					}
					r := strings.ToLower(strings.TrimSpace(body.Role))
					m := strings.ToLower(strings.TrimSpace(body.Mode))
					if activeLeader != "" && nodeName != "" {
						if activeLeader == nodeName {
							cand.Role = labelLeader
						}
					} else if r == "leader" || r == "primary" || m == "primary" {
						cand.Role = labelLeader
					}
				}
			}
			_ = resp.Body.Close()
		}

		out = append(out, cand)
	}

	return out
}

func (r *CubestoreRouterReconciler) chooseLeader(candidates []candidate, electionStrategy string, previousLeader string) (*candidate, int) {
	sortedCandidates := make([]candidate, len(candidates))
	copy(sortedCandidates, candidates)
	sort.SliceStable(sortedCandidates, func(i, j int) bool {
		return sortedCandidates[i].CreatedAt.Before(sortedCandidates[j].CreatedAt)
	})

	var leaders []candidate
	for _, c := range sortedCandidates {
		if c.PodIP != "" && c.Ready && c.Role == labelLeader {
			leaders = append(leaders, c)
		}
	}

	if len(leaders) > 1 {
		return nil, len(leaders)
	}

	if len(leaders) == 1 {
		return &leaders[0], len(leaders)
	}

	if strings.EqualFold(strings.TrimSpace(electionStrategy), "strict") || strings.EqualFold(strings.TrimSpace(electionStrategy), "require-leader") {
		for _, c := range sortedCandidates {
			if c.Name == previousLeader && c.PodIP != "" && c.Ready {
				return &c, len(leaders)
			}
		}
		for _, c := range sortedCandidates {
			if c.PodIP != "" && c.Ready {
				return &c, len(leaders)
			}
		}
		return nil, len(leaders)
	}

	if strings.EqualFold(strings.TrimSpace(electionStrategy), "ready-first") || strings.EqualFold(strings.TrimSpace(electionStrategy), "fallback-ready") {
		for _, c := range sortedCandidates {
			if c.PodIP != "" && c.Ready {
				return &c, len(leaders)
			}
		}
	}

	for _, c := range sortedCandidates {
		if c.PodIP != "" && c.Ready {
			return &c, len(leaders)
		}
	}

	return nil, len(leaders)
}

func (r *CubestoreRouterReconciler) withLeaderCondition(
	candidates []candidate,
	leader *candidate,
	leaderCandidateCount int,
	conditions []metav1.Condition,
	observedGeneration int64,
) []metav1.Condition {
	cond := metav1.Condition{
		Type:               leaderConditionType,
		ObservedGeneration: observedGeneration,
		LastTransitionTime: metav1.Now(),
	}

	if len(candidates) == 0 {
		cond.Status = metav1.ConditionFalse
		cond.Reason = "NoCandidates"
		cond.Message = "No router pods are available for leader election."
		return r.upsertCondition(conditions, cond)
	}

	if leader == nil {
		if leaderCandidateCount > 1 {
			cond.Status = metav1.ConditionFalse
			cond.Reason = "MultipleLeaders"
			cond.Message = "Role file/status indicates multiple leader candidates; controller is in no-leader state."
			return r.upsertCondition(conditions, cond)
		}

		readyCount := 0
		for _, cand := range candidates {
			if cand.Ready && cand.PodIP != "" {
				readyCount++
			}
		}

		cond.Status = metav1.ConditionFalse
		cond.Reason = "NoLeader"
		if readyCount == 0 {
			cond.Message = "No ready candidates found for leader election."
		} else {
			cond.Message = "No leader candidate detected from role state; waiting for reconciliation."
		}
		return r.upsertCondition(conditions, cond)
	}

	cond.Status = metav1.ConditionTrue
	cond.Reason = "LeaderSelected"
	cond.Message = fmt.Sprintf("Leader elected: %s (candidates: %d)", leader.Name, len(candidates))
	return r.upsertCondition(conditions, cond)
}

func (r *CubestoreRouterReconciler) upsertCondition(conditions []metav1.Condition, condition metav1.Condition) []metav1.Condition {
	out := make([]metav1.Condition, len(conditions))
	copy(out, conditions)

	for i := range out {
		if out[i].Type != condition.Type {
			continue
		}

		o := out[i]
		if o.Status != condition.Status || o.Reason != condition.Reason || o.Message != condition.Message || o.ObservedGeneration != condition.ObservedGeneration {
			o.Status = condition.Status
			o.Reason = condition.Reason
			o.Message = condition.Message
			o.ObservedGeneration = condition.ObservedGeneration
			o.LastTransitionTime = condition.LastTransitionTime
			out[i] = o
		}
		return out
	}

	return append(out, condition)
}

func (r *CubestoreRouterReconciler) syncRoles(ctx context.Context, namespace string, candidates []candidate, leader *candidate) error {
	sortedCandidates := make([]candidate, len(candidates))
	copy(sortedCandidates, candidates)
	sort.SliceStable(sortedCandidates, func(i, j int) bool {
		return sortedCandidates[i].CreatedAt.Before(sortedCandidates[j].CreatedAt)
	})

	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].Name == candidates[j].Name {
			return candidates[i].CreatedAt.Before(candidates[j].CreatedAt)
		}
		return candidates[i].Name < candidates[j].Name
	})

	ordered := make([]candidate, 0, len(candidates))
	if leader != nil {
		for _, cand := range sortedCandidates {
			if cand.Name == leader.Name {
				ordered = append(ordered, cand)
				break
			}
		}
	}

	for _, cand := range sortedCandidates {
		if leader != nil && cand.Name == leader.Name {
			continue
		}
		ordered = append(ordered, cand)
	}

	if len(ordered) == 0 {
		ordered = candidates
	}

	for _, cand := range ordered {
		var pod corev1.Pod
		if err := r.Get(ctx, types.NamespacedName{Name: cand.Name, Namespace: namespace}, &pod); err != nil {
			continue
		}

		role := labelFollower
		if leader != nil && cand.Name == leader.Name {
			role = labelLeader
		}

		if pod.Labels == nil {
			pod.Labels = map[string]string{}
		}

		if pod.Labels[labelNamespace] != role {
			pod.Labels[labelNamespace] = role
			if err := r.Update(ctx, &pod); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *CubestoreRouterReconciler) syncRoleState(
	ctx context.Context,
	namespace string,
	cr *v1alpha1.CubestoreRouter,
	candidates []candidate,
	leader *candidate,
	leaderEpoch int64,
	roleStateConfigMap string,
) error {
	state := r.buildLeaderState(candidates, leader, leaderEpoch)
	roleData, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if err := r.syncRoleStateConfigMap(ctx, namespace, cr, roleStateConfigMap, roleData); err != nil {
		return err
	}

	return r.syncRoleStateRemote(ctx, namespace, cr, state)
}

func (r *CubestoreRouterReconciler) readLeaderEpochFromState(
	ctx context.Context,
	namespace string,
	cr *v1alpha1.CubestoreRouter,
	configMapName string,
) (int64, error) {
	state, err := r.readLeaderState(ctx, namespace, cr, configMapName)
	if err != nil {
		return 0, err
	}

	return state.LeaderEpoch, nil
}

func resolveLeaderStateBackend(store *v1alpha1.LeaderStateStore) (leaderStateBackendType, error) {
	if store == nil {
		return leaderStateStoreTypeConfigMap, nil
	}

	raw := strings.TrimSpace(strings.ToLower(store.Type))
	if raw == "" {
		return leaderStateStoreTypeConfigMap, nil
	}

	switch leaderStateBackendType(raw) {
	case leaderStateStoreTypeConfigMap, leaderStateStoreTypePostgres, leaderStateStoreTypeRedis:
		return leaderStateBackendType(raw), nil
	default:
		return "", fmt.Errorf("unsupported leaderStateStore.type: %s", store.Type)
	}
}

func resolvePostgresTable(raw string) (string, error) {
	table := strings.TrimSpace(raw)
	if table == "" {
		table = defaultPgTable
	}

	if !leaderStateStoreTypeRegex.MatchString(table) {
		return "", fmt.Errorf("invalid pgTable=%q; must be <ident> or <schema>.<table> format", table)
	}

	return table, nil
}

func resolveLeaderStateRecordID(namespace, name string) string {
	ns := strings.TrimSpace(namespace)
	if ns == "" {
		ns = "default"
	}
	n := strings.TrimSpace(name)
	if n == "" {
		n = "default"
	}
	return ns + "/" + n
}

func resolveRedisLeaderStateKey(raw, namespace, name string) string {
	key := strings.TrimSpace(raw)
	if key == "" {
		key = fmt.Sprintf("%s/%s", defaultRedisKey, resolveLeaderStateRecordID(namespace, name))
	}
	return key
}

func (r *CubestoreRouterReconciler) syncRoleStateConfigMap(
	ctx context.Context,
	namespace string,
	cr *v1alpha1.CubestoreRouter,
	roleStateConfigMap string,
	roleState []byte,
) error {
	cm := &corev1.ConfigMap{}
	if err := r.Get(ctx, types.NamespacedName{Name: roleStateConfigMap, Namespace: namespace}, cm); err != nil {
		if client.IgnoreNotFound(err) != nil {
			return err
		}

		cm = &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      roleStateConfigMap,
				Namespace: namespace,
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: "cubestore.io/v1alpha1",
					Kind:       "CubestoreRouter",
					Name:       cr.Name,
					UID:        cr.UID,
					Controller: func() *bool {
						b := true
						return &b
					}(),
				}},
			},
			Data: map[string]string{defaultRoleDataKey: string(roleState)},
		}
		return r.Create(ctx, cm)
	}

	if cm.Data == nil {
		cm.Data = map[string]string{}
	}

	if cm.Data[defaultRoleDataKey] == string(roleState) {
		return nil
	}

	cm.Data = map[string]string{
		defaultRoleDataKey: string(roleState),
	}
	return r.Update(ctx, cm)
}

func (r *CubestoreRouterReconciler) syncRoleStateRemote(
	ctx context.Context,
	namespace string,
	cr *v1alpha1.CubestoreRouter,
	state routerLeaderState,
) error {
	backend, err := resolveLeaderStateBackend(cr.Spec.LeaderStateStore)
	if err != nil {
		return err
	}

	if backend == leaderStateStoreTypeConfigMap {
		return nil
	}

	switch backend {
	case leaderStateStoreTypePostgres:
		return r.writeLeaderStateToPostgres(ctx, namespace, cr, state)
	case leaderStateStoreTypeRedis:
		return r.writeLeaderStateToRedis(ctx, cr, state)
	default:
		return nil
	}
}

func (r *CubestoreRouterReconciler) readLeaderState(
	ctx context.Context,
	namespace string,
	cr *v1alpha1.CubestoreRouter,
	configMapName string,
) (routerLeaderState, error) {
	if cr == nil {
		return routerLeaderState{}, nil
	}

	backend, err := resolveLeaderStateBackend(cr.Spec.LeaderStateStore)
	if err != nil {
		return routerLeaderState{}, err
	}

	switch backend {
	case leaderStateStoreTypePostgres:
		return r.readLeaderStateFromPostgres(ctx, cr)
	case leaderStateStoreTypeRedis:
		return r.readLeaderStateFromRedis(ctx, cr)
	case leaderStateStoreTypeConfigMap:
		return r.readLeaderStateFromConfigMap(ctx, namespace, configMapName)
	default:
		return r.readLeaderStateFromConfigMap(ctx, namespace, configMapName)
	}
}

func (r *CubestoreRouterReconciler) readLeaderStateFromConfigMap(
	ctx context.Context,
	namespace string,
	configMapName string,
) (routerLeaderState, error) {
	cm := &corev1.ConfigMap{}
	if err := r.Get(ctx, types.NamespacedName{Name: configMapName, Namespace: namespace}, cm); err != nil {
		if client.IgnoreNotFound(err) == nil {
			return routerLeaderState{}, nil
		}
		return routerLeaderState{}, err
	}

	raw, ok := cm.Data[defaultRoleDataKey]
	if !ok || strings.TrimSpace(raw) == "" {
		return routerLeaderState{}, nil
	}

	state := routerLeaderState{}
	if err := json.Unmarshal([]byte(raw), &state); err != nil {
		return routerLeaderState{}, err
	}

	if state.Candidates == nil {
		state.Candidates = map[string]map[string]any{}
	}

	return state, nil
}

func (r *CubestoreRouterReconciler) readLeaderStateFromPostgres(ctx context.Context, cr *v1alpha1.CubestoreRouter) (routerLeaderState, error) {
	if cr == nil || cr.Spec.LeaderStateStore == nil {
		return routerLeaderState{}, nil
	}

	dsn := strings.TrimSpace(cr.Spec.LeaderStateStore.DSN)
	if dsn == "" {
		return routerLeaderState{}, fmt.Errorf("leaderStateStore.dsn is required for postgres backend")
	}

	backendCtx, cancel := context.WithTimeout(ctx, leaderStateOpTimeout)
	defer cancel()

	pool, err := pgxpool.New(backendCtx, dsn)
	if err != nil {
		return routerLeaderState{}, err
	}
	defer pool.Close()

	table, err := resolvePostgresTable(cr.Spec.LeaderStateStore.PGTable)
	if err != nil {
		return routerLeaderState{}, err
	}
	recordID := resolveLeaderStateRecordID(cr.Namespace, cr.Name)

	if err := r.ensurePostgresLeaderStateTable(backendCtx, pool, table); err != nil {
		return routerLeaderState{}, err
	}

	row := pool.QueryRow(backendCtx, fmt.Sprintf("SELECT state FROM %s WHERE id = $1", table), recordID)
	var rawState string
	if err := row.Scan(&rawState); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return routerLeaderState{}, nil
		}
		return routerLeaderState{}, err
	}

	state := routerLeaderState{}
	if strings.TrimSpace(rawState) == "" {
		return routerLeaderState{}, nil
	}

	if err := json.Unmarshal([]byte(rawState), &state); err != nil {
		return routerLeaderState{}, err
	}

	if state.Candidates == nil {
		state.Candidates = map[string]map[string]any{}
	}

	return state, nil
}

func (r *CubestoreRouterReconciler) readLeaderStateFromRedis(ctx context.Context, cr *v1alpha1.CubestoreRouter) (routerLeaderState, error) {
	if cr == nil || cr.Spec.LeaderStateStore == nil {
		return routerLeaderState{}, nil
	}

	dsn := strings.TrimSpace(cr.Spec.LeaderStateStore.DSN)
	if dsn == "" {
		return routerLeaderState{}, fmt.Errorf("leaderStateStore.dsn is required for redis backend")
	}

	opt, err := redis.ParseURL(dsn)
	if err != nil {
		return routerLeaderState{}, err
	}

	backendCtx, cancel := context.WithTimeout(ctx, leaderStateOpTimeout)
	defer cancel()

	rdb := redis.NewClient(opt)
	defer rdb.Close()

	key := resolveRedisLeaderStateKey(cr.Spec.LeaderStateStore.RedisKey, cr.Namespace, cr.Name)
	rawState, err := rdb.Get(backendCtx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return routerLeaderState{}, nil
		}
		return routerLeaderState{}, err
	}

	state := routerLeaderState{}
	if strings.TrimSpace(rawState) == "" {
		return routerLeaderState{}, nil
	}

	if err := json.Unmarshal([]byte(rawState), &state); err != nil {
		return routerLeaderState{}, err
	}

	if state.Candidates == nil {
		state.Candidates = map[string]map[string]any{}
	}

	return state, nil
}

func (r *CubestoreRouterReconciler) writeLeaderStateToPostgres(
	ctx context.Context,
	namespace string,
	cr *v1alpha1.CubestoreRouter,
	state routerLeaderState,
) error {
	if cr == nil || cr.Spec.LeaderStateStore == nil {
		return nil
	}

	dsn := strings.TrimSpace(cr.Spec.LeaderStateStore.DSN)
	if dsn == "" {
		return fmt.Errorf("leaderStateStore.dsn is required for postgres backend")
	}

	backendCtx, cancel := context.WithTimeout(ctx, leaderStateOpTimeout)
	defer cancel()

	pool, err := pgxpool.New(backendCtx, dsn)
	if err != nil {
		return err
	}
	defer pool.Close()

	table, err := resolvePostgresTable(cr.Spec.LeaderStateStore.PGTable)
	if err != nil {
		return err
	}
	recordID := resolveLeaderStateRecordID(namespace, cr.Name)

	if err := r.ensurePostgresLeaderStateTable(backendCtx, pool, table); err != nil {
		return err
	}

	stateJSON, err := json.Marshal(state)
	if err != nil {
		return err
	}

	query := fmt.Sprintf(`INSERT INTO %s (id, state) VALUES ($1, $2::jsonb) ON CONFLICT (id) DO UPDATE SET state = EXCLUDED.state, updated_at = now()`, table)
	_, err = pool.Exec(backendCtx, query, recordID, string(stateJSON))
	return err
}

func (r *CubestoreRouterReconciler) writeLeaderStateToRedis(
	ctx context.Context,
	cr *v1alpha1.CubestoreRouter,
	state routerLeaderState,
) error {
	if cr == nil || cr.Spec.LeaderStateStore == nil {
		return nil
	}

	dsn := strings.TrimSpace(cr.Spec.LeaderStateStore.DSN)
	if dsn == "" {
		return fmt.Errorf("leaderStateStore.dsn is required for redis backend")
	}

	opt, err := redis.ParseURL(dsn)
	if err != nil {
		return err
	}

	backendCtx, cancel := context.WithTimeout(ctx, leaderStateOpTimeout)
	defer cancel()

	rdb := redis.NewClient(opt)
	defer rdb.Close()

	stateJSON, err := json.Marshal(state)
	if err != nil {
		return err
	}

	key := resolveRedisLeaderStateKey(cr.Spec.LeaderStateStore.RedisKey, cr.Namespace, cr.Name)
	return rdb.Set(backendCtx, key, stateJSON, 0).Err()
}

func (r *CubestoreRouterReconciler) ensurePostgresLeaderStateTable(ctx context.Context, pool *pgxpool.Pool, table string) error {
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id text PRIMARY KEY, state jsonb NOT NULL, updated_at timestamptz NOT NULL DEFAULT now())`, table)
	_, err := pool.Exec(ctx, query)
	return err
}

func (r *CubestoreRouterReconciler) buildLeaderState(candidates []candidate, leader *candidate, leaderEpoch int64) routerLeaderState {
	activeLeader := ""
	if leader != nil {
		activeLeader = leader.Name
	}

	candidateRoles := map[string]map[string]any{}
	for _, cand := range candidates {
		candidateRoles[cand.Name] = map[string]any{
			"role":   cand.Role,
			"ready":  cand.Ready,
			"ip":     cand.PodIP,
			"create": cand.CreatedAt.Format(time.RFC3339Nano),
		}
	}

	return routerLeaderState{
		LeaderEpoch:  leaderEpoch,
		ActiveLeader: activeLeader,
		UpdatedAt:    metav1.Now().Format(time.RFC3339Nano),
		Candidates:   candidateRoles,
	}
}

func (r *CubestoreRouterReconciler) withSyncCondition(
	conditions []metav1.Condition,
	syncErr error,
	observedGeneration int64,
) []metav1.Condition {
	if syncErr == nil {
		conditions = r.upsertCondition(conditions, metav1.Condition{
			Type:               syncConditionType,
			Status:             metav1.ConditionTrue,
			Reason:             "StateSyncSucceeded",
			Message:            "Role state and labels synchronized.",
			ObservedGeneration: observedGeneration,
			LastTransitionTime: metav1.Now(),
		})
		return conditions
	}

	return r.upsertCondition(conditions, metav1.Condition{
		Type:               syncConditionType,
		Status:             metav1.ConditionFalse,
		Reason:             "StateSyncFailed",
		Message:            fmt.Sprintf("Failed to sync role state: %s", syncErr.Error()),
		ObservedGeneration: observedGeneration,
		LastTransitionTime: metav1.Now(),
	})
}

func (r *CubestoreRouterReconciler) normalizeConditions(conditions []metav1.Condition) []metav1.Condition {
	out := make([]metav1.Condition, len(conditions))
	copy(out, conditions)
	sort.SliceStable(out, func(i, j int) bool {
		order := func(typ string) int {
			switch typ {
			case leaderConditionType:
				return 0
			case syncConditionType:
				return 1
			default:
				return 2
			}
		}
		if out[i].Type == out[j].Type {
			return out[i].ObservedGeneration < out[j].ObservedGeneration
		}
		oi := order(out[i].Type)
		oj := order(out[j].Type)
		if oi != oj {
			return oi < oj
		}
		return out[i].Type < out[j].Type
	})
	return out
}

func (r *CubestoreRouterReconciler) candidatesToStatus(candidates []candidate) []v1alpha1.RouterCandidateStatus {
	out := make([]v1alpha1.RouterCandidateStatus, 0, len(candidates))
	for _, candidate := range candidates {
		c := candidate
		out = append(out, v1alpha1.RouterCandidateStatus{
			Name:          c.Name,
			Namespace:     c.Namespace,
			Role:          c.Role,
			IP:            c.PodIP,
			Ready:         c.Ready,
			LastProbeTime: &c.LastProbe,
		})
	}
	return out
}

func isPodReady(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

func statusEqual(a, b v1alpha1.CubestoreRouterStatus) bool {
	if a.Leader != b.Leader || a.LeaderIP != b.LeaderIP || a.LeaderRole != b.LeaderRole || a.LeaderEpoch != b.LeaderEpoch {
		return false
	}

	if (a.LastSwitchedAt == nil) != (b.LastSwitchedAt == nil) {
		return false
	}

	if a.LastSwitchedAt != nil && b.LastSwitchedAt != nil {
		if !a.LastSwitchedAt.Time.Equal(b.LastSwitchedAt.Time) {
			return false
		}
	}

	if len(a.Candidates) != len(b.Candidates) {
		return false
	}

	for i := range a.Candidates {
		if !candidateStatusEqual(a.Candidates[i], b.Candidates[i]) {
			return false
		}
	}

	if len(a.Conditions) != len(b.Conditions) {
		return false
	}

	for i := range a.Conditions {
		if !conditionEqual(a.Conditions[i], b.Conditions[i]) {
			return false
		}
	}

	return true
}

func conditionEqual(a, b metav1.Condition) bool {
	return a.Type == b.Type &&
		a.Status == b.Status &&
		a.Reason == b.Reason &&
		a.Message == b.Message &&
		a.ObservedGeneration == b.ObservedGeneration
}

func candidateStatusEqual(a, b v1alpha1.RouterCandidateStatus) bool {
	if a.Name != b.Name || a.Namespace != b.Namespace || a.Role != b.Role || a.IP != b.IP || a.Ready != b.Ready {
		return false
	}
	return true
}
