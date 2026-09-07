package controllers

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cube-js/cube-operator/api/v1alpha1"
	"github.com/cube-js/cube-operator/internal/leadership"
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
	labelNamespace                     = "cubestore.io/router-role"
	labelLeader                        = "leader"
	labelFollower                      = "follower"
	defaultPort                  int32 = 3030
	defaultPath                        = "/router/status"
	defaultRoleDataKey                 = "route-role.json"
	defaultRoleConfigMap               = "router-role-state"
	defaultPgTable                     = "cubestore_router_leader_state"
	defaultRedisKey                    = "cube-router/leader-state"
	leaderStateBackendConfigMap        = "configmap"
	leaderStateBackendPostgres         = "postgres"
	leaderStateBackendRedis            = "redis"
	leaderStateBackendKubernetes       = "kubernetes"
	leaderConditionType                = "LeaderElection"
	syncConditionType                  = "RoleStateSync"
	leaderStateRecordID                = "leader-state"
	leaderStateRecordIDPrefix          = "route-state"
	leaseClusterAnnotation             = "cubestore.io/lease-cluster"
	leaseEpochAnnotation               = "cubestore.io/lease-epoch"
	leaseTokenAnnotation               = "cubestore.io/lease-token"
)

const leaderStateOpTimeout = 4 * time.Second

var redisStateFenceScript = redis.NewScript(`
local current = redis.call('GET', KEYS[1])
if not current then
  redis.call('SET', KEYS[1], ARGV[1])
  return 1
end
local ok, decoded = pcall(cjson.decode, current)
if not ok or type(decoded.leaseEpoch) ~= 'number' or type(decoded.leaseClusterID) ~= 'string' or type(decoded.leaseToken) ~= 'string' then
  return 2
end
local current_epoch = tonumber(decoded.leaseEpoch)
local proposed_epoch = tonumber(ARGV[2])
if not current_epoch or not proposed_epoch or current_epoch > proposed_epoch then
  return 0
end
if current_epoch == proposed_epoch and (decoded.leaseClusterID ~= ARGV[3] or decoded.leaseToken ~= ARGV[4]) then
  return 0
end
redis.call('SET', KEYS[1], ARGV[1])
return 1
`)

type CubestoreRouterReconciler struct {
	client.Client
	APIReader client.Reader
	// Set only when this reconciler runs under Manager leader election.
	ManagerLeaderElection bool
	*runtime.Scheme
	leaseMu      sync.Mutex
	routerLeases map[string]leadership.LeaseRecord
	leaseStore   leadership.LeaseStore
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
	podList, err := r.listRouters(ctx, targetNS, cr.Spec.Selector)
	if err != nil {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, err
	}

	candidates := r.probeCandidates(ctx, podList, cr)
	leader, lease, leaseErr := r.resolveRouterLease(ctx, &cr, candidates)
	leaderCandidates := 0
	if leader != nil {
		leaderCandidates = 1
	}

	nextStatus := cr.Status
	nextStatus.Candidates = r.candidatesToStatus(candidates)
	nextStatus.Conditions = nil
	promotionLeader := (*candidate)(nil)
	promotionPhase := promotionPhaseFrom(&cr)
	promotionErr := leaseErr
	if promotionErr == nil && lease.Epoch <= 0 {
		promotionErr = errors.New("no valid router lease; refusing role and promotion marker writes")
	}
	if promotionErr == nil {
		promotionLeader, promotionPhase, promotionErr = r.reconcilePromotion(ctx, &cr, targetNS, candidates, leader, lease)
		if promotionErr != nil && !errors.Is(promotionErr, errPromotionPending) {
			log.Error(promotionErr, "promotion reconciliation failed", "phase", promotionPhase, "epoch", lease.Epoch)
		}
	}

	syncStateErr := promotionErr
	if syncStateErr == nil {
		// Publish the fenced lease holder before waiting for its Router
		// acknowledgement. The Router needs activeLeader, epoch, and token
		// from this marker to produce the readiness acknowledgement; delaying
		// the marker until serving creates a circular promotion deadlock.
		stateLeader := leader
		if promotionPhase == promotionPhaseServing && promotionLeader != nil {
			stateLeader = promotionLeader
		}
		syncStateErr = r.syncRoleState(ctx, targetNS, &cr, candidates, stateLeader, lease.Epoch, resolveRoleConfigMapName(&cr), lease)
		if syncStateErr != nil && promotionLeader != nil {
			// A serving label is never retained when the authoritative state
			// publication failed. Keep the Service in the no-leader state.
			_ = r.fenceRouterEndpoints(ctx, targetNS, &cr, candidates, lease)
			promotionLeader = nil
			promotionPhase = promotionPhaseFenced
		}
	}
	// Publication failure invalidates readiness as well as the serving label.
	if syncStateErr != nil {
		promotionLeader = nil
		promotionPhase = promotionPhaseFenced
	}
	nextStatus = withRecoveryStatus(nextStatus, promotionLeader != nil && promotionPhase == promotionPhaseServing, promotionLeader)
	nextStatus.Conditions = r.withLeaderCondition(candidates, promotionLeader, leaderCandidates, nil, int64(cr.Generation))
	nextStatus.Conditions = r.withSyncCondition(nextStatus.Conditions, syncStateErr, int64(cr.Generation))
	nextStatus.Conditions = r.withRecoveryConditions(nextStatus.Conditions, nextStatus.Recovery, int64(cr.Generation))
	nextStatus.Conditions = r.normalizeConditions(nextStatus.Conditions)
	if errors.Is(leaseErr, errLeaseStateLost) {
		nextStatus.Conditions = append(nextStatus.Conditions, metav1.Condition{
			Type: "LeaseStateIntegrity", Status: metav1.ConditionFalse,
			Reason: "LeaseStateLost", Message: leaseErr.Error(),
			ObservedGeneration: cr.Generation, LastTransitionTime: metav1.Now(),
		})
	}

	if promotionLeader != nil && promotionPhase == promotionPhaseServing {
		nextStatus.Leader = promotionLeader.Name
		nextStatus.LeaderIP = promotionLeader.PodIP
		nextStatus.LeaderRole = labelLeader
		nextStatus.LeaderEpoch = lease.Epoch
		if cr.Status.Leader != promotionLeader.Name {
			now := metav1.Now()
			nextStatus.LastSwitchedAt = &now
		}
	} else {
		nextStatus.Leader = ""
		nextStatus.LeaderIP = ""
		nextStatus.LeaderRole = ""
		nextStatus.LeaderEpoch = lease.Epoch
		nextStatus.LastSwitchedAt = cr.Status.LastSwitchedAt
	}

	if !statusEqual(cr.Status, nextStatus) {
		var err error
		if lease.Epoch > 0 && leaseErr == nil {
			err = r.updateRouterStatusWithFence(ctx, &cr, nextStatus, lease)
		} else {
			// Only publish an unavailable status without a lease. Kubernetes RV
			// concurrency rejects a stale observer racing a newer controller.
			unavailable := cr.DeepCopy()
			unavailable.Status = nextStatus
			err = r.Status().Update(ctx, unavailable)
		}
		if err != nil {
			log.Error(err, "update status failed")
			return ctrl.Result{RequeueAfter: 5 * time.Second}, err
		}
	}

	if syncStateErr != nil {
		if errors.Is(syncStateErr, errPromotionPending) {
			// Expected propagation waits must use the fixed poll interval. A
			// non-nil error makes controller-runtime ignore RequeueAfter and
			// accumulate exponential backoff, delaying the next acknowledgement.
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
		log.Error(syncStateErr, "role state synchronization failed", "phase", promotionPhase, "epoch", lease.Epoch)
		return ctrl.Result{RequeueAfter: 5 * time.Second}, syncStateErr
	}

	return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
}

func (r *CubestoreRouterReconciler) resolveRouterLease(ctx context.Context, cr *v1alpha1.CubestoreRouter, candidates []candidate) (*candidate, leadership.LeaseRecord, error) {
	store, closeStore, err := r.routerLeaseStore(ctx, cr)
	if err != nil {
		return nil, leadership.LeaseRecord{}, err
	}
	defer closeStore()

	clusterID := resolveLeaderStateRecordID(cr.Namespace, cr.Name)
	current, err := store.Get(ctx, clusterID)
	if err == nil {
		currentCandidate := readyCandidateByName(candidates, current.HolderID)
		if currentCandidate == nil {
			// Never renew a lease for a Pod that is no longer a ready
			// candidate. During a rollout or deletion the old holder must
			// expire naturally so a current Pod can acquire the next epoch.
			r.clearLocalRouterLease(clusterID, current.Token)
			return nil, current, nil
		}
		local, owned := r.localRouterLease(clusterID)
		if !owned && r.ManagerLeaderElection && r.APIReader != nil {
			// Manager starts this controller only after election and cancels
			// reconcile contexts on shutdown. Adopt only an acknowledged holder;
			// Renew re-reads the API and CAS-checks the full lease identity.
			if err := ctx.Err(); err != nil {
				return nil, current, err
			}
			if promotionAcknowledged(*currentCandidate, current) {
				local, owned = current, true
			}
		}
		if owned && local.HolderID == current.HolderID && local.Token == current.Token {
			renewed, renewedOK, renewErr := store.Renew(ctx, local, routerLeaseTTL(cr))
			if renewErr != nil {
				return nil, current, renewErr
			}
			if !renewedOK {
				r.clearLocalRouterLease(clusterID, local.Token)
				return nil, current, nil
			}
			r.setLocalRouterLease(clusterID, renewed)
			current = renewed
		}
		return currentCandidate, current, nil
	}
	if !errors.Is(err, leadership.ErrLeaseNotFound) {
		return nil, leadership.LeaseRecord{}, err
	}

	preferred, _ := r.chooseLeader(candidates, cr.Spec.ElectionStrategy, "")
	if preferred == nil {
		return nil, leadership.LeaseRecord{}, nil
	}

	lease, acquired, err := store.Acquire(ctx, clusterID, preferred.Name, routerLeaseTTL(cr))
	if err != nil {
		return nil, leadership.LeaseRecord{}, err
	}
	if acquired {
		r.setLocalRouterLease(clusterID, lease)
		return readyCandidateByName(candidates, lease.HolderID), lease, nil
	}

	return readyCandidateByName(candidates, lease.HolderID), lease, nil
}

func (r *CubestoreRouterReconciler) routerLeaseStore(ctx context.Context, cr *v1alpha1.CubestoreRouter) (leadership.LeaseStore, func(), error) {
	if r.leaseStore != nil {
		return r.leaseStore, func() {}, nil
	}
	backend, dsn, redisKey, pgTable, err := r.externalLeaseConfig(ctx, cr)
	if err != nil {
		return nil, nil, err
	}

	switch backend {
	case leaderStateBackendKubernetes:
		leaseName := kubernetesLeaseName(cr)
		if r.APIReader == nil {
			return nil, nil, errors.New("Kubernetes LeaseStore requires an uncached APIReader")
		}
		store := leadership.NewKubernetesStoreWithReader(r.Client, r.APIReader, cr.Namespace, leaseName)
		store.BeforeCreate = func(ctx context.Context) error { return r.reserveLeaseBootstrap(ctx, cr) }
		return store, func() {}, nil
	case leaderStateStoreTypeRedis:
		options, err := redis.ParseURL(dsn)
		if err != nil {
			return nil, nil, err
		}
		client := redis.NewClient(options)
		prefix := strings.TrimSpace(redisKey)
		if prefix == "" {
			prefix = defaultRedisKey
		}
		return leadership.NewRedisStore(client, prefix), func() { _ = client.Close() }, nil
	case leaderStateStoreTypePostgres:
		pool, err := pgxpool.New(ctx, dsn)
		if err != nil {
			return nil, nil, err
		}
		store, err := leadership.NewPostgresStore(pool, pgTable)
		if err != nil {
			pool.Close()
			return nil, nil, err
		}
		if err := store.Ensure(ctx); err != nil {
			pool.Close()
			return nil, nil, err
		}
		return store, pool.Close, nil
	default:
		return nil, nil, fmt.Errorf("leaderStateStore.type %q is not an external lease backend", backend)
	}
}

func (r *CubestoreRouterReconciler) externalLeaseConfig(ctx context.Context, cr *v1alpha1.CubestoreRouter) (leaderStateBackendType, string, string, string, error) {
	if cr == nil {
		return "", "", "", "", fmt.Errorf("router resource is required")
	}

	var backend leaderStateBackendType
	var secretRef corev1.SecretReference
	var redisKey, pgTable string
	if cr.Spec.StateStore != nil {
		backend = leaderStateBackendType(strings.TrimSpace(strings.ToLower(cr.Spec.StateStore.Type)))
		if cr.Spec.StateStore.SecretRef != nil {
			secretRef = *cr.Spec.StateStore.SecretRef
		}
	} else if cr.Spec.LeaderStateStore != nil {
		backend = leaderStateBackendType(strings.TrimSpace(strings.ToLower(cr.Spec.LeaderStateStore.Type)))
		if cr.Spec.LeaderStateStore.SecretRef != nil {
			secretRef = *cr.Spec.LeaderStateStore.SecretRef
		}
		redisKey = cr.Spec.LeaderStateStore.RedisKey
		pgTable = cr.Spec.LeaderStateStore.PGTable
	} else {
		return "", "", "", "", fmt.Errorf("an external stateStore is required for router leadership")
	}

	if backend == leaderStateBackendKubernetes {
		return backend, "", "", "", nil
	}

	if backend != leaderStateStoreTypeRedis && backend != leaderStateStoreTypePostgres {
		return "", "", "", "", fmt.Errorf("stateStore.type %q is not an external lease backend", backend)
	}
	if strings.TrimSpace(secretRef.Name) == "" || strings.TrimSpace(secretRef.Namespace) == "" {
		return "", "", "", "", fmt.Errorf("stateStore.secretRef must include name and namespace")
	}

	secret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Name: secretRef.Name, Namespace: secretRef.Namespace}, secret); err != nil {
		return "", "", "", "", fmt.Errorf("read stateStore secret: %w", err)
	}
	dsn := strings.TrimSpace(string(secret.Data["dsn"]))
	if dsn == "" {
		return "", "", "", "", fmt.Errorf("stateStore secret %s/%s must contain a non-empty dsn key", secretRef.Namespace, secretRef.Name)
	}
	if backend == leaderStateStoreTypeRedis {
		var err error
		dsn, err = redisDSNWithSecretPassword(dsn, secret.Data["password"])
		if err != nil {
			return "", "", "", "", err
		}
	}
	return backend, dsn, redisKey, pgTable, nil
}

func redisDSNWithSecretPassword(dsn string, password []byte) (string, error) {
	parsed, err := url.Parse(dsn)
	if err != nil {
		return "", fmt.Errorf("parse Redis DSN: %w", err)
	}
	if parsed.Scheme != "redis" && parsed.Scheme != "rediss" {
		return dsn, nil
	}
	if parsed.User != nil {
		if _, hasPassword := parsed.User.Password(); hasPassword {
			return dsn, nil
		}
	}
	if len(password) == 0 {
		return dsn, nil
	}

	username := ""
	if parsed.User != nil {
		username = parsed.User.Username()
	}
	parsed.User = url.UserPassword(username, string(password))
	return parsed.String(), nil
}

func (r *CubestoreRouterReconciler) stateStoreDSN(ctx context.Context, cr *v1alpha1.CubestoreRouter) (string, error) {
	_, dsn, _, _, err := r.externalLeaseConfig(ctx, cr)
	return dsn, err
}

func routerLeaseTTL(cr *v1alpha1.CubestoreRouter) time.Duration {
	seconds := int32(v1alpha1.DefaultLeaseDurationSeconds)
	if cr != nil && cr.Spec.LeaseDurationSeconds > 0 {
		seconds = cr.Spec.LeaseDurationSeconds
	}
	return time.Duration(seconds) * time.Second
}

func kubernetesLeaseName(cr *v1alpha1.CubestoreRouter) string {
	namespace := strings.TrimSpace(cr.Namespace)
	if namespace == "" {
		namespace = "default"
	}
	name := strings.TrimSpace(cr.Name)
	if name == "" {
		name = "router"
	}
	return "cube-router-" + strings.ReplaceAll(namespace+"/"+name, "/", "-")
}

func readyCandidateByName(candidates []candidate, name string) *candidate {
	for i := range candidates {
		if candidates[i].Name == name && candidates[i].Ready && candidates[i].PodIP != "" {
			candidate := candidates[i]
			return &candidate
		}
	}
	return nil
}

func (r *CubestoreRouterReconciler) localRouterLease(clusterID string) (leadership.LeaseRecord, bool) {
	r.leaseMu.Lock()
	defer r.leaseMu.Unlock()
	lease, ok := r.routerLeases[clusterID]
	return lease, ok
}

func (r *CubestoreRouterReconciler) setLocalRouterLease(clusterID string, lease leadership.LeaseRecord) {
	r.leaseMu.Lock()
	defer r.leaseMu.Unlock()
	if r.routerLeases == nil {
		r.routerLeases = make(map[string]leadership.LeaseRecord)
	}
	r.routerLeases[clusterID] = lease
}

func (r *CubestoreRouterReconciler) clearLocalRouterLease(clusterID, token string) {
	r.leaseMu.Lock()
	defer r.leaseMu.Unlock()
	if current, ok := r.routerLeases[clusterID]; ok && current.Token == token {
		delete(r.routerLeases, clusterID)
	}
}

func (r *CubestoreRouterReconciler) validateLeaseBeforeWrite(ctx context.Context, cr *v1alpha1.CubestoreRouter, presented leadership.LeaseRecord) error {
	if presented.ClusterID == "" || presented.HolderID == "" || presented.Epoch <= 0 || presented.Token == "" {
		return leadership.ErrStaleLease
	}
	store, closeStore, err := r.routerLeaseStore(ctx, cr)
	if err != nil {
		return err
	}
	defer closeStore()
	current, err := store.Get(ctx, presented.ClusterID)
	if err != nil {
		return err
	}
	if err := leadership.ValidateLeaseFence(current, presented); err != nil {
		if errors.Is(err, leadership.ErrStaleLease) {
			r.clearLocalRouterLease(presented.ClusterID, presented.Token)
		}
		return err
	}
	return nil
}

type leaseJSONPatchOperation struct {
	Op    string      `json:"op"`
	Path  string      `json:"path"`
	Value interface{} `json:"value,omitempty"`
}

func hasLeaseFence(annotations map[string]string, lease leadership.LeaseRecord) bool {
	return annotations[leaseClusterAnnotation] == lease.ClusterID &&
		annotations[leaseEpochAnnotation] == strconv.FormatInt(lease.Epoch, 10) &&
		annotations[leaseTokenAnnotation] == lease.Token
}

func isRoleStateStaleButRecoverable(cm *corev1.ConfigMap, lease leadership.LeaseRecord) bool {
	annotations := cm.GetAnnotations()
	if annotations == nil {
		return true
	}
	if annotations[leaseClusterAnnotation] == "" && annotations[leaseEpochAnnotation] == "" && annotations[leaseTokenAnnotation] == "" {
		return true
	}
	if annotations[leaseClusterAnnotation] != lease.ClusterID {
		return false
	}
	currentEpoch, epochErr := strconv.ParseInt(annotations[leaseEpochAnnotation], 10, 64)
	if epochErr != nil {
		return true
	}
	if currentEpoch <= lease.Epoch {
		return false
	}
	// A complete fence from a newer epoch is authoritative and must never be
	// rebuilt by an older writer. Recovery is only safe for a legacy or
	// malformed marker that lacks a usable token.
	return strings.TrimSpace(annotations[leaseTokenAnnotation]) == ""
}

func leaseFenceValues(lease leadership.LeaseRecord) map[string]string {
	return map[string]string{
		leaseClusterAnnotation: lease.ClusterID,
		leaseEpochAnnotation:   strconv.FormatInt(lease.Epoch, 10),
		leaseTokenAnnotation:   lease.Token,
	}
}

func jsonPointerEscape(value string) string {
	return strings.ReplaceAll(strings.ReplaceAll(value, "~", "~0"), "/", "~1")
}

func (r *CubestoreRouterReconciler) ensureObjectFence(ctx context.Context, obj client.Object, lease leadership.LeaseRecord) error {
	if hasLeaseFence(obj.GetAnnotations(), lease) {
		return nil
	}
	operations := []leaseJSONPatchOperation{{Op: "test", Path: "/metadata/resourceVersion", Value: obj.GetResourceVersion()}}
	annotations := obj.GetAnnotations()
	if currentCluster, clusterOK := annotations[leaseClusterAnnotation]; clusterOK {
		currentEpoch, epochErr := strconv.ParseInt(annotations[leaseEpochAnnotation], 10, 64)
		currentToken, tokenOK := annotations[leaseTokenAnnotation]
		if epochErr != nil || !tokenOK || currentCluster != lease.ClusterID || currentEpoch > lease.Epoch || (currentEpoch == lease.Epoch && currentToken != lease.Token) {
			return leadership.ErrStaleLease
		}
	}
	if annotations == nil {
		operations = append(operations, leaseJSONPatchOperation{Op: "add", Path: "/metadata/annotations", Value: leaseFenceValues(lease)})
	} else {
		for key, value := range leaseFenceValues(lease) {
			path := "/metadata/annotations/" + jsonPointerEscape(key)
			if current, ok := annotations[key]; ok {
				operations = append(operations, leaseJSONPatchOperation{Op: "test", Path: path, Value: current})
				operations = append(operations, leaseJSONPatchOperation{Op: "replace", Path: path, Value: value})
			} else {
				operations = append(operations, leaseJSONPatchOperation{Op: "add", Path: path, Value: value})
			}
		}
	}
	patch, err := json.Marshal(operations)
	if err != nil {
		return err
	}
	return r.Patch(ctx, obj, client.RawPatch(types.JSONPatchType, patch))
}

func fencedRolePatch(obj client.Object, role string, lease leadership.LeaseRecord) ([]byte, error) {
	operations := []leaseJSONPatchOperation{
		{Op: "test", Path: "/metadata/resourceVersion", Value: obj.GetResourceVersion()},
		{Op: "test", Path: "/metadata/annotations/" + jsonPointerEscape(leaseClusterAnnotation), Value: lease.ClusterID},
		{Op: "test", Path: "/metadata/annotations/" + jsonPointerEscape(leaseEpochAnnotation), Value: strconv.FormatInt(lease.Epoch, 10)},
		{Op: "test", Path: "/metadata/annotations/" + jsonPointerEscape(leaseTokenAnnotation), Value: lease.Token},
	}
	labels := obj.GetLabels()
	rolePath := "/metadata/labels/" + jsonPointerEscape(labelNamespace)
	if labels == nil {
		operations = append(operations, leaseJSONPatchOperation{Op: "add", Path: "/metadata/labels", Value: map[string]string{labelNamespace: role}})
	} else if _, ok := labels[labelNamespace]; ok {
		operations = append(operations, leaseJSONPatchOperation{Op: "replace", Path: rolePath, Value: role})
	} else {
		operations = append(operations, leaseJSONPatchOperation{Op: "add", Path: rolePath, Value: role})
	}
	return json.Marshal(operations)
}

func fencedConfigMapPatch(cm *corev1.ConfigMap, data map[string]string, lease leadership.LeaseRecord) ([]byte, error) {
	operations := []leaseJSONPatchOperation{
		{Op: "test", Path: "/metadata/resourceVersion", Value: cm.GetResourceVersion()},
		{Op: "test", Path: "/metadata/annotations/" + jsonPointerEscape(leaseClusterAnnotation), Value: lease.ClusterID},
		{Op: "test", Path: "/metadata/annotations/" + jsonPointerEscape(leaseEpochAnnotation), Value: strconv.FormatInt(lease.Epoch, 10)},
		{Op: "test", Path: "/metadata/annotations/" + jsonPointerEscape(leaseTokenAnnotation), Value: lease.Token},
	}
	path := "/data"
	if cm.Data == nil {
		operations = append(operations, leaseJSONPatchOperation{Op: "add", Path: path, Value: data})
	} else {
		operations = append(operations, leaseJSONPatchOperation{Op: "replace", Path: path, Value: data})
	}
	return json.Marshal(operations)
}

func (r *CubestoreRouterReconciler) updatePodRoleWithFence(ctx context.Context, pod *corev1.Pod, role string, cr *v1alpha1.CubestoreRouter, lease leadership.LeaseRecord) error {
	reader := client.Reader(r.Client)
	if r.APIReader != nil {
		reader = r.APIReader
	}
	current := &corev1.Pod{}
	if err := reader.Get(ctx, types.NamespacedName{Name: pod.Name, Namespace: pod.Namespace}, current); err != nil {
		return err
	}
	if err := r.validateLeaseBeforeWrite(ctx, cr, lease); err != nil {
		return err
	}
	if err := r.ensureObjectFence(ctx, current, lease); err != nil {
		return err
	}
	if err := reader.Get(ctx, types.NamespacedName{Name: pod.Name, Namespace: pod.Namespace}, current); err != nil {
		return err
	}
	if err := r.validateLeaseBeforeWrite(ctx, cr, lease); err != nil {
		return err
	}
	patch, err := fencedRolePatch(current, role, lease)
	if err != nil {
		return err
	}
	return r.Patch(ctx, current, client.RawPatch(types.JSONPatchType, patch))
}

func (r *CubestoreRouterReconciler) updateRouterStatusWithFence(ctx context.Context, cr *v1alpha1.CubestoreRouter, status v1alpha1.CubestoreRouterStatus, lease leadership.LeaseRecord) error {
	reader := client.Reader(r.Client)
	if r.APIReader != nil {
		reader = r.APIReader
	}
	fresh := &v1alpha1.CubestoreRouter{}
	if err := reader.Get(ctx, types.NamespacedName{Name: cr.Name, Namespace: cr.Namespace}, fresh); err != nil {
		return err
	}
	if err := r.validateLeaseBeforeWrite(ctx, fresh, lease); err != nil {
		return err
	}
	if err := r.ensureObjectFence(ctx, fresh, lease); err != nil {
		return err
	}
	if err := reader.Get(ctx, types.NamespacedName{Name: cr.Name, Namespace: cr.Namespace}, fresh); err != nil {
		return err
	}
	if err := r.validateLeaseBeforeWrite(ctx, fresh, lease); err != nil {
		return err
	}
	patch := []leaseJSONPatchOperation{
		// The lease annotations are the authoritative fencing predicate. A
		// resourceVersion test is unsafe here because controller-runtime's cache
		// can lag immediately after the lease/promotion annotation patch.
		{Op: "test", Path: "/metadata/annotations/" + jsonPointerEscape(leaseClusterAnnotation), Value: lease.ClusterID},
		{Op: "test", Path: "/metadata/annotations/" + jsonPointerEscape(leaseEpochAnnotation), Value: strconv.FormatInt(lease.Epoch, 10)},
		{Op: "test", Path: "/metadata/annotations/" + jsonPointerEscape(leaseTokenAnnotation), Value: lease.Token},
		{Op: "replace", Path: "/status", Value: status},
	}
	patchData, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	return r.Status().Patch(ctx, fresh, client.RawPatch(types.JSONPatchType, patchData))
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
	Name                 string
	Namespace            string
	PodIP                string
	Role                 string
	Ready                bool
	LastProbe            metav1.Time
	CreatedAt            time.Time
	StatusContract       bool
	IsLeader             bool
	LeaderEpoch          int64
	LeaseEpoch           int64
	LeaseTokenHash       string
	MetaStoreReady       bool
	RecoveryCapabilities *runtimeRecoveryCapabilities
}

type routerLeaderState struct {
	LeaderEpoch    int64                     `json:"leaderEpoch"`
	ActiveLeader   string                    `json:"activeLeader"`
	UpdatedAt      string                    `json:"updatedAt"`
	Candidates     map[string]map[string]any `json:"candidates"`
	LeaseClusterID string                    `json:"leaseClusterID"`
	LeaseEpoch     int64                     `json:"leaseEpoch"`
	LeaseToken     string                    `json:"leaseToken"`
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
	h := &http.Client{Timeout: 2 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	out := make([]candidate, 0, len(pods))
	for _, p := range pods {
		cand := candidate{Name: p.Name, Namespace: p.Namespace, PodIP: p.Status.PodIP, LastProbe: metav1.Now(), Role: labelFollower, CreatedAt: p.CreationTimestamp.Time}
		if cand.PodIP == "" || !isPodReady(&p) || !p.DeletionTimestamp.IsZero() {
			out = append(out, cand)
			continue
		}
		endpoint := "http://" + net.JoinHostPort(cand.PodIP, fmt.Sprintf("%d", port)) + path
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			out = append(out, cand)
			continue
		}
		resp, err := h.Do(req)
		if err == nil {
			if resp.StatusCode == http.StatusOK {
				var body struct {
					IsLeader             bool                         `json:"isLeader"`
					LeaderEpoch          int64                        `json:"leaderEpoch"`
					LeaseEpoch           int64                        `json:"leaseEpoch"`
					LeaseTokenHash       string                       `json:"leaseTokenHash"`
					MetaStoreReady       bool                         `json:"metaStoreReady"`
					Draining             bool                         `json:"draining"`
					RecoveryCapabilities *runtimeRecoveryCapabilities `json:"recoveryCapabilities"`
				}
				if err := json.NewDecoder(resp.Body).Decode(&body); err == nil {
					// A follower can be a healthy candidate without any leadership marker.
					// /router/lease.writeReady only validates local files: it MUST NOT
					// overwrite the actual asynchronous MetaStore probe in /router/status.
					cand.Ready = body.MetaStoreReady && !body.Draining
					cand.IsLeader = body.IsLeader && !body.Draining
					if cand.IsLeader {
						cand.Role = labelLeader
					}
					cand.LeaderEpoch = body.LeaderEpoch
					cand.LeaseEpoch = body.LeaseEpoch
					cand.LeaseTokenHash = strings.TrimSpace(body.LeaseTokenHash)
					cand.MetaStoreReady = body.MetaStoreReady
					cand.RecoveryCapabilities = body.RecoveryCapabilities
					cand.StatusContract = cand.IsLeader && cand.LeaderEpoch > 0 && cand.LeaseEpoch > 0 && cand.LeaseTokenHash != "" && cand.MetaStoreReady
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

func hashLeaseToken(token string) string {
	return fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(token)))
}

func promotionAcknowledged(candidate candidate, lease leadership.LeaseRecord) bool {
	return candidate.StatusContract && candidate.IsLeader && candidate.MetaStoreReady &&
		candidate.LeaderEpoch == lease.Epoch && candidate.LeaseEpoch == lease.Epoch &&
		strings.TrimSpace(candidate.LeaseTokenHash) == hashLeaseToken(lease.Token)
}

func withRecoveryStatus(status v1alpha1.CubestoreRouterStatus, promotionReady bool, promoted ...*candidate) v1alpha1.CubestoreRouterStatus {
	if promotionReady {
		status.Recovery.Promotion = v1alpha1.RecoveryGateStatus{
			State: v1alpha1.RecoveryStateReady, Reason: "PromotionAcknowledged",
			Message: "Router acknowledged the exact lease epoch, token hash, and MetaStore readiness.",
		}
	} else {
		status.Recovery.Promotion = v1alpha1.RecoveryGateStatus{
			State: v1alpha1.RecoveryStateNeedsContext, Reason: "NeedsContext",
			Message: "Router status must expose isLeader, leaderEpoch, leaseEpoch, leaseTokenHash, and metaStoreReady before promotion can route traffic.",
		}
	}
	var capabilities *runtimeRecoveryCapabilities
	if promotionReady && len(promoted) > 0 && promoted[0] != nil {
		capabilities = promoted[0].RecoveryCapabilities
	}
	status.Recovery.JobRecovery = capabilityGate(capabilities, "jobs")
	status.Recovery.MutationReconcile = capabilityGate(capabilities, "mutations")
	status.Recovery.Refresher = v1alpha1.RecoveryGateStatus{State: v1alpha1.RecoveryStateNeedsContext, Reason: "ManagedByCubeCluster", Message: "Rust Router does not own scheduled refresh processes; shared queue attempt recovery and scheduler ownership require separate evidence."}
	return status
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

func (r *CubestoreRouterReconciler) syncRoles(ctx context.Context, namespace string, cr *v1alpha1.CubestoreRouter, candidates []candidate, leader *candidate, lease leadership.LeaseRecord) error {
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

		if pod.Labels[labelNamespace] != role || !hasLeaseFence(pod.GetAnnotations(), lease) {
			if err := r.updatePodRoleWithFence(ctx, &pod, role, cr, lease); err != nil {
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
	lease leadership.LeaseRecord,
) error {
	state := r.buildLeaderState(candidates, leader, leaderEpoch)
	state.LeaseClusterID = lease.ClusterID
	state.LeaseEpoch = lease.Epoch
	state.LeaseToken = lease.Token
	roleData, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if err := r.syncRoleStateConfigMap(ctx, namespace, cr, roleStateConfigMap, roleData, lease); err != nil {
		return err
	}

	return r.syncRoleStateRemote(ctx, namespace, cr, state, lease)
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
	case leaderStateStoreTypeConfigMap, leaderStateBackendKubernetes, leaderStateStoreTypePostgres, leaderStateStoreTypeRedis:
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
	lease leadership.LeaseRecord,
) error {
	reader := client.Reader(r.Client)
	if r.APIReader != nil {
		reader = r.APIReader
	}
	cm := &corev1.ConfigMap{}
	if err := reader.Get(ctx, types.NamespacedName{Name: roleStateConfigMap, Namespace: namespace}, cm); err != nil {
		if client.IgnoreNotFound(err) != nil {
			return err
		}

		cm = &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:        roleStateConfigMap,
				Namespace:   namespace,
				Annotations: leaseFenceValues(lease),
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
		if err := r.validateLeaseBeforeWrite(ctx, cr, lease); err != nil {
			return err
		}
		return r.Create(ctx, cm)
	}

	if err := r.validateLeaseBeforeWrite(ctx, cr, lease); err != nil {
		return err
	}
	if err := r.ensureObjectFence(ctx, cm, lease); err != nil {
		if errors.Is(err, leadership.ErrStaleLease) && isRoleStateStaleButRecoverable(cm, lease) {
			return r.rebuildRoleStateConfigMap(ctx, cm, roleState, lease)
		}
		return err
	}
	fresh := &corev1.ConfigMap{}
	if err := reader.Get(ctx, types.NamespacedName{Name: roleStateConfigMap, Namespace: namespace}, fresh); err != nil {
		return err
	}
	if fresh.Data[defaultRoleDataKey] == string(roleState) {
		return nil
	}
	fresh.Data = map[string]string{
		defaultRoleDataKey: string(roleState),
	}
	if err := r.validateLeaseBeforeWrite(ctx, cr, lease); err != nil {
		return err
	}
	patch, err := fencedConfigMapPatch(fresh, fresh.Data, lease)
	if err != nil {
		return err
	}
	return r.Patch(ctx, fresh, client.RawPatch(types.JSONPatchType, patch))
}

func (r *CubestoreRouterReconciler) rebuildRoleStateConfigMap(
	ctx context.Context,
	cm *corev1.ConfigMap,
	roleState []byte,
	lease leadership.LeaseRecord,
) error {
	cm.Data = map[string]string{
		defaultRoleDataKey: string(roleState),
	}
	annotations := cm.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}
	for key, value := range leaseFenceValues(lease) {
		annotations[key] = value
	}
	cm.Annotations = annotations
	return r.Update(ctx, cm)
}

func (r *CubestoreRouterReconciler) syncRoleStateRemote(
	ctx context.Context,
	namespace string,
	cr *v1alpha1.CubestoreRouter,
	state routerLeaderState,
	lease leadership.LeaseRecord,
) error {
	backend, err := resolveLeaderStateBackend(cr.Spec.LeaderStateStore)
	if err != nil {
		return err
	}

	if backend == leaderStateStoreTypeConfigMap || backend == leaderStateBackendKubernetes {
		return nil
	}
	if err := r.validateLeaseBeforeWrite(ctx, cr, lease); err != nil {
		return err
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
	case leaderStateBackendKubernetes:
		return r.readLeaderStateFromConfigMap(ctx, namespace, configMapName)
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
	if cr == nil || (cr.Spec.StateStore == nil && cr.Spec.LeaderStateStore == nil) {
		return routerLeaderState{}, nil
	}

	dsn, err := r.stateStoreDSN(ctx, cr)
	if err != nil {
		return routerLeaderState{}, err
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
	if cr == nil || (cr.Spec.StateStore == nil && cr.Spec.LeaderStateStore == nil) {
		return routerLeaderState{}, nil
	}

	dsn, err := r.stateStoreDSN(ctx, cr)
	if err != nil {
		return routerLeaderState{}, err
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

func validatePersistedLeaseState(state routerLeaderState) error {
	if strings.TrimSpace(state.LeaseClusterID) == "" || state.LeaseEpoch <= 0 || strings.TrimSpace(state.LeaseToken) == "" {
		return leadership.ErrStaleLease
	}
	return nil
}

func (r *CubestoreRouterReconciler) writeLeaderStateToPostgres(
	ctx context.Context,
	namespace string,
	cr *v1alpha1.CubestoreRouter,
	state routerLeaderState,
) error {
	if cr == nil || (cr.Spec.StateStore == nil && cr.Spec.LeaderStateStore == nil) {
		return nil
	}

	dsn, err := r.stateStoreDSN(ctx, cr)
	if err != nil {
		return err
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
	if err := validatePersistedLeaseState(state); err != nil {
		return err
	}

	query := fmt.Sprintf(`INSERT INTO %s (id, state) VALUES ($1, $2::jsonb)
		ON CONFLICT (id) DO UPDATE SET state = EXCLUDED.state, updated_at = clock_timestamp()
		WHERE CASE WHEN (state->>'leaseEpoch') ~ '^[0-9]+$' THEN (state->>'leaseEpoch')::bigint ELSE 0 END < $3
		   OR (CASE WHEN (state->>'leaseEpoch') ~ '^[0-9]+$' THEN (state->>'leaseEpoch')::bigint ELSE 0 END = $3
		       AND state->>'leaseClusterID' = $4 AND state->>'leaseToken' = $5)`, table)
	tag, err := pool.Exec(backendCtx, query, recordID, string(stateJSON), state.LeaseEpoch, state.LeaseClusterID, state.LeaseToken)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return leadership.ErrStaleLease
	}
	return nil
}

func (r *CubestoreRouterReconciler) writeLeaderStateToRedis(
	ctx context.Context,
	cr *v1alpha1.CubestoreRouter,
	state routerLeaderState,
) error {
	if cr == nil || (cr.Spec.StateStore == nil && cr.Spec.LeaderStateStore == nil) {
		return nil
	}

	dsn, err := r.stateStoreDSN(ctx, cr)
	if err != nil {
		return err
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
	if err := validatePersistedLeaseState(state); err != nil {
		return err
	}

	key := resolveRedisLeaderStateKey(cr.Spec.LeaderStateStore.RedisKey, cr.Namespace, cr.Name)
	result, err := redisStateFenceScript.Run(backendCtx, rdb, []string{key}, string(stateJSON), state.LeaseEpoch, state.LeaseClusterID, state.LeaseToken).Result()
	if err != nil {
		return err
	}
	code, err := strconv.ParseInt(fmt.Sprint(result), 10, 64)
	if err != nil {
		return err
	}
	switch code {
	case 1:
		return nil
	case 2:
		return leadership.ErrLeaseUnknown
	default:
		return leadership.ErrStaleLease
	}
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

func (r *CubestoreRouterReconciler) withRecoveryConditions(conditions []metav1.Condition, status v1alpha1.RouterRecoveryStatus, observedGeneration int64) []metav1.Condition {
	gates := []struct {
		typ  string
		gate v1alpha1.RecoveryGateStatus
	}{
		{v1alpha1.CubestoreRouterConditionPromotionReady, status.Promotion},
		{v1alpha1.CubestoreRouterConditionJobRecovery, status.JobRecovery},
		{v1alpha1.CubestoreRouterConditionMutationReconcile, status.MutationReconcile},
		{v1alpha1.CubestoreRouterConditionRefresherReady, status.Refresher},
	}
	for _, item := range gates {
		conditionStatus := metav1.ConditionFalse
		if item.gate.State == v1alpha1.RecoveryStateNeedsContext {
			conditionStatus = metav1.ConditionUnknown
		}
		if item.gate.State == v1alpha1.RecoveryStateReady {
			conditionStatus = metav1.ConditionTrue
		}
		conditions = r.upsertCondition(conditions, metav1.Condition{
			Type: item.typ, Status: conditionStatus, Reason: item.gate.Reason,
			Message: item.gate.Message, ObservedGeneration: observedGeneration,
			LastTransitionTime: metav1.Now(),
		})
	}
	return conditions
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
	if a.Recovery != b.Recovery {
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
