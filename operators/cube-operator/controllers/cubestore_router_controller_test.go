package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cube-js/cube-operator/api/v1alpha1"
	"github.com/cube-js/cube-operator/internal/leadership"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type controllerLeaseStore struct {
	current leadership.LeaseRecord
	err     error
}

func (s *controllerLeaseStore) Acquire(context.Context, string, string, time.Duration) (leadership.LeaseRecord, bool, error) {
	return leadership.LeaseRecord{}, false, errors.New("not used by controller role-write test")
}

func (s *controllerLeaseStore) Renew(context.Context, leadership.LeaseRecord, time.Duration) (leadership.LeaseRecord, bool, error) {
	return leadership.LeaseRecord{}, false, errors.New("not used by controller role-write test")
}

func (s *controllerLeaseStore) Release(context.Context, leadership.LeaseRecord) error {
	return errors.New("not used by controller role-write test")
}

func (s *controllerLeaseStore) Get(context.Context, string) (leadership.LeaseRecord, error) {
	if s.err != nil {
		return leadership.LeaseRecord{}, s.err
	}
	return s.current, nil
}

type roleWriteClient struct {
	client.Client
	pod *corev1.Pod
}

type stateWriteClient struct {
	client.Client
	configMap *corev1.ConfigMap
}

func (c *stateWriteClient) Get(_ context.Context, key client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	if key.Name != c.configMap.Name || key.Namespace != c.configMap.Namespace {
		return errors.New("configmap not found")
	}
	cm, ok := obj.(*corev1.ConfigMap)
	if !ok {
		return errors.New("expected configmap object")
	}
	*cm = *c.configMap.DeepCopy()
	return nil
}

func (c *stateWriteClient) Patch(_ context.Context, obj client.Object, patch client.Patch, _ ...client.PatchOption) error {
	data, err := patch.Data(obj)
	if err != nil {
		return err
	}
	var operations []struct {
		Op    string      `json:"op"`
		Path  string      `json:"path"`
		Value interface{} `json:"value"`
	}
	if err := json.Unmarshal(data, &operations); err != nil {
		return err
	}
	for _, operation := range operations {
		if operation.Op != "test" {
			continue
		}
		value, ok := operation.Value.(string)
		if !ok {
			return errors.New("test value must be a string")
		}
		if operation.Path == "/metadata/resourceVersion" && value != c.configMap.ResourceVersion {
			return leadership.ErrStaleLease
		}
		if strings.HasPrefix(operation.Path, "/metadata/annotations/") {
			key := strings.TrimPrefix(operation.Path, "/metadata/annotations/")
			key = strings.ReplaceAll(strings.ReplaceAll(key, "~1", "/"), "~0", "~")
			if c.configMap.Annotations[key] != value {
				return leadership.ErrStaleLease
			}
		}
	}
	for _, operation := range operations {
		if operation.Op != "replace" && operation.Op != "add" || operation.Path != "/data" {
			continue
		}
		values, ok := operation.Value.(map[string]interface{})
		if !ok {
			return errors.New("configmap data must be an object")
		}
		c.configMap.Data = map[string]string{}
		for key, item := range values {
			value, ok := item.(string)
			if !ok {
				return errors.New("configmap value must be a string")
			}
			c.configMap.Data[key] = value
		}
	}
	version, _ := strconv.Atoi(c.configMap.ResourceVersion)
	c.configMap.ResourceVersion = strconv.Itoa(version + 1)
	return nil
}

type statusWriteClient struct {
	client.Client
	router           *v1alpha1.CubestoreRouter
	statusPatchCount int
}

func (c *statusWriteClient) Get(_ context.Context, key client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	if key.Name != c.router.Name || key.Namespace != c.router.Namespace {
		return errors.New("router not found")
	}
	router, ok := obj.(*v1alpha1.CubestoreRouter)
	if !ok {
		return errors.New("expected router object")
	}
	*router = *c.router.DeepCopy()
	return nil
}

func (c *statusWriteClient) Patch(_ context.Context, obj client.Object, patch client.Patch, _ ...client.PatchOption) error {
	return errors.New("metadata patch should use the main client")
}

func (c *statusWriteClient) Status() client.SubResourceWriter {
	return &statusPatchWriter{owner: c}
}

type statusPatchWriter struct{ owner *statusWriteClient }

func (w *statusPatchWriter) Create(context.Context, client.Object, client.Object, ...client.SubResourceCreateOption) error {
	return errors.New("not used")
}

func (w *statusPatchWriter) Update(context.Context, client.Object, ...client.SubResourceUpdateOption) error {
	return errors.New("status update must use patch")
}

func (w *statusPatchWriter) Patch(_ context.Context, obj client.Object, patch client.Patch, _ ...client.SubResourcePatchOption) error {
	data, err := patch.Data(obj)
	if err != nil {
		return err
	}
	var operations []struct {
		Op    string      `json:"op"`
		Path  string      `json:"path"`
		Value interface{} `json:"value"`
	}
	if err := json.Unmarshal(data, &operations); err != nil {
		return err
	}
	for _, operation := range operations {
		if operation.Op == "test" && operation.Path == "/metadata/resourceVersion" && operation.Value != w.owner.router.ResourceVersion {
			return leadership.ErrStaleLease
		}
	}
	for _, operation := range operations {
		if operation.Path != "/status" || operation.Op != "replace" {
			continue
		}
		statusData, err := json.Marshal(operation.Value)
		if err != nil {
			return err
		}
		var status v1alpha1.CubestoreRouterStatus
		if err := json.Unmarshal(statusData, &status); err != nil {
			return err
		}
		w.owner.router.Status = status
		w.owner.statusPatchCount++
	}
	return nil
}

func (c *roleWriteClient) Get(_ context.Context, key client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	if key.Name != c.pod.Name || key.Namespace != c.pod.Namespace {
		return errors.New("pod not found")
	}
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return errors.New("expected pod object")
	}
	*pod = *c.pod.DeepCopy()
	return nil
}

func (c *roleWriteClient) Update(_ context.Context, obj client.Object, _ ...client.UpdateOption) error {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return errors.New("expected pod object")
	}
	c.pod = pod.DeepCopy()
	return nil
}

func (c *roleWriteClient) Patch(_ context.Context, obj client.Object, patch client.Patch, _ ...client.PatchOption) error {
	data, err := patch.Data(obj)
	if err != nil {
		return err
	}
	var operations []struct {
		Op    string      `json:"op"`
		Path  string      `json:"path"`
		Value interface{} `json:"value"`
	}
	if err := json.Unmarshal(data, &operations); err != nil {
		return err
	}
	for _, operation := range operations {
		if operation.Op != "test" {
			continue
		}
		value, ok := operation.Value.(string)
		if !ok {
			return errors.New("test value must be a string")
		}
		switch operation.Path {
		case "/metadata/resourceVersion":
			if value != c.pod.ResourceVersion {
				return leadership.ErrStaleLease
			}
		default:
			key := strings.TrimPrefix(operation.Path, "/metadata/annotations/")
			key = strings.ReplaceAll(strings.ReplaceAll(key, "~1", "/"), "~0", "~")
			if !strings.HasPrefix(operation.Path, "/metadata/annotations/") || c.pod.Annotations[key] != value {
				return leadership.ErrStaleLease
			}
		}
	}
	for _, operation := range operations {
		if operation.Op != "add" && operation.Op != "replace" {
			continue
		}
		if operation.Path == "/metadata/annotations" {
			values, ok := operation.Value.(map[string]interface{})
			if !ok {
				return errors.New("annotations value must be an object")
			}
			c.pod.Annotations = map[string]string{}
			for key, item := range values {
				value, ok := item.(string)
				if !ok {
					return errors.New("annotation value must be a string")
				}
				c.pod.Annotations[key] = value
			}
			continue
		}
		if operation.Path == "/metadata/labels" {
			values, ok := operation.Value.(map[string]interface{})
			if !ok {
				return errors.New("labels value must be an object")
			}
			c.pod.Labels = map[string]string{}
			for key, item := range values {
				value, ok := item.(string)
				if !ok {
					return errors.New("label value must be a string")
				}
				c.pod.Labels[key] = value
			}
			continue
		}
		value, ok := operation.Value.(string)
		if strings.HasPrefix(operation.Path, "/metadata/annotations/") && ok {
			if c.pod.Annotations == nil {
				c.pod.Annotations = map[string]string{}
			}
			key := strings.TrimPrefix(operation.Path, "/metadata/annotations/")
			key = strings.ReplaceAll(strings.ReplaceAll(key, "~1", "/"), "~0", "~")
			c.pod.Annotations[key] = value
		}
		if strings.HasPrefix(operation.Path, "/metadata/labels/") && ok {
			if c.pod.Labels == nil {
				c.pod.Labels = map[string]string{}
			}
			key := strings.TrimPrefix(operation.Path, "/metadata/labels/")
			key = strings.ReplaceAll(strings.ReplaceAll(key, "~1", "/"), "~0", "~")
			c.pod.Labels[key] = value
		}
	}
	if c.pod.ResourceVersion == "" {
		c.pod.ResourceVersion = "1"
	}
	version, _ := strconv.Atoi(c.pod.ResourceVersion)
	c.pod.ResourceVersion = strconv.Itoa(version + 1)
	return nil
}

func newRoleWriteTestReconciler(t *testing.T, current leadership.LeaseRecord) *CubestoreRouterReconciler {
	t.Helper()
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "router-0", Namespace: "router-ns"}}
	return &CubestoreRouterReconciler{
		Client:     &roleWriteClient{pod: pod},
		leaseStore: &controllerLeaseStore{current: current},
	}
}

func roleWriteCandidate() candidate {
	return candidate{Name: "router-0", Namespace: "router-ns", PodIP: "10.0.0.1", Ready: true, CreatedAt: time.Unix(1, 0)}
}

func roleWriteLease(epoch int64, token string) leadership.LeaseRecord {
	return leadership.LeaseRecord{
		ClusterID: "router-ns/router",
		HolderID:  "router-0",
		Epoch:     epoch,
		Token:     token,
		IssuedAt:  time.Unix(10, 0),
		ExpiresAt: time.Now().Add(time.Minute),
	}
}

func TestSyncRolesRejectsStaleLeaseBeforePodWrite(t *testing.T) {
	current := roleWriteLease(2, "new-token")
	reconciler := newRoleWriteTestReconciler(t, current)
	cand := roleWriteCandidate()

	err := reconciler.syncRoles(context.Background(), "router-ns", nil, []candidate{cand}, &cand, roleWriteLease(1, "old-token"))
	if !errors.Is(err, leadership.ErrStaleLease) {
		t.Fatalf("syncRoles error = %v, want ErrStaleLease", err)
	}

	pod := reconciler.Client.(*roleWriteClient).pod
	if pod.Labels[labelNamespace] != "" {
		t.Fatalf("stale lease changed pod role to %q", pod.Labels[labelNamespace])
	}
}

func TestSyncRolesRejectsStaleLeaseAfterPromotionMarker(t *testing.T) {
	oldLease := roleWriteLease(1, "old-token")
	newLease := roleWriteLease(2, "new-token")
	reconciler := newRoleWriteTestReconciler(t, oldLease)
	reconciler.Client.(*roleWriteClient).pod.Annotations = leaseFenceValues(newLease)
	cand := roleWriteCandidate()

	err := reconciler.syncRoles(context.Background(), "router-ns", nil, []candidate{cand}, &cand, oldLease)
	if !errors.Is(err, leadership.ErrStaleLease) {
		t.Fatalf("syncRoles error = %v, want ErrStaleLease", err)
	}
	if reconciler.Client.(*roleWriteClient).pod.Labels[labelNamespace] != "" {
		t.Fatal("stale writer committed after the promotion fencing marker")
	}
}

func TestSyncRolesAcceptsCurrentLeaseForPodWrite(t *testing.T) {
	current := roleWriteLease(2, "new-token")
	reconciler := newRoleWriteTestReconciler(t, current)
	cand := roleWriteCandidate()

	if err := reconciler.syncRoles(context.Background(), "router-ns", nil, []candidate{cand}, &cand, current); err != nil {
		t.Fatal(err)
	}

	pod := reconciler.Client.(*roleWriteClient).pod
	if pod.Labels[labelNamespace] != labelLeader {
		t.Fatalf("current lease role = %q, want %q", pod.Labels[labelNamespace], labelLeader)
	}
}

func TestSyncRoleStateConfigMapUsesFencePredicate(t *testing.T) {
	lease := roleWriteLease(2, "current-token")
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Name: "role-state", Namespace: "router-ns", ResourceVersion: "1", Annotations: leaseFenceValues(lease),
	}, Data: map[string]string{defaultRoleDataKey: "old"}}
	stateClient := &stateWriteClient{configMap: cm}
	reconciler := &CubestoreRouterReconciler{Client: stateClient, leaseStore: &controllerLeaseStore{current: lease}}
	cr := &v1alpha1.CubestoreRouter{ObjectMeta: metav1.ObjectMeta{Name: "router", Namespace: "router-ns"}}

	if err := reconciler.syncRoleStateConfigMap(context.Background(), "router-ns", cr, "role-state", []byte("new"), lease); err != nil {
		t.Fatal(err)
	}
	if stateClient.configMap.Data[defaultRoleDataKey] != "new" {
		t.Fatal("current lease did not persist ConfigMap state")
	}

	stateClient.configMap.Annotations = leaseFenceValues(roleWriteLease(3, "promoted-token"))
	stateClient.configMap.Data[defaultRoleDataKey] = "promoted"
	if err := reconciler.syncRoleStateConfigMap(context.Background(), "router-ns", cr, "role-state", []byte("stale"), lease); !errors.Is(err, leadership.ErrStaleLease) {
		t.Fatalf("stale ConfigMap write error = %v, want ErrStaleLease", err)
	}
	if stateClient.configMap.Data[defaultRoleDataKey] != "promoted" {
		t.Fatal("stale lease changed ConfigMap state")
	}
}

func TestRouterStatusUsesFencedStatusPatch(t *testing.T) {
	lease := roleWriteLease(2, "current-token")
	router := &v1alpha1.CubestoreRouter{ObjectMeta: metav1.ObjectMeta{
		Name: "router", Namespace: "router-ns", ResourceVersion: "1", Annotations: leaseFenceValues(lease),
	}}
	statusClient := &statusWriteClient{router: router}
	reconciler := &CubestoreRouterReconciler{Client: statusClient, leaseStore: &controllerLeaseStore{current: lease}}
	wanted := v1alpha1.CubestoreRouterStatus{Leader: "router-0", LeaderEpoch: lease.Epoch}

	if err := reconciler.updateRouterStatusWithFence(context.Background(), router, wanted, lease); err != nil {
		t.Fatal(err)
	}
	if statusClient.statusPatchCount != 1 || statusClient.router.Status.Leader != "router-0" {
		t.Fatalf("status patch count/status = (%d, %#v), want one fenced patch", statusClient.statusPatchCount, statusClient.router.Status)
	}
}

func newExternalLeaseTestReconciler(t *testing.T, data map[string][]byte) *CubestoreRouterReconciler {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "lease-store", Namespace: "router-ns"},
		Data:       data,
	}
	return &CubestoreRouterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build(),
	}
}

func externalLeaseTestRouter() *v1alpha1.CubestoreRouter {
	return externalLeaseTestRouterWithType("redis")
}

func externalLeaseTestRouterWithType(stateStoreType string) *v1alpha1.CubestoreRouter {
	return &v1alpha1.CubestoreRouter{
		ObjectMeta: metav1.ObjectMeta{Name: "router", Namespace: "router-ns"},
		Spec: v1alpha1.CubestoreRouterSpec{
			StateStore: &v1alpha1.StateStore{
				Type:      stateStoreType,
				SecretRef: corev1.SecretReference{Name: "lease-store", Namespace: "router-ns"},
			},
		},
	}
}

func TestExternalLeaseConfigAddsSecretPasswordToRedisDSN(t *testing.T) {
	reconciler := newExternalLeaseTestReconciler(t, map[string][]byte{
		"dsn":      []byte("redis://127.0.0.1:6379/0"),
		"password": []byte("secret"),
	})

	_, dsn, _, _, err := reconciler.externalLeaseConfig(context.Background(), externalLeaseTestRouter())
	if err != nil {
		t.Fatal(err)
	}
	if dsn != "redis://:secret@127.0.0.1:6379/0" {
		t.Fatalf("Redis DSN did not include Secret password")
	}
}

func TestExternalLeaseConfigPreservesRedisDSNAuthentication(t *testing.T) {
	const dsn = "rediss://user:embedded@redis.example:6380/0"
	reconciler := newExternalLeaseTestReconciler(t, map[string][]byte{
		"dsn":      []byte(dsn),
		"password": []byte("ignored"),
	})

	_, got, _, _, err := reconciler.externalLeaseConfig(context.Background(), externalLeaseTestRouter())
	if err != nil {
		t.Fatal(err)
	}
	if got != dsn {
		t.Fatalf("Redis DSN with built-in authentication was changed")
	}
}

func TestExternalLeaseConfigSupportsKubernetesBackendWithoutSecretReference(t *testing.T) {
	reconciler := newExternalLeaseTestReconciler(t, map[string][]byte{
		"dsn": []byte("ignored-when-kubernetes"),
	})

	backend, dsn, _, _, err := reconciler.externalLeaseConfig(context.Background(), externalLeaseTestRouterWithType("kubernetes"))
	if err != nil {
		t.Fatal(err)
	}
	if backend != leaderStateBackendKubernetes {
		t.Fatalf("backend = %q, want %q", backend, leaderStateBackendKubernetes)
	}
	if dsn != "" {
		t.Fatalf("dsn = %q, want empty", dsn)
	}
}
