package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cube-js/cube-operator/internal/leadership"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
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
