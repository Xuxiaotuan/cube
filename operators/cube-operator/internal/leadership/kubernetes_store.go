package leadership

import (
	"context"
	"fmt"
	"strings"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	kmacErrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	leaseClusterAnnotation    = "cubejs.io/lease-cluster-id"
	leaseTokenAnnotation      = "cubejs.io/lease-token"
	leaseGenerationAnnotation = "cubejs.io/lease-generation"
	leaseHolderUIDAnnotation  = "cubejs.io/lease-holder-uid"
	leaseDefaultGeneration    = "1"
)

type kubernetesClock interface {
	Now() time.Time
}

type wallClock struct{}

func (wallClock) Now() time.Time { return time.Now() }

type KubernetesStore struct {
	client client.Client
	reader client.Reader
	// BeforeCreate must reserve bootstrap before a missing Lease is created.
	BeforeCreate func(context.Context) error
	namespace    string
	leaseName    string
	clock        kubernetesClock
}

func NewKubernetesStore(c client.Client, namespace, leaseName string, clock kubernetesClock) *KubernetesStore {
	if clock == nil {
		clock = wallClock{}
	}
	return &KubernetesStore{client: c, reader: c, namespace: namespace, leaseName: leaseName, clock: clock}
}

// NewKubernetesStoreWithReader separates authoritative reads from cached writes.
// The reader must be a direct API reader, never an informer-backed client.
func NewKubernetesStoreWithReader(c client.Client, reader client.Reader, namespace, leaseName string) *KubernetesStore {
	s := NewKubernetesStore(c, namespace, leaseName, nil)
	s.reader = reader
	return s
}

func (s *KubernetesStore) Acquire(ctx context.Context, clusterID, holderID string, ttl time.Duration) (LeaseRecord, bool, error) {
	if err := validateLeaseRequest(clusterID, holderID, ttl); err != nil {
		return LeaseRecord{}, false, err
	}

	for i := 0; i < 8; i++ {
		current, leaseObj, err := s.getLeaseObject(ctx, clusterID)
		if kmacErrors.IsNotFound(err) {
			if s.BeforeCreate != nil {
				if err := s.BeforeCreate(ctx); err != nil {
					return LeaseRecord{}, false, err
				}
			}
			lease, err := s.newLeaseRecord(clusterID, holderID, leaseDefaultGeneration, 1, ttl)
			if err != nil {
				return LeaseRecord{}, false, err
			}
			if err := s.client.Create(ctx, buildLeaseObject(s.namespace, s.leaseName, lease)); err != nil {
				if kmacErrors.IsAlreadyExists(err) {
					continue
				}
				return LeaseRecord{}, false, err
			}
			return lease, true, nil
		}
		if err != nil {
			if kmacErrors.IsConflict(err) {
				continue
			}
			return LeaseRecord{}, false, err
		}
		if !s.isExpired(current) {
			return current, false, nil
		}

		token, err := newLeaseToken()
		if err != nil {
			return LeaseRecord{}, false, err
		}

		nextEpoch := current.Epoch + 1
		nextIssued := s.now().UTC()
		next := LeaseRecord{
			ClusterID:  clusterID,
			HolderID:   holderID,
			HolderUID:  current.HolderUID,
			Epoch:      nextEpoch,
			Generation: current.Generation,
			Token:      token,
			IssuedAt:   nextIssued,
			ExpiresAt:  nextIssued.Add(ttl),
		}

		leaseObj.Spec.HolderIdentity = stringPtr(holderID)
		leaseObj.Spec.LeaseTransitions = int32Ptr(int32(nextEpoch))
		leaseObj.Spec.AcquireTime = &metav1.MicroTime{Time: next.IssuedAt}
		leaseObj.Spec.RenewTime = &metav1.MicroTime{Time: next.IssuedAt}
		durationSeconds := int32(ttl.Seconds())
		if durationSeconds <= 0 {
			durationSeconds = 1
		}
		leaseObj.Spec.LeaseDurationSeconds = &durationSeconds
		leaseObj.Annotations = mergeLeaseAnnotations(leaseObj.Annotations, next)

		if err := s.client.Update(ctx, leaseObj); err != nil {
			if kmacErrors.IsConflict(err) {
				continue
			}
			return LeaseRecord{}, false, err
		}
		return next, true, nil
	}

	return LeaseRecord{}, false, fmt.Errorf("acquire retried without progress")
}

func (s *KubernetesStore) Renew(ctx context.Context, lease LeaseRecord, ttl time.Duration) (LeaseRecord, bool, error) {
	if err := validateLeaseRequest(lease.ClusterID, lease.HolderID, ttl); err != nil {
		return LeaseRecord{}, false, err
	}
	if err := validateLeaseIdentity(lease); err != nil {
		return LeaseRecord{}, false, err
	}

	for i := 0; i < 8; i++ {
		current, leaseObj, err := s.getLeaseObject(ctx, lease.ClusterID)
		if err != nil {
			if kmacErrors.IsConflict(err) {
				continue
			}
			if kmacErrors.IsNotFound(err) {
				return LeaseRecord{}, false, ErrLeaseNotFound
			}
			return LeaseRecord{}, false, err
		}

		if err := ValidateLeaseFence(current, lease); err != nil {
			return current, false, nil
		}
		if s.isExpired(current) {
			return current, false, nil
		}

		nextIssued := s.now().UTC()
		next := current
		next.IssuedAt = nextIssued
		next.ExpiresAt = nextIssued.Add(ttl)

		leaseObj.Spec.RenewTime = &metav1.MicroTime{Time: next.IssuedAt}
		durationSeconds := int32(ttl.Seconds())
		if durationSeconds <= 0 {
			durationSeconds = 1
		}
		leaseObj.Spec.LeaseDurationSeconds = &durationSeconds
		leaseObj.Annotations = mergeLeaseAnnotations(leaseObj.Annotations, next)

		if err := s.client.Update(ctx, leaseObj); err != nil {
			if kmacErrors.IsConflict(err) {
				continue
			}
			return LeaseRecord{}, false, err
		}

		return next, true, nil
	}

	return LeaseRecord{}, false, fmt.Errorf("renew retried without progress")
}

func (s *KubernetesStore) Release(ctx context.Context, lease LeaseRecord) error {
	if err := validateLeaseIdentity(lease); err != nil {
		return err
	}

	for i := 0; i < 8; i++ {
		current, leaseObj, err := s.getLeaseObject(ctx, lease.ClusterID)
		if err != nil {
			if kmacErrors.IsConflict(err) {
				continue
			}
			return ErrStaleLease
		}

		if err := ValidateLeaseFence(current, lease); err != nil {
			return ErrStaleLease
		}
		if s.isExpired(current) {
			return ErrStaleLease
		}

		if err := s.client.Delete(ctx, leaseObj); err != nil {
			if kmacErrors.IsConflict(err) {
				continue
			}
			if kmacErrors.IsNotFound(err) {
				return ErrStaleLease
			}
			return err
		}
		return nil
	}

	return fmt.Errorf("release retried without progress")
}

func (s *KubernetesStore) Get(ctx context.Context, clusterID string) (LeaseRecord, error) {
	if strings.TrimSpace(clusterID) == "" {
		return LeaseRecord{}, fmt.Errorf("cluster ID is required")
	}

	record, _, err := s.getLeaseObject(ctx, clusterID)
	if err != nil {
		if kmacErrors.IsNotFound(err) {
			return LeaseRecord{}, ErrLeaseNotFound
		}
		if kmacErrors.IsConflict(err) {
			return LeaseRecord{}, err
		}
		return LeaseRecord{}, err
	}

	if s.isExpired(record) {
		return LeaseRecord{}, ErrLeaseNotFound
	}
	return record, nil
}

func (s *KubernetesStore) getLeaseObject(ctx context.Context, clusterID string) (LeaseRecord, *coordinationv1.Lease, error) {
	if s.reader == nil {
		return LeaseRecord{}, nil, fmt.Errorf("authoritative Lease reader is required")
	}
	var lease coordinationv1.Lease
	if err := s.reader.Get(ctx, client.ObjectKey{Namespace: s.namespace, Name: s.leaseName}, &lease); err != nil {
		return LeaseRecord{}, nil, err
	}
	record, err := parseLeaseObject(&lease, clusterID)
	if err != nil {
		return LeaseRecord{}, nil, err
	}
	return record, &lease, nil
}

func (s *KubernetesStore) isExpired(record LeaseRecord) bool {
	if record.ExpiresAt.IsZero() {
		return true
	}
	return !s.now().UTC().Before(record.ExpiresAt)
}

func (s *KubernetesStore) now() time.Time { return s.clock.Now() }

func (s *KubernetesStore) newLeaseRecord(clusterID, holderID, generation string, epoch int64, ttl time.Duration) (LeaseRecord, error) {
	token, err := newLeaseToken()
	if err != nil {
		return LeaseRecord{}, err
	}
	generation = strings.TrimSpace(generation)
	if generation == "" {
		generation = leaseDefaultGeneration
	}
	expires := s.now().UTC().Add(ttl)
	return LeaseRecord{
		ClusterID:  clusterID,
		HolderID:   holderID,
		Epoch:      epoch,
		Generation: strings.TrimSpace(generation),
		Token:      token,
		IssuedAt:   s.now().UTC(),
		ExpiresAt:  expires,
	}, nil
}

func buildLeaseObject(namespace, leaseName string, lease LeaseRecord) *coordinationv1.Lease {
	durationSeconds := int32(lease.ExpiresAt.Sub(lease.IssuedAt).Seconds())
	if durationSeconds <= 0 {
		durationSeconds = 1
	}
	obj := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      leaseName,
			Namespace: namespace,
			Annotations: map[string]string{
				leaseClusterAnnotation:    lease.ClusterID,
				leaseGenerationAnnotation: lease.Generation,
				leaseTokenAnnotation:      lease.Token,
				leaseHolderUIDAnnotation:  lease.HolderUID,
			},
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       stringPtr(lease.HolderID),
			LeaseTransitions:     int32Ptr(int32(lease.Epoch)),
			AcquireTime:          &metav1.MicroTime{Time: lease.IssuedAt},
			RenewTime:            &metav1.MicroTime{Time: lease.IssuedAt},
			LeaseDurationSeconds: &durationSeconds,
		},
	}
	obj.Annotations = mergeLeaseAnnotations(obj.Annotations, lease)
	return obj
}

func parseLeaseObject(leaseObj *coordinationv1.Lease, expectedCluster string) (LeaseRecord, error) {
	annotations := leaseObj.GetAnnotations()
	if annotations == nil {
		return LeaseRecord{}, ErrLeaseUnknown
	}

	holderID := ""
	if leaseObj.Spec.HolderIdentity != nil {
		holderID = strings.TrimSpace(*leaseObj.Spec.HolderIdentity)
	}
	if holderID == "" {
		return LeaseRecord{}, ErrLeaseUnknown
	}
	if strings.TrimSpace(annotations[leaseClusterAnnotation]) != strings.TrimSpace(expectedCluster) {
		return LeaseRecord{}, ErrLeaseUnknown
	}

	if leaseObj.Spec.LeaseTransitions == nil || *leaseObj.Spec.LeaseTransitions <= 0 {
		return LeaseRecord{}, ErrLeaseUnknown
	}
	ttl := leaseObj.Spec.LeaseDurationSeconds
	if ttl == nil || *ttl <= 0 {
		return LeaseRecord{}, ErrLeaseUnknown
	}
	issuedAt := leaseObj.Spec.AcquireTime
	if issuedAt == nil {
		return LeaseRecord{}, ErrLeaseUnknown
	}
	rawToken := strings.TrimSpace(annotations[leaseTokenAnnotation])
	if rawToken == "" {
		return LeaseRecord{}, ErrLeaseUnknown
	}

	record := LeaseRecord{
		ClusterID:  strings.TrimSpace(expectedCluster),
		HolderID:   holderID,
		HolderUID:  strings.TrimSpace(annotations[leaseHolderUIDAnnotation]),
		Epoch:      int64(*leaseObj.Spec.LeaseTransitions),
		Generation: strings.TrimSpace(annotations[leaseGenerationAnnotation]),
		Token:      rawToken,
		IssuedAt:   issuedAt.Time,
		ExpiresAt:  issuedAt.Time.Add(time.Duration(*ttl) * time.Second),
	}
	if record.Generation == "" {
		record.Generation = leaseDefaultGeneration
	}

	if leaseObj.Spec.RenewTime != nil {
		record.ExpiresAt = leaseObj.Spec.RenewTime.Time.Add(time.Duration(*ttl) * time.Second)
	}

	return record, nil
}

func mergeLeaseAnnotations(base map[string]string, record LeaseRecord) map[string]string {
	next := map[string]string{}
	for k, v := range base {
		next[k] = v
	}
	next[leaseTokenAnnotation] = strings.TrimSpace(record.Token)
	next[leaseGenerationAnnotation] = strings.TrimSpace(record.Generation)
	next[leaseClusterAnnotation] = strings.TrimSpace(record.ClusterID)
	next[leaseHolderUIDAnnotation] = strings.TrimSpace(record.HolderUID)
	if next[leaseGenerationAnnotation] == "" {
		next[leaseGenerationAnnotation] = leaseDefaultGeneration
	}
	return next
}

func stringPtr(v string) *string {
	if v == "" {
		return nil
	}
	out := v
	return &out
}

func int32Ptr(v int32) *int32 {
	out := v
	return &out
}
