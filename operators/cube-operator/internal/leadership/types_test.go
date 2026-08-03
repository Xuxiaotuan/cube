package leadership

import (
	"context"
	"testing"
	"time"

	v1alpha1 "github.com/cube-js/cube-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
)

type testLeaseStore struct{}

func (testLeaseStore) Acquire(context.Context, string, string, time.Duration) (LeaseRecord, bool, error) {
	return LeaseRecord{}, false, nil
}

func (testLeaseStore) Renew(context.Context, LeaseRecord, time.Duration) (LeaseRecord, bool, error) {
	return LeaseRecord{}, false, nil
}

func (testLeaseStore) Release(context.Context, LeaseRecord) error { return nil }

func (testLeaseStore) Get(context.Context, string) (LeaseRecord, error) { return LeaseRecord{}, nil }

var _ LeaseStore = testLeaseStore{}

func TestRouterSpecApplyDefaults(t *testing.T) {
	spec := validRouterSpec()
	spec.ElectionStrategy = ""
	spec.LeaseDurationSeconds = 0
	spec.RenewDeadlineSeconds = 0
	spec.RetryPeriodSeconds = 0

	spec.ApplyDefaults()

	if spec.ElectionStrategy != v1alpha1.ElectionStrategyLease ||
		spec.LeaseDurationSeconds != v1alpha1.DefaultLeaseDurationSeconds ||
		spec.RenewDeadlineSeconds != v1alpha1.DefaultRenewDeadlineSeconds ||
		spec.RetryPeriodSeconds != v1alpha1.DefaultRetryPeriodSeconds {
		t.Fatalf("defaults were not applied: %#v", spec)
	}
}

func TestRouterSpecRejectsInvalidSelector(t *testing.T) {
	spec := validRouterSpec()
	spec.Selector = map[string]string{}

	if err := spec.Validate(); err == nil {
		t.Fatal("expected empty selector to be rejected")
	}
}

func TestRouterSpecRejectsInvalidTiming(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*v1alpha1.CubestoreRouterSpec)
	}{
		{name: "non-positive duration", mutate: func(spec *v1alpha1.CubestoreRouterSpec) { spec.LeaseDurationSeconds = 0 }},
		{name: "non-positive deadline", mutate: func(spec *v1alpha1.CubestoreRouterSpec) { spec.RenewDeadlineSeconds = -1 }},
		{name: "deadline equals duration", mutate: func(spec *v1alpha1.CubestoreRouterSpec) { spec.RenewDeadlineSeconds = spec.LeaseDurationSeconds }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := validRouterSpec()
			tt.mutate(&spec)
			if err := spec.Validate(); err == nil {
				t.Fatal("expected invalid timing to be rejected")
			}
		})
	}
}

func TestRouterSpecValidatesSecretReferences(t *testing.T) {
	t.Run("requires state store secret name", func(t *testing.T) {
		spec := validRouterSpec()
		spec.StateStore.SecretRef.Name = ""
		if err := spec.Validate(); err == nil {
			t.Fatal("expected missing state store secret name to be rejected")
		}
	})

	t.Run("requires object store secret namespace", func(t *testing.T) {
		spec := validRouterSpec()
		spec.Storage.ObjectStoreSecretRef = &corev1.SecretReference{Name: "object-store"}
		if err := spec.Validate(); err == nil {
			t.Fatal("expected missing object store secret namespace to be rejected")
		}
	})
}

func validRouterSpec() v1alpha1.CubestoreRouterSpec {
	return v1alpha1.CubestoreRouterSpec{
		Selector:             map[string]string{"app": "cubestore-router"},
		Namespace:            "router-system",
		ElectionStrategy:     v1alpha1.ElectionStrategyLease,
		LeaseDurationSeconds: 30,
		RenewDeadlineSeconds: 20,
		RetryPeriodSeconds:   5,
		StateStore: &v1alpha1.StateStore{
			Type:      "redis",
			SecretRef: corev1.SecretReference{Name: "router-state", Namespace: "router-system"},
		},
		MetaStore: v1alpha1.MetaStore{Address: "http://metastore.router-system.svc:9090"},
		Storage:   v1alpha1.Storage{DataPVC: "router-data"},
	}
}
