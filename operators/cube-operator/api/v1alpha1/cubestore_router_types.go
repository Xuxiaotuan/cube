package v1alpha1

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

const (
	DefaultLeaseDurationSeconds int32 = 30
	DefaultRenewDeadlineSeconds int32 = 20
	DefaultRetryPeriodSeconds   int32 = 5
	ElectionStrategyLease             = "lease"

	CubestoreRouterConditionLeaseAcquired     = "LeaseAcquired"
	CubestoreRouterConditionLeaderReady       = "LeaderReady"
	CubestoreRouterConditionMetaStoreReady    = "MetaStoreReady"
	CubestoreRouterConditionDataPlaneReady    = "DataPlaneReady"
	CubestoreRouterConditionDegraded          = "Degraded"
	CubestoreRouterConditionPromotionReady    = "PromotionReady"
	CubestoreRouterConditionJobRecovery       = "JobRecovery"
	CubestoreRouterConditionMutationReconcile = "MutationReconcile"
	CubestoreRouterConditionRefresherReady    = "RefresherReady"

	RecoveryStateReady        = "Ready"
	RecoveryStateBlocked      = "Blocked"
	RecoveryStateNeedsContext = "NeedsContext"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:validation:XValidation:rule="has(self.spec.stateStore) != has(self.spec.leaderStateStore)",message="exactly one of stateStore or the deprecated leaderStateStore migration field must be configured"
type CubestoreRouter struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   CubestoreRouterSpec   `json:"spec,omitempty"`
	Status CubestoreRouterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type CubestoreRouterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []CubestoreRouter `json:"items"`
}

type CubestoreRouterSpec struct {
	// +kubebuilder:validation:MinProperties=1
	Selector  map[string]string `json:"selector"`
	Namespace string            `json:"namespace"`

	RouterPort int32  `json:"routerPort,omitempty"`
	HealthPath string `json:"healthPath,omitempty"`

	// +kubebuilder:default:=lease
	// +kubebuilder:validation:Enum=lease
	ElectionStrategy string `json:"electionStrategy,omitempty"`
	RoleConfigMap    string `json:"roleConfigMapName,omitempty"`

	// +kubebuilder:default:=30
	// +kubebuilder:validation:Minimum=1
	LeaseDurationSeconds int32 `json:"leaseDurationSeconds,omitempty"`
	// +kubebuilder:default:=20
	// +kubebuilder:validation:Minimum=1
	RenewDeadlineSeconds int32 `json:"renewDeadlineSeconds,omitempty"`
	// +kubebuilder:default:=5
	// +kubebuilder:validation:Minimum=1
	RetryPeriodSeconds int32 `json:"retryPeriodSeconds,omitempty"`

	StateStore *StateStore `json:"stateStore,omitempty"`
	MetaStore  MetaStore   `json:"metaStore"`
	Storage    Storage     `json:"storage"`

	// LeaderStateStore is a deprecated v1alpha1 migration-only field. Move its
	// secretRef and backend settings to stateStore before updating the resource.
	// Plaintext DSNs are intentionally unsupported and are never accepted.
	LeaderStateStore *LeaderStateStore `json:"leaderStateStore,omitempty"`
}

type StateStore struct {
	// +kubebuilder:validation:Enum=redis;postgres;kubernetes
	Type string `json:"type"`

	SecretRef *corev1.SecretReference `json:"secretRef,omitempty"`
}

type MetaStore struct {
	// +kubebuilder:validation:MinLength=1
	Address string `json:"address"`
}

type Storage struct {
	// +kubebuilder:validation:MinLength=1
	DataPVC string `json:"dataPVC"`

	ObjectStoreSecretRef *corev1.SecretReference `json:"objectStoreSecretRef,omitempty"`
}

type CubestoreRouterStatus struct {
	Leader         string                  `json:"leader,omitempty"`
	LeaderRole     string                  `json:"leaderRole,omitempty"`
	LeaderIP       string                  `json:"leaderIP,omitempty"`
	LeaderEpoch    int64                   `json:"leaderEpoch"`
	LastSwitchedAt *metav1.Time            `json:"lastSwitchedAt,omitempty"`
	Candidates     []RouterCandidateStatus `json:"candidates,omitempty"`
	Conditions     []metav1.Condition      `json:"conditions,omitempty"`
	Recovery       RouterRecoveryStatus    `json:"recovery,omitempty"`
}

// RecoveryGateStatus is explicit about capabilities that are not wired to a
// runtime entry point yet. The controller must not infer readiness from CR
// status alone.
type RecoveryGateStatus struct {
	State   string `json:"state"`
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
}

type RouterRecoveryStatus struct {
	Promotion         RecoveryGateStatus `json:"promotion"`
	JobRecovery       RecoveryGateStatus `json:"jobRecovery"`
	MutationReconcile RecoveryGateStatus `json:"mutationReconcile"`
	Refresher         RecoveryGateStatus `json:"refresher"`
}

type RouterCandidateStatus struct {
	Name          string       `json:"name"`
	Namespace     string       `json:"namespace"`
	Role          string       `json:"role"`
	IP            string       `json:"ip"`
	Ready         bool         `json:"ready"`
	LastProbeTime *metav1.Time `json:"lastProbeTime,omitempty"`
}

type LeaderStateStore struct {
	Type string `json:"type"`
	// DSN is retained only so existing v1alpha1 objects can be read and
	// migrated. New objects must use SecretRef on stateStore.
	DSN       string                  `json:"dsn,omitempty"`
	SecretRef *corev1.SecretReference `json:"secretRef,omitempty"`
	RedisKey  string                  `json:"redisKey,omitempty"`
	PGTable   string                  `json:"pgTable,omitempty"`
}

// ApplyDefaults applies the CRD defaults for callers that construct router
// specs in-process, such as tests and migration tooling.
func (in *CubestoreRouterSpec) ApplyDefaults() {
	if in.ElectionStrategy == "" {
		in.ElectionStrategy = ElectionStrategyLease
	}
	if in.LeaseDurationSeconds == 0 {
		in.LeaseDurationSeconds = DefaultLeaseDurationSeconds
	}
	if in.RenewDeadlineSeconds == 0 {
		in.RenewDeadlineSeconds = DefaultRenewDeadlineSeconds
	}
	if in.RetryPeriodSeconds == 0 {
		in.RetryPeriodSeconds = DefaultRetryPeriodSeconds
	}
}

// Validate mirrors the CRD admission constraints for clients that need to
// validate a router spec before submitting it to the API server. Defaults are
// applied before validation so callers cannot accidentally validate an
// un-defaulted object.
func (in *CubestoreRouterSpec) Validate() error {
	in.ApplyDefaults()
	if len(in.Selector) == 0 {
		return fmt.Errorf("selector must not be empty")
	}
	if in.Namespace == "" {
		return fmt.Errorf("spec.namespace must not be empty")
	}
	if in.ElectionStrategy != ElectionStrategyLease {
		return fmt.Errorf("electionStrategy must be %q", ElectionStrategyLease)
	}
	if in.LeaseDurationSeconds <= 0 || in.RenewDeadlineSeconds <= 0 || in.RetryPeriodSeconds <= 0 {
		return fmt.Errorf("lease duration, renew deadline, and retry period must be positive")
	}
	if in.RenewDeadlineSeconds >= in.LeaseDurationSeconds {
		return fmt.Errorf("renewDeadlineSeconds must be less than leaseDurationSeconds")
	}
	if (in.StateStore == nil) == (in.LeaderStateStore == nil) {
		return fmt.Errorf("exactly one of stateStore or deprecated leaderStateStore must be configured")
	}
	if in.StateStore != nil {
		if err := validateBackendType("stateStore.type", in.StateStore.Type); err != nil {
			return err
		}
		if strings.EqualFold(strings.TrimSpace(in.StateStore.Type), "kubernetes") {
			return nil
		}
		if in.StateStore.SecretRef == nil {
			return fmt.Errorf("stateStore.secretRef is required for %s", in.StateStore.Type)
		}
		if err := validateSecretReference("stateStore.secretRef", *in.StateStore.SecretRef); err != nil {
			return err
		}
	}
	if in.LeaderStateStore != nil {
		if in.LeaderStateStore.Type != "" {
			if err := validateBackendType("leaderStateStore.type", in.LeaderStateStore.Type); err != nil {
				return err
			}
		}
		if in.LeaderStateStore.DSN != "" && in.LeaderStateStore.SecretRef != nil {
			return fmt.Errorf("leaderStateStore must use either dsn or secretRef, not both")
		}
		if in.LeaderStateStore.DSN == "" && in.LeaderStateStore.SecretRef == nil {
			return fmt.Errorf("leaderStateStore migration field must include dsn or secretRef")
		}
		if in.LeaderStateStore.SecretRef != nil {
			if err := validateSecretReference("leaderStateStore.secretRef", *in.LeaderStateStore.SecretRef); err != nil {
				return err
			}
		}
	}
	if in.MetaStore.Address == "" {
		return fmt.Errorf("metaStore.address must not be empty")
	}
	if in.Storage.DataPVC == "" {
		return fmt.Errorf("storage.dataPVC must not be empty")
	}
	if in.Storage.ObjectStoreSecretRef != nil {
		if err := validateSecretReference("storage.objectStoreSecretRef", *in.Storage.ObjectStoreSecretRef); err != nil {
			return err
		}
	}
	return nil
}

func validateBackendType(field, backendType string) error {
	if backendType != "redis" && backendType != "postgres" && backendType != "kubernetes" {
		return fmt.Errorf("%s must be redis, kubernetes or postgres", field)
	}
	return nil
}

func validateSecretReference(field string, ref corev1.SecretReference) error {
	if ref.Name == "" || ref.Namespace == "" {
		return fmt.Errorf("%s must include name and namespace", field)
	}
	return nil
}

func (in *CubestoreRouter) DeepCopyInto(out *CubestoreRouter) {
	*out = *in
	out.ObjectMeta = *in.ObjectMeta.DeepCopy()
	out.TypeMeta = in.TypeMeta

	out.Spec = CubestoreRouterSpec{
		Selector:             in.Spec.Selector,
		Namespace:            in.Spec.Namespace,
		RouterPort:           in.Spec.RouterPort,
		HealthPath:           in.Spec.HealthPath,
		ElectionStrategy:     in.Spec.ElectionStrategy,
		RoleConfigMap:        in.Spec.RoleConfigMap,
		LeaseDurationSeconds: in.Spec.LeaseDurationSeconds,
		RenewDeadlineSeconds: in.Spec.RenewDeadlineSeconds,
		RetryPeriodSeconds:   in.Spec.RetryPeriodSeconds,
		MetaStore:            in.Spec.MetaStore,
		Storage:              in.Spec.Storage,
		StateStore:           nil,
		LeaderStateStore:     nil,
	}
	if in.Spec.StateStore != nil {
		store := *in.Spec.StateStore
		if in.Spec.StateStore.SecretRef != nil {
			secretRef := *in.Spec.StateStore.SecretRef
			store.SecretRef = &secretRef
		}
		out.Spec.StateStore = &store
	}
	if in.Spec.LeaderStateStore != nil {
		ls := *in.Spec.LeaderStateStore
		if in.Spec.LeaderStateStore.SecretRef != nil {
			secretRef := *in.Spec.LeaderStateStore.SecretRef
			ls.SecretRef = &secretRef
		}
		out.Spec.LeaderStateStore = &ls
	}
	if in.Spec.Storage.ObjectStoreSecretRef != nil {
		secretRef := *in.Spec.Storage.ObjectStoreSecretRef
		out.Spec.Storage.ObjectStoreSecretRef = &secretRef
	}
	if in.Spec.Selector != nil {
		out.Spec.Selector = make(map[string]string, len(in.Spec.Selector))
		for k, v := range in.Spec.Selector {
			out.Spec.Selector[k] = v
		}
	}

	out.Status = CubestoreRouterStatus{
		Leader:      in.Status.Leader,
		LeaderRole:  in.Status.LeaderRole,
		LeaderIP:    in.Status.LeaderIP,
		LeaderEpoch: in.Status.LeaderEpoch,
		Recovery:    in.Status.Recovery,
	}
	if in.Status.LastSwitchedAt != nil {
		t := in.Status.LastSwitchedAt.DeepCopy()
		out.Status.LastSwitchedAt = t
	}

	if len(in.Status.Conditions) > 0 {
		out.Status.Conditions = make([]metav1.Condition, len(in.Status.Conditions))
		copy(out.Status.Conditions, in.Status.Conditions)
	}

	if len(in.Status.Candidates) > 0 {
		out.Status.Candidates = make([]RouterCandidateStatus, len(in.Status.Candidates))
		copy(out.Status.Candidates, in.Status.Candidates)
	}
}

func (in *CubestoreRouter) DeepCopy() *CubestoreRouter {
	if in == nil {
		return nil
	}
	out := new(CubestoreRouter)
	in.DeepCopyInto(out)
	return out
}

func (in *CubestoreRouter) DeepCopyObject() runtime.Object { return in.DeepCopy() }

func (in *CubestoreRouterList) DeepCopyInto(out *CubestoreRouterList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	out.ListMeta = *in.ListMeta.DeepCopy()
	if len(in.Items) > 0 {
		out.Items = make([]CubestoreRouter, len(in.Items))
		for i := range in.Items {
			in.Items[i].DeepCopyInto(&out.Items[i])
		}
	}
}

func (in *CubestoreRouterList) DeepCopy() *CubestoreRouterList {
	if in == nil {
		return nil
	}
	out := new(CubestoreRouterList)
	in.DeepCopyInto(out)
	return out
}

func (in *CubestoreRouterList) DeepCopyObject() runtime.Object { return in.DeepCopy() }
