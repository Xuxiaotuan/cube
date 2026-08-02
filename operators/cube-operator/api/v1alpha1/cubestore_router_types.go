package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
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
	Selector         map[string]string `json:"selector"`
	Namespace        string            `json:"namespace"`
	RouterPort       int32             `json:"routerPort,omitempty"`
	HealthPath       string            `json:"healthPath,omitempty"`
	ElectionStrategy string            `json:"electionStrategy,omitempty"`
	RoleConfigMap    string            `json:"roleConfigMapName,omitempty"`
	LeaderStateStore *LeaderStateStore `json:"leaderStateStore,omitempty"`
}

type CubestoreRouterStatus struct {
	Leader         string                  `json:"leader,omitempty"`
	LeaderRole     string                  `json:"leaderRole,omitempty"`
	LeaderIP       string                  `json:"leaderIP,omitempty"`
	LeaderEpoch    int64                   `json:"leaderEpoch"`
	LastSwitchedAt *metav1.Time            `json:"lastSwitchedAt,omitempty"`
	Candidates     []RouterCandidateStatus `json:"candidates,omitempty"`
	Conditions     []metav1.Condition      `json:"conditions,omitempty"`
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
	Type      string `json:"type,omitempty"`
	DSN       string `json:"dsn,omitempty"`
	RedisKey string `json:"redisKey,omitempty"`
	PGTable  string `json:"pgTable,omitempty"`
}

func (in *CubestoreRouter) DeepCopyInto(out *CubestoreRouter) {
	*out = *in
	out.ObjectMeta = *in.ObjectMeta.DeepCopy()
	out.TypeMeta = in.TypeMeta

	out.Spec = CubestoreRouterSpec{
		Selector:         in.Spec.Selector,
		Namespace:        in.Spec.Namespace,
		RouterPort:       in.Spec.RouterPort,
		HealthPath:       in.Spec.HealthPath,
		ElectionStrategy: in.Spec.ElectionStrategy,
		RoleConfigMap:    in.Spec.RoleConfigMap,
		LeaderStateStore: nil,
	}
	if in.Spec.LeaderStateStore != nil {
		ls := *in.Spec.LeaderStateStore
		out.Spec.LeaderStateStore = &ls
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
