package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type CubeCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              CubeClusterSpec   `json:"spec,omitempty"`
	Status            CubeClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type CubeClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CubeCluster `json:"items"`
}

type CubeClusterSpec struct {
	Images    CubeClusterImages     `json:"images"`
	API       CubeComponentSpec     `json:"api,omitempty"`
	Router    CubeRouterClusterSpec `json:"router,omitempty"`
	MetaStore CubeComponentSpec     `json:"metaStore,omitempty"`
	Workers   CubeComponentSpec     `json:"workers,omitempty"`
	Storage   CubeStorageSpec       `json:"storage"`
}

type CubeClusterImages struct {
	API        string `json:"api"`
	Router     string `json:"router"`
	MetaStore  string `json:"metaStore"`
	Worker     string `json:"worker"`
	LeaseAgent string `json:"leaseAgent"`
}

type CubeComponentSpec struct {
	// +kubebuilder:validation:Minimum=1
	Replicas int32 `json:"replicas,omitempty"`
}

type CubeRouterClusterSpec struct {
	// +kubebuilder:validation:Minimum=1
	Replicas int32 `json:"replicas,omitempty"`
	// +kubebuilder:default:=true
	HighAvailability bool `json:"highAvailability,omitempty"`
}

type CubeStorageSpec struct {
	// +kubebuilder:validation:MinLength=1
	Endpoint string `json:"endpoint"`
	// +kubebuilder:validation:MinLength=1
	Bucket               string                  `json:"bucket"`
	SubPath              string                  `json:"subPath,omitempty"`
	DataSize             string                  `json:"dataSize,omitempty"`
	ObjectStoreSecretRef *corev1.SecretReference `json:"objectStoreSecretRef,omitempty"`
}

type CubeClusterStatus struct {
	Phase         string             `json:"phase,omitempty"`
	ReadyReplicas int32              `json:"readyReplicas,omitempty"`
	Conditions    []metav1.Condition `json:"conditions,omitempty"`
}

func (in *CubeCluster) DeepCopyInto(out *CubeCluster) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	out.ObjectMeta = *in.ObjectMeta.DeepCopy()
	out.Spec = in.Spec
	if in.Spec.Storage.ObjectStoreSecretRef != nil {
		ref := *in.Spec.Storage.ObjectStoreSecretRef
		out.Spec.Storage.ObjectStoreSecretRef = &ref
	}
	out.Status = in.Status
	if in.Status.Conditions != nil {
		out.Status.Conditions = make([]metav1.Condition, len(in.Status.Conditions))
		copy(out.Status.Conditions, in.Status.Conditions)
	}
}

func (in *CubeCluster) DeepCopy() *CubeCluster {
	if in == nil {
		return nil
	}
	out := new(CubeCluster)
	in.DeepCopyInto(out)
	return out
}
func (in *CubeCluster) DeepCopyObject() runtime.Object { return in.DeepCopy() }

func (in *CubeClusterList) DeepCopyInto(out *CubeClusterList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	out.ListMeta = *in.ListMeta.DeepCopy()
	if in.Items != nil {
		out.Items = make([]CubeCluster, len(in.Items))
		for i := range in.Items {
			in.Items[i].DeepCopyInto(&out.Items[i])
		}
	}
}
func (in *CubeClusterList) DeepCopy() *CubeClusterList {
	if in == nil {
		return nil
	}
	out := new(CubeClusterList)
	in.DeepCopyInto(out)
	return out
}
func (in *CubeClusterList) DeepCopyObject() runtime.Object { return in.DeepCopy() }
