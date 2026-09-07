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
	// Presence requests strict authority for a fresh installation only.
	Authority    *CubeAuthoritySpec        `json:"authority,omitempty"`
	Images       CubeClusterImages         `json:"images"`
	API          CubeComponentSpec         `json:"api,omitempty"`
	Router       CubeRouterClusterSpec     `json:"router,omitempty"`
	MetaStore    CubeComponentSpec         `json:"metaStore,omitempty"`
	Workers      CubeComponentSpec         `json:"workers,omitempty"`
	Storage      CubeStorageSpec           `json:"storage"`
	APISecretRef *corev1.SecretKeySelector `json:"apiSecretRef,omitempty"`
	// Presence enables a singleton scheduled-refresh process, not refresher HA.
	Refresher *CubeComponentSpec `json:"refresher,omitempty"`
}

// Budgets must be supplied from the target environment, not inferred by the operator.
type CubeAuthoritySpec struct {
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=2147483647
	APITimeoutMS int64 `json:"apiTimeoutMs"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=2147483647
	ValidationTimeoutMS int64 `json:"validationTimeoutMs"`
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=2147483647
	MaxClockSkewMS *int64 `json:"maxClockSkewMs"`
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
	Replicas                     int32       `json:"replicas,omitempty"`
	Pod                          CubePodSpec `json:"pod,omitempty"`
	AllowSingleReplicaDisruption bool        `json:"allowSingleReplicaDisruption,omitempty"`
}

// CubePodSpec exposes native Kubernetes configuration without permitting a
// second container to replace the operator's topology or fencing contract.
type CubePodSpec struct {
	Env                           []corev1.EnvVar                   `json:"env,omitempty"`
	EnvFrom                       []corev1.EnvFromSource            `json:"envFrom,omitempty"`
	Volumes                       []corev1.Volume                   `json:"volumes,omitempty"`
	VolumeMounts                  []corev1.VolumeMount              `json:"volumeMounts,omitempty"`
	Resources                     corev1.ResourceRequirements       `json:"resources,omitempty"`
	NodeSelector                  map[string]string                 `json:"nodeSelector,omitempty"`
	Affinity                      *corev1.Affinity                  `json:"affinity,omitempty"`
	Tolerations                   []corev1.Toleration               `json:"tolerations,omitempty"`
	TopologySpreadConstraints     []corev1.TopologySpreadConstraint `json:"topologySpreadConstraints,omitempty"`
	ImagePullSecrets              []corev1.LocalObjectReference     `json:"imagePullSecrets,omitempty"`
	StartupProbe                  *corev1.Probe                     `json:"startupProbe,omitempty"`
	ReadinessProbe                *corev1.Probe                     `json:"readinessProbe,omitempty"`
	LivenessProbe                 *corev1.Probe                     `json:"livenessProbe,omitempty"`
	TerminationGracePeriodSeconds *int64                            `json:"terminationGracePeriodSeconds,omitempty"`
}

type CubeRouterClusterSpec struct {
	// +kubebuilder:validation:Minimum=1
	Replicas int32 `json:"replicas,omitempty"`
	// +kubebuilder:default:=true
	HighAvailability             bool        `json:"highAvailability,omitempty"`
	Pod                          CubePodSpec `json:"pod,omitempty"`
	DrainCommand                 []string    `json:"drainCommand,omitempty"`
	AllowSingleReplicaDisruption bool        `json:"allowSingleReplicaDisruption,omitempty"`
}

type CubeStorageSpec struct {
	// +kubebuilder:validation:MinLength=1
	Endpoint string `json:"endpoint"`
	// +kubebuilder:validation:MinLength=1
	Bucket               string                  `json:"bucket"`
	SubPath              string                  `json:"subPath,omitempty"`
	DataSize             string                  `json:"dataSize,omitempty"`
	StorageClassName     *string                 `json:"storageClassName,omitempty"`
	ObjectStoreSecretRef *corev1.SecretReference `json:"objectStoreSecretRef,omitempty"`
}

type CubeClusterStatus struct {
	Phase              string                         `json:"phase,omitempty"`
	ReadyReplicas      int32                          `json:"readyReplicas,omitempty"`
	Conditions         []metav1.Condition             `json:"conditions,omitempty"`
	ObservedGeneration int64                          `json:"observedGeneration,omitempty"`
	Components         map[string]CubeComponentStatus `json:"components,omitempty"`
	Recovery           RouterRecoveryStatus           `json:"recovery,omitempty"`
}

type CubeComponentStatus struct {
	DesiredReplicas    int32                  `json:"desiredReplicas"`
	ReadyReplicas      int32                  `json:"readyReplicas"`
	UpdatedReplicas    int32                  `json:"updatedReplicas"`
	WorkloadGeneration int64                  `json:"workloadGeneration"`
	ObservedGeneration int64                  `json:"observedGeneration"`
	Ready              bool                   `json:"ready"`
	ApplicationReady   metav1.ConditionStatus `json:"applicationReady"`
}

func (in *CubePodSpec) DeepCopy() *CubePodSpec {
	if in == nil {
		return nil
	}
	out := *in
	// Kubernetes deepcopy routines cover all nested selectors, projections and probes.
	pod := corev1.PodSpec{Containers: []corev1.Container{{Env: in.Env, EnvFrom: in.EnvFrom, VolumeMounts: in.VolumeMounts, Resources: in.Resources, StartupProbe: in.StartupProbe, ReadinessProbe: in.ReadinessProbe, LivenessProbe: in.LivenessProbe}}, Volumes: in.Volumes, NodeSelector: in.NodeSelector, Affinity: in.Affinity, Tolerations: in.Tolerations, TopologySpreadConstraints: in.TopologySpreadConstraints, ImagePullSecrets: in.ImagePullSecrets, TerminationGracePeriodSeconds: in.TerminationGracePeriodSeconds}
	cp := pod.DeepCopy()
	container := cp.Containers[0]
	out.Env, out.EnvFrom, out.VolumeMounts, out.Resources = container.Env, container.EnvFrom, container.VolumeMounts, container.Resources
	out.StartupProbe, out.ReadinessProbe, out.LivenessProbe = container.StartupProbe, container.ReadinessProbe, container.LivenessProbe
	out.Volumes, out.NodeSelector, out.Affinity, out.Tolerations = cp.Volumes, cp.NodeSelector, cp.Affinity, cp.Tolerations
	out.TopologySpreadConstraints, out.ImagePullSecrets, out.TerminationGracePeriodSeconds = cp.TopologySpreadConstraints, cp.ImagePullSecrets, cp.TerminationGracePeriodSeconds
	return &out
}

func (in *CubeCluster) DeepCopyInto(out *CubeCluster) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	out.ObjectMeta = *in.ObjectMeta.DeepCopy()
	out.Spec = in.Spec
	if in.Spec.Authority != nil {
		a := *in.Spec.Authority
		if a.MaxClockSkewMS != nil {
			n := *a.MaxClockSkewMS
			a.MaxClockSkewMS = &n
		}
		out.Spec.Authority = &a
	}
	out.Spec.API.Pod = *in.Spec.API.Pod.DeepCopy()
	out.Spec.Router.Pod = *in.Spec.Router.Pod.DeepCopy()
	out.Spec.Router.DrainCommand = append([]string(nil), in.Spec.Router.DrainCommand...)
	out.Spec.MetaStore.Pod = *in.Spec.MetaStore.Pod.DeepCopy()
	out.Spec.Workers.Pod = *in.Spec.Workers.Pod.DeepCopy()
	if in.Spec.Refresher != nil {
		ref := *in.Spec.Refresher
		ref.Pod = *in.Spec.Refresher.Pod.DeepCopy()
		out.Spec.Refresher = &ref
	}
	if in.Spec.APISecretRef != nil {
		out.Spec.APISecretRef = in.Spec.APISecretRef.DeepCopy()
	}
	if in.Spec.Storage.StorageClassName != nil {
		value := *in.Spec.Storage.StorageClassName
		out.Spec.Storage.StorageClassName = &value
	}
	if in.Spec.Storage.ObjectStoreSecretRef != nil {
		ref := *in.Spec.Storage.ObjectStoreSecretRef
		out.Spec.Storage.ObjectStoreSecretRef = &ref
	}
	out.Status = in.Status
	if in.Status.Components != nil {
		out.Status.Components = make(map[string]CubeComponentStatus, len(in.Status.Components))
		for k, v := range in.Status.Components {
			out.Status.Components[k] = v
		}
	}
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
