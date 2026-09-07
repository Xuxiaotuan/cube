package controllers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cube-js/cube-operator/api/v1alpha1"
	"github.com/prometheus/client_golang/prometheus"
	apps "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	validation "k8s.io/apimachinery/pkg/util/validation"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

const cubeComponentRefresher = "refresher"
const configurationAnnotation = "cubestore.io/configuration-hash"

// These are observations, not business recovery or production certification.
var clusterConditionMetric = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "cube_cluster_condition", Help: "Current-generation condition: 1 true, 0 false, -1 unknown or stale; use observation timestamp for freshness.",
}, []string{"namespace", "cluster", "condition"})
var clusterObservationMetric = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "cube_cluster_observation_timestamp_seconds", Help: "Time of the last successful full cluster status observation, not reconcile activity.",
}, []string{"namespace", "cluster"})
var clusterReplicaMetric = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "cube_cluster_component_replicas", Help: "Observed desired/ready/updated replicas; -1 means no complete current-generation observation.",
}, []string{"namespace", "cluster", "component", "state"})
var clusterGenerationLagMetric = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "cube_cluster_component_generation_lag", Help: "Workload generation minus observed generation, clamped to zero; -1 means unknown. Not elapsed time.",
}, []string{"namespace", "cluster", "component"})

func init() {
	metrics.Registry.MustRegister(clusterConditionMetric, clusterObservationMetric, clusterReplicaMetric, clusterGenerationLagMetric)
}

func observeClusterConditions(c *v1alpha1.CubeCluster, complete bool) {
	observeClusterComponents(c, complete)
	for _, name := range []string{"ResourcesReady", "RouterServingReady", "JobRecovery", "MutationReconcile", "RefresherRecovery", "ProductionReady", "UpgradeReady"} {
		value := float64(-1)
		condition := apiMeta.FindStatusCondition(c.Status.Conditions, name)
		if condition != nil && condition.ObservedGeneration == c.Generation {
			if condition.Status == metav1.ConditionTrue {
				value = 1
			}
			if condition.Status == metav1.ConditionFalse {
				value = 0
			}
		}
		clusterConditionMetric.WithLabelValues(c.Namespace, c.Name, name).Set(value)
	}
	if complete {
		clusterObservationMetric.WithLabelValues(c.Namespace, c.Name).SetToCurrentTime()
	}
}

func observeClusterComponents(c *v1alpha1.CubeCluster, complete bool) {
	// Fixed component/state labels prevent arbitrary status keys from creating
	// unbounded series. Removed Refresher state must not retain stale values.
	for _, component := range []string{cubeComponentAPI, cubeComponentRouter, cubeComponentMeta, cubeComponentWorker, cubeComponentRefresher} {
		if component == cubeComponentRefresher && c.Spec.Refresher == nil {
			clusterReplicaMetric.DeletePartialMatch(map[string]string{"namespace": c.Namespace, "cluster": c.Name, "component": component})
			clusterGenerationLagMetric.DeleteLabelValues(c.Namespace, c.Name, component)
			continue
		}
		state, present := c.Status.Components[component]
		known := complete && present && c.Status.ObservedGeneration == c.Generation
		for label, count := range map[string]int32{"desired": state.DesiredReplicas, "ready": state.ReadyReplicas, "updated": state.UpdatedReplicas} {
			value := float64(-1)
			if known {
				value = float64(count)
			}
			clusterReplicaMetric.WithLabelValues(c.Namespace, c.Name, component, label).Set(value)
		}
		lag := float64(-1)
		if known {
			lag = float64(state.WorkloadGeneration - state.ObservedGeneration)
			if lag < 0 {
				lag = 0
			}
		}
		clusterGenerationLagMetric.WithLabelValues(c.Namespace, c.Name, component).Set(lag)
	}
}

func componentSpecs(c *v1alpha1.CubeCluster) map[string]v1alpha1.CubeComponentSpec {
	specs := map[string]v1alpha1.CubeComponentSpec{
		cubeComponentAPI: c.Spec.API, cubeComponentMeta: c.Spec.MetaStore, cubeComponentWorker: c.Spec.Workers,
		cubeComponentRouter: {Replicas: c.Spec.Router.Replicas, Pod: c.Spec.Router.Pod, AllowSingleReplicaDisruption: c.Spec.Router.AllowSingleReplicaDisruption},
	}
	defaults := map[string]int32{cubeComponentAPI: 1, cubeComponentRouter: 2, cubeComponentMeta: 1, cubeComponentWorker: 2}
	for name, spec := range specs {
		spec.Replicas = clusterReplicas(spec.Replicas, defaults[name])
		specs[name] = spec
	}
	if c.Spec.Refresher != nil {
		spec := *c.Spec.Refresher
		spec.Replicas = clusterReplicas(spec.Replicas, 1)
		spec.Pod = refresherPodOptions(c)
		specs[cubeComponentRefresher] = spec
	}
	return specs
}

// Refresher inherits the API business model and datasource config. Non-nil
// Kubernetes lists replace their inherited list; env is merged by name.
func refresherPodOptions(c *v1alpha1.CubeCluster) v1alpha1.CubePodSpec {
	base := c.Spec.API.Pod.DeepCopy()
	if c.Spec.Refresher == nil {
		return *base
	}
	override := c.Spec.Refresher.Pod.DeepCopy()
	base.Env = mergeEnv(base.Env, override.Env)
	if override.EnvFrom != nil {
		base.EnvFrom = override.EnvFrom
	}
	if override.Volumes != nil {
		base.Volumes = override.Volumes
	}
	if override.VolumeMounts != nil {
		base.VolumeMounts = override.VolumeMounts
	}
	if override.Resources.Requests != nil || override.Resources.Limits != nil {
		base.Resources = override.Resources
	}
	if override.NodeSelector != nil {
		base.NodeSelector = override.NodeSelector
	}
	if override.Affinity != nil {
		base.Affinity = override.Affinity
	}
	if override.Tolerations != nil {
		base.Tolerations = override.Tolerations
	}
	if override.TopologySpreadConstraints != nil {
		base.TopologySpreadConstraints = override.TopologySpreadConstraints
	}
	if override.ImagePullSecrets != nil {
		base.ImagePullSecrets = override.ImagePullSecrets
	}
	if override.StartupProbe != nil {
		base.StartupProbe = override.StartupProbe
	}
	if override.ReadinessProbe != nil {
		base.ReadinessProbe = override.ReadinessProbe
	}
	if override.LivenessProbe != nil {
		base.LivenessProbe = override.LivenessProbe
	}
	if override.TerminationGracePeriodSeconds != nil {
		base.TerminationGracePeriodSeconds = override.TerminationGracePeriodSeconds
	}
	return *base
}

func validateClusterOptions(c *v1alpha1.CubeCluster) error {
	// Digest-pinned releases are atomic artifact sets. Equal Rust digests are
	// necessary here, but do NOT prove API/agent or rolling RPC compatibility.
	images := []string{c.Spec.Images.API, c.Spec.Images.Router, c.Spec.Images.MetaStore, c.Spec.Images.Worker, c.Spec.Images.LeaseAgent}
	pinned := false
	for _, image := range images {
		pinned = pinned || strings.Contains(image, "@")
	}
	if pinned {
		for _, image := range images {
			parts := strings.Split(image, "@sha256:")
			if len(parts) != 2 || parts[0] == "" || len(parts[1]) != 64 {
				return fmt.Errorf("digest-pinned releases require every component image to use repository@sha256:<64 hex digits>")
			}
			if _, err := hex.DecodeString(parts[1]); err != nil {
				return fmt.Errorf("invalid image digest: %w", err)
			}
		}
		digest := func(image string) string { return strings.Split(image, "@sha256:")[1] }
		if digest(c.Spec.Images.Router) != digest(c.Spec.Images.MetaStore) || digest(c.Spec.Images.Router) != digest(c.Spec.Images.Worker) {
			return fmt.Errorf("router, metaStore and worker must use the same frozen Rust digest; mixed Rust artifacts require a separately implemented compatibility policy")
		}
	}
	for name, count := range map[string]int32{"api": c.Spec.API.Replicas, "router": c.Spec.Router.Replicas, "metaStore": c.Spec.MetaStore.Replicas, "workers": c.Spec.Workers.Replicas} {
		if count < 0 {
			return fmt.Errorf("%s.replicas must be positive or omitted", name)
		}
	}
	if c.Spec.Refresher != nil && c.Spec.Refresher.Replicas != 0 && c.Spec.Refresher.Replicas != 1 {
		return fmt.Errorf("refresher.replicas must be 1; active/standby refresh ownership is not implemented")
	}
	if c.Spec.APISecretRef != nil && (c.Spec.APISecretRef.Name == "" || c.Spec.APISecretRef.Key == "" || (c.Spec.APISecretRef.Optional != nil && *c.Spec.APISecretRef.Optional)) {
		return fmt.Errorf("apiSecretRef requires name/key and cannot be optional")
	}
	if ref := c.Spec.Storage.ObjectStoreSecretRef; ref != nil && ref.Name == "" {
		return fmt.Errorf("storage.objectStoreSecretRef.name must not be empty")
	}
	if c.Spec.Storage.DataSize != "" {
		q, err := resource.ParseQuantity(c.Spec.Storage.DataSize)
		if err != nil || q.Sign() <= 0 {
			return fmt.Errorf("storage.dataSize must be a positive Kubernetes quantity")
		}
	}
	if len(c.Spec.Router.DrainCommand) > 0 && strings.TrimSpace(c.Spec.Router.DrainCommand[0]) == "" {
		return fmt.Errorf("router.drainCommand must start with an executable")
	}
	if len(c.Spec.Router.DrainCommand) > 0 && c.Spec.Router.Pod.TerminationGracePeriodSeconds != nil && *c.Spec.Router.Pod.TerminationGracePeriodSeconds < 40 {
		return fmt.Errorf("router drain requires terminationGracePeriodSeconds >= 40 (30s client timeout plus shutdown)")
	}
	for name, spec := range componentSpecs(c) {
		p := spec.Pod
		seen := map[string]bool{}
		for _, env := range p.Env {
			if len(validation.IsEnvVarName(env.Name)) > 0 || seen[env.Name] {
				return fmt.Errorf("%s.pod.env has invalid or duplicate name %q", name, env.Name)
			}
			seen[env.Name] = true
			if env.Value != "" && env.ValueFrom != nil {
				return fmt.Errorf("%s env %s cannot specify value and valueFrom", name, env.Name)
			}
			if reservedEnvironment(env.Name) {
				return fmt.Errorf("%s env %s is operator-owned; use the corresponding CR field", name, env.Name)
			}
		}
		volumes := map[string]bool{}
		for _, v := range p.Volumes {
			if strings.HasPrefix(v.Name, "authority-") || v.Name == "data" || v.Name == "local-data" || v.Name == "leadership" || v.Name == "promotion" || volumes[v.Name] {
				return fmt.Errorf("%s volume %q is reserved or duplicated", name, v.Name)
			}
			volumes[v.Name] = true
		}
		for _, m := range p.VolumeMounts {
			if !volumes[m.Name] {
				return fmt.Errorf("%s mount %q has no configured volume", name, m.Name)
			}
			if strings.HasPrefix(m.MountPath, "/var/run/cubestore") || strings.HasPrefix(m.MountPath, "/cube/.cubestore") || m.MountPath == "/cube/data" {
				return fmt.Errorf("%s mount %s shadows managed data/fencing", name, m.MountPath)
			}
		}
		if p.TerminationGracePeriodSeconds != nil && *p.TerminationGracePeriodSeconds < 0 {
			return fmt.Errorf("%s terminationGracePeriodSeconds must be nonnegative", name)
		}
		for key, q := range p.Resources.Requests {
			if q.Sign() < 0 {
				return fmt.Errorf("%s resource request %s must be nonnegative", name, key)
			}
			if limit, ok := p.Resources.Limits[key]; ok && q.Cmp(limit) > 0 {
				return fmt.Errorf("%s resource request %s exceeds limit", name, key)
			}
		}
		for _, probe := range []*corev1.Probe{p.StartupProbe, p.ReadinessProbe, p.LivenessProbe} {
			if probe == nil {
				continue
			}
			handlers := 0
			if probe.HTTPGet != nil {
				handlers++
			}
			if probe.TCPSocket != nil {
				handlers++
			}
			if probe.Exec != nil {
				handlers++
			}
			if probe.GRPC != nil {
				handlers++
			}
			if handlers != 1 {
				return fmt.Errorf("%s probe must have exactly one handler", name)
			}
		}
	}
	return nil
}

func reservedEnvironment(name string) bool {
	if strings.HasPrefix(name, "CUBESTORE_AUTHORITY_") {
		return true
	}
	switch name {
	case "POD_NAME", "POD_NAMESPACE", "CUBEJS_API_SECRET", "CUBEJS_CUBESTORE_HOST", "CUBEJS_CUBESTORE_PORT", "CUBEJS_EXT_DB_TYPE", "CUBEJS_PORT", "CUBEJS_CACHE_AND_QUEUE_DRIVER", "CUBEJS_REFRESH_WORKER", "CUBEJS_SCHEDULED_REFRESH", "CUBEJS_SCHEDULED_REFRESH_TIMER",
		"CUBESTORE_SERVER_NAME", "CUBESTORE_NODE_NAME", "CUBESTORE_DATA_DIR", "CUBESTORE_REMOTE_DIR", "CUBESTORE_META_ADDR", "CUBESTORE_META_BIND_ADDR", "CUBESTORE_WORKER_PORT", "CUBESTORE_WORKERS", "CUBESTORE_HTTP_PORT", "CUBESTORE_HTTP_BIND_ADDR", "CUBESTORE_BIND_ADDR", "CUBESTORE_ROUTER_ROLE_STRICT", "CUBESTORE_ROUTER_LEADERSHIP_FILE", "CUBESTORE_ROUTER_PROMOTION_FILE", "CUBESTORE_MINIO_BUCKET", "CUBESTORE_MINIO_SUB_PATH", "CUBESTORE_MINIO_SERVER_ENDPOINT", "CUBESTORE_MINIO_ACCESS_KEY_ID", "CUBESTORE_MINIO_SECRET_ACCESS_KEY":
		return true
	}
	return false
}

func mergeEnv(base, extra []corev1.EnvVar) []corev1.EnvVar {
	out := append([]corev1.EnvVar(nil), base...)
	positions := map[string]int{}
	for i, e := range out {
		positions[e.Name] = i
	}
	for _, e := range extra {
		if i, ok := positions[e.Name]; ok {
			out[i] = e
		} else {
			positions[e.Name] = len(out)
			out = append(out, e)
		}
	}
	return out
}
func rustSharedEnv(c *v1alpha1.CubeCluster) []corev1.EnvVar {
	return appendObjectStoreCredentials([]corev1.EnvVar{{Name: "CUBESTORE_WORKERS", Value: workerAddresses(c)}, {Name: "CUBESTORE_MINIO_BUCKET", Value: c.Spec.Storage.Bucket}, {Name: "CUBESTORE_MINIO_SUB_PATH", Value: c.Spec.Storage.SubPath}, {Name: "CUBESTORE_MINIO_SERVER_ENDPOINT", Value: c.Spec.Storage.Endpoint}, {Name: "CUBESTORE_HTTP_BIND_ADDR", Value: "0.0.0.0:3030"}, {Name: "CUBESTORE_HTTP_PORT", Value: "3030"}}, c)
}
func httpProbe(path string, port int32) *corev1.Probe {
	return &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: path, Port: intstr.FromInt32(port)}}, PeriodSeconds: 5, TimeoutSeconds: 3, FailureThreshold: 3}
}
func (r *CubeClusterReconciler) configurePod(ctx context.Context, c *v1alpha1.CubeCluster, t *corev1.PodTemplateSpec, options v1alpha1.CubePodSpec, index int, api bool) error {
	p := options.DeepCopy()
	container := &t.Spec.Containers[index]
	container.Env = mergeEnv(container.Env, p.Env)
	container.EnvFrom = p.EnvFrom
	container.VolumeMounts = append(container.VolumeMounts, p.VolumeMounts...)
	t.Spec.Volumes = append(t.Spec.Volumes, p.Volumes...)
	container.Resources = p.Resources
	t.Spec.NodeSelector = p.NodeSelector
	t.Spec.Tolerations = p.Tolerations
	t.Spec.TopologySpreadConstraints = p.TopologySpreadConstraints
	t.Spec.ImagePullSecrets = p.ImagePullSecrets
	if p.Affinity != nil {
		t.Spec.Affinity = p.Affinity
	}
	if p.TerminationGracePeriodSeconds != nil {
		t.Spec.TerminationGracePeriodSeconds = p.TerminationGracePeriodSeconds
	}
	port := int32(3030)
	if api {
		port = 4000
	}
	// HA API roles require a matching custom API image that exposes the
	// process-only endpoint. The legacy /livez checks datasource/queue health
	// and must not trigger restarts during an expected Router failover.
	processPath := "/livez"
	if api && c.Spec.Router.HighAvailability {
		processPath = "/livez/process"
	}
	container.StartupProbe = httpProbe(processPath, port)
	container.StartupProbe.FailureThreshold = 60
	container.LivenessProbe = httpProbe(processPath, port)
	container.ReadinessProbe = httpProbe("/readyz", port)
	if p.StartupProbe != nil {
		container.StartupProbe = p.StartupProbe
	}
	if p.LivenessProbe != nil {
		container.LivenessProbe = p.LivenessProbe
	}
	if p.ReadinessProbe != nil {
		container.ReadinessProbe = p.ReadinessProbe
	}
	digest, err := r.configurationDigest(ctx, c)
	if err != nil {
		return err
	}
	if t.Annotations == nil {
		t.Annotations = map[string]string{}
	}
	t.Annotations[configurationAnnotation] = digest
	return nil
}

func apiSecretSelector(c *v1alpha1.CubeCluster) *corev1.SecretKeySelector {
	if c.Spec.APISecretRef != nil {
		return c.Spec.APISecretRef.DeepCopy()
	}
	return &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: apiSecretName(c)}, Key: cubeAPISecretKey}
}
func (r *CubeClusterReconciler) apiPod(ctx context.Context, c *v1alpha1.CubeCluster, component string, p v1alpha1.CubePodSpec) (corev1.PodTemplateSpec, error) {
	name := clusterName(c, component)
	env := []corev1.EnvVar{{Name: "CUBEJS_EXT_DB_TYPE", Value: "cubestore"}, {Name: "CUBEJS_CUBESTORE_HOST", Value: fmt.Sprintf("%s.%s.svc", nameRouterLeader(c), c.Namespace)}, {Name: "CUBEJS_CUBESTORE_PORT", Value: "3030"}, {Name: "CUBEJS_API_SECRET", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: apiSecretSelector(c)}}, {Name: "CUBEJS_PORT", Value: "4000"}, {Name: "CUBEJS_CACHE_AND_QUEUE_DRIVER", Value: "cubestore"}}
	// Don't mask a datasource or schema path supplied via envFrom.
	if len(p.EnvFrom) == 0 {
		env = append(env, corev1.EnvVar{Name: "CUBEJS_DB_TYPE", Value: "cubestore"}, corev1.EnvVar{Name: "CUBEJS_SCHEMA_PATH", Value: "schema"})
	}
	if component == cubeComponentRefresher {
		env = append(env, corev1.EnvVar{Name: "CUBEJS_REFRESH_WORKER", Value: "true"})
	} else {
		suppress := c.Spec.Refresher != nil
		if !suppress {
			var old apps.Deployment
			err := r.Get(ctx, client.ObjectKey{Namespace: c.Namespace, Name: clusterName(c, cubeComponentRefresher)}, &old)
			if err != nil && !apierrors.IsNotFound(err) {
				return corev1.PodTemplateSpec{}, err
			}
			suppress = err == nil && (old.Spec.Replicas == nil || *old.Spec.Replicas > 0 || old.Status.Replicas > 0 || old.Status.ObservedGeneration < old.Generation)
		}
		if suppress {
			env = append(env, corev1.EnvVar{Name: "CUBEJS_REFRESH_WORKER", Value: "false"})
		}
	}
	t := podTemplate(clusterLabels(c, component), name, c.Spec.Images.API, name, []corev1.ContainerPort{{Name: "http", ContainerPort: 4000}}, env, nil)
	err := r.configurePod(ctx, c, &t, p, 0, true)
	return t, err
}

func deploymentCurrent(d *apps.Deployment, desired int32) bool {
	return d.Status.ObservedGeneration >= d.Generation && d.Status.ReadyReplicas >= desired && d.Status.AvailableReplicas >= desired && d.Status.UpdatedReplicas >= desired && d.Status.Replicas == d.Status.UpdatedReplicas
}
func (r *CubeClusterReconciler) reconcileRefresher(ctx context.Context, c *v1alpha1.CubeCluster) error {
	d := &apps.Deployment{ObjectMeta: metav1.ObjectMeta{Name: clusterName(c, cubeComponentRefresher), Namespace: c.Namespace}}
	if c.Spec.Refresher == nil {
		if err := r.Get(ctx, client.ObjectKeyFromObject(d), d); err != nil {
			return client.IgnoreNotFound(err)
		}
		if !metav1.IsControlledBy(d, c) {
			return fmt.Errorf("refresher deployment is not owned by this cluster")
		}
		if d.Spec.Replicas != nil && *d.Spec.Replicas == 0 {
			return nil
		}
		d.Spec.Replicas = ptr32(0)
		return r.Update(ctx, d)
	}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, d, func() error {
		if err := r.own(c, d); err != nil {
			return err
		}
		d.Labels = clusterLabels(c, cubeComponentRefresher)
		d.Spec.Selector = &metav1.LabelSelector{MatchLabels: clusterLabels(c, cubeComponentRefresher)}
		d.Spec.Strategy = apps.DeploymentStrategy{Type: apps.RecreateDeploymentStrategyType}
		// Wait for the API rollout with scheduling disabled before starting a
		// separate scheduler. Recreate is NOT fencing after node partitions.
		var api apps.Deployment
		if err := r.Get(ctx, client.ObjectKey{Namespace: c.Namespace, Name: clusterName(c, cubeComponentAPI)}, &api); err != nil {
			return err
		}
		desired := int32(0)
		if deploymentCurrent(&api, clusterReplicas(c.Spec.API.Replicas, 1)) {
			desired = 1
		}
		d.Spec.Replicas = ptr32(desired)
		p, err := r.apiPod(ctx, c, cubeComponentRefresher, refresherPodOptions(c))
		if err != nil {
			return err
		}
		d.Spec.Template = p
		return nil
	})
	return err
}

// Reference data, not resourceVersion, drives rollouts. Status/metadata churn
// on configuration objects must not restart running business workloads.
type configReference struct {
	kind, name, key string
	optional        bool
}

func configurationReferences(c *v1alpha1.CubeCluster) []configReference {
	refs := []configReference{{kind: "Secret", name: apiSecretName(c), key: apiSecretSelector(c).Key}}
	if ref := c.Spec.Storage.ObjectStoreSecretRef; ref != nil {
		refs = append(refs, configReference{kind: "Secret", name: ref.Name, key: "accessKey"}, configReference{kind: "Secret", name: ref.Name, key: "secretKey"})
	}
	optional := func(v *bool) bool { return v != nil && *v }
	for _, spec := range componentSpecs(c) {
		p := spec.Pod
		for _, e := range p.Env {
			if e.ValueFrom == nil {
				continue
			}
			if v := e.ValueFrom.SecretKeyRef; v != nil {
				refs = append(refs, configReference{"Secret", v.Name, v.Key, optional(v.Optional)})
			}
			if v := e.ValueFrom.ConfigMapKeyRef; v != nil {
				refs = append(refs, configReference{"ConfigMap", v.Name, v.Key, optional(v.Optional)})
			}
		}
		for _, e := range p.EnvFrom {
			if v := e.SecretRef; v != nil {
				refs = append(refs, configReference{"Secret", v.Name, "", optional(v.Optional)})
			}
			if v := e.ConfigMapRef; v != nil {
				refs = append(refs, configReference{"ConfigMap", v.Name, "", optional(v.Optional)})
			}
		}
		for _, v := range p.Volumes {
			if a := v.Secret; a != nil {
				refs = append(refs, configReference{"Secret", a.SecretName, "", optional(a.Optional)})
			}
			if a := v.ConfigMap; a != nil {
				refs = append(refs, configReference{"ConfigMap", a.Name, "", optional(a.Optional)})
			}
			if v.Projected != nil {
				for _, a := range v.Projected.Sources {
					if a.Secret != nil {
						refs = append(refs, configReference{"Secret", a.Secret.Name, "", optional(a.Secret.Optional)})
					}
					if a.ConfigMap != nil {
						refs = append(refs, configReference{"ConfigMap", a.ConfigMap.Name, "", optional(a.ConfigMap.Optional)})
					}
				}
			}
		}
		for _, v := range p.ImagePullSecrets {
			refs = append(refs, configReference{"Secret", v.Name, "", false})
		}
	}
	sort.Slice(refs, func(i, j int) bool {
		a, b := refs[i], refs[j]
		return a.kind+"/"+a.name+"/"+a.key+strconv.FormatBool(a.optional) < b.kind+"/"+b.name+"/"+b.key+strconv.FormatBool(b.optional)
	})
	return refs
}
func (r *CubeClusterReconciler) configurationDigest(ctx context.Context, c *v1alpha1.CubeCluster) (string, error) {
	data := map[string]any{}
	for _, ref := range configurationReferences(c) {
		key := client.ObjectKey{Namespace: c.Namespace, Name: ref.name}
		id := ref.kind + "/" + ref.name
		var err error
		var hasKey bool
		if ref.kind == "Secret" {
			var s corev1.Secret
			err = r.Get(ctx, key, &s)
			if err == nil {
				hasKey = len(s.Data[ref.key]) > 0
				data[id] = s.Data
			}
		} else {
			var cm corev1.ConfigMap
			err = r.Get(ctx, key, &cm)
			if err == nil {
				_, hasKey = cm.Data[ref.key]
				if !hasKey {
					_, hasKey = cm.BinaryData[ref.key]
				}
				data[id] = []any{cm.Data, cm.BinaryData}
			}
		}
		if err != nil {
			if ref.optional && apierrors.IsNotFound(err) {
				data[id] = nil
				continue
			}
			return "", fmt.Errorf("configuration %s: %w", id, err)
		}
		if ref.key != "" && !hasKey && !ref.optional {
			return "", fmt.Errorf("configuration %s missing required nonempty key %s", id, ref.key)
		}
	}
	raw, err := json.Marshal(data)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}
func (r *CubeClusterReconciler) configurationChanged(ctx context.Context, obj client.Object) []ctrl.Request {
	var list v1alpha1.CubeClusterList
	if err := r.List(ctx, &list, client.InNamespace(obj.GetNamespace())); err != nil {
		ctrl.LoggerFrom(ctx).Error(err, "map configuration change")
		return nil
	}
	kind := "ConfigMap"
	if _, ok := obj.(*corev1.Secret); ok {
		kind = "Secret"
	}
	var requests []ctrl.Request
	for i := range list.Items {
		c := &list.Items[i]
		for _, ref := range configurationReferences(c) {
			if ref.kind == kind && ref.name == obj.GetName() {
				requests = append(requests, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(c)})
				break
			}
		}
	}
	return requests
}
func (r *CubeClusterReconciler) configurationError(ctx context.Context, c *v1alpha1.CubeCluster, err error) (ctrl.Result, error) {
	return r.setClusterCondition(ctx, c, cubeClusterConditionResources, metav1.ConditionFalse, "ConfigurationUnavailable", err.Error())
}
func printableSecret(value []byte) bool {
	if len(value) == 0 {
		return false
	}
	for _, b := range value {
		if b < 32 || b > 126 {
			return false
		}
	}
	return true
}

// StatefulSet claim templates are immutable. Defaulted storageClassName is
// retained, and no implicit destructive recreation or PVC shrink is attempted.
func (r *CubeClusterReconciler) validateStorageTransition(ctx context.Context, c *v1alpha1.CubeCluster) error {
	desired := pvcSpec(c)
	for _, component := range []string{cubeComponentMeta, cubeComponentWorker} {
		var set apps.StatefulSet
		if err := r.Get(ctx, client.ObjectKey{Namespace: c.Namespace, Name: clusterName(c, component)}, &set); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return err
		}
		for _, claim := range set.Spec.VolumeClaimTemplates {
			old := claim.Spec.Resources.Requests[corev1.ResourceStorage]
			requested := desired.Resources.Requests[corev1.ResourceStorage]
			if old.Cmp(requested) != 0 {
				return fmt.Errorf("%s claim %s size change %s -> %s requires an explicit PVC expansion/migration and StatefulSet recreation plan; templates are immutable", set.Name, claim.Name, old.String(), requested.String())
			}
			if c.Spec.Storage.StorageClassName != nil && !reflect.DeepEqual(claim.Spec.StorageClassName, c.Spec.Storage.StorageClassName) {
				return fmt.Errorf("%s claim %s storageClass change requires a data migration; no PVC or StatefulSet was modified", set.Name, claim.Name)
			}
		}
	}
	return nil
}

// applicationReady is the dependency-upgrade probe. Build recovery capability
// is deliberately not a prerequisite for process rollout or liveness.
func (r *CubeClusterReconciler) applicationReady(ctx context.Context, c *v1alpha1.CubeCluster, component string, desired int32) (bool, error) {
	ready, _, err := r.applicationObservation(ctx, c, component, desired)
	return ready, err
}

// applicationObservation requires every responding ready replica to advertise
// the capability before returning true. Omitted, malformed or partial evidence
// remains unknown, even when /readyz returns HTTP 200.
func (r *CubeClusterReconciler) applicationObservation(ctx context.Context, c *v1alpha1.CubeCluster, component string, desired int32) (bool, *bool, error) {
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(c.Namespace), client.MatchingLabels(clusterLabels(c, component))); err != nil {
		return false, nil, err
	}
	if desired < 1 {
		return false, nil, nil
	}
	count := int32(0)
	unknown, unsupported := false, false
	port := int32(3030)
	if component == cubeComponentAPI || component == cubeComponentRefresher {
		port = 4000
	}
	h := r.HTTPClient
	if h == nil {
		h = &http.Client{Timeout: 3 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	for _, pod := range pods.Items {
		if !pod.DeletionTimestamp.IsZero() || !isPodReady(&pod) || pod.Status.PodIP == "" {
			continue
		}
		probeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, "http://"+net.JoinHostPort(pod.Status.PodIP, strconv.Itoa(int(port)))+"/readyz", nil)
		if err != nil {
			cancel()
			return false, nil, err
		}
		resp, err := h.Do(req)
		if err == nil {
			if resp.StatusCode == http.StatusOK {
				count++
				var body struct {
					RecoveryCapabilities *struct {
						FileImportBuildScheduling *bool `json:"fileImportBuildScheduling"`
					} `json:"recoveryCapabilities"`
				}
				// Read bounded JSON in full: truncated/invalid/multiple documents must
				// never produce a partial capability acknowledgement.
				raw, readErr := io.ReadAll(io.LimitReader(resp.Body, (64<<10)+1))
				if readErr != nil || len(raw) > 64<<10 || json.Unmarshal(raw, &body) != nil || body.RecoveryCapabilities == nil || body.RecoveryCapabilities.FileImportBuildScheduling == nil {
					unknown = true
				} else if !*body.RecoveryCapabilities.FileImportBuildScheduling {
					unsupported = true
				}
			}
			_ = resp.Body.Close()
		}
		cancel()
	}
	ready := count >= desired
	if !ready {
		return false, nil, nil
	}
	if unsupported {
		supported := false
		return true, &supported, nil
	}
	if unknown {
		return true, nil, nil
	}
	supported := true
	return true, &supported, nil
}

func (r *CubeClusterReconciler) reconcileStatus(ctx context.Context, c *v1alpha1.CubeCluster) error {
	next := c.DeepCopy()
	next.Status.ObservedGeneration = c.Generation
	next.Status.ReadyReplicas = 0
	next.Status.Components = map[string]v1alpha1.CubeComponentStatus{}
	buildCapabilities := map[string]*bool{}
	allReady := true
	specs := componentSpecs(c)
	names := make([]string, 0, len(specs))
	for name := range specs {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, component := range names {
		spec := specs[component]
		key := client.ObjectKey{Namespace: c.Namespace, Name: clusterName(c, component)}
		state := v1alpha1.CubeComponentStatus{DesiredReplicas: spec.Replicas, ApplicationReady: metav1.ConditionFalse}
		if component == cubeComponentMeta || component == cubeComponentWorker {
			var set apps.StatefulSet
			if err := r.Get(ctx, key, &set); err != nil {
				return err
			}
			state.ReadyReplicas = set.Status.ReadyReplicas
			state.UpdatedReplicas = set.Status.UpdatedReplicas
			state.WorkloadGeneration = set.Generation
			state.ObservedGeneration = set.Status.ObservedGeneration
			state.Ready = set.Status.ObservedGeneration >= set.Generation && set.Status.ReadyReplicas >= spec.Replicas && set.Status.UpdatedReplicas >= spec.Replicas && set.Status.CurrentRevision != "" && set.Status.CurrentRevision == set.Status.UpdateRevision
		} else {
			var d apps.Deployment
			if err := r.Get(ctx, key, &d); err != nil {
				return err
			}
			state.ReadyReplicas = d.Status.ReadyReplicas
			state.UpdatedReplicas = d.Status.UpdatedReplicas
			state.WorkloadGeneration = d.Generation
			state.ObservedGeneration = d.Status.ObservedGeneration
			state.Ready = deploymentCurrent(&d, spec.Replicas)
		}
		live, capability, err := r.applicationObservation(ctx, c, component, spec.Replicas)
		if err != nil {
			return err
		}
		if live {
			state.ApplicationReady = metav1.ConditionTrue
		}
		state.Ready = state.Ready && live
		buildCapabilities[component] = capability
		next.Status.Components[component] = state
		next.Status.ReadyReplicas += state.ReadyReplicas
		allReady = allReady && state.Ready
		apiMeta.SetStatusCondition(&next.Status.Conditions, newClusterCondition(c, componentCondition(component), state.Ready, "ComponentObservation", fmt.Sprintf("ready=%d desired=%d updated=%d generation=%d observed=%d application=%s", state.ReadyReplicas, state.DesiredReplicas, state.UpdatedReplicas, state.WorkloadGeneration, state.ObservedGeneration, state.ApplicationReady)))
	}
	if c.Spec.Refresher == nil {
		apiMeta.RemoveStatusCondition(&next.Status.Conditions, componentCondition(cubeComponentRefresher))
	}
	var router v1alpha1.CubestoreRouter
	if err := r.Get(ctx, client.ObjectKey{Namespace: c.Namespace, Name: clusterName(c, cubeComponentRouter)}, &router); err != nil {
		return err
	}
	currentGate := func(typ string, gate v1alpha1.RecoveryGateStatus) v1alpha1.RecoveryGateStatus {
		condition := apiMeta.FindStatusCondition(router.Status.Conditions, typ)
		if condition == nil || condition.ObservedGeneration != router.Generation {
			return v1alpha1.RecoveryGateStatus{State: v1alpha1.RecoveryStateNeedsContext, Reason: "EvidenceIncomplete", Message: "No current-generation runtime recovery acknowledgement: " + typ}
		}
		if gate.State == v1alpha1.RecoveryStateReady && condition.Status != metav1.ConditionTrue {
			return v1alpha1.RecoveryGateStatus{State: v1alpha1.RecoveryStateNeedsContext, Reason: "EvidenceIncomplete", Message: "Runtime condition and recovery gate disagree: " + typ}
		}
		if gate.State == "" {
			return v1alpha1.RecoveryGateStatus{State: v1alpha1.RecoveryStateNeedsContext, Reason: "EvidenceIncomplete", Message: "Runtime recovery gate is absent: " + typ}
		}
		return gate
	}
	next.Status.Recovery = v1alpha1.RouterRecoveryStatus{Promotion: currentGate(v1alpha1.CubestoreRouterConditionPromotionReady, router.Status.Recovery.Promotion), JobRecovery: currentGate(v1alpha1.CubestoreRouterConditionJobRecovery, router.Status.Recovery.JobRecovery), MutationReconcile: currentGate(v1alpha1.CubestoreRouterConditionMutationReconcile, router.Status.Recovery.MutationReconcile), Refresher: currentGate(v1alpha1.CubestoreRouterConditionRefresherReady, router.Status.Recovery.Refresher)}
	next.Status.Recovery.Refresher = refresherBuildRecovery(c, next.Status.Components, next.Status.Recovery, buildCapabilities)

	serving := next.Status.Recovery.Promotion.State == v1alpha1.RecoveryStateReady && next.Status.Components[cubeComponentRouter].Ready
	apiMeta.SetStatusCondition(&next.Status.Conditions, newClusterCondition(c, cubeClusterConditionResources, allReady, "ComponentObservations", "Each workload must observe its generation, update every replica, and pass live application probes"))
	apiMeta.SetStatusCondition(&next.Status.Conditions, newClusterCondition(c, cubeClusterConditionStorage, true, "ConfigurationOnly", "Object store configuration and credentials resolved; durability/access are not certified by this condition"))
	apiMeta.SetStatusCondition(&next.Status.Conditions, newClusterCondition(c, "RouterServingReady", serving, "RouterController", "Current promotion acknowledgement plus live Router readiness"))
	apiMeta.SetStatusCondition(&next.Status.Conditions, newClusterCondition(c, cubeClusterConditionHA, serving && c.Spec.Router.HighAvailability && specs[cubeComponentRouter].Replicas >= 2, "RouterController", "Router role cutover readiness only; not lossless business recovery"))
	for typ, gate := range map[string]v1alpha1.RecoveryGateStatus{"JobRecovery": next.Status.Recovery.JobRecovery, "MutationReconcile": next.Status.Recovery.MutationReconcile, "RefresherRecovery": next.Status.Recovery.Refresher} {
		condition := newClusterCondition(c, typ, gate.State == v1alpha1.RecoveryStateReady, gate.Reason, gate.Message)
		if gate.State == v1alpha1.RecoveryStateNeedsContext {
			condition.Status = metav1.ConditionUnknown
		}
		apiMeta.SetStatusCondition(&next.Status.Conditions, condition)
	}
	apiMeta.SetStatusCondition(&next.Status.Conditions, newClusterCondition(c, "ProductionReady", false, "EvidenceIncomplete", "MetaStore is single-writer; multi-node disaster recovery and durable refresher ownership are not certified by workload readiness"))
	apiMeta.SetStatusCondition(&next.Status.Conditions, newClusterCondition(c, "UpgradeReady", true, "DesiredResourcesApplied", "Desired child specifications applied in dependency order; readiness remains independently observed"))
	next.Status.Phase = "Provisioning"
	if allReady {
		next.Status.Phase = "Degraded"
		if serving && next.Status.Recovery.JobRecovery.State == v1alpha1.RecoveryStateReady && next.Status.Recovery.MutationReconcile.State == v1alpha1.RecoveryStateReady {
			next.Status.Phase = "Running"
		}
	}
	sort.Slice(next.Status.Conditions, func(i, j int) bool { return next.Status.Conditions[i].Type < next.Status.Conditions[j].Type })
	if statusEqualCubeCluster(c.Status, next.Status) {
		observeClusterConditions(next, true)
		return nil
	}
	if err := r.Status().Update(ctx, next); err != nil {
		return err
	}
	observeClusterConditions(next, true)
	return nil
}

// Only an empty installation may bootstrap together. Existing workloads roll
// strictly MetaStore -> Workers -> Router -> API -> Refresher. This ordering
// does not prove old clients can speak to a new MetaStore. A frozen release
// transition needs mixed-version evidence or an explicitly fenced maintenance
// window before changing the CR; Ready alone is not protocol negotiation.
func (r *CubeClusterReconciler) isBootstrap(ctx context.Context, c *v1alpha1.CubeCluster) (bool, error) {
	reader := client.Reader(r.Client)
	if r.APIReader != nil {
		reader = r.APIReader
	}
	for _, name := range []string{cubeComponentMeta, cubeComponentWorker, cubeComponentRouter, cubeComponentAPI} {
		var obj client.Object
		if name == cubeComponentMeta || name == cubeComponentWorker {
			obj = &apps.StatefulSet{}
		} else {
			obj = &apps.Deployment{}
		}
		err := reader.Get(ctx, client.ObjectKey{Namespace: c.Namespace, Name: clusterName(c, name)}, obj)
		if err == nil {
			return false, nil
		}
		if !apierrors.IsNotFound(err) {
			return false, err
		}
	}
	return true, nil
}
func (r *CubeClusterReconciler) upgradeComponentReady(ctx context.Context, c *v1alpha1.CubeCluster, component string) (bool, error) {
	reader := client.Reader(r.Client)
	if r.APIReader != nil {
		reader = r.APIReader
	}
	key := client.ObjectKey{Namespace: c.Namespace, Name: clusterName(c, component)}
	desired := componentSpecs(c)[component].Replicas
	ready := false
	if component == cubeComponentMeta || component == cubeComponentWorker {
		var set apps.StatefulSet
		if err := reader.Get(ctx, key, &set); err != nil {
			return false, err
		}
		ready = set.Status.ObservedGeneration >= set.Generation && set.Status.ReadyReplicas >= desired && set.Status.UpdatedReplicas >= desired && set.Status.CurrentRevision != "" && set.Status.CurrentRevision == set.Status.UpdateRevision
	} else {
		var deployment apps.Deployment
		if err := reader.Get(ctx, key, &deployment); err != nil {
			return false, err
		}
		ready = deploymentCurrent(&deployment, desired)
	}
	if !ready {
		return false, nil
	}
	return r.applicationReady(ctx, c, component, desired)
}
func (r *CubeClusterReconciler) waitForUpgrade(ctx context.Context, c *v1alpha1.CubeCluster, component string) (ctrl.Result, error) {
	next := c.DeepCopy()
	next.Status.Phase = "Upgrading"
	// observedGeneration is not advanced until all desired child specs apply.
	apiMeta.SetStatusCondition(&next.Status.Conditions, newClusterCondition(c, "UpgradeReady", false, "WaitingForDependency", "Waiting for "+component+" observed generation, updated replicas and live readiness before rolling dependent RPC clients"))
	apiMeta.SetStatusCondition(&next.Status.Conditions, newClusterCondition(c, cubeClusterConditionResources, false, "UpgradeInProgress", "Dependent components have not yet applied the desired generation"))
	apiMeta.SetStatusCondition(&next.Status.Conditions, newClusterCondition(c, "ProductionReady", false, "EvidenceIncomplete", "An ordered upgrade is in progress"))
	observeClusterConditions(next, false)
	if statusEqualCubeCluster(c.Status, next.Status) {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	return ctrl.Result{RequeueAfter: 5 * time.Second}, r.Status().Update(ctx, next)
}

func componentCondition(component string) string {
	switch component {
	case cubeComponentAPI:
		return "APIReady"
	case cubeComponentMeta:
		return "MetaStoreReady"
	case cubeComponentWorker:
		return "WorkersReady"
	case cubeComponentRouter:
		return "RouterReady"
	case cubeComponentRefresher:
		return "RefresherReady"
	}
	return component + "Ready"
}
