package controllers

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"time"

	"github.com/cube-js/cube-operator/api/v1alpha1"
	apps "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
)

const (
	cubeClusterNameLabel          = "cubestore.io/cube-cluster"
	cubeComponentLabel            = "cubestore.io/component"
	cubeComponentAPI              = "api"
	cubeComponentRouter           = "router"
	cubeComponentMeta             = "metastore"
	cubeComponentWorker           = "worker"
	cubeClusterConditionResources = "ResourcesReady"
	cubeClusterConditionHA        = "RouterHAReady"
	cubeClusterConditionStorage   = "StorageConfigured"
)

// +kubebuilder:rbac:groups=cubestore.io,resources=cubeclusters,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=cubestore.io,resources=cubeclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=cubestore.io,resources=cubestorerouters,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=apps,resources=deployments;statefulsets,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups="",resources=configmaps;services;serviceaccounts;secrets,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles;rolebindings,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch
type CubeClusterReconciler struct {
	client.Client
	APIReader client.Reader
	*runtime.Scheme
	HTTPClient *http.Client
}

func (r *CubeClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// For() registers an informer whose initial Add events enter the ordinary
	// keyed workqueue. Never call Reconcile directly from a startup runnable:
	// that bypasses per-key serialization, retries and controller shutdown.
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.CubeCluster{}).
		Owns(&apps.Deployment{}).
		Owns(&apps.StatefulSet{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ServiceAccount{}).
		Owns(&rbacv1.Role{}).
		Owns(&rbacv1.RoleBinding{}).
		Owns(&policyv1.PodDisruptionBudget{}).
		Owns(&v1alpha1.CubestoreRouter{}).
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(r.configurationChanged)).
		Watches(&corev1.ConfigMap{}, handler.EnqueueRequestsFromMapFunc(r.configurationChanged)).
		Complete(r)
}

func (r *CubeClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var cluster v1alpha1.CubeCluster
	if err := r.Get(ctx, req.NamespacedName, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			clusterConditionMetric.DeletePartialMatch(map[string]string{"namespace": req.Namespace, "cluster": req.Name})
			clusterObservationMetric.DeleteLabelValues(req.Namespace, req.Name)
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if err := validateCubeCluster(&cluster); err != nil {
		return r.setClusterCondition(ctx, &cluster, cubeClusterConditionResources, metav1.ConditionFalse, "InvalidSpec", err.Error())
	}
	if !cluster.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}
	if err := r.validateStorageTransition(ctx, &cluster); err != nil {
		return r.setClusterCondition(ctx, &cluster, cubeClusterConditionResources, metav1.ConditionFalse, "StorageTransitionBlocked", err.Error())
	}
	if err := r.reconcileAPISecret(ctx, &cluster); err != nil {
		return r.configurationError(ctx, &cluster, err)
	}
	if _, err := r.configurationDigest(ctx, &cluster); err != nil {
		return r.configurationError(ctx, &cluster, err)
	}
	bootstrap, err := r.isBootstrap(ctx, &cluster)
	if err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcileConfigMap(ctx, &cluster); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcileServiceAccounts(ctx, &cluster); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcileRouterRBAC(ctx, &cluster); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcileMetaStore(ctx, &cluster); err != nil {
		return ctrl.Result{}, err
	}
	if !bootstrap {
		ready, err := r.upgradeComponentReady(ctx, &cluster, cubeComponentMeta)
		if err != nil {
			return ctrl.Result{}, err
		}
		if !ready {
			return r.waitForUpgrade(ctx, &cluster, cubeComponentMeta)
		}
	}
	if err := r.reconcileWorkers(ctx, &cluster); err != nil {
		return ctrl.Result{}, err
	}
	if !bootstrap {
		ready, err := r.upgradeComponentReady(ctx, &cluster, cubeComponentWorker)
		if err != nil {
			return ctrl.Result{}, err
		}
		if !ready {
			return r.waitForUpgrade(ctx, &cluster, cubeComponentWorker)
		}
	}
	if err := r.reconcileRouters(ctx, &cluster); err != nil {
		return ctrl.Result{}, err
	}
	if !bootstrap {
		ready, err := r.upgradeComponentReady(ctx, &cluster, cubeComponentRouter)
		if err != nil {
			return ctrl.Result{}, err
		}
		if !ready {
			return r.waitForUpgrade(ctx, &cluster, cubeComponentRouter)
		}
	}
	if err := r.reconcileAPISecret(ctx, &cluster); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcileAPI(ctx, &cluster); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcileRefresher(ctx, &cluster); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcilePDBs(ctx, &cluster); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcileStatus(ctx, &cluster); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
}

func validateCubeCluster(cluster *v1alpha1.CubeCluster) error {
	images := cluster.Spec.Images
	if images.API == "" || images.Router == "" || images.MetaStore == "" || images.Worker == "" || images.LeaseAgent == "" {
		return fmt.Errorf("spec.images.api, router, metaStore, worker and leaseAgent are required")
	}
	if cluster.Spec.Storage.Endpoint == "" || cluster.Spec.Storage.Bucket == "" {
		return fmt.Errorf("spec.storage.endpoint and spec.storage.bucket are required")
	}
	if cluster.Spec.Storage.ObjectStoreSecretRef != nil && (cluster.Spec.Storage.ObjectStoreSecretRef.Namespace != "" && cluster.Spec.Storage.ObjectStoreSecretRef.Namespace != cluster.Namespace) {
		return fmt.Errorf("spec.storage.objectStoreSecretRef must be in the CubeCluster namespace")
	}
	if cluster.Spec.Router.HighAvailability && clusterReplicas(cluster.Spec.Router.Replicas, 2) < 2 {
		return fmt.Errorf("spec.router.replicas must be at least 2 when highAvailability is true")
	}
	if clusterReplicas(cluster.Spec.MetaStore.Replicas, 1) != 1 {
		return fmt.Errorf("spec.metaStore.replicas must be 1 because CubeStore MetaStore is single-writer")
	}
	return validateClusterOptions(cluster)
}

func clusterReplicas(value, fallback int32) int32 {
	if value > 0 {
		return value
	}
	return fallback
}
func clusterName(c *v1alpha1.CubeCluster, component string) string { return c.Name + "-" + component }
func clusterLabels(c *v1alpha1.CubeCluster, component string) map[string]string {
	return map[string]string{cubeClusterNameLabel: c.Name, cubeComponentLabel: component, "app.kubernetes.io/name": "cube", "app.kubernetes.io/instance": c.Name}
}
func (r *CubeClusterReconciler) own(c *v1alpha1.CubeCluster, obj client.Object) error {
	return controllerutil.SetControllerReference(c, obj, r.Scheme)
}

func (r *CubeClusterReconciler) reconcileConfigMap(ctx context.Context, c *v1alpha1.CubeCluster) error {
	obj := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: clusterName(c, "config"), Namespace: c.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, obj, func() error {
		if err := r.own(c, obj); err != nil {
			return err
		}
		obj.Labels = clusterLabels(c, "config")
		obj.Data = map[string]string{"CUBESTORE_META_ADDR": fmt.Sprintf("%s.%s.svc:9999", clusterName(c, cubeComponentMeta), c.Namespace), "CUBESTORE_WORKERS": workerAddresses(c), "CUBESTORE_OBJECT_STORE_ENDPOINT": c.Spec.Storage.Endpoint, "CUBESTORE_OBJECT_STORE_BUCKET": c.Spec.Storage.Bucket, "CUBESTORE_OBJECT_STORE_SUB_PATH": c.Spec.Storage.SubPath}
		return nil
	})
	return err
}

func workerAddresses(c *v1alpha1.CubeCluster) string {
	replicas := clusterReplicas(c.Spec.Workers.Replicas, 2)
	items := make([]string, 0, replicas)
	for i := int32(0); i < replicas; i++ {
		items = append(items, fmt.Sprintf("%s-%d.%s.%s.svc:10001", clusterName(c, cubeComponentWorker), i, clusterName(c, cubeComponentWorker), c.Namespace))
	}
	return strings.Join(items, ",")
}

func (r *CubeClusterReconciler) reconcileServiceAccounts(ctx context.Context, c *v1alpha1.CubeCluster) error {
	for _, component := range []string{cubeComponentRouter, cubeComponentAPI, cubeComponentMeta, cubeComponentWorker, cubeComponentRefresher} {
		obj := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: clusterName(c, component), Namespace: c.Namespace}}
		if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, obj, func() error {
			if err := r.own(c, obj); err != nil {
				return err
			}
			obj.Labels = clusterLabels(c, component)
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

func (r *CubeClusterReconciler) reconcileRouterRBAC(ctx context.Context, c *v1alpha1.CubeCluster) error {
	role := &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: clusterName(c, "router-lease-reader"), Namespace: c.Namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, role, func() error {
		if err := r.own(c, role); err != nil {
			return err
		}
		role.Labels = clusterLabels(c, cubeComponentRouter)
		role.Rules = []rbacv1.PolicyRule{{APIGroups: []string{"coordination.k8s.io"}, Resources: []string{"leases"}, Verbs: []string{"get"}}, {APIGroups: []string{""}, Resources: []string{"configmaps"}, ResourceNames: []string{clusterName(c, cubeComponentRouter) + "-role-state"}, Verbs: []string{"get"}}}
		return nil
	}); err != nil {
		return err
	}
	binding := &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: clusterName(c, "router-lease-reader"), Namespace: c.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, binding, func() error {
		if err := r.own(c, binding); err != nil {
			return err
		}
		binding.Labels = clusterLabels(c, cubeComponentRouter)
		binding.RoleRef = rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: role.Name}
		binding.Subjects = []rbacv1.Subject{{Kind: "ServiceAccount", Name: clusterName(c, cubeComponentRouter), Namespace: c.Namespace}}
		return nil
	})
	return err
}

func (r *CubeClusterReconciler) reconcileMetaStore(ctx context.Context, c *v1alpha1.CubeCluster) error {
	name := clusterName(c, cubeComponentMeta)
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: c.Namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, service, func() error {
		if err := r.own(c, service); err != nil {
			return err
		}
		service.Labels = clusterLabels(c, cubeComponentMeta)
		service.Spec.Selector = clusterLabels(c, cubeComponentMeta)
		service.Spec.Ports = []corev1.ServicePort{{Name: "metastore", Port: 9999, TargetPort: intstr.FromString("metastore")}}
		return nil
	}); err != nil {
		return err
	}
	set := &apps.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: c.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, set, func() error {
		if err := r.own(c, set); err != nil {
			return err
		}
		set.Labels = clusterLabels(c, cubeComponentMeta)
		set.Spec.ServiceName = name
		set.Spec.Replicas = ptr32(clusterReplicas(c.Spec.MetaStore.Replicas, 1))
		set.Spec.Selector = &metav1.LabelSelector{MatchLabels: clusterLabels(c, cubeComponentMeta)}
		set.Spec.Template = podTemplate(clusterLabels(c, cubeComponentMeta), clusterName(c, cubeComponentMeta), c.Spec.Images.MetaStore, clusterName(c, cubeComponentMeta), []corev1.ContainerPort{{Name: "metastore", ContainerPort: 9999}}, []corev1.EnvVar{{Name: "CUBESTORE_SERVER_NAME", ValueFrom: podNameField()}, {Name: "CUBESTORE_DATA_DIR", Value: "/cube/.cubestore/data"}, {Name: "CUBESTORE_META_BIND_ADDR", Value: "0.0.0.0:9999"}, {Name: "CUBESTORE_BIND_ADDR", Value: "127.0.0.1:3306"}, {Name: "CUBESTORE_HTTP_BIND_ADDR", Value: "127.0.0.1:3030"}}, []corev1.VolumeMount{{Name: "data", MountPath: "/cube/.cubestore/data"}})
		if set.CreationTimestamp.IsZero() && len(set.Spec.VolumeClaimTemplates) == 0 {
			set.Spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{{ObjectMeta: metav1.ObjectMeta{Name: "data", Labels: clusterLabels(c, cubeComponentMeta)}, Spec: pvcSpec(c)}}
		}
		set.Spec.Template.Spec.Containers[0].Env = mergeEnv(set.Spec.Template.Spec.Containers[0].Env, rustSharedEnv(c))
		return r.configurePod(ctx, c, &set.Spec.Template, c.Spec.MetaStore.Pod, 0, false)
	})
	return err
}

func (r *CubeClusterReconciler) reconcileWorkers(ctx context.Context, c *v1alpha1.CubeCluster) error {
	name := clusterName(c, cubeComponentWorker)
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: c.Namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, service, func() error {
		if err := r.own(c, service); err != nil {
			return err
		}
		service.Labels = clusterLabels(c, cubeComponentWorker)
		service.Spec.ClusterIP = corev1.ClusterIPNone
		// Stable worker DNS must resolve before dependency readiness succeeds.
		service.Spec.PublishNotReadyAddresses = true
		service.Spec.Selector = clusterLabels(c, cubeComponentWorker)
		service.Spec.Ports = []corev1.ServicePort{{Name: "worker", Port: 10001, TargetPort: intstr.FromString("worker")}}
		return nil
	}); err != nil {
		return err
	}
	set := &apps.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: c.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, set, func() error {
		if err := r.own(c, set); err != nil {
			return err
		}
		set.Labels = clusterLabels(c, cubeComponentWorker)
		set.Spec.ServiceName = name
		set.Spec.PodManagementPolicy = apps.ParallelPodManagement
		set.Spec.Replicas = ptr32(clusterReplicas(c.Spec.Workers.Replicas, 2))
		set.Spec.Selector = &metav1.LabelSelector{MatchLabels: clusterLabels(c, cubeComponentWorker)}
		env := []corev1.EnvVar{{Name: "POD_NAME", ValueFrom: podNameField()}, {Name: "CUBESTORE_SERVER_NAME", Value: fmt.Sprintf("$(POD_NAME).%s.%s.svc:10001", name, c.Namespace)}, {Name: "CUBESTORE_WORKER_PORT", Value: "10001"}, {Name: "CUBESTORE_NODE_NAME", ValueFrom: podNameField()}, {Name: "CUBESTORE_META_ADDR", Value: fmt.Sprintf("%s.%s.svc:9999", clusterName(c, cubeComponentMeta), c.Namespace)}, {Name: "CUBESTORE_WORKERS", Value: workerAddresses(c)}, {Name: "CUBESTORE_REMOTE_DIR", Value: "/cube/data"}, {Name: "CUBESTORE_DATA_DIR", Value: "/cube/.cubestore/data"}, {Name: "CUBESTORE_MINIO_BUCKET", Value: c.Spec.Storage.Bucket}, {Name: "CUBESTORE_MINIO_SUB_PATH", Value: c.Spec.Storage.SubPath}, {Name: "CUBESTORE_MINIO_SERVER_ENDPOINT", Value: c.Spec.Storage.Endpoint}}
		env = appendObjectStoreCredentials(env, c)
		set.Spec.Template = podTemplate(clusterLabels(c, cubeComponentWorker), name, c.Spec.Images.Worker, clusterName(c, cubeComponentWorker), []corev1.ContainerPort{{Name: "worker", ContainerPort: 10001}}, env, []corev1.VolumeMount{{Name: "data", MountPath: "/cube/data"}, {Name: "local-data", MountPath: "/cube/.cubestore/data"}})
		if set.CreationTimestamp.IsZero() && len(set.Spec.VolumeClaimTemplates) == 0 {
			set.Spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{{ObjectMeta: metav1.ObjectMeta{Name: "data", Labels: clusterLabels(c, cubeComponentWorker)}, Spec: pvcSpec(c)}, {ObjectMeta: metav1.ObjectMeta{Name: "local-data", Labels: clusterLabels(c, cubeComponentWorker)}, Spec: pvcSpec(c)}}
		}
		set.Spec.Template.Spec.Containers[0].Env = mergeEnv(set.Spec.Template.Spec.Containers[0].Env, rustSharedEnv(c))
		return r.configurePod(ctx, c, &set.Spec.Template, c.Spec.Workers.Pod, 0, false)
	})
	return err
}

func (r *CubeClusterReconciler) reconcileRouters(ctx context.Context, c *v1alpha1.CubeCluster) error {
	name := clusterName(c, cubeComponentRouter)
	all := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: c.Namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, all, func() error {
		if err := r.own(c, all); err != nil {
			return err
		}
		all.Labels = clusterLabels(c, cubeComponentRouter)
		all.Spec.Selector = clusterLabels(c, cubeComponentRouter)
		all.Spec.Ports = []corev1.ServicePort{{Name: "http", Port: 3030, TargetPort: intstr.FromInt(3030)}, {Name: "mysql", Port: 3306, TargetPort: intstr.FromInt(3306)}}
		return nil
	}); err != nil {
		return err
	}
	leader := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name + "-leader", Namespace: c.Namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, leader, func() error {
		if err := r.own(c, leader); err != nil {
			return err
		}
		leader.Labels = clusterLabels(c, cubeComponentRouter)
		leader.Spec.Selector = map[string]string{cubeClusterNameLabel: c.Name, cubeComponentLabel: cubeComponentRouter, "cubestore.io/router-role": "leader"}
		leader.Spec.Ports = []corev1.ServicePort{{Name: "http", Port: 3030, TargetPort: intstr.FromInt(3030)}, {Name: "mysql", Port: 3306, TargetPort: intstr.FromInt(3306)}}
		return nil
	}); err != nil {
		return err
	}
	roleConfig := name + "-role-state"
	routerCR := &v1alpha1.CubestoreRouter{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: c.Namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, routerCR, func() error {
		if err := r.own(c, routerCR); err != nil {
			return err
		}
		routerCR.Spec = v1alpha1.CubestoreRouterSpec{Selector: map[string]string{cubeClusterNameLabel: c.Name, cubeComponentLabel: cubeComponentRouter}, Namespace: c.Namespace, RouterPort: 3030, HealthPath: "/router/status", ElectionStrategy: v1alpha1.ElectionStrategyLease, RoleConfigMap: roleConfig, MetaStore: v1alpha1.MetaStore{Address: fmt.Sprintf("%s.%s.svc:9999", clusterName(c, cubeComponentMeta), c.Namespace)}, Storage: v1alpha1.Storage{DataPVC: "data"}, StateStore: &v1alpha1.StateStore{Type: "kubernetes"}}
		if c.Spec.Storage.ObjectStoreSecretRef != nil {
			ref := *c.Spec.Storage.ObjectStoreSecretRef
			if ref.Namespace == "" {
				ref.Namespace = c.Namespace
			}
			routerCR.Spec.Storage.ObjectStoreSecretRef = &ref
		}
		return nil
	}); err != nil {
		return err
	}
	set := &apps.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: c.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, set, func() error {
		if err := r.own(c, set); err != nil {
			return err
		}
		set.Labels = clusterLabels(c, cubeComponentRouter)
		set.Spec.Replicas = ptr32(clusterReplicas(c.Spec.Router.Replicas, 2))
		set.Spec.Selector = &metav1.LabelSelector{MatchLabels: clusterLabels(c, cubeComponentRouter)}
		routerEnv := []corev1.EnvVar{{Name: "POD_NAME", ValueFrom: podNameField()}, {Name: "CUBESTORE_SERVER_NAME", ValueFrom: podNameField()}, {Name: "CUBESTORE_ROUTER_ROLE_STRICT", Value: "true"}, {Name: "CUBESTORE_ROUTER_LEADERSHIP_FILE", Value: "/var/run/cubestore-ha/leadership.json"}, {Name: "CUBESTORE_ROUTER_PROMOTION_FILE", Value: "/var/run/cubestore-promotion/promotion.json"}, {Name: "CUBESTORE_HTTP_PORT", Value: "3030"}, {Name: "CUBESTORE_META_ADDR", Value: fmt.Sprintf("%s.%s.svc:9999", clusterName(c, cubeComponentMeta), c.Namespace)}, {Name: "CUBESTORE_NODE_NAME", ValueFrom: podNameField()}, {Name: "CUBESTORE_DATA_DIR", Value: "/cube/.cubestore/data"}, {Name: "CUBESTORE_MINIO_BUCKET", Value: c.Spec.Storage.Bucket}, {Name: "CUBESTORE_MINIO_SUB_PATH", Value: c.Spec.Storage.SubPath}, {Name: "CUBESTORE_MINIO_SERVER_ENDPOINT", Value: c.Spec.Storage.Endpoint}}
		routerEnv = mergeEnv(routerEnv, rustSharedEnv(c))
		router := corev1.Container{Name: "cube-studio-router", Image: c.Spec.Images.Router, ImagePullPolicy: corev1.PullIfNotPresent, Env: routerEnv, Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 3030}, {Name: "mysql", ContainerPort: 3306}}, VolumeMounts: []corev1.VolumeMount{{Name: "leadership", MountPath: "/var/run/cubestore-ha", ReadOnly: true}, {Name: "promotion", MountPath: "/var/run/cubestore-promotion", ReadOnly: true}, {Name: "local-data", MountPath: "/cube/.cubestore/data"}}}
		agent := corev1.Container{Name: "lease-agent", Image: c.Spec.Images.LeaseAgent, ImagePullPolicy: corev1.PullIfNotPresent, Command: []string{"/usr/local/bin/lease-agent"}, Args: []string{"--cluster-id=" + c.Namespace + "/" + name, "--holder-id=$(POD_NAME)", "--backend=kubernetes", "--retry-period=2s", "--sync-timeout=2s", "--promotion-config-map=" + roleConfig}, Env: []corev1.EnvVar{{Name: "POD_NAME", ValueFrom: podNameField()}, {Name: "POD_NAMESPACE", ValueFrom: namespaceField()}, {Name: "CUBESTORE_LEASE_K8S_NAMESPACE", Value: c.Namespace}, {Name: "CUBESTORE_LEASE_K8S_NAME", Value: name}}, VolumeMounts: []corev1.VolumeMount{{Name: "leadership", MountPath: "/var/run/cubestore-ha"}, {Name: "promotion", MountPath: "/var/run/cubestore-promotion"}}}
		set.Spec.Template = corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: clusterLabels(c, cubeComponentRouter)}, Spec: corev1.PodSpec{ServiceAccountName: name, TerminationGracePeriodSeconds: ptr64(30), Containers: []corev1.Container{agent, router}, Volumes: []corev1.Volume{{Name: "leadership", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory}}}, {Name: "promotion", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory}}}, {Name: "local-data", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}}}}
		set.Spec.Template.Spec.Affinity = routerAntiAffinity(c)
		if len(c.Spec.Router.DrainCommand) > 0 {
			set.Spec.Template.Spec.TerminationGracePeriodSeconds = ptr64(60)
			set.Spec.Template.Spec.Containers[1].Lifecycle = &corev1.Lifecycle{PreStop: &corev1.LifecycleHandler{Exec: &corev1.ExecAction{Command: append([]string(nil), c.Spec.Router.DrainCommand...)}}}
		}
		return r.configurePod(ctx, c, &set.Spec.Template, c.Spec.Router.Pod, 1, false)
	})
	return err
}

func (r *CubeClusterReconciler) reconcileAPI(ctx context.Context, c *v1alpha1.CubeCluster) error {
	name := clusterName(c, cubeComponentAPI)
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: c.Namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, service, func() error {
		if err := r.own(c, service); err != nil {
			return err
		}
		service.Labels = clusterLabels(c, cubeComponentAPI)
		service.Spec.Selector = clusterLabels(c, cubeComponentAPI)
		service.Spec.Ports = []corev1.ServicePort{{Name: "http", Port: 4000, TargetPort: intstr.FromInt(4000)}}
		return nil
	}); err != nil {
		return err
	}
	set := &apps.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: c.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, set, func() error {
		if err := r.own(c, set); err != nil {
			return err
		}
		set.Labels = clusterLabels(c, cubeComponentAPI)
		set.Spec.Replicas = ptr32(clusterReplicas(c.Spec.API.Replicas, 1))
		set.Spec.Selector = &metav1.LabelSelector{MatchLabels: clusterLabels(c, cubeComponentAPI)}
		pod, err := r.apiPod(ctx, c, cubeComponentAPI, c.Spec.API.Pod)
		if err != nil {
			return err
		}
		set.Spec.Template = pod
		return nil
	})
	return err
}

const cubeAPISecretKey = "apiSecret"

func apiSecretName(c *v1alpha1.CubeCluster) string {
	if c.Spec.APISecretRef != nil {
		return c.Spec.APISecretRef.Name
	}
	return c.Name + "-api-secret"
}

func (r *CubeClusterReconciler) reconcileAPISecret(ctx context.Context, c *v1alpha1.CubeCluster) error {
	if c.Spec.APISecretRef != nil {
		var secret corev1.Secret
		if err := r.Get(ctx, client.ObjectKey{Namespace: c.Namespace, Name: c.Spec.APISecretRef.Name}, &secret); err != nil {
			return err
		}
		if len(secret.Data[c.Spec.APISecretRef.Key]) == 0 {
			return fmt.Errorf("API secret %s is missing nonempty key %s", secret.Name, c.Spec.APISecretRef.Key)
		}
		return nil
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: apiSecretName(c), Namespace: c.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, secret, func() error {
		if err := r.own(c, secret); err != nil {
			return err
		}
		secret.Labels = clusterLabels(c, cubeComponentAPI)
		secret.Type = corev1.SecretTypeOpaque
		if !printableSecret(secret.Data[cubeAPISecretKey]) {
			value := make([]byte, 32)
			if _, err := rand.Read(value); err != nil {
				return err
			}
			if secret.Data == nil {
				secret.Data = map[string][]byte{}
			}
			secret.Data[cubeAPISecretKey] = []byte(hex.EncodeToString(value))
		}
		return nil
	})
	return err
}

func nameRouterLeader(c *v1alpha1.CubeCluster) string {
	return clusterName(c, cubeComponentRouter) + "-leader"
}

func routerAntiAffinity(c *v1alpha1.CubeCluster) *corev1.Affinity {
	return &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{{Weight: 100, PodAffinityTerm: corev1.PodAffinityTerm{TopologyKey: "kubernetes.io/hostname", LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{cubeClusterNameLabel: c.Name, cubeComponentLabel: cubeComponentRouter}}}}}}}
}

func (r *CubeClusterReconciler) reconcilePDBs(ctx context.Context, c *v1alpha1.CubeCluster) error {
	for component, spec := range componentSpecs(c) {
		obj := &policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Name: clusterName(c, component), Namespace: c.Namespace}}
		if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, obj, func() error {
			if err := r.own(c, obj); err != nil {
				return err
			}
			obj.Labels = clusterLabels(c, component)
			obj.Spec.MinAvailable, obj.Spec.MaxUnavailable = nil, nil
			if spec.Replicas == 1 && !spec.AllowSingleReplicaDisruption {
				// Protect singleton data/API processes by default: node drains block.
				obj.Spec.MinAvailable = intstrPtr(1)
			} else {
				obj.Spec.MaxUnavailable = intstrPtr(1)
			}
			obj.Spec.Selector = &metav1.LabelSelector{MatchLabels: clusterLabels(c, component)}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

func (r *CubeClusterReconciler) setClusterCondition(ctx context.Context, c *v1alpha1.CubeCluster, typ string, status metav1.ConditionStatus, reason, message string) (ctrl.Result, error) {
	next := c.DeepCopy()
	next.Status = v1alpha1.CubeClusterStatus{Phase: "Invalid", ObservedGeneration: c.Generation}
	apiMeta.SetStatusCondition(&next.Status.Conditions, newClusterCondition(c, typ, status == metav1.ConditionTrue, reason, message))
	apiMeta.SetStatusCondition(&next.Status.Conditions, newClusterCondition(c, "ProductionReady", false, reason, message))
	observeClusterConditions(next, false)
	if statusEqualCubeCluster(c.Status, next.Status) {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	return ctrl.Result{RequeueAfter: 5 * time.Second}, r.Status().Update(ctx, next)
}
func newClusterCondition(c *v1alpha1.CubeCluster, typ string, ok bool, reason, message string) metav1.Condition {
	status := metav1.ConditionFalse
	if ok {
		status = metav1.ConditionTrue
	}
	return metav1.Condition{Type: typ, Status: status, ObservedGeneration: c.Generation, LastTransitionTime: metav1.Now(), Reason: reason, Message: message}
}
func statusEqualCubeCluster(a, b v1alpha1.CubeClusterStatus) bool {
	// Ignore timestamps only, never generations, component counters or recovery.
	a.Conditions = append([]metav1.Condition(nil), a.Conditions...)
	b.Conditions = append([]metav1.Condition(nil), b.Conditions...)
	for i := range a.Conditions {
		a.Conditions[i].LastTransitionTime = metav1.Time{}
	}
	for i := range b.Conditions {
		b.Conditions[i].LastTransitionTime = metav1.Time{}
	}
	return reflect.DeepEqual(a, b)
}

func podTemplate(labels map[string]string, serviceAccount, image, name string, ports []corev1.ContainerPort, env []corev1.EnvVar, mounts []corev1.VolumeMount) corev1.PodTemplateSpec {
	return corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels}, Spec: corev1.PodSpec{ServiceAccountName: serviceAccount, Containers: []corev1.Container{{Name: name, Image: image, ImagePullPolicy: corev1.PullIfNotPresent, Env: env, Ports: ports, VolumeMounts: mounts}}}}
}
func appendObjectStoreCredentials(env []corev1.EnvVar, c *v1alpha1.CubeCluster) []corev1.EnvVar {
	if ref := c.Spec.Storage.ObjectStoreSecretRef; ref != nil {
		env = append(env, corev1.EnvVar{Name: "CUBESTORE_MINIO_ACCESS_KEY_ID", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: ref.Name}, Key: "accessKey"}}}, corev1.EnvVar{Name: "CUBESTORE_MINIO_SECRET_ACCESS_KEY", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: ref.Name}, Key: "secretKey"}}})
	}
	return env
}
func pvcSpec(c *v1alpha1.CubeCluster) corev1.PersistentVolumeClaimSpec {
	return corev1.PersistentVolumeClaimSpec{StorageClassName: c.Spec.Storage.StorageClassName, AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resourceQuantity(c.Spec.Storage.DataSize)}}}
}
func resourceQuantity(value string) resource.Quantity {
	if value == "" {
		value = "2Gi"
	}
	q, err := resource.ParseQuantity(value)
	if err != nil {
		return resource.MustParse("2Gi")
	}
	return q
}
func ptr32(v int32) *int32                { return &v }
func ptr64(v int64) *int64                { return &v }
func ptrBool(v bool) *bool                { return &v }
func intstrPtr(v int) *intstr.IntOrString { x := intstr.FromInt(v); return &x }
func podNameField() *corev1.EnvVarSource {
	return &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}
}
func namespaceField() *corev1.EnvVarSource {
	return &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}
}
