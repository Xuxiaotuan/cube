package controllers

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cube-js/cube-operator/api/v1alpha1"
	apps "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
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
// +kubebuilder:rbac:groups="",resources=configmaps;services;serviceaccounts,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles;rolebindings,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch
type CubeClusterReconciler struct {
	client.Client
	*runtime.Scheme
}

func (r *CubeClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.CubeCluster{}).
		Owns(&apps.Deployment{}).
		Owns(&apps.StatefulSet{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&v1alpha1.CubestoreRouter{}).
		Complete(r)
}

func (r *CubeClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var cluster v1alpha1.CubeCluster
	if err := r.Get(ctx, req.NamespacedName, &cluster); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if err := validateCubeCluster(&cluster); err != nil {
		return r.setClusterCondition(ctx, &cluster, cubeClusterConditionResources, metav1.ConditionFalse, "InvalidSpec", err.Error())
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
	if err := r.reconcileWorkers(ctx, &cluster); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcileRouters(ctx, &cluster); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcileAPI(ctx, &cluster); err != nil {
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
	if cluster.Spec.Storage.ObjectStoreSecretRef != nil && cluster.Spec.Storage.ObjectStoreSecretRef.Namespace != cluster.Namespace {
		return fmt.Errorf("spec.storage.objectStoreSecretRef must be in the CubeCluster namespace")
	}
	if cluster.Spec.Router.HighAvailability && clusterReplicas(cluster.Spec.Router.Replicas, 2) < 2 {
		return fmt.Errorf("spec.router.replicas must be at least 2 when highAvailability is true")
	}
	if clusterReplicas(cluster.Spec.MetaStore.Replicas, 1) != 1 {
		return fmt.Errorf("spec.metaStore.replicas must be 1 because CubeStore MetaStore is single-writer")
	}
	return nil
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
	for _, component := range []string{cubeComponentRouter, cubeComponentAPI, cubeComponentMeta, cubeComponentWorker} {
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
		role.Rules = []rbacv1.PolicyRule{{APIGroups: []string{"coordination.k8s.io"}, Resources: []string{"leases"}, Verbs: []string{"get"}}}
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
		set.Spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{{ObjectMeta: metav1.ObjectMeta{Name: "data", Labels: clusterLabels(c, cubeComponentMeta)}, Spec: pvcSpec(c)}}
		return nil
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
		set.Spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{{ObjectMeta: metav1.ObjectMeta{Name: "data", Labels: clusterLabels(c, cubeComponentWorker)}, Spec: pvcSpec(c)}, {ObjectMeta: metav1.ObjectMeta{Name: "local-data", Labels: clusterLabels(c, cubeComponentWorker)}, Spec: pvcSpec(c)}}
		return nil
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
		routerEnv = appendObjectStoreCredentials(routerEnv, c)
		router := corev1.Container{Name: "cube-studio-router", Image: c.Spec.Images.Router, ImagePullPolicy: corev1.PullIfNotPresent, Env: routerEnv, Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 3030}, {Name: "mysql", ContainerPort: 3306}}, VolumeMounts: []corev1.VolumeMount{{Name: "leadership", MountPath: "/var/run/cubestore-ha", ReadOnly: true}, {Name: "promotion", MountPath: "/var/run/cubestore-promotion", ReadOnly: true}, {Name: "local-data", MountPath: "/cube/.cubestore/data"}}}
		agent := corev1.Container{Name: "lease-agent", Image: c.Spec.Images.LeaseAgent, ImagePullPolicy: corev1.PullIfNotPresent, Command: []string{"/usr/local/bin/lease-agent"}, Args: []string{"--cluster-id=" + c.Namespace + "/" + name, "--holder-id=$(POD_NAME)", "--backend=kubernetes", "--retry-period=2s"}, Env: []corev1.EnvVar{{Name: "POD_NAME", ValueFrom: podNameField()}, {Name: "POD_NAMESPACE", ValueFrom: namespaceField()}, {Name: "CUBESTORE_LEASE_K8S_NAMESPACE", Value: c.Namespace}, {Name: "CUBESTORE_LEASE_K8S_NAME", Value: name}}, VolumeMounts: []corev1.VolumeMount{{Name: "leadership", MountPath: "/var/run/cubestore-ha"}}}
		set.Spec.Template = corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: clusterLabels(c, cubeComponentRouter)}, Spec: corev1.PodSpec{ServiceAccountName: name, TerminationGracePeriodSeconds: ptr64(30), Containers: []corev1.Container{agent, router}, Volumes: []corev1.Volume{{Name: "leadership", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory}}}, {Name: "promotion", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: roleConfig}, Optional: ptrBool(true), Items: []corev1.KeyToPath{{Key: "route-role.json", Path: "promotion.json"}}}}}, {Name: "local-data", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}}}}
		return nil
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
		set.Spec.Template = podTemplate(clusterLabels(c, cubeComponentAPI), name, c.Spec.Images.API, name, []corev1.ContainerPort{{Name: "http", ContainerPort: 4000}}, []corev1.EnvVar{{Name: "CUBEJS_DB_TYPE", Value: "cubestore"}, {Name: "CUBEJS_EXT_DB_TYPE", Value: "cubestore"}, {Name: "CUBEJS_CUBESTORE_HOST", Value: fmt.Sprintf("%s.%s.svc.cluster.local", nameRouterLeader(c), c.Namespace)}, {Name: "CUBEJS_CUBESTORE_PORT", Value: "3030"}, {Name: "CUBEJS_API_SECRET", Value: c.Name + "-api-secret"}, {Name: "CUBEJS_SCHEMA_PATH", Value: "schema"}, {Name: "CUBEJS_PORT", Value: "4000"}}, nil)
		return nil
	})
	return err
}

func nameRouterLeader(c *v1alpha1.CubeCluster) string {
	return clusterName(c, cubeComponentRouter) + "-leader"
}

func (r *CubeClusterReconciler) reconcilePDBs(ctx context.Context, c *v1alpha1.CubeCluster) error {
	for _, component := range []string{cubeComponentRouter, cubeComponentMeta, cubeComponentWorker} {
		obj := &policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Name: clusterName(c, component), Namespace: c.Namespace}}
		if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, obj, func() error {
			if err := r.own(c, obj); err != nil {
				return err
			}
			obj.Labels = clusterLabels(c, component)
			obj.Spec.MinAvailable = intstrPtr(1)
			obj.Spec.Selector = &metav1.LabelSelector{MatchLabels: clusterLabels(c, component)}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

func (r *CubeClusterReconciler) reconcileStatus(ctx context.Context, c *v1alpha1.CubeCluster) error {
	var api apps.Deployment
	var router apps.Deployment
	var meta apps.StatefulSet
	var worker apps.StatefulSet
	var routerCR v1alpha1.CubestoreRouter
	get := func(obj client.Object, name string) error {
		return r.Get(ctx, client.ObjectKey{Namespace: c.Namespace, Name: name}, obj)
	}
	if err := get(&api, clusterName(c, cubeComponentAPI)); err != nil {
		return err
	}
	if err := get(&router, clusterName(c, cubeComponentRouter)); err != nil {
		return err
	}
	if err := get(&meta, clusterName(c, cubeComponentMeta)); err != nil {
		return err
	}
	if err := get(&worker, clusterName(c, cubeComponentWorker)); err != nil {
		return err
	}
	if err := get(&routerCR, clusterName(c, cubeComponentRouter)); err != nil {
		return err
	}
	ready := api.Status.ReadyReplicas + router.Status.ReadyReplicas + meta.Status.ReadyReplicas + worker.Status.ReadyReplicas
	desired := clusterReplicas(c.Spec.API.Replicas, 1) + clusterReplicas(c.Spec.Router.Replicas, 2) + clusterReplicas(c.Spec.MetaStore.Replicas, 1) + clusterReplicas(c.Spec.Workers.Replicas, 2)
	resourcesOK := ready >= desired
	haOK := false
	for _, condition := range routerCR.Status.Conditions {
		if condition.Type == v1alpha1.CubestoreRouterConditionPromotionReady {
			haOK = condition.Status == metav1.ConditionTrue
		}
	}
	phase := "Provisioning"
	if resourcesOK {
		phase = "Running"
	}
	if resourcesOK && !haOK {
		phase = "Degraded"
	}
	next := c.DeepCopy()
	next.Status.Phase = phase
	next.Status.ReadyReplicas = ready
	next.Status.Conditions = []metav1.Condition{newClusterCondition(c, cubeClusterConditionResources, resourcesOK, "ChildResources", fmt.Sprintf("%d/%d replicas ready", ready, desired)), newClusterCondition(c, cubeClusterConditionStorage, true, "Configured", "external object storage endpoint is configured"), newClusterCondition(c, cubeClusterConditionHA, haOK, "RouterController", "Router HA readiness is delegated to CubestoreRouter status")}
	if statusEqualCubeCluster(c.Status, next.Status) {
		return nil
	}
	return r.Status().Update(ctx, next)
}

func (r *CubeClusterReconciler) setClusterCondition(ctx context.Context, c *v1alpha1.CubeCluster, typ string, status metav1.ConditionStatus, reason, message string) (ctrl.Result, error) {
	next := c.DeepCopy()
	next.Status.Phase = "Invalid"
	next.Status.Conditions = []metav1.Condition{newClusterCondition(c, typ, status == metav1.ConditionTrue, reason, message)}
	if statusEqualCubeCluster(c.Status, next.Status) {
		return ctrl.Result{}, nil
	}
	if err := r.Status().Update(ctx, next); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}
func newClusterCondition(c *v1alpha1.CubeCluster, typ string, ok bool, reason, message string) metav1.Condition {
	status := metav1.ConditionFalse
	if ok {
		status = metav1.ConditionTrue
	}
	return metav1.Condition{Type: typ, Status: status, ObservedGeneration: c.Generation, LastTransitionTime: metav1.Now(), Reason: reason, Message: message}
}
func statusEqualCubeCluster(a, b v1alpha1.CubeClusterStatus) bool {
	if a.Phase != b.Phase || a.ReadyReplicas != b.ReadyReplicas || len(a.Conditions) != len(b.Conditions) {
		return false
	}
	for i := range a.Conditions {
		if a.Conditions[i].Type != b.Conditions[i].Type || a.Conditions[i].Status != b.Conditions[i].Status || a.Conditions[i].Reason != b.Conditions[i].Reason || a.Conditions[i].Message != b.Conditions[i].Message || a.Conditions[i].ObservedGeneration != b.Conditions[i].ObservedGeneration {
			return false
		}
	}
	return true
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
	return corev1.PersistentVolumeClaimSpec{AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resourceQuantity(c.Spec.Storage.DataSize)}}}
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
