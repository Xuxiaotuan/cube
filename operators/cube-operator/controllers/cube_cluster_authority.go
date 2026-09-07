package controllers

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/cube-js/cube-operator/api/v1alpha1"
	"github.com/cube-js/cube-operator/internal/leadership"
	apps "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	authorityAudience = "cubestore-metastore-authority-v1"
	// Deployment port, explicitly passed to Rust BIND_ADDR (no Rust default).
	authorityPort                int32 = 9443
	authorityBootstrapAnnotation       = "cubestore.io/authority-bootstrap"
	authorityLeaseUIDAnnotation        = "cubestore.io/authority-lease-uid"
	authorityTLSUIDAnnotation          = "cubestore.io/authority-tls-uid"
	authorityBindingFinalizer          = "cubestore.io/authority-tokenreview-binding"
	authorityReviewRole                = "cubestore-authority-tokenreview"
	authorityDirectory                 = "/var/run/cubestore-authority"
	authorityAPIDirectory              = "/var/run/secrets/kubernetes.io/serviceaccount"
	// Actual LeaseStore protocol keys, NOT the cubestore.io CR/Pod fence keys.
	authorityLeaseClusterAnnotation = "cubejs.io/lease-cluster-id"
	authorityLeaseTokenAnnotation   = "cubejs.io/lease-token"
)

func authorityLeaseName(c *v1alpha1.CubeCluster) string {
	return kubernetesLeaseName(&v1alpha1.CubestoreRouter{ObjectMeta: metav1.ObjectMeta{Name: clusterName(c, cubeComponentRouter), Namespace: c.Namespace}})
}

func validateAuthoritySpec(c *v1alpha1.CubeCluster) error {
	a := c.Spec.Authority
	if a == nil {
		return nil
	}
	if a.APITimeoutMS <= 0 || a.APITimeoutMS > 2147483647 || a.ValidationTimeoutMS <= 0 || a.ValidationTimeoutMS > 2147483647 || a.MaxClockSkewMS == nil || *a.MaxClockSkewMS < 0 || *a.MaxClockSkewMS > 2147483647 {
		return fmt.Errorf("authority requires explicit positive API/validation budgets and nonnegative maxClockSkewMs, all <= 2147483647")
	}
	if a.APITimeoutMS > a.ValidationTimeoutMS {
		return fmt.Errorf("authority apiTimeoutMs must not exceed validationTimeoutMs")
	}
	return nil
}

// This is deliberately not a migration protocol. No annotation can authorize
// rolling a legacy writer into strict mode or adopting restored durable state.
func (r *CubeClusterReconciler) validateAuthorityTransition(ctx context.Context, c *v1alpha1.CubeCluster) error {
	reserved := c.Annotations[authorityBootstrapAnnotation] != ""
	if c.Spec.Authority == nil {
		if reserved {
			return fmt.Errorf("strict authority cannot be removed by an ordinary rollout; stop-write migration required")
		}
		return nil
	}
	if r.APIReader == nil || c.UID == "" {
		return fmt.Errorf("authority requires persisted identity and uncached APIReader")
	}
	var fresh v1alpha1.CubeCluster
	if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(c), &fresh); err != nil {
		return err
	}
	if fresh.UID != c.UID || fresh.ResourceVersion != c.ResourceVersion || fresh.DeletionTimestamp != nil {
		return fmt.Errorf("CubeCluster changed; retry with current identity/spec")
	}
	if reserved && c.Annotations[authorityBootstrapAnnotation] != string(c.UID) {
		return fmt.Errorf("authority bootstrap belongs to a different cluster UID")
	}
	for _, component := range []string{cubeComponentMeta, cubeComponentWorker, cubeComponentRouter, cubeComponentAPI, cubeComponentRefresher} {
		var obj client.Object
		var template *corev1.PodTemplateSpec
		if component == cubeComponentMeta || component == cubeComponentWorker {
			s := &apps.StatefulSet{}
			obj, template = s, &s.Spec.Template
		} else {
			d := &apps.Deployment{}
			obj, template = d, &d.Spec.Template
		}
		err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: c.Namespace, Name: clusterName(c, component)}, obj)
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return err
		}
		if !reserved || !metav1.IsControlledBy(obj, c) {
			return fmt.Errorf("existing %s requires an explicit stopped-write migration; no workload changed", obj.GetName())
		}
		if component == cubeComponentAPI || component == cubeComponentRefresher {
			continue
		}
		if len(template.Spec.Containers) == 0 {
			return fmt.Errorf("missing strict container contract on %s", obj.GetName())
		}
		for _, container := range template.Spec.Containers {
			if authorityEnvValue(container.Env, "STRICT") != "true" || authorityEnvValue(container.Env, "LEASE_UID") != c.Annotations[authorityLeaseUIDAnnotation] || c.Annotations[authorityLeaseUIDAnnotation] == "" {
				return fmt.Errorf("existing %s is not bound to this strict Lease; stop-write migration required", obj.GetName())
			}
		}
	}
	if reserved {
		return nil
	}
	// Include orphaned Pods/PVCs and election history, not only Deployments.
	var pods corev1.PodList
	if err := r.APIReader.List(ctx, &pods, client.InNamespace(c.Namespace), client.MatchingLabels{cubeClusterNameLabel: c.Name}); err != nil {
		return err
	}
	if len(pods.Items) != 0 {
		return fmt.Errorf("surviving cluster Pods require stopped-write recovery")
	}
	var claims corev1.PersistentVolumeClaimList
	if err := r.APIReader.List(ctx, &claims, client.InNamespace(c.Namespace)); err != nil {
		return err
	}
	for _, pvc := range claims.Items {
		if pvc.Labels[cubeClusterNameLabel] == c.Name || strings.HasPrefix(pvc.Name, "data-"+clusterName(c, cubeComponentMeta)+"-") || strings.HasPrefix(pvc.Name, "data-"+clusterName(c, cubeComponentWorker)+"-") || strings.HasPrefix(pvc.Name, "local-data-"+clusterName(c, cubeComponentWorker)+"-") {
			return fmt.Errorf("retained PVC %s requires a separately verified durable-state migration", pvc.Name)
		}
	}
	for _, obj := range []client.Object{
		&v1alpha1.CubestoreRouter{ObjectMeta: metav1.ObjectMeta{Name: clusterName(c, cubeComponentRouter), Namespace: c.Namespace}},
		&coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: authorityLeaseName(c), Namespace: c.Namespace}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: clusterName(c, cubeComponentRouter) + "-role-state", Namespace: c.Namespace}},
	} {
		err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(obj), obj)
		if err == nil {
			return fmt.Errorf("existing election object %s requires explicit recovery", obj.GetName())
		}
		if !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

func (r *CubeClusterReconciler) prepareAuthority(ctx context.Context, c *v1alpha1.CubeCluster) error {
	if !controllerutil.ContainsFinalizer(c, authorityBindingFinalizer) || c.Annotations[authorityBootstrapAnnotation] == "" {
		controllerutil.AddFinalizer(c, authorityBindingFinalizer)
		if c.Annotations == nil {
			c.Annotations = map[string]string{}
		}
		c.Annotations[authorityBootstrapAnnotation] = string(c.UID)
		if err := r.Update(ctx, c); err != nil {
			return err
		}
	}
	if uid := c.Annotations[authorityTLSUIDAnnotation]; uid != "" {
		var secret corev1.Secret
		if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: c.Namespace, Name: metaStoreAuthorityTLSName(c)}, &secret); err != nil {
			return fmt.Errorf("pinned TLS Secret unavailable; maintenance required: %w", err)
		}
		if string(secret.UID) != uid {
			return fmt.Errorf("TLS Secret UID changed; automatic CA replacement forbidden")
		}
	}
	tls, err := r.ensureMetaStoreAuthorityTLS(ctx, c)
	if err != nil {
		return err
	}
	if tls.UID == "" {
		return fmt.Errorf("TLS Secret has no API-assigned UID")
	}
	if err := r.reconcileAuthorityRBAC(ctx, c); err != nil {
		return err
	}
	if c.Annotations[authorityLeaseUIDAnnotation] != "" {
		var cr v1alpha1.CubestoreRouter
		if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: c.Namespace, Name: clusterName(c, cubeComponentRouter)}, &cr); err != nil {
			return fmt.Errorf("pinned router definition unavailable: %w", err)
		}
		if !metav1.IsControlledBy(&cr, c) || cr.Annotations[leaseBootstrapAnnotation] == "" {
			return fmt.Errorf("router bootstrap history lost")
		}
	}
	if err := r.reconcileRouterDefinition(ctx, c); err != nil {
		return err
	}
	var lease coordinationv1.Lease
	key := client.ObjectKey{Namespace: c.Namespace, Name: authorityLeaseName(c)}
	err = r.APIReader.Get(ctx, key, &lease)
	if apierrors.IsNotFound(err) {
		if c.Annotations[authorityLeaseUIDAnnotation] != "" {
			return errLeaseStateLost
		}
		var cr v1alpha1.CubestoreRouter
		if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: c.Namespace, Name: clusterName(c, cubeComponentRouter)}, &cr); err != nil {
			return err
		}
		router := &CubestoreRouterReconciler{Client: r.Client, APIReader: r.APIReader, Scheme: r.Scheme}
		if err := router.reserveLeaseBootstrap(ctx, &cr); err != nil {
			return err
		}
		lease = coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace, Annotations: map[string]string{authorityBootstrapAnnotation: string(c.UID), authorityLeaseClusterAnnotation: c.Namespace + "/" + clusterName(c, cubeComponentRouter), "cubejs.io/lease-generation": "1"}}}
		if err := r.own(c, &lease); err != nil {
			return err
		}
		// Intentionally no holder, epoch or token until an actual Pod candidate exists.
		if err := r.Create(ctx, &lease); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	if lease.UID == "" || !metav1.IsControlledBy(&lease, c) || lease.DeletionTimestamp != nil || lease.Annotations[authorityBootstrapAnnotation] != string(c.UID) || lease.Annotations[authorityLeaseClusterAnnotation] != c.Namespace+"/"+clusterName(c, cubeComponentRouter) {
		return fmt.Errorf("authority Lease ownership/identity rejected")
	}
	if uid := c.Annotations[authorityLeaseUIDAnnotation]; uid != "" && uid != string(lease.UID) {
		return fmt.Errorf("Lease UID changed; durable authority recovery required")
	}
	if c.Annotations[authorityLeaseUIDAnnotation] != string(lease.UID) || c.Annotations[authorityTLSUIDAnnotation] != string(tls.UID) {
		c.Annotations[authorityLeaseUIDAnnotation], c.Annotations[authorityTLSUIDAnnotation] = string(lease.UID), string(tls.UID)
		return r.Update(ctx, c)
	}
	return nil
}

func authorityTokenReviewRules() []rbacv1.PolicyRule {
	return []rbacv1.PolicyRule{{APIGroups: []string{"authentication.k8s.io"}, Resources: []string{"tokenreviews"}, Verbs: []string{"create"}}}
}
func authorityBindingName(c *v1alpha1.CubeCluster) string {
	return "cubestore-authority-" + string(c.UID)
}
func authorityBinding(c *v1alpha1.CubeCluster) *rbacv1.ClusterRoleBinding {
	return &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: authorityBindingName(c), Labels: map[string]string{authorityBootstrapAnnotation: string(c.UID)}},
		RoleRef:  rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: authorityReviewRole},
		Subjects: []rbacv1.Subject{{Kind: "ServiceAccount", Namespace: c.Namespace, Name: clusterName(c, cubeComponentMeta)}}}
}
func (r *CubeClusterReconciler) reconcileAuthorityRBAC(ctx context.Context, c *v1alpha1.CubeCluster) error {
	// The installer, not the Operator, installs this fixed, unaggregated role.
	var review rbacv1.ClusterRole
	if err := r.APIReader.Get(ctx, client.ObjectKey{Name: authorityReviewRole}, &review); err != nil {
		return err
	}
	if review.AggregationRule != nil || !reflect.DeepEqual(review.Rules, authorityTokenReviewRules()) {
		return fmt.Errorf("TokenReview ClusterRole must contain only create tokenreviews")
	}
	want := authorityBinding(c)
	var existing rbacv1.ClusterRoleBinding
	err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(want), &existing)
	if apierrors.IsNotFound(err) {
		if err := r.Create(ctx, want); err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else if existing.DeletionTimestamp != nil || existing.Labels[authorityBootstrapAnnotation] != string(c.UID) || !reflect.DeepEqual(existing.RoleRef, want.RoleRef) || !reflect.DeepEqual(existing.Subjects, want.Subjects) {
		return fmt.Errorf("foreign or broadened authority ClusterRoleBinding; refusing adoption")
	}
	role := &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: clusterName(c, "authority-reader"), Namespace: c.Namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, role, func() error {
		if err := r.own(c, role); err != nil {
			return err
		}
		role.Rules = []rbacv1.PolicyRule{{APIGroups: []string{"coordination.k8s.io"}, Resources: []string{"leases"}, ResourceNames: []string{authorityLeaseName(c)}, Verbs: []string{"get"}}, {APIGroups: []string{""}, Resources: []string{"pods"}, Verbs: []string{"get"}}}
		return nil
	}); err != nil {
		return err
	}
	binding := &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: role.Name, Namespace: c.Namespace}}
	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, binding, func() error {
		if err := r.own(c, binding); err != nil {
			return err
		}
		binding.RoleRef = rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: role.Name}
		binding.Subjects = want.Subjects
		return nil
	})
	return err
}

func (r *CubeClusterReconciler) cleanupAuthorityBinding(ctx context.Context, c *v1alpha1.CubeCluster) error {
	if !controllerutil.ContainsFinalizer(c, authorityBindingFinalizer) {
		return nil
	}
	if r.APIReader == nil {
		return fmt.Errorf("authority cleanup requires APIReader")
	}
	var binding rbacv1.ClusterRoleBinding
	err := r.APIReader.Get(ctx, client.ObjectKey{Name: authorityBindingName(c)}, &binding)
	if err == nil {
		want := authorityBinding(c)
		if binding.Labels[authorityBootstrapAnnotation] != string(c.UID) || !reflect.DeepEqual(binding.RoleRef, want.RoleRef) || !reflect.DeepEqual(binding.Subjects, want.Subjects) {
			return fmt.Errorf("authority binding ownership changed; manual cleanup required")
		}
		uid, rv := binding.UID, binding.ResourceVersion
		if err := r.Delete(ctx, &binding, &client.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &rv}}); client.IgnoreNotFound(err) != nil {
			return err
		}
	} else if !apierrors.IsNotFound(err) {
		return err
	}
	controllerutil.RemoveFinalizer(c, authorityBindingFinalizer)
	return r.Update(ctx, c)
}

func authorityEnvValue(env []corev1.EnvVar, suffix string) string {
	for _, v := range env {
		if v.Name == "CUBESTORE_AUTHORITY_"+suffix && v.ValueFrom == nil {
			return v.Value
		}
	}
	return ""
}

func (r *CubeClusterReconciler) configureAuthorityPod(ctx context.Context, c *v1alpha1.CubeCluster, t *corev1.PodTemplateSpec, role string) error {
	if c.Spec.Authority == nil {
		return nil
	}
	if err := validateAuthoritySpec(c); err != nil {
		return err
	}
	if r.APIReader == nil {
		return fmt.Errorf("authority rendering requires APIReader")
	}
	var lease coordinationv1.Lease
	if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: c.Namespace, Name: authorityLeaseName(c)}, &lease); err != nil {
		return err
	}
	if lease.UID == "" || string(lease.UID) != c.Annotations[authorityLeaseUIDAnnotation] || !metav1.IsControlledBy(&lease, c) || lease.DeletionTimestamp != nil {
		return fmt.Errorf("cannot render unpinned authority Lease")
	}
	a := c.Spec.Authority
	common := []corev1.EnvVar{}
	for _, pair := range [][2]string{{"STRICT", "true"}, {"ROLE", role}, {"TOKEN_AUDIENCE", authorityAudience}, {"CLUSTER_UID", string(c.UID)}, {"LEASE_NAMESPACE", c.Namespace}, {"LEASE_NAME", lease.Name}, {"LEASE_UID", string(lease.UID)}, {"API_TIMEOUT_MS", strconv.FormatInt(a.APITimeoutMS, 10)}, {"VALIDATION_TIMEOUT_MS", strconv.FormatInt(a.ValidationTimeoutMS, 10)}, {"MAX_CLOCK_SKEW_MS", strconv.FormatInt(*a.MaxClockSkewMS, 10)}} {
		common = append(common, corev1.EnvVar{Name: "CUBESTORE_AUTHORITY_" + pair[0], Value: pair[1]})
	}
	t.Spec.AutomountServiceAccountToken = ptrBool(false)
	t.Spec.ServiceAccountName = clusterName(c, role)
	items := []corev1.KeyToPath{{Key: "ca.crt", Path: "ca.crt"}}
	if role == cubeComponentMeta {
		items = append(items, corev1.KeyToPath{Key: "tls.crt", Path: "tls.crt"}, corev1.KeyToPath{Key: "tls.key", Path: "tls.key"})
	}
	t.Spec.Volumes = append(t.Spec.Volumes, corev1.Volume{Name: "authority-tls", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: metaStoreAuthorityTLSName(c), Items: items}}})
	if role != cubeComponentMeta {
		t.Spec.Volumes = append(t.Spec.Volumes, corev1.Volume{Name: "authority-token", VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{Sources: []corev1.VolumeProjection{{ServiceAccountToken: &corev1.ServiceAccountTokenProjection{Audience: authorityAudience, ExpirationSeconds: ptr64(3600), Path: "token"}}}}}})
	}
	if role == cubeComponentMeta || role == cubeComponentRouter {
		// Empty audience requests the API server's default audience, not the
		// authority audience. Namespace/CA preserve client-go InClusterConfig.
		t.Spec.Volumes = append(t.Spec.Volumes, corev1.Volume{Name: "authority-api", VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{Sources: []corev1.VolumeProjection{
			{ServiceAccountToken: &corev1.ServiceAccountTokenProjection{ExpirationSeconds: ptr64(3600), Path: "token"}},
			{ConfigMap: &corev1.ConfigMapProjection{LocalObjectReference: corev1.LocalObjectReference{Name: "kube-root-ca.crt"}, Items: []corev1.KeyToPath{{Key: "ca.crt", Path: "ca.crt"}}}},
			{DownwardAPI: &corev1.DownwardAPIProjection{Items: []corev1.DownwardAPIVolumeFile{{Path: "namespace", FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: "metadata.namespace"}}}}},
		}}}})
	}
	for i := range t.Spec.Containers {
		container := &t.Spec.Containers[i]
		container.Env = mergeEnv(container.Env, common)
		container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{Name: "authority-tls", MountPath: authorityDirectory + "/tls", ReadOnly: true})
		if role == cubeComponentMeta {
			container.Ports = []corev1.ContainerPort{{Name: "authority", ContainerPort: authorityPort}}
			container.Env = mergeEnv(container.Env, []corev1.EnvVar{{Name: "CUBESTORE_META_BIND_ADDR", Value: "127.0.0.1:9999"}})
			for _, pair := range [][2]string{{"BIND_ADDR", fmt.Sprintf("0.0.0.0:%d", authorityPort)}, {"TLS_CERT_FILE", authorityDirectory + "/tls/tls.crt"}, {"TLS_KEY_FILE", authorityDirectory + "/tls/tls.key"}, {"K8S_API_URL", "https://kubernetes.default.svc"}, {"K8S_CA_FILE", authorityAPIDirectory + "/ca.crt"}, {"K8S_TOKEN_FILE", authorityAPIDirectory + "/token"}, {"EXPECTED_LEASE_CLUSTER_ID", c.Namespace + "/" + clusterName(c, cubeComponentRouter)}, {"ROUTER_SERVICE_ACCOUNT", clusterName(c, cubeComponentRouter)}, {"WORKER_SERVICE_ACCOUNT", clusterName(c, cubeComponentWorker)}} {
				container.Env = append(container.Env, corev1.EnvVar{Name: "CUBESTORE_AUTHORITY_" + pair[0], Value: pair[1]})
			}
		} else {
			for _, pair := range [][2]string{{"URL", fmt.Sprintf("https://%s.%s.svc:%d", clusterName(c, cubeComponentMeta), c.Namespace, authorityPort)}, {"CA_FILE", authorityDirectory + "/tls/ca.crt"}, {"TOKEN_FILE", authorityDirectory + "/identity/token"}} {
				container.Env = append(container.Env, corev1.EnvVar{Name: "CUBESTORE_AUTHORITY_" + pair[0], Value: pair[1]})
			}
			container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{Name: "authority-token", MountPath: authorityDirectory + "/identity", ReadOnly: true})
		}
		if role == cubeComponentMeta || container.Name == "lease-agent" {
			container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{Name: "authority-api", MountPath: authorityAPIDirectory, ReadOnly: true})
		}
		if container.Name == "lease-agent" {
			// The backend and authority validator must read the same actual Lease.
			container.Env = mergeEnv(container.Env, []corev1.EnvVar{{Name: "CUBESTORE_LEASE_K8S_NAME", Value: lease.Name}})
		}
	}
	return nil
}

// Complete an empty, pre-reserved Lease using the actual candidate Pod. This
// initial CAS is the only epoch-1 path; ordinary LeaseStore handles all renewals
// and subsequent epochs. It never publishes a serving label or grant.
func (r *CubestoreRouterReconciler) initializeAuthorityLease(ctx context.Context, cr *v1alpha1.CubestoreRouter, candidates []candidate) (bool, error) {
	if cr.Annotations[authorityBootstrapAnnotation] == "" {
		return false, nil
	}
	if r.APIReader == nil {
		return true, fmt.Errorf("authority bootstrap requires APIReader")
	}
	owner := metav1.GetControllerOf(cr)
	if owner == nil || owner.Kind != "CubeCluster" || owner.APIVersion != "cubestore.io/v1alpha1" {
		return true, fmt.Errorf("authority router owner invalid")
	}
	var c v1alpha1.CubeCluster
	if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: cr.Namespace, Name: owner.Name}, &c); err != nil {
		return true, err
	}
	if c.UID != owner.UID || c.Spec.Authority == nil || c.DeletionTimestamp != nil || string(c.UID) != cr.Annotations[authorityBootstrapAnnotation] {
		return true, fmt.Errorf("authority cluster identity invalid")
	}
	var lease coordinationv1.Lease
	if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: cr.Namespace, Name: authorityLeaseName(&c)}, &lease); err != nil {
		if apierrors.IsNotFound(err) && c.Annotations[authorityLeaseUIDAnnotation] == "" {
			return true, nil
		}
		return true, errLeaseStateLost
	}
	if c.Annotations[authorityLeaseUIDAnnotation] == "" {
		return true, nil
	}
	if lease.UID == "" || string(lease.UID) != c.Annotations[authorityLeaseUIDAnnotation] || !metav1.IsControlledBy(&lease, &c) || lease.DeletionTimestamp != nil {
		return true, fmt.Errorf("authority Lease identity changed")
	}
	if lease.Spec.LeaseTransitions != nil {
		return false, nil
	}
	if lease.Spec.HolderIdentity != nil || lease.Spec.AcquireTime != nil || lease.Spec.RenewTime != nil || lease.Spec.LeaseDurationSeconds != nil || lease.Annotations[authorityLeaseTokenAnnotation] != "" || lease.Annotations[leaseTokenAnnotation] != "" || lease.Annotations[authorityBootstrapAnnotation] != string(c.UID) || lease.Annotations[authorityLeaseClusterAnnotation] != c.Namespace+"/"+cr.Name || lease.Annotations["cubejs.io/lease-generation"] != "1" || cr.Annotations[leaseBootstrapAnnotation] == "" || cr.Status.LeaderEpoch != 0 || cr.Annotations[leaseEpochAnnotation] != "" || cr.Annotations[promotionEpochAnnotation] != "" {
		return true, errLeaseStateLost
	}
	preferred, _ := r.chooseLeader(candidates, cr.Spec.ElectionStrategy, "")
	if preferred == nil {
		return true, nil
	}
	var pod corev1.Pod
	if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: cr.Namespace, Name: preferred.Name}, &pod); err != nil {
		return true, err
	}
	if pod.UID == "" || pod.DeletionTimestamp != nil || pod.Spec.ServiceAccountName != clusterName(&c, cubeComponentRouter) {
		return true, fmt.Errorf("bootstrap Pod identity invalid")
	}
	for k, v := range cr.Spec.Selector {
		if pod.Labels[k] != v {
			return true, fmt.Errorf("bootstrap Pod selector mismatch")
		}
	}
	if pod.Annotations[leaseEpochAnnotation] != "" || pod.Annotations[leaseTokenAnnotation] != "" {
		return true, errLeaseStateLost
	}
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return true, err
	}
	now := time.Now().UTC()
	ttl := routerLeaseTTL(cr)
	holder := pod.Name
	lease.Spec = coordinationv1.LeaseSpec{HolderIdentity: &holder, LeaseTransitions: ptr32(1), LeaseDurationSeconds: ptr32(int32(ttl / time.Second)), AcquireTime: &metav1.MicroTime{Time: now}, RenewTime: &metav1.MicroTime{Time: now}}
	lease.Annotations[authorityLeaseTokenAnnotation] = hex.EncodeToString(raw)
	// Keep optional holder-uid empty: current LeaseStore retains it on takeover.
	// TokenReview + direct Pod reads bind the real Pod UID independently.
	lease.Annotations["cubejs.io/lease-holder-uid"] = ""
	if err := r.Update(ctx, &lease); err != nil {
		return true, err
	}
	r.setLocalRouterLease(c.Namespace+"/"+cr.Name, leadership.LeaseRecord{ClusterID: c.Namespace + "/" + cr.Name, HolderID: holder, Epoch: 1, Generation: "1", Token: lease.Annotations[authorityLeaseTokenAnnotation], IssuedAt: now, ExpiresAt: now.Add(ttl)})
	return false, nil
}
