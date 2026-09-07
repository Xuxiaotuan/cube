package controllers

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/cube-js/cube-operator/api/v1alpha1"
	"github.com/cube-js/cube-operator/internal/agent"
	apps "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Fake API assigns deterministic UIDs only at Create, never in the renderer.
type authorityUIDClient struct{ client.Client }

func (c authorityUIDClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if obj.GetUID() == "" {
		obj.SetUID(types.UID("uid-" + obj.GetName()))
	}
	return c.Client.Create(ctx, obj, opts...)
}
func strictAuthorityFixture(t *testing.T) (*v1alpha1.CubeCluster, *CubeClusterReconciler) {
	t.Helper()
	c := businessCluster()
	c.Spec.Authority = &v1alpha1.CubeAuthoritySpec{APITimeoutMS: 1000, ValidationTimeoutMS: 3000, MaxClockSkewMS: ptr64(250)}
	r := clusterReconciler(t, c, &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: authorityReviewRole}, Rules: authorityTokenReviewRules()})
	if err := coordinationv1.AddToScheme(r.Scheme); err != nil {
		t.Fatal(err)
	}
	r.Client = authorityUIDClient{r.Client}
	r.APIReader = r.Client
	return c, r
}
func TestAuthorityIntegrationRendersContract(t *testing.T) {
	c, r := strictAuthorityFixture(t)
	reconcileClusterTest(t, r, c)
	var meta, worker apps.StatefulSet
	var router apps.Deployment
	getComponent(t, r, c, cubeComponentMeta, &meta)
	getComponent(t, r, c, cubeComponentWorker, &worker)
	getComponent(t, r, c, cubeComponentRouter, &router)
	var lease coordinationv1.Lease
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: c.Namespace, Name: authorityLeaseName(c)}, &lease); err != nil {
		t.Fatal(err)
	}
	if lease.UID == "" || lease.Spec.HolderIdentity != nil || lease.Spec.LeaseTransitions != nil {
		t.Fatal("bootstrap forged a holder or epoch")
	}
	for role, pod := range map[string]corev1.PodSpec{"metastore": meta.Spec.Template.Spec, "worker": worker.Spec.Template.Spec, "router": router.Spec.Template.Spec} {
		if pod.AutomountServiceAccountToken == nil || *pod.AutomountServiceAccountToken || pod.ServiceAccountName != clusterName(c, role) {
			t.Fatalf("%s service account/token isolation", role)
		}
		for _, container := range pod.Containers {
			env := environment(container.Env)
			for key, want := range map[string]string{"STRICT": "true", "ROLE": role, "TOKEN_AUDIENCE": authorityAudience, "CLUSTER_UID": string(c.UID), "LEASE_UID": string(lease.UID), "LEASE_NAME": lease.Name} {
				if env["CUBESTORE_AUTHORITY_"+key].Value != want {
					t.Fatalf("%s %s mismatch", container.Name, key)
				}
			}
			if container.Name == "lease-agent" {
				cfg, err := agent.AuthorityConfigFromEnv(func(key string) string { return env[key].Value })
				if err != nil || cfg == nil {
					t.Fatalf("actual Go agent rejects rendered contract: %v", err)
				}
				if env["CUBESTORE_LEASE_K8S_NAME"].Value != lease.Name {
					t.Fatal("agent uses different Lease")
				}
			}
		}
		for _, volume := range pod.Volumes {
			if volume.Name == "authority-tls" && role != "metastore" && !reflect.DeepEqual(volume.Secret.Items, []corev1.KeyToPath{{Key: "ca.crt", Path: "ca.crt"}}) {
				t.Fatal("private key exposed to client")
			}
			if volume.Name == "authority-token" && volume.Projected.Sources[0].ServiceAccountToken.Audience != authorityAudience {
				t.Fatal("wrong projected audience")
			}
			if volume.Name == "authority-api" && volume.Projected.Sources[0].ServiceAccountToken.Audience != "" {
				t.Fatal("API token uses authority audience")
			}
		}
	}
	metaEnv := environment(meta.Spec.Template.Spec.Containers[0].Env)
	if metaEnv["CUBESTORE_AUTHORITY_EXPECTED_LEASE_CLUSTER_ID"].Value != c.Namespace+"/"+clusterName(c, cubeComponentRouter) {
		t.Fatal("cluster-id confused with UID")
	}
	if metaEnv["CUBESTORE_META_BIND_ADDR"].Value != "127.0.0.1:9999" {
		t.Fatal("legacy metastore port exposed")
	}
	var service corev1.Service
	getComponent(t, r, c, cubeComponentMeta, &service)
	if len(service.Spec.Ports) != 1 || service.Spec.Ports[0].Port != 9443 {
		t.Fatal("authority Service port")
	}
	var binding rbacv1.ClusterRoleBinding
	if err := r.Get(context.Background(), client.ObjectKey{Name: authorityBindingName(c)}, &binding); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(binding.Subjects, authorityBinding(c).Subjects) {
		t.Fatal("TokenReview bound to wrong principal")
	}
	var rr rbacv1.Role
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: c.Namespace, Name: clusterName(c, "router-lease-reader")}, &rr); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(rr.Rules[0].ResourceNames, []string{lease.Name}) || rr.Rules[2].Resources[0] != "pods" {
		t.Fatal("agent direct-read permissions missing")
	}
}

func TestAuthorityIntegrationLegacyAndDowngradeBlocked(t *testing.T) {
	c, r := strictAuthorityFixture(t)
	old := &apps.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: clusterName(c, cubeComponentMeta), Namespace: c.Namespace}, Spec: apps.StatefulSetSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "legacy", Image: "legacy"}}}}}}
	if err := r.Create(context.Background(), old); err != nil {
		t.Fatal(err)
	}
	reconcileClusterTest(t, r, c)
	var retained apps.StatefulSet
	getComponent(t, r, c, cubeComponentMeta, &retained)
	if !reflect.DeepEqual(old.Spec, retained.Spec) {
		t.Fatal("legacy workload changed")
	}
	var secret corev1.Secret
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: c.Namespace, Name: metaStoreAuthorityTLSName(c)}, &secret); !apierrors.IsNotFound(err) {
		t.Fatalf("legacy activation created TLS: %v", err)
	}
	c.Spec.Authority = nil
	c.Annotations = map[string]string{authorityBootstrapAnnotation: string(c.UID)}
	if err := r.validateAuthorityTransition(context.Background(), c); err == nil {
		t.Fatal("strict downgrade permitted")
	}
}

func TestAuthorityIntegrationInitialCandidateAndMissingLease(t *testing.T) {
	c, r := strictAuthorityFixture(t)
	reconcileClusterTest(t, r, c)
	var cr v1alpha1.CubestoreRouter
	getComponent(t, r, c, cubeComponentRouter, &cr)
	rr := &CubestoreRouterReconciler{Client: r.Client, APIReader: r.APIReader, Scheme: r.Scheme}
	if pending, err := rr.initializeAuthorityLease(context.Background(), &cr, nil); err != nil || !pending {
		t.Fatalf("no candidate: %v %v", pending, err)
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "actual-router-pod", Namespace: c.Namespace, Labels: cr.Spec.Selector}, Spec: corev1.PodSpec{ServiceAccountName: clusterName(c, cubeComponentRouter)}}
	if err := r.Create(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	candidates := []candidate{{Name: pod.Name, Namespace: c.Namespace, Ready: true, PodIP: "127.0.0.1"}}
	_, lease, err := rr.resolveRouterLease(context.Background(), &cr, candidates)
	if err != nil || lease.HolderID != pod.Name || lease.Epoch != 1 || lease.ClusterID != c.Namespace+"/"+cr.Name {
		t.Fatalf("actual Lease acquisition: %#v %v", lease, err)
	}
	var obj coordinationv1.Lease
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: c.Namespace, Name: authorityLeaseName(c)}, &obj); err != nil {
		t.Fatal(err)
	}
	if err := r.Delete(context.Background(), &obj); err != nil {
		t.Fatal(err)
	}
	// Literal protocol assertions must not share the renderer's constants:
	// the old CR/Pod annotation names produced an unreadable LeaseStore record.
	if obj.Annotations["cubejs.io/lease-cluster-id"] != c.Namespace+"/"+cr.Name || obj.Annotations["cubejs.io/lease-token"] == "" || obj.Annotations["cubejs.io/lease-token"] != lease.Token || obj.Annotations["cubejs.io/lease-generation"] != "1" {
		t.Fatal("bootstrap Lease does not use the LeaseStore annotation protocol")
	}
	if obj.Annotations["cubestore.io/lease-cluster"] != "" || obj.Annotations["cubestore.io/lease-token"] != "" {
		t.Fatal("CR/Pod fence annotations leaked into authoritative Lease")
	}
	if _, err := rr.initializeAuthorityLease(context.Background(), &cr, candidates); err == nil {
		t.Fatal("lost Lease recreated")
	}
	var fresh v1alpha1.CubeCluster
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(c), &fresh); err != nil {
		t.Fatal(err)
	}
	if err := r.prepareAuthority(context.Background(), &fresh); err == nil {
		t.Fatal("lost Lease renderer reset")
	}
}

func TestAuthorityIntegrationRejectsReplacedTLS(t *testing.T) {
	c, r := strictAuthorityFixture(t)
	reconcileClusterTest(t, r, c)
	var fresh v1alpha1.CubeCluster
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(c), &fresh); err != nil {
		t.Fatal(err)
	}
	var secret corev1.Secret
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: c.Namespace, Name: metaStoreAuthorityTLSName(c)}, &secret); err != nil {
		t.Fatal(err)
	}
	if err := r.Delete(context.Background(), &secret); err != nil {
		t.Fatal(err)
	}
	if err := r.prepareAuthority(context.Background(), &fresh); err == nil {
		t.Fatal("missing pinned TLS automatically rotated")
	}
}

func TestAuthorityIntegrationValidationAndDeepCopy(t *testing.T) {
	c, _ := strictAuthorityFixture(t)
	copy := c.DeepCopy()
	*copy.Spec.Authority.MaxClockSkewMS = 0
	if *c.Spec.Authority.MaxClockSkewMS != 250 {
		t.Fatal("authority deepcopy aliases skew")
	}
	c.Spec.Authority.MaxClockSkewMS = nil
	if err := validateCubeCluster(c); err == nil {
		t.Fatal("implicit clock skew permitted")
	}
	if !reservedEnvironment("CUBESTORE_AUTHORITY_STRICT") {
		t.Fatal("strict env is not reserved")
	}
	c.Spec.Authority = nil
	c.Spec.Router.Pod.Env = []corev1.EnvVar{{Name: "CUBESTORE_AUTHORITY_ROLE", Value: "worker"}}
	if err := validateCubeCluster(c); err == nil || !strings.Contains(err.Error(), "operator-owned") {
		t.Fatalf("role override: %v", err)
	}
}

func TestAuthorityIntegrationReservedLeaseLostBeforeUIDPin(t *testing.T) {
	c, r := strictAuthorityFixture(t)
	reconcileClusterTest(t, r, c)
	ctx := context.Background()
	var fresh v1alpha1.CubeCluster
	if err := r.Get(ctx, client.ObjectKeyFromObject(c), &fresh); err != nil {
		t.Fatal(err)
	}
	// Model loss after Lease creation but before its UID pin was persisted.
	// The durable child reservation still forbids a second bootstrap Create.
	delete(fresh.Annotations, authorityLeaseUIDAnnotation)
	if err := r.Update(ctx, &fresh); err != nil {
		t.Fatal(err)
	}
	var lease coordinationv1.Lease
	key := client.ObjectKey{Namespace: c.Namespace, Name: authorityLeaseName(c)}
	if err := r.Get(ctx, key, &lease); err != nil {
		t.Fatal(err)
	}
	if err := r.Delete(ctx, &lease); err != nil {
		t.Fatal(err)
	}
	if err := r.prepareAuthority(ctx, &fresh); err != errLeaseStateLost {
		t.Fatalf("reserved missing Lease must fail closed, got %v", err)
	}
	if err := r.Get(ctx, key, &coordinationv1.Lease{}); !apierrors.IsNotFound(err) {
		t.Fatalf("missing Lease was recreated: %v", err)
	}
}
