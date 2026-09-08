package controllers

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/cube-js/cube-operator/api/v1alpha1"
	apps "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestStrictSQLAuthWiresOnlyRouterAndDrivers(t *testing.T) {
	c, r := strictAuthorityFixture(t)
	c.Spec.Refresher = &v1alpha1.CubeComponentSpec{}
	c.Spec.Router.DrainCommand = []string{"/cube/cubestored", "--drain"}
	ctx := context.Background()
	if err := r.Update(ctx, c); err != nil {
		t.Fatal(err)
	}
	reconcileClusterTest(t, r, c)
	user, ref, err := cubeSQLAuth(c)
	if err != nil {
		t.Fatal(err)
	}
	for _, role := range []string{cubeComponentRouter, cubeComponentAPI, cubeComponentRefresher} {
		var deployment apps.Deployment
		getComponent(t, r, c, role, &deployment)
		index := 0
		if role == cubeComponentRouter {
			index = 1
			for _, env := range deployment.Spec.Template.Spec.Containers[0].Env {
				if cubeSQLAuthManagedEnvironment(env.Name) {
					t.Fatal("SQL credential/Driver switch leaked to lease-agent")
				}
			}
		}
		container := deployment.Spec.Template.Spec.Containers[index]
		env := environment(container.Env)
		for _, expected := range cubeSQLAuthEnvironment(user, ref, role) {
			if !reflect.DeepEqual(env[expected.Name], expected) {
				t.Fatalf("%s does not share %s contract", role, expected.Name)
			}
		}
		if role == cubeComponentRouter {
			// Literal assertions bind this renderer to the actual Rust CLI names.
			if env["CUBESTORE_DRAIN_USER"].Value != user || env["CUBESTORE_DRAIN_PASSWORD"].ValueFrom.SecretKeyRef.Name != "router-sql-auth" || env["CUBESTORE_DRAIN_PASSWORD"].ValueFrom.SecretKeyRef.Key != "password" {
				t.Fatal("cubestored --drain authentication disconnected")
			}
			if !reflect.DeepEqual(container.Lifecycle.PreStop.Exec.Command, c.Spec.Router.DrainCommand) {
				t.Fatal("drain executable changed")
			}
			if container.ReadinessProbe.HTTPGet.Path != "/readyz" || container.LivenessProbe.HTTPGet.Path != "/livez" || len(container.ReadinessProbe.HTTPGet.HTTPHeaders) != 0 || len(container.LivenessProbe.HTTPGet.HTTPHeaders) != 0 {
				t.Fatal("public process probes changed or contain credentials")
			}
		} else {
			if env["CUBEJS_CUBESTORE_USER"].Value != user || env["CUBEJS_CUBESTORE_PASS"].ValueFrom.SecretKeyRef.Key != "password" || env["CUBEJS_CUBESTORE_PRE_AGGREGATION_LEDGER_STRICT"].Value != "true" {
				t.Fatal("Driver authentication or strict ledger switch disconnected")
			}
		}
		raw, err := json.Marshal(deployment)
		if err != nil || strings.Contains(string(raw), "test-only-password") {
			t.Fatal("rendered Deployment contains Secret value")
		}
	}
	for _, role := range []string{cubeComponentMeta, cubeComponentWorker} {
		var set apps.StatefulSet
		getComponent(t, r, c, role, &set)
		for _, container := range set.Spec.Template.Spec.Containers {
			for _, env := range container.Env {
				if cubeSQLAuthManagedEnvironment(env.Name) {
					t.Fatalf("SQL credential/Driver switch leaked to %s", role)
				}
			}
		}
	}
	var configs corev1.ConfigMapList
	if err := r.List(ctx, &configs, client.InNamespace(c.Namespace)); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(configs)
	if err != nil || strings.Contains(string(raw), "test-only-password") {
		t.Fatal("ConfigMap contains SQL password")
	}
}

func TestStrictSQLAuthRejectsMissingOrConflictingConfiguration(t *testing.T) {
	for name, mutate := range map[string]func(*v1alpha1.CubeCluster){
		"no password": func(c *v1alpha1.CubeCluster) { c.Spec.Router.Pod.Env = nil },
		"literal password": func(c *v1alpha1.CubeCluster) { c.Spec.Router.Pod.Env[0] = corev1.EnvVar{Name: "CUBESTORE_SQL_PASSWORD", Value: "must-not-be-reported"} },
		"ConfigMap password": func(c *v1alpha1.CubeCluster) { c.Spec.Router.Pod.Env[0].ValueFrom = &corev1.EnvVarSource{ConfigMapKeyRef: &corev1.ConfigMapKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "wrong"}, Key: "password"}} },
		"optional password": func(c *v1alpha1.CubeCluster) { c.Spec.Router.Pod.Env[0].ValueFrom.SecretKeyRef.Optional = ptrBool(true) },
		"empty key": func(c *v1alpha1.CubeCluster) { c.Spec.Router.Pod.Env[0].ValueFrom.SecretKeyRef.Key = "" },
		"no user": func(c *v1alpha1.CubeCluster) { c.Spec.API.Pod.Env = nil },
		"invalid user": func(c *v1alpha1.CubeCluster) { c.Spec.API.Pod.Env[0].Value = "user:ambiguous" },
		"expanded user": func(c *v1alpha1.CubeCluster) { c.Spec.API.Pod.Env[0].Value = "$(UNKNOWN)" },
		"Driver password override": func(c *v1alpha1.CubeCluster) { c.Spec.API.Pod.Env = append(c.Spec.API.Pod.Env, corev1.EnvVar{Name: "CUBEJS_CUBESTORE_PASS", Value: "must-not-be-reported"}) },
		"drain password override": func(c *v1alpha1.CubeCluster) { c.Spec.Router.Pod.Env = append(c.Spec.Router.Pod.Env, corev1.EnvVar{Name: "CUBESTORE_DRAIN_PASSWORD", Value: "must-not-be-reported"}) },
		"strict fallback": func(c *v1alpha1.CubeCluster) { c.Spec.API.Pod.Env = append(c.Spec.API.Pod.Env, corev1.EnvVar{Name: "CUBEJS_CUBESTORE_PRE_AGGREGATION_LEDGER_STRICT", Value: "false"}) },
		"refresher override": func(c *v1alpha1.CubeCluster) { c.Spec.Refresher = &v1alpha1.CubeComponentSpec{Pod: v1alpha1.CubePodSpec{Env: []corev1.EnvVar{{Name: "CUBEJS_CUBESTORE_USER", Value: "different"}}}} },
		"worker password": func(c *v1alpha1.CubeCluster) { c.Spec.Workers.Pod.Env = append(c.Spec.Workers.Pod.Env, c.Spec.Router.Pod.Env[0]) },
	} {
		t.Run(name, func(t *testing.T) {
			c, _ := strictAuthorityFixture(t)
			mutate(c)
			err := validateCubeCluster(c)
			if err == nil || strings.Contains(err.Error(), "must-not-be-reported") {
				t.Fatal("invalid auth configuration accepted or credential exposed")
			}
		})
	}
}

func TestStrictSQLSecretFailureBlocksWorkloadsAndReportsStatus(t *testing.T) {
	for _, name := range []string{"missing", "missing key", "empty", "non UTF8", "NUL", "deleting", "read error"} {
		t.Run(name, func(t *testing.T) {
			c, r := strictAuthorityFixture(t)
			ctx := context.Background()
			var secret corev1.Secret
			key := client.ObjectKey{Namespace: c.Namespace, Name: "router-sql-auth"}
			if err := r.Get(ctx, key, &secret); err != nil {
				t.Fatal(err)
			}
			switch name {
			case "missing":
				if err := r.Delete(ctx, &secret); err != nil {
					t.Fatal(err)
				}
			case "missing key":
				delete(secret.Data, "password")
			case "empty":
				secret.Data["password"] = nil
			case "non UTF8":
				secret.Data["password"] = []byte{255}
			case "NUL":
				secret.Data["password"] = []byte("value\x00suffix")
			case "deleting":
				secret.Finalizers = []string{"test-retain-secret"}
			case "read error":
				r.APIReader = sqlSecretFailReader{Reader: r.APIReader}
			}
			if name != "missing" {
				if err := r.Update(ctx, &secret); err != nil {
					t.Fatal(err)
				}
				if name == "deleting" {
					if err := r.Delete(ctx, &secret); err != nil {
						t.Fatal(err)
					}
				}
			}
			reconcileClusterTest(t, r, c)
			var observed v1alpha1.CubeCluster
			if err := r.Get(ctx, client.ObjectKeyFromObject(c), &observed); err != nil {
				t.Fatal(err)
			}
			condition := apiMeta.FindStatusCondition(observed.Status.Conditions, cubeClusterConditionResources)
			if condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != "ConfigurationUnavailable" || strings.Contains(condition.Message, "test-only-password") {
				t.Fatal("missing credential did not produce safe refusal status")
			}
			var deployments apps.DeploymentList
			var sets apps.StatefulSetList
			if err := r.List(ctx, &deployments, client.InNamespace(c.Namespace)); err != nil {
				t.Fatal(err)
			}
			if err := r.List(ctx, &sets, client.InNamespace(c.Namespace)); err != nil {
				t.Fatal(err)
			}
			if len(deployments.Items) != 0 || len(sets.Items) != 0 {
				t.Fatal("strict workload created despite invalid credentials")
			}
			if name == "missing" && !apierrors.IsNotFound(r.Get(ctx, key, &corev1.Secret{})) {
				t.Fatal("missing external password was generated")
			}
		})
	}
}

type sqlSecretFailReader struct{ client.Reader }

func (r sqlSecretFailReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*corev1.Secret); ok && key.Name == "router-sql-auth" {
		return context.DeadlineExceeded
	}
	return r.Reader.Get(ctx, key, obj, opts...)
}

func TestStrictSQLSecretReuseWatchAndRotation(t *testing.T) {
	c, r := strictAuthorityFixture(t)
	ctx := context.Background()
	if err := r.reconcileAPISecret(ctx, c); err != nil {
		t.Fatal(err)
	}
	var secret corev1.Secret
	if err := r.Get(ctx, client.ObjectKey{Namespace: c.Namespace, Name: "router-sql-auth"}, &secret); err != nil {
		t.Fatal(err)
	}
	before, err := r.configurationDigest(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	secret.Data["password"] = []byte("  exact UTF-8 password\n")
	if err := r.Update(ctx, &secret); err != nil {
		t.Fatal(err)
	}
	after, err := r.configurationDigest(ctx, c)
	if err != nil || before == after {
		t.Fatal("password rotation did not invalidate configuration digest")
	}
	if len(r.configurationChanged(ctx, &secret)) != 1 {
		t.Fatal("reused Router env Secret is not watched")
	}
	var retained corev1.Secret
	if err := r.Get(ctx, client.ObjectKeyFromObject(&secret), &retained); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(retained.Data, secret.Data) || len(retained.OwnerReferences) != 0 {
		t.Fatal("user Secret was normalized or adopted")
	}
}

func TestLegacySQLAuthenticationEnvironmentUnchanged(t *testing.T) {
	c := businessCluster()
	c.Spec.Router.Pod.Env = []corev1.EnvVar{{Name: "CUBESTORE_SQL_PASSWORD", Value: "legacy-router-config"}}
	c.Spec.API.Pod.Env = []corev1.EnvVar{{Name: "CUBEJS_CUBESTORE_PASS", Value: "legacy-driver-config"}}
	r := clusterReconciler(t, c)
	reconcileClusterTest(t, r, c)
	var router, api apps.Deployment
	getComponent(t, r, c, cubeComponentRouter, &router)
	getComponent(t, r, c, cubeComponentAPI, &api)
	env := environment(api.Spec.Template.Spec.Containers[0].Env)
	if env["CUBEJS_CUBESTORE_PASS"].Value != "legacy-driver-config" {
		t.Fatal("legacy password configuration changed")
	}
	if _, present := env["CUBEJS_CUBESTORE_PRE_AGGREGATION_LEDGER_STRICT"]; present {
		t.Fatal("strict ledger enabled on legacy cluster")
	}
	env = environment(router.Spec.Template.Spec.Containers[1].Env)
	if env["CUBESTORE_SQL_PASSWORD"].Value != "legacy-router-config" {
		t.Fatal("legacy Router configuration changed")
	}
	if _, present := env["CUBESTORE_DRAIN_PASSWORD"]; present {
		t.Fatal("legacy drain configuration changed")
	}
}
