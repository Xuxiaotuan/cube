package controllers

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/cube-js/cube-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Reuse the existing native pod.env API. In strict mode the Router password
// selector is the only password source; the API supplies the Basic auth user.
// Legacy clusters retain their existing independently configured environment.
func cubeSQLAuth(c *v1alpha1.CubeCluster) (string, *corev1.SecretKeySelector, error) {
	if c.Spec.Authority == nil {
		return "", nil, nil
	}
	var password, user *corev1.EnvVar
	for i := range c.Spec.Router.Pod.Env {
		if c.Spec.Router.Pod.Env[i].Name == "CUBESTORE_SQL_PASSWORD" {
			if password != nil {
				return "", nil, fmt.Errorf("router.pod.env duplicates CUBESTORE_SQL_PASSWORD")
			}
			password = &c.Spec.Router.Pod.Env[i]
		}
	}
	if password == nil || password.Value != "" || password.ValueFrom == nil || password.ValueFrom.SecretKeyRef == nil {
		return "", nil, fmt.Errorf("strict authority requires router.pod.env CUBESTORE_SQL_PASSWORD from a nonoptional SecretKeyRef")
	}
	ref := password.ValueFrom.SecretKeyRef
	if ref.Name == "" || ref.Key == "" || (ref.Optional != nil && *ref.Optional) || !reflect.DeepEqual(password.ValueFrom, &corev1.EnvVarSource{SecretKeyRef: ref}) {
		return "", nil, fmt.Errorf("strict SQL password requires only SecretKeyRef with name/key and optional omitted or false")
	}
	for i := range c.Spec.API.Pod.Env {
		if c.Spec.API.Pod.Env[i].Name == "CUBEJS_CUBESTORE_USER" {
			if user != nil {
				return "", nil, fmt.Errorf("api.pod.env duplicates CUBEJS_CUBESTORE_USER")
			}
			user = &c.Spec.API.Pod.Env[i]
		}
	}
	if user == nil || user.ValueFrom != nil || strings.TrimSpace(user.Value) == "" || !utf8.ValidString(user.Value) || strings.Contains(user.Value, ":") || strings.Contains(user.Value, "$(") || strings.IndexFunc(user.Value, unicode.IsControl) >= 0 {
		return "", nil, fmt.Errorf("strict authority requires api.pod.env CUBEJS_CUBESTORE_USER with a nonempty literal Basic auth username, without colon, controls or environment expansion")
	}
	return user.Value, ref.DeepCopy(), nil
}

func cubeSQLAuthEnvironment(user string, ref *corev1.SecretKeySelector, role string) []corev1.EnvVar {
	secret := func(name string) corev1.EnvVar {
		return corev1.EnvVar{Name: name, ValueFrom: &corev1.EnvVarSource{SecretKeyRef: ref.DeepCopy()}}
	}
	switch role {
	case cubeComponentRouter:
		// cubestored --drain does NOT read SQL_PASSWORD. Its Basic auth client
		// reads this distinct pair, inherited by exec/preStop in this container.
		return []corev1.EnvVar{secret("CUBESTORE_SQL_PASSWORD"), secret("CUBESTORE_DRAIN_PASSWORD"), {Name: "CUBESTORE_DRAIN_USER", Value: user}}
	case cubeComponentAPI, cubeComponentRefresher:
		return []corev1.EnvVar{{Name: "CUBEJS_CUBESTORE_USER", Value: user}, secret("CUBEJS_CUBESTORE_PASS"), {Name: "CUBEJS_CUBESTORE_PRE_AGGREGATION_LEDGER_STRICT", Value: "true"}}
	}
	// lease-agent uses projected Bearer tokens, not SQL. Workers/MetaStore
	// use the authority RPC contract and must not receive this shared password.
	return nil
}

func cubeSQLAuthManagedEnvironment(name string) bool {
	switch name {
	case "CUBESTORE_SQL_PASSWORD", "CUBESTORE_DRAIN_USER", "CUBESTORE_DRAIN_PASSWORD", "CUBEJS_CUBESTORE_USER", "CUBEJS_CUBESTORE_PASS", "CUBEJS_CUBESTORE_PRE_AGGREGATION_LEDGER_STRICT":
		return true
	}
	return false
}

func validateCubeSQLAuth(c *v1alpha1.CubeCluster) error {
	user, ref, err := cubeSQLAuth(c)
	if err != nil || ref == nil {
		return err
	}
	for role, spec := range componentSpecs(c) {
		want := cubeSQLAuthEnvironment(user, ref, role)
		for _, env := range spec.Pod.Env {
			if !cubeSQLAuthManagedEnvironment(env.Name) {
				continue
			}
			matched := false
			for _, expected := range want {
				if env.Name != expected.Name || env.Value != expected.Value {
					continue
				}
				actual := env.DeepCopy()
				// nil and false are the same nonoptional selector contract.
				if actual.ValueFrom != nil && actual.ValueFrom.SecretKeyRef != nil && (actual.ValueFrom.SecretKeyRef.Optional == nil || !*actual.ValueFrom.SecretKeyRef.Optional) {
					actual.ValueFrom.SecretKeyRef.Optional = nil
				}
				if expected.ValueFrom != nil {
					expected.ValueFrom.SecretKeyRef.Optional = nil
				}
				matched = reflect.DeepEqual(*actual, expected)
			}
			if !matched {
				return fmt.Errorf("strict authority %s env %s conflicts with the shared SQL authentication contract", role, env.Name)
			}
		}
	}
	return nil
}

func (r *CubeClusterReconciler) validateCubeSQLSecret(ctx context.Context, c *v1alpha1.CubeCluster) error {
	_, ref, err := cubeSQLAuth(c)
	if err != nil || ref == nil {
		return err
	}
	if r.APIReader == nil {
		return fmt.Errorf("strict SQL authentication requires uncached APIReader")
	}
	var secret corev1.Secret
	if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: c.Namespace, Name: ref.Name}, &secret); err != nil {
		// Do not propagate arbitrary client/transport error text into status.
		return fmt.Errorf("strict SQL authentication Secret is unavailable")
	}
	value := secret.Data[ref.Key]
	if secret.DeletionTimestamp != nil || len(value) == 0 || !utf8.Valid(value) || bytes.IndexByte(value, 0) >= 0 {
		return fmt.Errorf("strict SQL authentication Secret requires a nonempty UTF-8 key without NUL and must not be deleting")
	}
	// Kubernetes injects the exact bytes. Do not trim, encode, log, or persist
	// the password in CR status, generated env values or ordinary ConfigMaps.
	return nil
}
