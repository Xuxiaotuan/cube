package controllers

import (
	"context"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestRouterPromotionUsesBoundedAgentDelivery(t *testing.T) {
	c := businessCluster()
	r := clusterReconciler(t, c)
	reconcileClusterTest(t, r, c)
	var deployment appsv1.Deployment
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: c.Namespace, Name: c.Name + "-router"}, &deployment); err != nil {
		t.Fatal(err)
	}
	var agent, router *corev1.Container
	for i := range deployment.Spec.Template.Spec.Containers {
		container := &deployment.Spec.Template.Spec.Containers[i]
		if strings.Contains(strings.Join(container.Args, " "), "--promotion-config-map=") {
			agent = container
		} else {
			router = container
		}
	}
	if agent == nil || router == nil {
		t.Fatal("router and direct-promotion agent must both exist")
	}
	for _, expected := range []string{"--promotion-config-map=" + c.Name + "-router-role-state", "--sync-timeout=2s", "--retry-period=2s"} {
		found := false
		for _, arg := range agent.Args {
			if arg == expected {
				found = true
			}
		}
		if !found {
			t.Errorf("missing bounded direct API argument %q", expected)
		}
	}
	var volumeName string
	for _, container := range []*corev1.Container{agent, router} {
		found := false
		for _, mount := range container.VolumeMounts {
			if mount.MountPath != "/var/run/cubestore-promotion" {
				continue
			}
			found = true
			if mount.ReadOnly != (container == router) {
				t.Errorf("incorrect promotion write ownership for %s", container.Name)
			}
			if volumeName != "" && volumeName != mount.Name {
				t.Fatal("agent and runtime do not share promotion volume")
			}
			volumeName = mount.Name
		}
		if !found {
			t.Errorf("promotion mount missing for %s", container.Name)
		}
	}
	foundVolume := false
	for _, volume := range deployment.Spec.Template.Spec.Volumes {
		if volume.Name != volumeName {
			continue
		}
		foundVolume = true
		if volume.ConfigMap != nil || volume.EmptyDir == nil || volume.EmptyDir.Medium != corev1.StorageMediumMemory {
			t.Fatal("promotion remains dependent on kubelet ConfigMap projection")
		}
	}
	if !foundVolume {
		t.Fatal("missing shared promotion volume")
	}
	var roles rbacv1.RoleList
	if err := r.List(context.Background(), &roles, client.InNamespace(c.Namespace)); err != nil {
		t.Fatal(err)
	}
	foundRule := false
	for _, role := range roles.Items {
		for _, rule := range role.Rules {
			for _, resource := range rule.Resources {
				if resource != "configmaps" {
					continue
				}
				foundRule = true
				if len(rule.Verbs) != 1 || rule.Verbs[0] != "get" || len(rule.ResourceNames) != 1 || rule.ResourceNames[0] != c.Name+"-router-role-state" {
					t.Errorf("promotion RBAC must be a named read only: %+v", rule)
				}
			}
		}
	}
	if !foundRule {
		t.Fatal("agent cannot read authoritative promotion ConfigMap")
	}
}
