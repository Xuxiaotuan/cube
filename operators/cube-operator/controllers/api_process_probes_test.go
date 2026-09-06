package controllers

import (
	"reflect"
	"testing"

	"github.com/cube-js/cube-operator/api/v1alpha1"
	apps "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
)

func TestAPIAndRefresherDefaultProcessProbesRespectHAMode(t *testing.T) {
	for _, tc := range []struct {
		name string
		ha   bool
		path string
	}{
		{"HA requires custom process-only API endpoint", true, "/livez/process"},
		{"nonHA preserves legacy API endpoint", false, "/livez"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := businessCluster()
			c.Spec.Router.HighAvailability = tc.ha
			c.Spec.Refresher = &v1alpha1.CubeComponentSpec{}
			r := clusterReconciler(t, c)
			reconcileClusterTest(t, r, c)
			for _, role := range []string{cubeComponentAPI, cubeComponentRefresher} {
				var deployment apps.Deployment
				getComponent(t, r, c, role, &deployment)
				container := deployment.Spec.Template.Spec.Containers[0]
				for name, probe := range map[string]*corev1.Probe{"startup": container.StartupProbe, "liveness": container.LivenessProbe} {
					if probe == nil || probe.HTTPGet == nil || probe.HTTPGet.Path != tc.path || probe.HTTPGet.Port.IntVal != 4000 {
						t.Fatalf("%s %s probe=%#v, want %s on4000", role, name, probe, tc.path)
					}
				}
				if container.ReadinessProbe == nil || container.ReadinessProbe.HTTPGet == nil || container.ReadinessProbe.HTTPGet.Path != "/readyz" {
					t.Fatalf("%s application readiness was weakened", role)
				}
				if container.StartupProbe.FailureThreshold != 60 {
					t.Fatalf("%s startup budget changed", role)
				}
			}
			// Rust already has a process-only /livez; never move it to an API path.
			var router apps.Deployment
			getComponent(t, r, c, cubeComponentRouter, &router)
			if router.Spec.Template.Spec.Containers[1].LivenessProbe.HTTPGet.Path != "/livez" {
				t.Fatal("Rust probe incorrectly changed to API endpoint")
			}
		})
	}
}

func TestAPIAndRefresherProcessProbeOverridesRemainAuthoritative(t *testing.T) {
	for _, ha := range []bool{false, true} {
		name := "nonHA"
		if ha {
			name = "HA"
		}
		t.Run(name, func(t *testing.T) {
			c := businessCluster()
			c.Spec.Router.HighAvailability = ha
			apiStartup := httpProbe("/custom-api-startup", 4100)
			apiLive := &corev1.Probe{ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"custom-process-check"}}}, TimeoutSeconds: 7}
			apiReady := httpProbe("/custom-api-ready", 4100)
			c.Spec.API.Pod.StartupProbe = apiStartup
			c.Spec.API.Pod.LivenessProbe = apiLive
			c.Spec.API.Pod.ReadinessProbe = apiReady
			refreshStartup := httpProbe("/custom-refresh-startup", 4200)
			refreshLive := httpProbe("/custom-refresh-live", 4200)
			refreshReady := httpProbe("/custom-refresh-ready", 4200)
			c.Spec.Refresher = &v1alpha1.CubeComponentSpec{Pod: v1alpha1.CubePodSpec{StartupProbe: refreshStartup, LivenessProbe: refreshLive, ReadinessProbe: refreshReady}}
			r := clusterReconciler(t, c)
			reconcileClusterTest(t, r, c)
			for role, want := range map[string][]*corev1.Probe{cubeComponentAPI: {apiStartup, apiLive, apiReady}, cubeComponentRefresher: {refreshStartup, refreshLive, refreshReady}} {
				var d apps.Deployment
				getComponent(t, r, c, role, &d)
				container := d.Spec.Template.Spec.Containers[0]
				got := []*corev1.Probe{container.StartupProbe, container.LivenessProbe, container.ReadinessProbe}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("%s user probe overrides changed: got=%#v want=%#v", role, got, want)
				}
			}
			c.Spec.Refresher.Pod = v1alpha1.CubePodSpec{}
			inherited := refresherPodOptions(c)
			if !reflect.DeepEqual(inherited.StartupProbe, apiStartup) || !reflect.DeepEqual(inherited.LivenessProbe, apiLive) || !reflect.DeepEqual(inherited.ReadinessProbe, apiReady) {
				t.Fatal("refresher stopped inheriting explicit API probes")
			}
		})
	}
}
