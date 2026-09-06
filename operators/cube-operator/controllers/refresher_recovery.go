package controllers

import "github.com/cube-js/cube-operator/api/v1alpha1"

// refresherBuildRecovery is scoped to resumable file-import builds. It does
// not assert scheduler leadership, arbitrary mutation exactly-once semantics,
// or successful completion of every persisted build.
func refresherBuildRecovery(c *v1alpha1.CubeCluster, components map[string]v1alpha1.CubeComponentStatus, recovery v1alpha1.RouterRecoveryStatus, capabilities map[string]*bool) v1alpha1.RecoveryGateStatus {
	unknown := v1alpha1.RecoveryGateStatus{State: v1alpha1.RecoveryStateNeedsContext, Reason: "EvidenceIncomplete", Message: "Current API and refresher must be application-ready; refresher /readyz must acknowledge shared CubeStore queue and resumable file-import build scheduling, with matching promoted Router recovery capabilities"}
	if c.Spec.Refresher == nil {
		unknown.Reason = "RefresherNotManaged"
		unknown.Message = "No independent refresher is managed; no file-import build scheduling recovery is certified"
		return unknown
	}
	for _, component := range []string{cubeComponentAPI, cubeComponentRefresher} {
		state, exists := components[component]
		if !exists || !state.Ready || state.ObservedGeneration < state.WorkloadGeneration {
			unknown.Message = "Waiting for current-generation, application-ready " + component + " before accepting file-import build recovery capabilities"
			return unknown
		}
	}
	// The API may deliberately use externalRefresh and only consume finished
	// builds. Its scheduling capability does not describe refresher support.
	flag := capabilities[cubeComponentRefresher]
	if flag == nil {
		unknown.Message = "refresher /readyz omitted a valid recoveryCapabilities.fileImportBuildScheduling acknowledgement"
		return unknown
	}
	if !*flag {
		return v1alpha1.RecoveryGateStatus{State: v1alpha1.RecoveryStateBlocked, Reason: "FileImportBuildSchedulingUnavailable", Message: "refresher explicitly reports fileImportBuildScheduling=false; shared resumable build recovery is unavailable"}
	}
	for _, gate := range []v1alpha1.RecoveryGateStatus{recovery.Promotion, recovery.JobRecovery, recovery.MutationReconcile} {
		if gate.State != v1alpha1.RecoveryStateReady {
			unknown.Message = "Refresher supports resumable file-import scheduling, but matching promoted Router upload receipts, pre-aggregation status and fenced Job attempts are not all acknowledged"
			return unknown
		}
	}
	return v1alpha1.RecoveryGateStatus{State: v1alpha1.RecoveryStateReady, Reason: "FileImportBuildSchedulingSupported", Message: "Current API and refresher are application-ready; refresher acknowledges shared CubeStore queue and resumable file-import manifests, with promoted Router upload receipts, pre-aggregation status and fenced Job attempts; this certifies scoped recovery support, not arbitrary mutation exactly-once, scheduler single leadership or completion of every build"}
}
