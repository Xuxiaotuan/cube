package controllers

import "github.com/cube-js/cube-operator/api/v1alpha1"

// Pointers preserve backward compatibility: an omitted capability is unknown,
// not an explicit denial and never an optimistic success.
type runtimeRecoveryCapabilities struct {
	UploadReceipts       *bool `json:"uploadReceipts,omitempty"`
	PreAggregationStatus *bool `json:"preAggregationStatus,omitempty"`
	JobAttemptFencing    *bool `json:"jobAttemptFencing,omitempty"`
}

func capabilityGate(c *runtimeRecoveryCapabilities, scope string) v1alpha1.RecoveryGateStatus {
	gate := v1alpha1.RecoveryGateStatus{State: v1alpha1.RecoveryStateNeedsContext, Reason: "EvidenceIncomplete", Message: "Matching promoted runtime did not report the required recovery capabilities"}
	if c == nil {
		return gate
	}
	flags := []*bool{c.JobAttemptFencing}
	reason := "JobAttemptFencingSupported"
	message := "Promoted runtime supports fenced Job attempts; this is not proof of zero lost work or successful recovery of every job"
	if scope == "mutations" {
		flags = []*bool{c.UploadReceipts, c.PreAggregationStatus}
		reason = "BuildReconciliationSupported"
		message = "Promoted runtime supports durable upload receipts and authoritative pre-aggregation status for scoped CSV builds; arbitrary mutations are not exactly-once"
	}
	for _, flag := range flags {
		if flag != nil && !*flag {
			return v1alpha1.RecoveryGateStatus{State: v1alpha1.RecoveryStateBlocked, Reason: "RuntimeCapabilityUnavailable", Message: message + "; at least one required capability is explicitly unavailable"}
		}
	}
	for _, flag := range flags {
		if flag == nil {
			return gate
		}
	}
	return v1alpha1.RecoveryGateStatus{State: v1alpha1.RecoveryStateReady, Reason: reason, Message: message}
}
