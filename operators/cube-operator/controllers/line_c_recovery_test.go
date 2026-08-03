package controllers

import (
	"testing"

	"github.com/cube-js/cube-operator/api/v1alpha1"
	"github.com/cube-js/cube-operator/internal/leadership"
)

func TestPromotionAcknowledgedRequiresExactEpochTokenHashAndMetaStore(t *testing.T) {
	lease := leadership.LeaseRecord{Epoch: 7, Token: "current-token"}
	acknowledged := candidate{
		StatusContract: true,
		IsLeader:       true,
		LeaderEpoch:    7,
		LeaseEpoch:     7,
		LeaseTokenHash: hashLeaseToken(lease.Token),
		MetaStoreReady: true,
	}

	if !promotionAcknowledged(acknowledged, lease) {
		t.Fatal("expected an exact promotion acknowledgement to be accepted")
	}

	for name, mutate := range map[string]func(*candidate){
		"old leader epoch":  func(c *candidate) { c.LeaderEpoch = 6 },
		"old lease epoch":   func(c *candidate) { c.LeaseEpoch = 6 },
		"old token":         func(c *candidate) { c.LeaseTokenHash = hashLeaseToken("old-token") },
		"missing metastore": func(c *candidate) { c.MetaStoreReady = false },
		"missing contract":  func(c *candidate) { c.StatusContract = false },
	} {
		t.Run(name, func(t *testing.T) {
			stale := acknowledged
			mutate(&stale)
			if promotionAcknowledged(stale, lease) {
				t.Fatal("stale or incomplete promotion acknowledgement was accepted")
			}
		})
	}
}

func TestRecoveryStatusDoesNotClaimMissingRuntimeEntries(t *testing.T) {
	status := withRecoveryStatus(v1alpha1.CubestoreRouterStatus{}, false)

	if status.Recovery.Promotion.State != v1alpha1.RecoveryStateNeedsContext {
		t.Fatalf("promotion state = %q, want NeedsContext", status.Recovery.Promotion.State)
	}
	if status.Recovery.JobRecovery.State != v1alpha1.RecoveryStateBlocked {
		t.Fatalf("job recovery state = %q, want Blocked", status.Recovery.JobRecovery.State)
	}
	if status.Recovery.MutationReconcile.State != v1alpha1.RecoveryStateNeedsContext {
		t.Fatalf("mutation reconcile state = %q, want NeedsContext", status.Recovery.MutationReconcile.State)
	}
	if status.Recovery.Refresher.State != v1alpha1.RecoveryStateNeedsContext {
		t.Fatalf("refresher state = %q, want NeedsContext", status.Recovery.Refresher.State)
	}
}
