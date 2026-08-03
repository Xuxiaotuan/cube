package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cube-js/cube-operator/api/v1alpha1"
	"github.com/cube-js/cube-operator/internal/leadership"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	promotionPhaseAnnotation     = "cubestore.io/promotion-phase"
	promotionCandidateAnnotation = "cubestore.io/promotion-candidate"
	promotionEpochAnnotation     = "cubestore.io/promotion-epoch"

	promotionPhaseCandidate = "candidate"
	promotionPhaseFenced    = "fenced"
	promotionPhaseReady     = "ready"
	promotionPhasePromoted  = "promoted"
	promotionPhaseServing   = "serving"
)

var errPromotionPending = errors.New("router promotion is waiting for a readiness gate")

type promotionState struct {
	Phase     string
	Candidate string
	Epoch     int64
}

func promotionPhaseFrom(cr *v1alpha1.CubestoreRouter) string {
	if cr == nil {
		return ""
	}
	phase := strings.TrimSpace(cr.Annotations[promotionPhaseAnnotation])
	switch phase {
	case promotionPhaseCandidate, promotionPhaseFenced, promotionPhaseReady, promotionPhasePromoted, promotionPhaseServing:
		return phase
	default:
		return ""
	}
}

func promotionStateFrom(cr *v1alpha1.CubestoreRouter) promotionState {
	state := promotionState{Phase: promotionPhaseFrom(cr)}
	if cr != nil {
		state.Candidate = strings.TrimSpace(cr.Annotations[promotionCandidateAnnotation])
		state.Epoch, _ = strconv.ParseInt(strings.TrimSpace(cr.Annotations[promotionEpochAnnotation]), 10, 64)
	}
	return state
}

func promotionStateMatches(state promotionState, candidate *candidate, lease leadership.LeaseRecord) bool {
	return candidate != nil && state.Candidate == candidate.Name && state.Epoch == lease.Epoch
}

func (r *CubestoreRouterReconciler) setPromotionState(ctx context.Context, cr *v1alpha1.CubestoreRouter, lease leadership.LeaseRecord, state promotionState) error {
	if cr == nil || lease.Epoch <= 0 {
		return leadership.ErrStaleLease
	}
	reader := client.Reader(r.Client)
	if r.APIReader != nil {
		reader = r.APIReader
	}
	fresh := &v1alpha1.CubestoreRouter{}
	if err := reader.Get(ctx, types.NamespacedName{Name: cr.Name, Namespace: cr.Namespace}, fresh); err != nil {
		return err
	}
	if err := r.validateLeaseBeforeWrite(ctx, fresh, lease); err != nil {
		return err
	}
	if err := r.ensureObjectFence(ctx, fresh, lease); err != nil {
		return err
	}
	if err := reader.Get(ctx, types.NamespacedName{Name: cr.Name, Namespace: cr.Namespace}, fresh); err != nil {
		return err
	}
	annotations := fresh.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}
	values := map[string]string{
		promotionPhaseAnnotation:     state.Phase,
		promotionCandidateAnnotation: state.Candidate,
		promotionEpochAnnotation:     strconv.FormatInt(state.Epoch, 10),
	}
	operations := []leaseJSONPatchOperation{
		{Op: "test", Path: "/metadata/resourceVersion", Value: fresh.GetResourceVersion()},
		{Op: "test", Path: "/metadata/annotations/" + jsonPointerEscape(leaseClusterAnnotation), Value: lease.ClusterID},
		{Op: "test", Path: "/metadata/annotations/" + jsonPointerEscape(leaseEpochAnnotation), Value: strconv.FormatInt(lease.Epoch, 10)},
		{Op: "test", Path: "/metadata/annotations/" + jsonPointerEscape(leaseTokenAnnotation), Value: lease.Token},
	}
	for key, value := range values {
		path := "/metadata/annotations/" + jsonPointerEscape(key)
		if _, ok := annotations[key]; ok {
			operations = append(operations, leaseJSONPatchOperation{Op: "replace", Path: path, Value: value})
		} else {
			operations = append(operations, leaseJSONPatchOperation{Op: "add", Path: path, Value: value})
		}
	}
	patch, err := json.Marshal(operations)
	if err != nil {
		return err
	}
	return r.Patch(ctx, fresh, client.RawPatch(types.JSONPatchType, patch))
}

func (r *CubestoreRouterReconciler) reconcilePromotion(
	ctx context.Context,
	cr *v1alpha1.CubestoreRouter,
	namespace string,
	candidates []candidate,
	leaseLeader *candidate,
	lease leadership.LeaseRecord,
) (*candidate, string, error) {
	if lease.Epoch <= 0 || lease.HolderID == "" {
		return nil, promotionPhaseFenced, leadership.ErrStaleLease
	}
	target := readyCandidateByName(candidates, lease.HolderID)
	state := promotionStateFrom(cr)
	if target == nil {
		return nil, promotionPhaseFenced, nil
	}

	// A new lease epoch always starts at candidate, even if an old CR
	// annotation says serving. This makes an operator restart fail closed.
	if !promotionStateMatches(state, target, lease) {
		state = promotionState{Phase: promotionPhaseCandidate, Candidate: target.Name, Epoch: lease.Epoch}
		if cr.Status.Leader == target.Name && promotionAcknowledged(*target, lease) {
			if endpointState, err := r.leaderEndpointState(ctx, namespace, cr); err == nil && endpointState.servingTarget(target.Name) {
				state.Phase = promotionPhaseServing
			}
		}
		if err := r.setPromotionState(ctx, cr, lease, state); err != nil {
			return nil, state.Phase, err
		}
	}

	switch state.Phase {
	case promotionPhaseCandidate:
		if err := r.fenceRouterEndpoints(ctx, namespace, cr, candidates, lease); err != nil {
			return nil, state.Phase, err
		}
		state.Phase = promotionPhaseFenced
		if err := r.setPromotionState(ctx, cr, lease, state); err != nil {
			return nil, state.Phase, err
		}
		return nil, state.Phase, nil
	case promotionPhaseFenced:
		endpointState, err := r.leaderEndpointState(ctx, namespace, cr)
		if err != nil {
			return nil, state.Phase, err
		}
		if endpointState.readyCount > 0 {
			return nil, state.Phase, errPromotionPending
		}
		if !target.Ready || !promotionAcknowledged(*target, lease) {
			// Endpoints are fenced. Publish the current lease holder in the
			// marker so the Router can acknowledge the exact epoch/token on
			// the next probe; returning pending here would skip marker sync
			// and deadlock promotion forever.
			return nil, state.Phase, nil
		}
		state.Phase = promotionPhaseReady
		if err := r.setPromotionState(ctx, cr, lease, state); err != nil {
			return nil, state.Phase, err
		}
		return nil, state.Phase, nil
	case promotionPhaseReady:
		if !target.Ready || !promotionAcknowledged(*target, lease) {
			return r.rollbackPromotion(ctx, namespace, cr, candidates, lease, state)
		}
		if err := r.syncRoles(ctx, namespace, cr, candidates, target, lease); err != nil {
			return nil, state.Phase, err
		}
		state.Phase = promotionPhasePromoted
		if err := r.setPromotionState(ctx, cr, lease, state); err != nil {
			return r.rollbackPromotion(ctx, namespace, cr, candidates, lease, state)
		}
		return nil, state.Phase, nil
	case promotionPhasePromoted, promotionPhaseServing:
		if !target.Ready || !promotionAcknowledged(*target, lease) {
			return r.rollbackPromotion(ctx, namespace, cr, candidates, lease, state)
		}
		endpointState, err := r.leaderEndpointState(ctx, namespace, cr)
		if err != nil {
			return nil, state.Phase, err
		}
		if endpointState.servingTarget(target.Name) {
			if state.Phase != promotionPhaseServing {
				state.Phase = promotionPhaseServing
				if err := r.setPromotionState(ctx, cr, lease, state); err != nil {
					return nil, state.Phase, err
				}
			}
			return target, promotionPhaseServing, nil
		}
		if endpointState.readyCount > 0 {
			return nil, state.Phase, r.rollbackPromotionState(ctx, namespace, cr, candidates, lease, state)
		}
		return nil, state.Phase, errPromotionPending
	default:
		return nil, state.Phase, fmt.Errorf("unsupported promotion phase %q", state.Phase)
	}
}

func (r *CubestoreRouterReconciler) rollbackPromotion(
	ctx context.Context,
	namespace string,
	cr *v1alpha1.CubestoreRouter,
	candidates []candidate,
	lease leadership.LeaseRecord,
	state promotionState,
) (*candidate, string, error) {
	err := r.rollbackPromotionState(ctx, namespace, cr, candidates, lease, state)
	return nil, promotionPhaseFenced, err
}

func (r *CubestoreRouterReconciler) rollbackPromotionState(
	ctx context.Context,
	namespace string,
	cr *v1alpha1.CubestoreRouter,
	candidates []candidate,
	lease leadership.LeaseRecord,
	state promotionState,
) error {
	if err := r.fenceRouterEndpoints(ctx, namespace, cr, candidates, lease); err != nil {
		return err
	}
	state.Phase = promotionPhaseFenced
	return r.setPromotionState(ctx, cr, lease, state)
}

func (r *CubestoreRouterReconciler) fenceRouterEndpoints(ctx context.Context, namespace string, cr *v1alpha1.CubestoreRouter, candidates []candidate, lease leadership.LeaseRecord) error {
	return r.syncRoles(ctx, namespace, cr, candidates, nil, lease)
}

func promotionGateMessage(state promotionState, candidate candidate, lease leadership.LeaseRecord) string {
	return fmt.Sprintf("promotion phase=%s candidate=%s epoch=%d ackEpoch=%d leaseEpoch=%d", state.Phase, candidate.Name, lease.Epoch, candidate.LeaderEpoch, candidate.LeaseEpoch)
}

var _ = corev1.Pod{}
var _ = metav1.Now
var _ = time.Second
