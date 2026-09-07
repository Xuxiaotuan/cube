package controllers

import (
	"context"
	"errors"
	"fmt"

	"github.com/cube-js/cube-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const leaseBootstrapAnnotation = "cubestore.io/lease-bootstrap-reserved"

var errLeaseStateLost = errors.New("LeaseStateLost: missing authoritative Lease after bootstrap or recorded history; automatic epoch reset is forbidden")

// Reserve before Create, not afterwards: a crash between the two writes must
// block bootstrap rather than silently recreate epoch 1. This is deliberately
// fail-closed, not a cross-resource transaction or an automatic restore protocol.
func (r *CubestoreRouterReconciler) reserveLeaseBootstrap(ctx context.Context, cr *v1alpha1.CubestoreRouter) error {
	if r.APIReader == nil {
		return errors.New("Lease bootstrap requires an uncached APIReader")
	}
	fresh := &v1alpha1.CubestoreRouter{}
	if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(cr), fresh); err != nil {
		return err
	}
	if fresh.UID != cr.UID || fresh.DeletionTimestamp != nil {
		return fmt.Errorf("router identity changed or is being deleted; refusing Lease bootstrap")
	}
	a := fresh.Annotations
	if a[leaseBootstrapAnnotation] != "" || a[leaseEpochAnnotation] != "" ||
		a[leaseTokenAnnotation] != "" || a[promotionEpochAnnotation] != "" || fresh.Status.LeaderEpoch > 0 {
		return errLeaseStateLost
	}
	namespace := fresh.Spec.Namespace
	if namespace == "" {
		namespace = fresh.Namespace
	}
	cm := &corev1.ConfigMap{}
	err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: resolveRoleConfigMapName(fresh)}, cm)
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	if err == nil && (cm.Annotations[leaseEpochAnnotation] != "" || cm.Annotations[leaseTokenAnnotation] != "" || len(cm.Data) > 0) {
		return errLeaseStateLost
	}
	if fresh.Annotations == nil {
		fresh.Annotations = map[string]string{}
	}
	fresh.Annotations[leaseBootstrapAnnotation] = "true"
	// Update includes the fresh resourceVersion: competing initializers cannot
	// both reserve creation. Never remove this marker as part of ordinary retry.
	return r.Update(ctx, fresh)
}
