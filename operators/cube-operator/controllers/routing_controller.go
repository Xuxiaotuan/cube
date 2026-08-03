package controllers

import (
	"context"
	"fmt"

	"github.com/cube-js/cube-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type leaderEndpointSnapshot struct {
	serviceFound bool
	readyCount   int
	names        []string
}

func (s leaderEndpointSnapshot) servingTarget(name string) bool {
	return s.serviceFound && s.readyCount == 1 && len(s.names) == 1 && s.names[0] == name
}

func (r *CubestoreRouterReconciler) leaderEndpointState(ctx context.Context, namespace string, cr *v1alpha1.CubestoreRouter) (leaderEndpointSnapshot, error) {
	service, err := r.findLeaderService(ctx, namespace, cr)
	if err != nil {
		return leaderEndpointSnapshot{}, err
	}
	if service == nil {
		return leaderEndpointSnapshot{}, nil
	}
	var slices discoveryv1.EndpointSliceList
	if err := r.List(ctx, &slices, client.InNamespace(namespace), client.MatchingLabels{discoveryv1.LabelServiceName: service.Name}); err != nil {
		return leaderEndpointSnapshot{}, err
	}
	snapshot := leaderEndpointSnapshot{serviceFound: true}
	for _, slice := range slices.Items {
		for _, endpoint := range slice.Endpoints {
			if endpoint.Conditions.Ready != nil && !*endpoint.Conditions.Ready {
				continue
			}
			if endpoint.Conditions.Serving != nil && !*endpoint.Conditions.Serving {
				continue
			}
			snapshot.readyCount++
			if endpoint.TargetRef != nil && endpoint.TargetRef.Name != "" {
				snapshot.names = append(snapshot.names, endpoint.TargetRef.Name)
			}
		}
	}
	return snapshot, nil
}

func (r *CubestoreRouterReconciler) findLeaderService(ctx context.Context, namespace string, cr *v1alpha1.CubestoreRouter) (*corev1.Service, error) {
	var services corev1.ServiceList
	if err := r.List(ctx, &services, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	for i := range services.Items {
		service := &services.Items[i]
		if service.Spec.Selector[labelNamespace] != labelLeader {
			continue
		}
		matches := true
		for key, value := range cr.Spec.Selector {
			if service.Spec.Selector[key] != value {
				matches = false
				break
			}
		}
		if matches {
			return service, nil
		}
	}
	return nil, nil
}

func endpointSliceFor(name, pod string) discoveryv1.EndpointSlice {
	ready := true
	return discoveryv1.EndpointSlice{
		ObjectMeta:  metav1.ObjectMeta{Name: name, Namespace: "default", Labels: map[string]string{discoveryv1.LabelServiceName: "router-leader"}},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints:   []discoveryv1.Endpoint{{Addresses: []string{"10.0.0.1"}, Conditions: discoveryv1.EndpointConditions{Ready: &ready}, TargetRef: &corev1.ObjectReference{Name: pod}}},
	}
}

func endpointStateDescription(snapshot leaderEndpointSnapshot) string {
	if !snapshot.serviceFound {
		return "leader Service not found"
	}
	return fmt.Sprintf("leader Service has %d ready EndpointSlice endpoints", snapshot.readyCount)
}
