package main

import (
	"context"
	"flag"
	"os"

	"github.com/cube-js/cube-operator/api/v1alpha1"
	"github.com/cube-js/cube-operator/controllers"
	apps "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(v1alpha1.AddToScheme(scheme))
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(apps.AddToScheme(scheme))
	utilruntime.Must(discoveryv1.AddToScheme(scheme))
	utilruntime.Must(coordinationv1.AddToScheme(scheme))
	utilruntime.Must(policyv1.AddToScheme(scheme))
	utilruntime.Must(rbacv1.AddToScheme(scheme))
}

func main() {
	var enableLeaderElection bool

	flag.BoolVar(&enableLeaderElection, "leader-elect", true, "Enable leader election for controller manager (required for lease adoption).")
	flag.Parse()

	opts := zap.Options{Development: true}
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	watchNamespace := os.Getenv("WATCH_NAMESPACE")

	cacheOpts := cache.Options{}
	if watchNamespace != "" {
		cacheOpts.DefaultNamespaces = map[string]cache.Config{
			watchNamespace: {},
		}
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:           scheme,
		LeaderElection:   enableLeaderElection,
		LeaderElectionID: "cube-operator",
		Cache:            cacheOpts,
	})
	if err != nil {
		os.Exit(1)
	}

	if err = (&controllers.CubestoreRouterReconciler{
		ManagerLeaderElection: enableLeaderElection,
		Client:                mgr.GetClient(),
		APIReader:             mgr.GetAPIReader(),
		Scheme:                mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		os.Exit(1)
	}

	if err = (&controllers.CubeClusterReconciler{
		APIReader: mgr.GetAPIReader(),
		Client:    mgr.GetClient(),
		Scheme:    mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		os.Exit(1)
	}

	if err := mgr.Start(context.Background()); err != nil {
		os.Exit(1)
	}
}
