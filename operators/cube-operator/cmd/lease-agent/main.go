package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/cube-js/cube-operator/internal/agent"
	"github.com/cube-js/cube-operator/internal/leadership"
	"github.com/redis/go-redis/v9"
	coordinationv1 "k8s.io/api/coordination/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const defaultRedisPrefix = "cube-router/leader-state"
const defaultKubernetesLeaseName = "cube-router-leader"

func main() {
	var (
		clusterID   = flag.String("cluster-id", os.Getenv("CUBESTORE_CLUSTER_ID"), "authoritative lease cluster ID")
		holderID    = flag.String("holder-id", os.Getenv("POD_NAME"), "this Router pod name")
		redisURL    = flag.String("redis-url", os.Getenv("REDIS_URL"), "authoritative Redis URL")
		redisPrefix = flag.String("redis-prefix", redisPrefixEnv(), "authoritative Redis lease key prefix")
		backend     = flag.String("backend", "", "lease backend: redis or kubernetes")
		namespace   = flag.String("kubernetes-namespace", os.Getenv("POD_NAMESPACE"), "namespace for Kubernetes Lease")
		leaseName   = flag.String("kubernetes-lease-name", "", "Kubernetes Lease name; default derived from cluster-id")
		path        = flag.String("path", agent.DefaultLeadershipFile, "leadership file path")
		retryPeriod = flag.Duration("retry-period", durationEnv("CUBESTORE_RETRY_PERIOD", 2*time.Second), "lease polling interval")
	)
	flag.Parse()

	mode := strings.ToLower(strings.TrimSpace(*backend))
	if mode == "" {
		if strings.TrimSpace(*redisURL) != "" {
			mode = "redis"
		} else {
			mode = "kubernetes"
		}
	}

	var (
		leaseStore leadership.LeaseStore
		closeFn    = func() {}
	)

	switch mode {
	case "redis":
		if strings.TrimSpace(*redisURL) == "" {
			log.Fatal("--redis-url or REDIS_URL is required when --backend=redis")
		}
		client, err := newRedisClient(*redisURL, os.Getenv("REDIS_PASSWORD"))
		if err != nil {
			log.Fatal(err)
		}
		closeFn = func() { _ = client.Close() }
		leaseStore = leadership.NewRedisStore(client, *redisPrefix)
	case "kubernetes":
		ns, resolvedLeaseName := resolveKubernetesLeaseConfig(*namespace, *leaseName, *clusterID)
		cfg, err := ctrl.GetConfig()
		if err != nil {
			log.Fatal(err)
		}
		scheme := runtime.NewScheme()
		if err := coordinationv1.AddToScheme(scheme); err != nil {
			log.Fatal(err)
		}
		c, err := client.New(cfg, client.Options{Scheme: scheme})
		if err != nil {
			log.Fatal(err)
		}
		leaseStore = leadership.NewKubernetesStore(c, ns, resolvedLeaseName, nil)
	default:
		log.Fatalf("unsupported backend %q", mode)
	}
	defer closeFn()

	leaseAgent, err := agent.New(agent.Config{
		Store:       leaseStore,
		ClusterID:   *clusterID,
		HolderID:    *holderID,
		Path:        *path,
		RetryPeriod: *retryPeriod,
	})
	if err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := leaseAgent.Run(ctx); err != nil {
		log.Fatal(err)
	}
}

func resolveKubernetesLeaseConfig(namespace, leaseName, clusterID string) (string, string) {
	ns := strings.TrimSpace(namespace)
	if ns == "" {
		ns = "default"
	}

	name := strings.TrimSpace(leaseName)
	if name == "" {
		base := strings.TrimSpace(clusterID)
		if base == "" {
			base = defaultKubernetesLeaseName
		}
		name = strings.ToLower(base)
		name = strings.ReplaceAll(name, "/", "-")
		name = strings.ReplaceAll(name, ".", "-")
		name = strings.ReplaceAll(name, "_", "-")
		name = strings.Trim(name, "-._")
		if name == "" {
			name = defaultKubernetesLeaseName
		}
		if len(name) > 220 {
			name = name[:220]
		}
		name = "cube-router-" + name
	}

	return ns, name
}

func newRedisClient(rawURL, password string) (*redis.Client, error) {
	options, err := redis.ParseURL(rawURL)
	if err != nil {
		return nil, err
	}
	if password != "" {
		options.Password = password
	}
	return redis.NewClient(options), nil
}

func redisPrefixEnv() string {
	if value := os.Getenv("CUBESTORE_LEASE_REDIS_PREFIX"); value != "" {
		return value
	}
	return defaultRedisPrefix
}

func durationEnv(name string, fallback time.Duration) time.Duration {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	duration, err := time.ParseDuration(value)
	if err != nil {
		log.Printf("ignoring invalid %s=%q: %v", name, value, err)
		return fallback
	}
	return duration
}

var _ agent.LeaseStore = (*leadership.RedisStore)(nil)
var _ agent.LeaseStore = (*leadership.KubernetesStore)(nil)
