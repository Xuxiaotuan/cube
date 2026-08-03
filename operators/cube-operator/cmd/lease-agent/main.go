package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cube-js/cube-operator/internal/agent"
	"github.com/cube-js/cube-operator/internal/leadership"
	"github.com/redis/go-redis/v9"
)

const defaultRedisPrefix = "cube-router/leader-state"

func main() {
	var (
		clusterID   = flag.String("cluster-id", os.Getenv("CUBESTORE_CLUSTER_ID"), "authoritative Redis lease cluster ID")
		holderID    = flag.String("holder-id", os.Getenv("POD_NAME"), "this Router pod name")
		redisURL    = flag.String("redis-url", os.Getenv("REDIS_URL"), "authoritative Redis URL")
		redisPrefix = flag.String("redis-prefix", redisPrefixEnv(), "authoritative Redis lease key prefix")
		path        = flag.String("path", agent.DefaultLeadershipFile, "leadership file path")
		retryPeriod = flag.Duration("retry-period", durationEnv("CUBESTORE_RETRY_PERIOD", 2*time.Second), "lease polling interval")
	)
	flag.Parse()
	if *redisURL == "" {
		log.Fatal("--redis-url or REDIS_URL is required")
	}
	client, err := newRedisClient(*redisURL, os.Getenv("REDIS_PASSWORD"))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	leaseAgent, err := agent.New(agent.Config{
		Store:       leadership.NewRedisStore(client, *redisPrefix),
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
