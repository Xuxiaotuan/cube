package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/cube-js/cube-operator/internal/agent"
	"github.com/cube-js/cube-operator/internal/leadership"
)

func main() {
	var (
		clusterID     = flag.String("cluster-id", os.Getenv("CUBESTORE_CLUSTER_ID"), "authoritative lease cluster ID")
		holderID      = flag.String("holder-id", os.Getenv("POD_NAME"), "this Router pod name")
		storeURL      = flag.String("lease-store-url", os.Getenv("CUBESTORE_LEASE_STORE_URL"), "authoritative LeaseStore HTTP endpoint")
		path          = flag.String("path", agent.DefaultLeadershipFile, "leadership file path")
		retryPeriod   = flag.Duration("retry-period", durationEnv("CUBESTORE_RETRY_PERIOD", 2*time.Second), "lease polling interval")
		renewDeadline = flag.Duration("renew-deadline", durationEnv("CUBESTORE_RENEW_DEADLINE", 10*time.Second), "maximum backend outage before expiry")
	)
	flag.Parse()
	if *storeURL == "" {
		log.Fatal("--lease-store-url or CUBESTORE_LEASE_STORE_URL is required")
	}

	leaseAgent, err := agent.New(agent.Config{
		Store:         httpLeaseStore{endpoint: *storeURL, client: &http.Client{Timeout: *retryPeriod}},
		ClusterID:     *clusterID,
		HolderID:      *holderID,
		Path:          *path,
		RetryPeriod:   *retryPeriod,
		RenewDeadline: *renewDeadline,
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

// httpLeaseStore is a read-only adapter for the LeaseStore's authoritative Get
// operation. The endpoint exposes GET /leases/{clusterID} and returns the
// LeaseRecord JSON maintained by the Task 4 store service.
type httpLeaseStore struct {
	endpoint string
	client   *http.Client
}

func (s httpLeaseStore) Get(ctx context.Context, clusterID string) (leadership.LeaseRecord, error) {
	endpoint := strings.TrimRight(s.endpoint, "/") + "/leases/" + url.PathEscape(clusterID)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return leadership.LeaseRecord{}, err
	}
	response, err := s.client.Do(request)
	if err != nil {
		return leadership.LeaseRecord{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return leadership.LeaseRecord{}, fmt.Errorf("lease store returned %s", response.Status)
	}
	var record leadership.LeaseRecord
	if err := json.NewDecoder(http.MaxBytesReader(nil, response.Body, 1<<20)).Decode(&record); err != nil {
		return leadership.LeaseRecord{}, err
	}
	return record, nil
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

var _ agent.LeaseStore = httpLeaseStore{}
