package leadership

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

const configuredRedisURL = "redis://100.82.226.63:30078/0"

func redisStoreForTest(t *testing.T) *RedisStore {
	t.Helper()
	dsn := strings.TrimSpace(os.Getenv("REDIS_URL"))
	if dsn == "" {
		dsn = configuredRedisURL
		t.Log("REDIS_URL is not set; using the configured Redis endpoint")
	}
	options, err := redis.ParseURL(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if password := os.Getenv("REDIS_PASSWORD"); password != "" {
		options.Password = password
	}
	client := redis.NewClient(options)
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("real Redis is not reachable: %v", err)
	}
	return NewRedisStore(client, "cubestore-test-lease-"+fmt.Sprint(time.Now().UnixNano()))
}

func TestRedisStoreContentionAndEpoch(t *testing.T) {
	store := redisStoreForTest(t)
	const contenders = 20
	var wg sync.WaitGroup
	leases := make(chan LeaseRecord, contenders)
	errs := make(chan error, contenders)
	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			lease, acquired, err := store.Acquire(context.Background(), "cluster", fmt.Sprintf("holder-%d", i), time.Second)
			if err != nil {
				errs <- err
				return
			}
			if acquired {
				leases <- lease
			}
		}(i)
	}
	wg.Wait()
	close(leases)
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	var winners []LeaseRecord
	for lease := range leases {
		winners = append(winners, lease)
	}
	if len(winners) != 1 || winners[0].Epoch != 1 {
		t.Fatalf("winners = %#v, want exactly one holder for epoch 1", winners)
	}
}

func TestParseRedisAcquireResultKeepsContendedLease(t *testing.T) {
	lease, acquired, err := parseRedisAcquireResult("cluster", []interface{}{int64(0), "other-holder", "other-token", int64(7), int64(1000), int64(2000)})
	if err != nil {
		t.Fatal(err)
	}
	if acquired {
		t.Fatal("contended acquire was reported as acquired")
	}
	if lease.HolderID != "other-holder" || lease.Token != "other-token" || lease.Epoch != 7 {
		t.Fatalf("contended lease = %#v", lease)
	}
}

func TestRedisStoreExpiryAndStaleToken(t *testing.T) {
	store := redisStoreForTest(t)
	ctx := context.Background()
	first, acquired, err := store.Acquire(ctx, "cluster", "first", 25*time.Millisecond)
	if err != nil || !acquired {
		t.Fatalf("first acquire = (%#v, %t, %v)", first, acquired, err)
	}
	time.Sleep(50 * time.Millisecond)
	second, acquired, err := store.Acquire(ctx, "cluster", "second", time.Second)
	if err != nil || !acquired || second.Epoch != first.Epoch+1 {
		t.Fatalf("second acquire = (%#v, %t, %v)", second, acquired, err)
	}
	if _, renewed, err := store.Renew(ctx, first, time.Second); err != nil || renewed {
		t.Fatalf("stale renew = (%t, %v), want false, nil", renewed, err)
	}
	if err := store.Release(ctx, first); !errors.Is(err, ErrStaleLease) {
		t.Fatalf("stale release error = %v, want ErrStaleLease", err)
	}
	got, err := store.Get(ctx, "cluster")
	if err != nil || got.Token != second.Token {
		t.Fatalf("Get after stale release = (%#v, %v)", got, err)
	}
	staleEpoch := second
	staleEpoch.Epoch--
	if _, renewed, err := store.Renew(ctx, staleEpoch, time.Second); err != nil || renewed {
		t.Fatalf("stale epoch renew = (%t, %v), want false, nil", renewed, err)
	}
	if err := store.Release(ctx, staleEpoch); !errors.Is(err, ErrStaleLease) {
		t.Fatalf("stale epoch release error = %v, want ErrStaleLease", err)
	}
	if err := store.Release(ctx, second); err != nil {
		t.Fatal(err)
	}
	third, acquired, err := store.Acquire(ctx, "cluster", "third", time.Second)
	if err != nil || !acquired || third.Epoch != second.Epoch+1 {
		t.Fatalf("post-release acquire = (%#v, %t, %v), want epoch %d", third, acquired, err, second.Epoch+1)
	}
}

func TestRedisStoreMissingTTLIsUnknown(t *testing.T) {
	store := redisStoreForTest(t)
	ctx := context.Background()
	lease, acquired, err := store.Acquire(ctx, "missing-ttl", "holder", time.Second)
	if err != nil || !acquired {
		t.Fatalf("Acquire = (%#v, %t, %v)", lease, acquired, err)
	}
	if err := store.client.Persist(ctx, store.leaseKey(lease.ClusterID)).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(ctx, lease.ClusterID); !errors.Is(err, ErrLeaseUnknown) {
		t.Fatalf("Get without TTL error = %v, want ErrLeaseUnknown", err)
	}
	if _, acquired, err := store.Acquire(ctx, lease.ClusterID, "other", time.Second); !errors.Is(err, ErrLeaseUnknown) || acquired {
		t.Fatalf("Acquire without TTL = (%t, %v), want false, ErrLeaseUnknown", acquired, err)
	}
	if err := store.Release(ctx, lease); !errors.Is(err, ErrLeaseUnknown) {
		t.Fatalf("Release without TTL error = %v, want ErrLeaseUnknown", err)
	}
}

func TestRedisStoreRenewReleaseCAS(t *testing.T) {
	store := redisStoreForTest(t)
	ctx := context.Background()
	lease, acquired, err := store.Acquire(ctx, "renew-release", "holder", time.Second)
	if err != nil || !acquired {
		t.Fatalf("Acquire = (%#v, %t, %v)", lease, acquired, err)
	}

	renewed, renewedOK, err := store.Renew(ctx, lease, 2*time.Second)
	if err != nil || !renewedOK || renewed.Epoch != lease.Epoch || renewed.Token != lease.Token {
		t.Fatalf("valid Renew = (%#v, %t, %v)", renewed, renewedOK, err)
	}

	staleToken := renewed
	staleToken.Token = "not-the-current-token"
	if _, renewedOK, err := store.Renew(ctx, staleToken, time.Second); err != nil || renewedOK {
		t.Fatalf("stale-token Renew = (%t, %v), want false, nil", renewedOK, err)
	}
	if err := store.Release(ctx, staleToken); !errors.Is(err, ErrStaleLease) {
		t.Fatalf("stale-token Release error = %v, want ErrStaleLease", err)
	}
	if err := store.Release(ctx, renewed); err != nil {
		t.Fatalf("valid Release error = %v", err)
	}

	next, acquired, err := store.Acquire(ctx, "renew-release", "next-holder", time.Second)
	if err != nil || !acquired || next.Epoch != lease.Epoch+1 {
		t.Fatalf("post-release Acquire = (%#v, %t, %v), want epoch %d", next, acquired, err, lease.Epoch+1)
	}
}

func TestRedisFencingValidationAndUnknownResponse(t *testing.T) {
	stale := LeaseRecord{ClusterID: "cluster", HolderID: "holder", Epoch: 0, Token: "token"}
	if err := validateLeaseIdentity(stale); err == nil {
		t.Fatal("zero epoch passed lease identity validation")
	}
	result := []interface{}{int64(2), "holder", "token", int64(3), int64(1000), int64(0)}
	if _, acquired, err := parseRedisAcquireResult("cluster", result); !errors.Is(err, ErrLeaseUnknown) || acquired {
		t.Fatalf("unknown acquire = (%t, %v), want false, ErrLeaseUnknown", acquired, err)
	}
	if _, found, err := parseRedisGetResult("cluster", result); !errors.Is(err, ErrLeaseUnknown) || found {
		t.Fatalf("unknown get = (%t, %v), want false, ErrLeaseUnknown", found, err)
	}
	for _, epoch := range []interface{}{int64(0), int64(-1), "malformed"} {
		malformed := []interface{}{int64(1), "holder", "token", epoch, int64(1000), int64(2000)}
		if _, _, err := parseRedisGetResult("cluster", malformed); err == nil {
			t.Fatalf("epoch %v was accepted, want rejected malformed lease", epoch)
		}
	}
}

func TestRedisStoreOutageAndServerClock(t *testing.T) {
	store := NewRedisStore(redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 10 * time.Millisecond}), "outage")
	t.Cleanup(func() { _ = store.client.Close() })
	ctx := context.Background()
	if _, acquired, err := store.Acquire(ctx, "cluster", "holder", time.Second); err == nil || acquired {
		t.Fatalf("Acquire during Redis outage = (acquired=%t, err=%v), want failed closed", acquired, err)
	}
	outageLease := LeaseRecord{ClusterID: "cluster", HolderID: "holder", Epoch: 1, Token: "token"}
	if _, renewed, err := store.Renew(ctx, outageLease, time.Second); err == nil || renewed {
		t.Fatalf("Renew during Redis outage = (renewed=%t, err=%v), want failed closed", renewed, err)
	}
	if err := store.Release(ctx, outageLease); err == nil {
		t.Fatal("Release succeeded during Redis outage")
	}
	if _, err := store.Get(ctx, "cluster"); err == nil {
		t.Fatal("Get succeeded during Redis outage")
	}
	valid := redisStoreForTest(t)
	lease, acquired, err := valid.Acquire(ctx, "clock", "holder", time.Second)
	if err != nil || !acquired || !lease.ExpiresAt.After(lease.IssuedAt) {
		t.Fatalf("server-clock lease = (%#v, %t, %v)", lease, acquired, err)
	}
	if _, err := valid.Get(context.Background(), "missing"); !errors.Is(err, ErrLeaseNotFound) {
		t.Fatalf("missing Get error = %v", err)
	}
}
