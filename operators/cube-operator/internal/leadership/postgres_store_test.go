package leadership

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func postgresStoreForTest(t *testing.T) *PostgresStore {
	t.Helper()
	dsn := os.Getenv("POSTGRES_URL")
	if dsn == "" {
		t.Skip("SKIP: POSTGRES_URL is not set; PostgreSQL CAS integration tests were not run")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	store, err := NewPostgresStore(pool, "cubestore_test_router_leases")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Ensure(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), "DELETE FROM cubestore_test_router_leases"); err != nil {
		t.Fatal(err)
	}
	return store
}

func TestPostgresStoreContentionAndEpoch(t *testing.T) {
	store := postgresStoreForTest(t)
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

func TestPostgresStoreExpiryAndStaleToken(t *testing.T) {
	store := postgresStoreForTest(t)
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

func TestPostgresStoreOutageAndServerClock(t *testing.T) {
	pool, err := pgxpool.New(context.Background(), "postgres://127.0.0.1:1/unavailable?connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	outage, err := NewPostgresStore(pool, "leases")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := outage.Acquire(context.Background(), "cluster", "holder", time.Second); err == nil {
		t.Fatal("Acquire succeeded during PostgreSQL outage")
	}
	valid := postgresStoreForTest(t)
	lease, acquired, err := valid.Acquire(context.Background(), "clock", "holder", time.Second)
	if err != nil || !acquired || !lease.ExpiresAt.After(lease.IssuedAt) {
		t.Fatalf("server-clock lease = (%#v, %t, %v)", lease, acquired, err)
	}
	if _, err := valid.Get(context.Background(), "missing"); !errors.Is(err, ErrLeaseNotFound) {
		t.Fatalf("missing Get error = %v", err)
	}
	if _, renewed, err := valid.Renew(context.Background(), LeaseRecord{ClusterID: "missing", HolderID: "holder", Epoch: 1, Token: "token"}, time.Second); !errors.Is(err, ErrLeaseNotFound) || renewed {
		t.Fatalf("missing Renew = (%t, %v), want false, ErrLeaseNotFound", renewed, err)
	}
}

func TestPostgresStoreRenewReleaseCAS(t *testing.T) {
	store := postgresStoreForTest(t)
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

func TestPostgresLeaseLookupDistinguishesTransportErrors(t *testing.T) {
	transportErr := errors.New("database connection reset")
	if got := classifyLeaseLookup(transportErr, false); !errors.Is(got, transportErr) || errors.Is(got, ErrLeaseNotFound) {
		t.Fatalf("transport classification = %v, want original transport error", got)
	}
	if got := classifyLeaseLookup(nil, false); !errors.Is(got, ErrLeaseNotFound) {
		t.Fatalf("inactive classification = %v, want ErrLeaseNotFound", got)
	}
}
