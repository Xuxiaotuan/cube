package agent

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cube-js/cube-operator/internal/leadership"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type authorityTestSource struct {
	mu     sync.Mutex
	id     AuthorityIdentity
	calls  int
	change func(*AuthorityIdentity, int)
}

func (s *authorityTestSource) Read(ctx context.Context) (AuthorityIdentity, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return AuthorityIdentity{}, err
	}
	s.calls++
	id := s.id
	if s.change != nil {
		s.change(&id, s.calls)
	}
	return id, nil
}

func authorityEnv() map[string]string {
	return map[string]string{
		"STRICT": "true", "URL": "https://metastore.example:9998", "CA_FILE": "/projected/ca.pem", "TOKEN_FILE": "/projected/token",
		"TOKEN_AUDIENCE": AuthorityTokenAudience, "CLUSTER_UID": "cluster-uid", "LEASE_NAMESPACE": "test", "LEASE_NAME": "router", "LEASE_UID": "lease-uid",
		"API_TIMEOUT_MS": "1000", "VALIDATION_TIMEOUT_MS": "1000", "MAX_CLOCK_SKEW_MS": "0",
	}
}

func TestAuthorityConfigurationIsExplicit(t *testing.T) {
	parse := func(values map[string]string) (*AuthorityConfig, error) {
		return AuthorityConfigFromEnv(func(key string) string { return values[strings.TrimPrefix(key, "CUBESTORE_AUTHORITY_")] })
	}
	if _, err := parse(authorityEnv()); err != nil {
		t.Fatal(err)
	}
	for key := range authorityEnv() {
		if key == "STRICT" {
			continue
		}
		t.Run("missing_"+key, func(t *testing.T) {
			env := authorityEnv()
			delete(env, key)
			if _, err := parse(env); err == nil {
				t.Fatal("missing strict configuration accepted")
			}
		})
	}
	for _, key := range []string{"API_TIMEOUT_MS", "VALIDATION_TIMEOUT_MS", "MAX_CLOCK_SKEW_MS"} {
		for _, value := range []string{"-1", "1.5", "NaN", "+1", "9223372036854775807"} {
			env := authorityEnv()
			env[key] = value
			if _, err := parse(env); err == nil {
				t.Fatalf("invalid numeric configuration accepted: %s", key)
			}
		}
	}
	for _, key := range []string{"API_TIMEOUT_MS", "VALIDATION_TIMEOUT_MS"} {
		env := authorityEnv()
		env[key] = "0"
		if _, err := parse(env); err == nil {
			t.Fatal("zero timeout accepted")
		}
	}
	for key, value := range map[string]string{"STRICT": "yes", "TOKEN_AUDIENCE": "other", "URL": "http://metastore:9998", "TOKEN_FILE": "relative"} {
		env := authorityEnv()
		env[key] = value
		if _, err := parse(env); err == nil {
			t.Fatalf("invalid %s accepted", key)
		}
	}
	for _, strict := range []string{"", "false"} {
		if cfg, err := parse(map[string]string{"STRICT": strict, "API_TIMEOUT_MS": "invalid"}); err != nil || cfg != nil {
			t.Fatal("strict=false changed legacy behavior")
		}
	}
}

func authorityFixture(t *testing.T, handler http.HandlerFunc) (*HTTPSAuthorityInstaller, *authorityTestSource, leadership.LeaseRecord) {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	dir := t.TempDir()
	caPath, tokenPath := filepath.Join(dir, "ca.pem"), filepath.Join(dir, "token")
	if err := os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tokenPath, []byte("token-one\n"), 0600); err != nil {
		t.Fatal(err)
	}
	record := validRecord("router-a", 9, "lease-secret", time.Now().UTC())
	record.Generation = "1"
	source := &authorityTestSource{id: AuthorityIdentity{Lease: record, LeaseUID: "lease-uid", HolderPodUID: "pod-uid"}}
	installer, err := NewHTTPSAuthorityInstaller(AuthorityConfig{URL: server.URL, CAFile: caPath, TokenFile: tokenPath, TokenAudience: AuthorityTokenAudience,
		ClusterUID: "cluster-uid", LeaseNamespace: "test", LeaseName: "router", LeaseUID: "lease-uid", APITimeout: time.Second, ValidationTimeout: time.Second}, source)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(installer.client.CloseIdleConnections)
	return installer, source, record
}

func authorityAck() map[string]any {
	return map[string]any{"protocolVersion": 1, "serverIncarnation": "server-1", "grantId": "grant-secret", "clusterUid": "cluster-uid", "leaseUid": "lease-uid", "holderPodUid": "pod-uid", "routerEpoch": "9", "validForMs": 5000}
}

func TestAuthorityInstallContractAndTokenRotation(t *testing.T) {
	var mu sync.Mutex
	var received []string
	installer, source, record := authorityFixture(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != authorityInstallPath {
			t.Error("unexpected install method/path")
		}
		body, err := io.ReadAll(r.Body)
		if err != nil || string(body) != `{"protocolVersion":1}` {
			t.Error("install request changed protocol")
		}
		mu.Lock()
		received = append(received, r.Header.Get("Authorization"))
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(authorityAck())
	})
	for i := 0; i < 2; i++ {
		if i == 1 {
			// Replace the path, as a projected-volume token rotation would.
			replacement := installer.config.TokenFile + ".new"
			if err := os.WriteFile(replacement, []byte("token-two"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.Rename(replacement, installer.config.TokenFile); err != nil {
				t.Fatal(err)
			}
		}
		deadline, err := installer.Confirm(context.Background(), record)
		if err != nil {
			t.Fatal(err)
		}
		if !deadline.After(time.Now()) || !deadline.Before(record.ExpiresAt) {
			t.Fatal("grant lifetime did not cap local Lease lifetime")
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(received) != 2 || received[0] != "Bearer token-one" || received[1] != "Bearer token-two" {
		t.Fatal("projected token was not reread")
	}
	if source.calls != 4 {
		t.Fatalf("want authoritative validation before/after both acknowledgements, got %d", source.calls)
	}
}

func TestAuthorityRejectsInvalidAcknowledgements(t *testing.T) {
	for key, value := range map[string]any{
		"protocolVersion": 2, "serverIncarnation": "", "grantId": "", "clusterUid": "wrong", "leaseUid": "wrong", "holderPodUid": "wrong", "routerEpoch": "09", "validForMs": 0,
	} {
		t.Run(key, func(t *testing.T) {
			installer, _, record := authorityFixture(t, func(w http.ResponseWriter, _ *http.Request) {
				ack := authorityAck()
				ack[key] = value
				_ = json.NewEncoder(w).Encode(ack)
			})
			if _, err := installer.Confirm(context.Background(), record); err == nil {
				t.Fatal("invalid acknowledgement accepted")
			}
		})
	}
	for _, body := range []string{`{"protocolVersion":1,"routerEpoch":9}`, `{`, strings.Repeat("x", (64<<10)+1)} {
		installer, _, record := authorityFixture(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, body) })
		if _, err := installer.Confirm(context.Background(), record); err == nil {
			t.Fatal("malformed or oversized response accepted")
		}
	}
}

func TestAuthorityRejectsLeaseChangeAfterACK(t *testing.T) {
	for _, field := range []string{"holder", "epoch", "token", "generation", "holder-uid", "lease-uid", "pod-uid", "expired"} {
		t.Run(field, func(t *testing.T) {
			installer, source, record := authorityFixture(t, func(w http.ResponseWriter, _ *http.Request) { _ = json.NewEncoder(w).Encode(authorityAck()) })
			source.change = func(id *AuthorityIdentity, call int) {
				if call != 2 {
					return
				}
				switch field {
				case "holder":
					id.Lease.HolderID = "other"
				case "epoch":
					id.Lease.Epoch++
				case "token":
					id.Lease.Token = "other"
				case "generation":
					id.Lease.Generation = "other"
				case "holder-uid":
					id.Lease.HolderUID = "other"
				case "lease-uid":
					id.LeaseUID = "other"
				case "pod-uid":
					id.HolderPodUID = "other"
				case "expired":
					id.Lease.ExpiresAt = time.Now().Add(-time.Second)
				}
			}
			if _, err := installer.Confirm(context.Background(), record); err == nil {
				t.Fatal("changed authoritative identity accepted")
			}
		})
	}
}

func TestAuthorityTLSBearerRedirectAndTimeoutFailures(t *testing.T) {
	for _, failure := range []string{"san", "ca", "bearer", "redirect", "timeout", "cancelled", "missing-token", "expired-grant"} {
		t.Run(failure, func(t *testing.T) {
			release := make(chan struct{})
			installer, _, record := authorityFixture(t, func(w http.ResponseWriter, r *http.Request) {
				switch failure {
				case "bearer":
					w.WriteHeader(http.StatusUnauthorized)
				case "redirect":
					w.Header().Set("Location", "https://elsewhere.invalid/install")
					w.WriteHeader(http.StatusTemporaryRedirect)
				case "timeout":
					if _, err := io.Copy(io.Discard, r.Body); err != nil {
						return
					}
					select {
					case <-r.Context().Done():
					case <-release:
					}
				default:
					_ = json.NewEncoder(w).Encode(authorityAck())
				}
			})
			// Registered after the fixture so this unblocks the handler before
			// httptest.Server.Close runs, even if cancellation is not observed.
			t.Cleanup(func() { close(release) })
			switch failure {
			case "san":
				u, _ := url.Parse(installer.endpoint)
				u.Host = "localhost:" + u.Port()
				installer.endpoint = u.String()
			case "ca":
				// Use a trusted root set with no server CA; never disable verification.
				installer.client.Transport.(*http.Transport).TLSClientConfig.RootCAs = nil
			case "timeout":
				installer.config.APITimeout = 30 * time.Millisecond
			case "missing-token":
				if err := os.Remove(installer.config.TokenFile); err != nil {
					t.Fatal(err)
				}
			case "expired-grant":
				installer.config.MaxClockSkew = 6 * time.Second
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if failure == "cancelled" {
				cancel()
			}
			_, err := installer.Confirm(ctx, record)
			if err == nil {
				t.Fatal("unsafe authority confirmation accepted")
			}
			if failure == "timeout" && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("want authority API deadline exceeded, got %v", err)
			}
		})
	}
}

type authorityConfirmFunc func(context.Context, leadership.LeaseRecord) (time.Time, error)

func (f authorityConfirmFunc) Confirm(ctx context.Context, r leadership.LeaseRecord) (time.Time, error) {
	return f(ctx, r)
}

func TestAuthorityGatesLocalPublicationAndDemotion(t *testing.T) {
	now := time.Now().UTC()
	record := validRecord("router-a", 9, "lease-secret", now)
	checks := 0
	var a *Agent
	confirmedUntil := now.Add(5 * time.Second)
	confirmer := authorityConfirmFunc(func(_ context.Context, got leadership.LeaseRecord) (time.Time, error) {
		checks++
		if leadership.ValidateLeaseFence(got, record) != nil {
			t.Error("confirmation fence changed")
		}
		if checks == 1 {
			assertPromotionFenced(t, a, now)
			return confirmedUntil, nil
		}
		return time.Time{}, errors.New("install rejected")
	})
	dir := t.TempDir()
	var err error
	a, err = New(Config{Store: fakeStore{get: func() (leadership.LeaseRecord, error) { return record, nil }}, Authority: confirmer,
		ClusterID: record.ClusterID, HolderID: record.HolderID, Path: filepath.Join(dir, "leadership.json"), PromotionPath: filepath.Join(dir, "promotion.json"),
		PromotionSource: promotionReaderFunc(func(context.Context) ([]byte, error) { return promotionJSON(t, record), nil }), RetryPeriod: time.Second, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if file := readLeadershipFile(t, a.path); file.HolderID != record.HolderID || !file.ExpiresAt.Equal(confirmedUntil) {
		t.Fatal("active file not capped to confirmed grant")
	}
	if err := a.Sync(context.Background()); err == nil {
		t.Fatal("install failure did not deny")
	}
	assertPromotionFenced(t, a, now)
	record.HolderID = "other"
	if err := a.Sync(context.Background()); !errors.Is(err, ErrNotHolder) {
		t.Fatal("demotion accepted")
	}
	if checks != 2 {
		t.Fatal("non-holder called install")
	}
	assertPromotionFenced(t, a, now)
}

func TestAuthorityKubernetesIdentityUsesExistingLeaseContract(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = coordinationv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	now := metav1.NewMicroTime(time.Now().UTC())
	holder := "router-a"
	epoch := int32(9)
	duration := int32(30)
	lease := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: "router", Namespace: "test", UID: "lease-uid", Annotations: map[string]string{
		"cubejs.io/lease-cluster-id": "test/router", "cubejs.io/lease-token": "lease-secret", "cubejs.io/lease-generation": "1",
	}}, Spec: coordinationv1.LeaseSpec{HolderIdentity: &holder, LeaseTransitions: &epoch, LeaseDurationSeconds: &duration, AcquireTime: &now, RenewTime: &now}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: holder, Namespace: "test", UID: "actual-pod-uid"}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(lease, pod).Build()
	source := &KubernetesAuthorityIdentitySource{Reader: c, Namespace: "test", LeaseName: "router", ClusterID: "test/router", HolderName: holder}
	id, err := source.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if id.Lease.HolderID != holder || id.HolderPodUID != "actual-pod-uid" || id.LeaseUID != "lease-uid" || id.Lease.Epoch != 9 {
		t.Fatal("Lease NAME/UID/epoch contract changed")
	}
	lease.Annotations["cubejs.io/lease-holder-uid"] = "wrong"
	if err := c.Update(context.Background(), lease); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Read(context.Background()); err == nil {
		t.Fatal("conflicting optional holder UID accepted")
	}
}
