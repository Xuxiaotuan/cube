package agent

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/cube-js/cube-operator/internal/leadership"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const AuthorityTokenAudience = "cubestore-metastore-authority-v1"
const authorityInstallPath = "/v1/router-authority/install"

// AuthorityConfirmer returns the conservative local authorization deadline.
// A confirmation never substitutes for the runtime's per-RPC authority guard.
type AuthorityConfirmer interface {
	Confirm(context.Context, leadership.LeaseRecord) (time.Time, error)
}

type AuthorityConfig struct {
	URL, CAFile, TokenFile, TokenAudience           string
	ClusterUID, LeaseNamespace, LeaseName, LeaseUID string
	APITimeout, ValidationTimeout, MaxClockSkew     time.Duration
}

func AuthorityConfigFromEnv(getenv func(string) string) (*AuthorityConfig, error) {
	strict := strings.TrimSpace(getenv("CUBESTORE_AUTHORITY_STRICT"))
	if strict == "" || strict == "false" {
		return nil, nil
	}
	if strict != "true" {
		return nil, errors.New("CUBESTORE_AUTHORITY_STRICT must be true or false")
	}
	c := &AuthorityConfig{}
	for key, target := range map[string]*string{
		"URL": &c.URL, "CA_FILE": &c.CAFile, "TOKEN_FILE": &c.TokenFile,
		"TOKEN_AUDIENCE": &c.TokenAudience, "CLUSTER_UID": &c.ClusterUID,
		"LEASE_NAMESPACE": &c.LeaseNamespace, "LEASE_NAME": &c.LeaseName, "LEASE_UID": &c.LeaseUID,
	} {
		*target = strings.TrimSpace(getenv("CUBESTORE_AUTHORITY_" + key))
		if *target == "" {
			return nil, fmt.Errorf("CUBESTORE_AUTHORITY_%s is required", key)
		}
	}
	for key, target := range map[string]*time.Duration{
		"API_TIMEOUT_MS": &c.APITimeout, "VALIDATION_TIMEOUT_MS": &c.ValidationTimeout, "MAX_CLOCK_SKEW_MS": &c.MaxClockSkew,
	} {
		raw := strings.TrimSpace(getenv("CUBESTORE_AUTHORITY_" + key))
		if raw == "" || strings.IndexFunc(raw, func(r rune) bool { return r < '0' || r > '9' }) >= 0 {
			return nil, fmt.Errorf("CUBESTORE_AUTHORITY_%s requires explicit integer milliseconds", key)
		}
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || n > int64((1<<63-1)/time.Millisecond) || (n == 0 && key != "MAX_CLOCK_SKEW_MS") {
			return nil, fmt.Errorf("CUBESTORE_AUTHORITY_%s is out of range", key)
		}
		*target = time.Duration(n) * time.Millisecond
	}
	if err := c.validate(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c AuthorityConfig) validate() error {
	if c.TokenAudience != AuthorityTokenAudience {
		return errors.New("authority token audience does not match protocol")
	}
	if c.ClusterUID == "" || c.LeaseNamespace == "" || c.LeaseName == "" || c.LeaseUID == "" {
		return errors.New("authority identity configuration is incomplete")
	}
	if !filepath.IsAbs(c.CAFile) || !filepath.IsAbs(c.TokenFile) {
		return errors.New("authority CA and token paths must be absolute")
	}
	if c.APITimeout <= 0 || c.ValidationTimeout <= 0 || c.MaxClockSkew < 0 {
		return errors.New("authority timing configuration is invalid")
	}
	u, err := url.Parse(c.URL)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/" && u.Path != authorityInstallPath) {
		return errors.New("authority URL must be an HTTPS origin or the exact install endpoint")
	}
	return nil
}

// The endpoint authenticates the projected token using TokenReview and the
// fixed audience. The response's holderPodUid is checked against the actual Pod
// object, not against holderIdentity (which is a Pod NAME) or unverified JWT data.
type AuthorityIdentity struct {
	Lease                  leadership.LeaseRecord
	LeaseUID, HolderPodUID string
}

type AuthorityIdentitySource interface {
	Read(context.Context) (AuthorityIdentity, error)
}

type KubernetesAuthorityIdentitySource struct {
	Reader                                      client.Reader
	Namespace, LeaseName, ClusterID, HolderName string
}

func (s *KubernetesAuthorityIdentitySource) Read(ctx context.Context) (AuthorityIdentity, error) {
	if s.Reader == nil {
		return AuthorityIdentity{}, errors.New("authority requires a direct API reader")
	}
	var obj coordinationv1.Lease
	if err := s.Reader.Get(ctx, client.ObjectKey{Namespace: s.Namespace, Name: s.LeaseName}, &obj); err != nil {
		return AuthorityIdentity{}, errors.New("authority Lease read failed")
	}
	a := obj.Annotations
	if obj.UID == "" || obj.DeletionTimestamp != nil || a["cubejs.io/lease-cluster-id"] != s.ClusterID ||
		obj.Spec.HolderIdentity == nil || *obj.Spec.HolderIdentity != s.HolderName ||
		obj.Spec.LeaseTransitions == nil || *obj.Spec.LeaseTransitions <= 0 ||
		obj.Spec.LeaseDurationSeconds == nil || *obj.Spec.LeaseDurationSeconds <= 0 || obj.Spec.AcquireTime == nil || strings.TrimSpace(a["cubejs.io/lease-token"]) == "" {
		return AuthorityIdentity{}, errors.New("authority Lease identity is invalid")
	}
	var pod corev1.Pod
	if err := s.Reader.Get(ctx, client.ObjectKey{Namespace: s.Namespace, Name: s.HolderName}, &pod); err != nil {
		return AuthorityIdentity{}, errors.New("authority holder Pod read failed")
	}
	if pod.UID == "" || pod.DeletionTimestamp != nil {
		return AuthorityIdentity{}, errors.New("authority holder Pod identity is invalid")
	}
	holderUID := strings.TrimSpace(a["cubejs.io/lease-holder-uid"])
	if holderUID != "" && holderUID != string(pod.UID) {
		return AuthorityIdentity{}, errors.New("authority Lease holder UID disagrees with Pod")
	}
	generation := strings.TrimSpace(a["cubejs.io/lease-generation"])
	if generation == "" {
		generation = "1"
	}
	renewed := obj.Spec.AcquireTime.Time
	if obj.Spec.RenewTime != nil {
		renewed = obj.Spec.RenewTime.Time
	}
	return AuthorityIdentity{LeaseUID: string(obj.UID), HolderPodUID: string(pod.UID), Lease: leadership.LeaseRecord{
		ClusterID: s.ClusterID, HolderID: s.HolderName, HolderUID: holderUID, Epoch: int64(*obj.Spec.LeaseTransitions), Generation: generation,
		Token: strings.TrimSpace(a["cubejs.io/lease-token"]), IssuedAt: obj.Spec.AcquireTime.Time, ExpiresAt: renewed.Add(time.Duration(*obj.Spec.LeaseDurationSeconds) * time.Second),
	}}, nil
}

type HTTPSAuthorityInstaller struct {
	config   AuthorityConfig
	source   AuthorityIdentitySource
	client   *http.Client
	endpoint string
}

func NewHTTPSAuthorityInstaller(c AuthorityConfig, source AuthorityIdentitySource) (*HTTPSAuthorityInstaller, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
	if source == nil {
		return nil, errors.New("authority identity source is required")
	}
	ca, err := readAuthorityFile(c.CAFile, 1<<20)
	if err != nil {
		return nil, errors.New("cannot read authority CA file")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return nil, errors.New("authority CA file contains no certificates")
	}
	u, _ := url.Parse(c.URL)
	u.Path = authorityInstallPath
	u.RawPath = ""
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}, TLSHandshakeTimeout: c.APITimeout, ResponseHeaderTimeout: c.APITimeout}
	return &HTTPSAuthorityInstaller{config: c, source: source, endpoint: u.String(), client: &http.Client{
		Transport: transport, Timeout: c.APITimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("authority redirects are forbidden") },
	}}, nil
}

type authorityInstallResponse struct {
	ProtocolVersion   int    `json:"protocolVersion"`
	ServerIncarnation string `json:"serverIncarnation"`
	GrantID           string `json:"grantId"`
	ClusterUID        string `json:"clusterUid"`
	LeaseUID          string `json:"leaseUid"`
	HolderPodUID      string `json:"holderPodUid"`
	RouterEpoch       string `json:"routerEpoch"`
	ValidForMs        int64  `json:"validForMs"`
}

func (a *HTTPSAuthorityInstaller) identity(ctx context.Context, expected leadership.LeaseRecord) (AuthorityIdentity, error) {
	validationCtx, cancel := context.WithTimeout(ctx, a.config.ValidationTimeout)
	defer cancel()
	id, err := a.source.Read(validationCtx)
	if err != nil {
		return AuthorityIdentity{}, fmt.Errorf("authority identity validation failed: %w", err)
	}
	if err := validationCtx.Err(); err != nil {
		return AuthorityIdentity{}, err
	}
	if id.LeaseUID != a.config.LeaseUID || id.HolderPodUID == "" || leadership.ValidateLeaseFence(id.Lease, expected) != nil || !id.Lease.ExpiresAt.Add(-a.config.MaxClockSkew).After(time.Now()) {
		return AuthorityIdentity{}, errors.New("authority identity or Lease validity mismatch")
	}
	return id, nil
}

func (a *HTTPSAuthorityInstaller) Confirm(ctx context.Context, expected leadership.LeaseRecord) (time.Time, error) {
	before, err := a.identity(ctx, expected)
	if err != nil {
		return time.Time{}, err
	}
	// Open for every call: Kubernetes rotates projected token files by symlink.
	raw, err := readAuthorityFile(a.config.TokenFile, 64<<10)
	if err != nil {
		return time.Time{}, errors.New("cannot read projected authority token")
	}
	token := strings.TrimSpace(string(raw))
	if token == "" || strings.IndexFunc(token, unicode.IsSpace) >= 0 {
		return time.Time{}, errors.New("projected authority token is empty or malformed")
	}
	apiCtx, cancel := context.WithTimeout(ctx, a.config.APITimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(apiCtx, http.MethodPost, a.endpoint, bytes.NewBufferString(`{"protocolVersion":1}`))
	if err != nil {
		return time.Time{}, errors.New("cannot construct authority request")
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+token)
	started := time.Now()
	response, err := a.client.Do(request)
	if err != nil {
		return time.Time{}, fmt.Errorf("authority HTTPS install failed: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return time.Time{}, fmt.Errorf("authority install rejected: HTTP %d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, (64<<10)+1))
	if err != nil || len(body) > 64<<10 {
		return time.Time{}, errors.New("authority response unreadable or oversized")
	}
	var ack authorityInstallResponse
	if err := json.Unmarshal(body, &ack); err != nil {
		return time.Time{}, errors.New("authority response is malformed")
	}
	if ack.ProtocolVersion != 1 || strings.TrimSpace(ack.ServerIncarnation) == "" || strings.TrimSpace(ack.GrantID) == "" ||
		ack.ClusterUID != a.config.ClusterUID || ack.LeaseUID != before.LeaseUID || ack.HolderPodUID != before.HolderPodUID || ack.RouterEpoch != strconv.FormatInt(expected.Epoch, 10) ||
		ack.ValidForMs <= 0 || ack.ValidForMs > int64((1<<63-1)/time.Millisecond) {
		return time.Time{}, errors.New("authority acknowledgement identity or validity mismatch")
	}
	if err := apiCtx.Err(); err != nil {
		return time.Time{}, err
	}
	after, err := a.identity(ctx, expected)
	if err != nil {
		return time.Time{}, err
	}
	if after.LeaseUID != before.LeaseUID || after.HolderPodUID != before.HolderPodUID {
		return time.Time{}, errors.New("authority identity changed after install")
	}
	// Using request start charges the full network/validation latency against
	// the relative server lifetime. Never extend the originally observed Lease.
	deadline := started.Add(time.Duration(ack.ValidForMs) * time.Millisecond).Add(-a.config.MaxClockSkew)
	for _, limit := range []time.Time{expected.ExpiresAt.Add(-a.config.MaxClockSkew), after.Lease.ExpiresAt.Add(-a.config.MaxClockSkew)} {
		if limit.Before(deadline) {
			deadline = limit
		}
	}
	if !deadline.After(time.Now()) {
		return time.Time{}, errors.New("authority grant expired before publication")
	}
	return deadline, nil
}

func readAuthorityFile(path string, limit int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil || int64(len(raw)) > limit {
		return nil, errors.New("authority file unreadable or oversized")
	}
	return raw, nil
}

// Fence before strict-mode configuration/CA loading can fail at process startup.
// This does not make an unwritable filesystem safe: callers must fail startup
// and the runtime must still enforce existing file expiry and per-RPC guards.
func FenceAuthorityFiles(path, promotionPath string) error {
	if filepath.Clean(path) == filepath.Clean(promotionPath) {
		return errors.New("authority inputs must use distinct paths")
	}
	now := time.Now().UTC()
	return errors.Join(writeAtomic(path, LeadershipFile{IssuedAt: now, ExpiresAt: now.Add(-time.Second)}), writeAtomic(promotionPath, promotionDocument{}))
}
