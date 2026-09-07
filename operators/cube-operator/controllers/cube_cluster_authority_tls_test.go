package controllers

import (
	"bytes"
	"context"
	"crypto/x509"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/cube-js/cube-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func authorityTLSCluster() *v1alpha1.CubeCluster {
	return &v1alpha1.CubeCluster{ObjectMeta: metav1.ObjectMeta{Namespace: "pki-fixture", Name: "analytics", UID: "cluster-uid-1"}}
}

type authorityTLSRecordingClient struct {
	client.Client
	creates       int
	unknownCreate bool
}

func (c *authorityTLSRecordingClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	c.creates++
	err := c.Client.Create(ctx, obj, opts...)
	if err == nil && c.unknownCreate {
		c.unknownCreate = false
		return context.DeadlineExceeded
	}
	return err
}

type authorityTLSRaceReader struct {
	client.Reader
	missSecretOnce bool
}

func (r *authorityTLSRaceReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*corev1.Secret); ok && r.missSecretOnce {
		r.missSecretOnce = false
		return apierrors.NewNotFound(schema.GroupResource{Resource: "secrets"}, key.Name)
	}
	return r.Reader.Get(ctx, key, obj, opts...)
}

func authorityTLSReconciler(t *testing.T, objects ...client.Object) (*CubeClusterReconciler, *authorityTLSRecordingClient) {
	t.Helper()
	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	s.AddKnownTypes(schema.GroupVersion{Group: "cubestore.io", Version: "v1alpha1"}, &v1alpha1.CubeCluster{}, &v1alpha1.CubeClusterList{})
	base := fake.NewClientBuilder().WithScheme(s).WithObjects(objects...).Build()
	recorded := &authorityTLSRecordingClient{Client: base}
	return &CubeClusterReconciler{Client: recorded, APIReader: base, Scheme: s}, recorded
}

func authorityTLSGenerated(t *testing.T, c *v1alpha1.CubeCluster, now time.Time) *corev1.Secret {
	t.Helper()
	s, err := generateMetaStoreAuthorityTLS(c, now)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestAuthorityTLSGenerateAndReuse(t *testing.T) {
	c := authorityTLSCluster()
	r, recorded := authorityTLSReconciler(t, c)
	first, err := r.ensureMetaStoreAuthorityTLS(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	second, err := r.ensureMetaStoreAuthorityTLS(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	if recorded.creates != 1 || !reflect.DeepEqual(first.Data, second.Data) {
		t.Fatal("existing certificate was regenerated or changed")
	}
	if first.Name != "analytics-metastore-tls" || first.Namespace != c.Namespace || len(first.Data) != 3 || first.Immutable == nil || !*first.Immutable {
		t.Fatal("wrong concrete Secret resource contract")
	}
	if _, ok := first.Data["ca.key"]; ok {
		t.Fatal("CA private key persisted")
	}
	leaf, err := authorityCertificate(first.Data[corev1.TLSCertKey])
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"analytics-metastore", "analytics-metastore.pki-fixture", "analytics-metastore.pki-fixture.svc", "analytics-metastore.pki-fixture.svc.cluster.local"}
	if !reflect.DeepEqual(leaf.DNSNames, want) || !reflect.DeepEqual(leaf.ExtKeyUsage, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}) {
		t.Fatal("wrong Service SANs or certificate usage")
	}
	if leaf.NotAfter.Sub(leaf.NotBefore) != metaStoreAuthorityCertificateLifetime {
		t.Fatal("wrong certificate lifetime")
	}
}

func TestAuthorityTLSRequiresAuthoritativeReader(t *testing.T) {
	c := authorityTLSCluster()
	r, recorded := authorityTLSReconciler(t, c)
	r.APIReader = nil
	if _, err := r.ensureMetaStoreAuthorityTLS(context.Background(), c); err == nil {
		t.Fatal("accepted cached-only path")
	}
	if recorded.creates != 0 {
		t.Fatal("created without authoritative identity")
	}
}

func TestAuthorityTLSRejectsChangedClusterIdentity(t *testing.T) {
	c := authorityTLSCluster()
	fresh := c.DeepCopy()
	fresh.UID = "replacement-uid"
	r, recorded := authorityTLSReconciler(t, fresh)
	if _, err := r.ensureMetaStoreAuthorityTLS(context.Background(), c); err == nil {
		t.Fatal("accepted old cluster UID")
	}
	if recorded.creates != 0 {
		t.Fatal("created for a replaced cluster")
	}
}

func TestAuthorityTLSRejectsUnpersistedCluster(t *testing.T) {
	c := authorityTLSCluster()
	c.UID = ""
	r, recorded := authorityTLSReconciler(t, c)
	if _, err := r.ensureMetaStoreAuthorityTLS(context.Background(), c); err == nil {
		t.Fatal("accepted empty UID")
	}
	if recorded.creates != 0 {
		t.Fatal("created without UID")
	}
}

func TestAuthorityTLSRejectsDeletingCluster(t *testing.T) {
	c := authorityTLSCluster()
	c.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	c.Finalizers = []string{"fixture"}
	r, recorded := authorityTLSReconciler(t, c)
	if _, err := r.ensureMetaStoreAuthorityTLS(context.Background(), c); err == nil {
		t.Fatal("accepted deleting cluster")
	}
	if recorded.creates != 0 {
		t.Fatal("created while deleting")
	}
}

func TestAuthorityTLSExistingSecretValidation(t *testing.T) {
	c := authorityTLSCluster()
	now := time.Now()
	valid := authorityTLSGenerated(t, c, now)
	other := authorityTLSGenerated(t, c, now)
	for _, test := range []struct {
		name   string
		mutate func(*corev1.Secret)
	}{
		{"foreign_uid", func(s *corev1.Secret) { s.OwnerReferences[0].UID = "foreign" }},
		{"no_owner", func(s *corev1.Secret) { s.OwnerReferences = nil }},
		{"wrong_owner_kind", func(s *corev1.Secret) { s.OwnerReferences[0].Kind = "Other" }},
		{"mutable", func(s *corev1.Secret) { s.Immutable = ptrBool(false) }},
		{"wrong_type", func(s *corev1.Secret) { s.Type = corev1.SecretTypeOpaque }},
		{"extra_private_key", func(s *corev1.Secret) { s.Data["ca.key"] = []byte("forbidden") }},
		{"bad_ca", func(s *corev1.Secret) { s.Data["ca.crt"] = []byte("not PEM") }},
		{"wrong_ca", func(s *corev1.Secret) { s.Data["ca.crt"] = other.Data["ca.crt"] }},
		{"mismatched_key", func(s *corev1.Secret) { s.Data[corev1.TLSPrivateKeyKey] = other.Data[corev1.TLSPrivateKeyKey] }},
		{"deleting", func(s *corev1.Secret) {
			s.DeletionTimestamp = &metav1.Time{Time: now}
			s.Finalizers = []string{"fixture"}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := valid.DeepCopy()
			test.mutate(s)
			r, recorded := authorityTLSReconciler(t, c, s)
			if _, err := r.ensureMetaStoreAuthorityTLS(context.Background(), c); err == nil {
				t.Fatal("accepted invalid Secret")
			}
			if recorded.creates != 0 {
				t.Fatal("attempted automatic replacement")
			}
			var retained corev1.Secret
			if err := r.Get(context.Background(), client.ObjectKeyFromObject(s), &retained); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(s.Data, retained.Data) {
				t.Fatal("modified rejected Secret")
			}
		})
	}
}

func TestAuthorityTLSRejectsExpiredWithoutRotation(t *testing.T) {
	c := authorityTLSCluster()
	s := authorityTLSGenerated(t, c, time.Now().Add(-metaStoreAuthorityCertificateLifetime-time.Hour))
	r, recorded := authorityTLSReconciler(t, c, s)
	if _, err := r.ensureMetaStoreAuthorityTLS(context.Background(), c); err == nil || !strings.Contains(err.Error(), "expired") {
		t.Fatalf("expected expiry refusal, got %v", err)
	}
	if recorded.creates != 0 {
		t.Fatal("rotated expired CA")
	}
}

func TestAuthorityTLSRejectsNotYetValid(t *testing.T) {
	c := authorityTLSCluster()
	s := authorityTLSGenerated(t, c, time.Now().Add(time.Hour))
	if err := validateMetaStoreAuthorityTLS(c, s, time.Now()); err == nil {
		t.Fatal("accepted future certificate")
	}
}

func TestAuthorityTLSRejectsWrongServiceSANs(t *testing.T) {
	c := authorityTLSCluster()
	other := c.DeepCopy()
	other.Name = "wrong-service"
	s := authorityTLSGenerated(t, other, time.Now())
	s.ObjectMeta = authorityTLSGenerated(t, c, time.Now()).ObjectMeta
	if err := validateMetaStoreAuthorityTLS(c, s, time.Now()); err == nil {
		t.Fatal("accepted wrong Service SANs")
	}
}

func TestAuthorityTLSConcurrentCreateValidatesWinner(t *testing.T) {
	c := authorityTLSCluster()
	winner := authorityTLSGenerated(t, c, time.Now())
	r, recorded := authorityTLSReconciler(t, c, winner)
	r.APIReader = &authorityTLSRaceReader{Reader: r.APIReader, missSecretOnce: true}
	s, err := r.ensureMetaStoreAuthorityTLS(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	if recorded.creates != 1 || !bytes.Equal(s.Data[corev1.TLSCertKey], winner.Data[corev1.TLSCertKey]) {
		t.Fatal("did not preserve race winner")
	}
}

func TestAuthorityTLSConcurrentForeignWinnerFailsClosed(t *testing.T) {
	c := authorityTLSCluster()
	winner := authorityTLSGenerated(t, c, time.Now())
	winner.OwnerReferences[0].UID = "foreign"
	r, _ := authorityTLSReconciler(t, c, winner)
	r.APIReader = &authorityTLSRaceReader{Reader: r.APIReader, missSecretOnce: true}
	if _, err := r.ensureMetaStoreAuthorityTLS(context.Background(), c); err == nil {
		t.Fatal("adopted foreign race winner")
	}
}

func TestAuthorityTLSUnknownCreateOutcomeReusesCommittedSecret(t *testing.T) {
	c := authorityTLSCluster()
	r, recorded := authorityTLSReconciler(t, c)
	recorded.unknownCreate = true
	if _, err := r.ensureMetaStoreAuthorityTLS(context.Background(), c); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected unknown create outcome, got %v", err)
	}
	var committed corev1.Secret
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: c.Namespace, Name: metaStoreAuthorityTLSName(c)}, &committed); err != nil {
		t.Fatal(err)
	}
	result, err := r.ensureMetaStoreAuthorityTLS(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	if recorded.creates != 1 || !reflect.DeepEqual(result.Data, committed.Data) {
		t.Fatal("regenerated after uncertain Create")
	}
}
