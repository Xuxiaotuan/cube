package controllers

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"time"

	"github.com/cube-js/cube-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// This is a certificate lifetime, NOT a Lease/clock-skew/validation budget.
// Renewal is an explicit maintenance operation; no CA private key is persisted.
const metaStoreAuthorityCertificateLifetime = 90 * 24 * time.Hour

func metaStoreAuthorityTLSName(c *v1alpha1.CubeCluster) string {
	return clusterName(c, cubeComponentMeta) + "-tls"
}

func metaStoreAuthorityDNSNames(c *v1alpha1.CubeCluster) []string {
	service := clusterName(c, cubeComponentMeta)
	return []string{service, service + "." + c.Namespace, service + "." + c.Namespace + ".svc", service + "." + c.Namespace + ".svc.cluster.local"}
}

// ensureMetaStoreAuthorityTLS is the CubeCluster renderer's PKI prerequisite.
// It neither enables authority mode nor changes workloads/RBAC. The caller must
// separately enforce the approved stop-write activation and grant ACK sequence.
func (r *CubeClusterReconciler) ensureMetaStoreAuthorityTLS(ctx context.Context, c *v1alpha1.CubeCluster) (*corev1.Secret, error) {
	if r.APIReader == nil {
		return nil, fmt.Errorf("MetaStore authority TLS requires an uncached APIReader")
	}
	if c.UID == "" || c.Namespace == "" || c.Name == "" {
		return nil, fmt.Errorf("MetaStore authority TLS requires a persisted CubeCluster identity")
	}
	var fresh v1alpha1.CubeCluster
	if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(c), &fresh); err != nil {
		return nil, err
	}
	if fresh.UID != c.UID || !fresh.DeletionTimestamp.IsZero() {
		return nil, fmt.Errorf("CubeCluster identity changed or is deleting; authority TLS creation refused")
	}
	key := client.ObjectKey{Namespace: c.Namespace, Name: metaStoreAuthorityTLSName(c)}
	var existing corev1.Secret
	err := r.APIReader.Get(ctx, key, &existing)
	if err == nil {
		if err := validateMetaStoreAuthorityTLS(c, &existing, time.Now()); err != nil {
			return nil, err
		}
		return &existing, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, err
	}
	secret, err := generateMetaStoreAuthorityTLS(c, time.Now())
	if err != nil {
		return nil, err
	}
	if err := r.Create(ctx, secret); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			// A timeout may mean Create committed. Do not regenerate/replace:
			// a later invocation must read and validate the authoritative object.
			return nil, err
		}
		if err := r.APIReader.Get(ctx, key, &existing); err != nil {
			return nil, err
		}
		if err := validateMetaStoreAuthorityTLS(c, &existing, time.Now()); err != nil {
			return nil, err
		}
		return &existing, nil
	}
	return secret, nil
}

func generateMetaStoreAuthorityTLS(c *v1alpha1.CubeCluster, now time.Time) (*corev1.Secret, error) {
	if c.UID == "" || c.Name == "" || c.Namespace == "" {
		return nil, fmt.Errorf("cannot sign authority TLS without CubeCluster identity")
	}
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	serverKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	serial := func() (*big.Int, error) {
		limit := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 128), big.NewInt(1))
		n, err := rand.Int(rand.Reader, limit)
		if err != nil {
			return nil, err
		}
		return n.Add(n, big.NewInt(1)), nil
	}
	caSerial, err := serial()
	if err != nil {
		return nil, err
	}
	serverSerial, err := serial()
	if err != nil {
		return nil, err
	}
	ca := &x509.Certificate{
		SerialNumber: caSerial, Subject: pkix.Name{CommonName: c.Namespace + "/" + metaStoreAuthorityTLSName(c) + " CA"},
		NotBefore: now.UTC(), NotAfter: now.UTC().Add(metaStoreAuthorityCertificateLifetime),
		IsCA: true, BasicConstraintsValid: true, MaxPathLen: 0, MaxPathLenZero: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	if err != nil {
		return nil, err
	}
	server := &x509.Certificate{
		SerialNumber: serverSerial, Subject: pkix.Name{CommonName: clusterName(c, cubeComponentMeta)},
		NotBefore: ca.NotBefore, NotAfter: ca.NotAfter, BasicConstraintsValid: true,
		DNSNames: metaStoreAuthorityDNSNames(c), KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	serverDER, err := x509.CreateCertificate(rand.Reader, server, ca, &serverKey.PublicKey, caKey)
	if err != nil {
		return nil, err
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(serverKey)
	if err != nil {
		return nil, err
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: metaStoreAuthorityTLSName(c), Namespace: c.Namespace,
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "cubestore.io/v1alpha1", Kind: "CubeCluster", Name: c.Name, UID: c.UID, Controller: ptrBool(true)}},
		},
		Immutable: ptrBool(true), Type: corev1.SecretTypeTLS,
		Data: map[string][]byte{
			"ca.crt":                pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}),
			corev1.TLSCertKey:       pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverDER}),
			corev1.TLSPrivateKeyKey: pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
		},
	}
	if err := validateMetaStoreAuthorityTLS(c, secret, now); err != nil {
		return nil, err
	}
	return secret, nil
}

func authorityCertificate(data []byte) (*x509.Certificate, error) {
	block, rest := pem.Decode(data)
	if block == nil || block.Type != "CERTIFICATE" || len(strings.TrimSpace(string(rest))) != 0 {
		return nil, fmt.Errorf("expected exactly one PEM certificate")
	}
	return x509.ParseCertificate(block.Bytes)
}

func validateMetaStoreAuthorityTLS(c *v1alpha1.CubeCluster, secret *corev1.Secret, now time.Time) error {
	fail := func(reason string) error {
		return fmt.Errorf("MetaStore authority TLS Secret %s/%s rejected: %s; explicit maintenance required, no automatic adoption or rotation", c.Namespace, metaStoreAuthorityTLSName(c), reason)
	}
	if c.UID == "" || secret.Name != metaStoreAuthorityTLSName(c) || secret.Namespace != c.Namespace || !secret.DeletionTimestamp.IsZero() {
		return fail("identity mismatch or deletion in progress")
	}
	owner := metav1.GetControllerOf(secret)
	if len(secret.OwnerReferences) != 1 || owner == nil || owner.UID != c.UID || owner.Name != c.Name || owner.Kind != "CubeCluster" || owner.APIVersion != "cubestore.io/v1alpha1" {
		return fail("foreign or absent controller owner")
	}
	if secret.Immutable == nil || !*secret.Immutable || secret.Type != corev1.SecretTypeTLS || len(secret.Data) != 3 {
		return fail("requires immutable TLS Secret with only ca.crt/tls.crt/tls.key")
	}
	ca, err := authorityCertificate(secret.Data["ca.crt"])
	if err != nil {
		return fail("invalid CA certificate")
	}
	leaf, err := authorityCertificate(secret.Data[corev1.TLSCertKey])
	if err != nil {
		return fail("invalid serving certificate")
	}
	if _, err := tls.X509KeyPair(secret.Data[corev1.TLSCertKey], secret.Data[corev1.TLSPrivateKeyKey]); err != nil {
		return fail("serving certificate/private key mismatch")
	}
	if !ca.IsCA || !ca.BasicConstraintsValid || ca.KeyUsage&x509.KeyUsageCertSign == 0 || ca.CheckSignatureFrom(ca) != nil {
		return fail("invalid dedicated CA")
	}
	if leaf.IsCA || len(leaf.ExtKeyUsage) != 1 || leaf.ExtKeyUsage[0] != x509.ExtKeyUsageServerAuth || len(leaf.UnknownExtKeyUsage) != 0 {
		return fail("serving certificate must be serverAuth only")
	}
	if leaf.NotAfter.After(ca.NotAfter) || !now.Before(leaf.NotAfter) || !now.Before(ca.NotAfter) || now.Before(leaf.NotBefore) || now.Before(ca.NotBefore) {
		return fail("expired, not yet valid, or inconsistent certificate validity")
	}
	want, got := metaStoreAuthorityDNSNames(c), append([]string(nil), leaf.DNSNames...)
	sort.Strings(want)
	sort.Strings(got)
	if strings.Join(want, "\n") != strings.Join(got, "\n") || len(leaf.IPAddresses)+len(leaf.URIs)+len(leaf.EmailAddresses) != 0 {
		return fail("serving SANs do not match the actual MetaStore Service")
	}
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	for _, name := range want {
		if _, err := leaf.Verify(x509.VerifyOptions{Roots: roots, DNSName: name, CurrentTime: now, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}); err != nil {
			return fail("serving certificate chain verification failed")
		}
	}
	return nil
}
