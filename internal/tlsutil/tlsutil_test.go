package tlsutil

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/thealexjo/binwave/internal/config"
)

// writeCA writes a throwaway self-signed CA certificate and returns its path.
func writeCA(t *testing.T) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestBuild(t *testing.T) {
	ca := writeCA(t)

	tests := []struct {
		name         string
		tls          config.TLS
		wantNil      bool
		wantInsecure bool
		wantRoots    bool
	}{
		{"disabled", config.TLS{Mode: config.TLSDisabled}, true, false, false},
		{"preferred encrypts without verifying", config.TLS{Mode: config.TLSPreferred}, false, true, false},
		{"required encrypts without verifying", config.TLS{Mode: config.TLSRequired}, false, true, false},
		{"verify-full uses system roots", config.TLS{Mode: config.TLSVerifyFull}, false, false, false},
		{"verify-full with CA", config.TLS{Mode: config.TLSVerifyFull, CAFile: ca}, false, false, true},
		{"required with CA verifies", config.TLS{Mode: config.TLSRequired, CAFile: ca}, false, false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Build(tt.tls, "db.internal")
			if err != nil {
				t.Fatal(err)
			}
			if (got == nil) != tt.wantNil {
				t.Fatalf("config nil = %v, want %v", got == nil, tt.wantNil)
			}
			if got == nil {
				return
			}
			if got.InsecureSkipVerify != tt.wantInsecure {
				t.Errorf("InsecureSkipVerify = %v, want %v", got.InsecureSkipVerify, tt.wantInsecure)
			}
			if (got.RootCAs != nil) != tt.wantRoots {
				t.Errorf("RootCAs set = %v, want %v", got.RootCAs != nil, tt.wantRoots)
			}
			if got.ServerName != "db.internal" {
				t.Errorf("ServerName = %q", got.ServerName)
			}
		})
	}
}

func TestBuildErrors(t *testing.T) {
	dir := t.TempDir()
	notPEM := filepath.Join(dir, "junk.pem")
	if err := os.WriteFile(notPEM, []byte("junk"), 0o600); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		tls  config.TLS
	}{
		{"missing CA file", config.TLS{Mode: config.TLSRequired, CAFile: filepath.Join(dir, "nope.pem")}},
		{"CA without certificates", config.TLS{Mode: config.TLSRequired, CAFile: notPEM}},
		{"bad client certificate", config.TLS{Mode: config.TLSRequired, CertFile: notPEM, KeyFile: notPEM}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := Build(tt.tls, "h"); err == nil {
				t.Error("Build() succeeded, want an error")
			}
		})
	}
}
