// Package tlsutil builds client TLS configurations from the binwave TLS settings.
package tlsutil

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"

	"github.com/thealexjo/binwave/internal/config"
)

// Build turns the configured TLS mode into a client configuration; nil means plaintext.
//
//   - disabled: no TLS.
//   - preferred: TLS when the server offers it, without verification unless a CA is configured.
//   - required: TLS or failure, without verification unless a CA is configured.
//   - verify-full: TLS with certificate and host name verification.
//
// A configured CA file turns verification on in every mode that uses TLS.
func Build(t config.TLS, host string) (*tls.Config, error) {
	if t.Mode == config.TLSDisabled {
		return nil, nil
	}

	cfg := &tls.Config{ServerName: host, MinVersion: tls.VersionTLS12}

	switch {
	case t.CAFile != "":
		pem, err := os.ReadFile(t.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("CA file " + t.CAFile + " contains no certificates")
		}
		cfg.RootCAs = pool
	case t.Mode == config.TLSVerifyFull:
		// System roots verify the server.
	default:
		cfg.InsecureSkipVerify = true
	}

	if t.CertFile != "" {
		cert, err := tls.LoadX509KeyPair(t.CertFile, t.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load client certificate: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}
