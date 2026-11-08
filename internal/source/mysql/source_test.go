package mysql

import (
	"crypto/tls"
	"strings"
	"testing"

	"github.com/thealexjo/binwave/internal/config"
)

// TestDriverConfigMakesSessionsReadOnly guards the rule that binwave never writes to MySQL.
// The server enforces it on every SQL connection the source opens (verified against a real
// server: a write fails with error 1792).
func TestDriverConfigMakesSessionsReadOnly(t *testing.T) {
	for _, mode := range []config.TLSMode{config.TLSDisabled, config.TLSPreferred, config.TLSRequired, config.TLSVerifyFull} {
		t.Run(string(mode), func(t *testing.T) {
			drv := driverConfig(config.MySQL{Host: "db", Port: 3306, User: "u", TLS: config.TLS{Mode: mode}}, nil)
			if got := drv.Params["transaction_read_only"]; got != "1" {
				t.Errorf("transaction_read_only = %q, want 1", got)
			}
		})
	}
}

func TestDriverConfigAddressAndTLS(t *testing.T) {
	cfg := config.MySQL{Host: "db.internal", Port: 3307, User: "u", Password: "p", TLS: config.TLS{Mode: config.TLSPreferred}}
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}

	drv := driverConfig(cfg, tlsCfg)

	if drv.Addr != "db.internal:3307" || drv.User != "u" || drv.Passwd != "p" {
		t.Errorf("connection settings = %s %s %s", drv.Addr, drv.User, drv.Passwd)
	}
	if drv.TLS != tlsCfg {
		t.Error("TLS configuration was not passed through")
	}
	if !drv.AllowFallbackToPlaintext {
		t.Error("preferred mode must fall back to plaintext")
	}

	cfg.TLS.Mode = config.TLSRequired
	if driverConfig(cfg, tlsCfg).AllowFallbackToPlaintext {
		t.Error("required mode must not fall back to plaintext")
	}
}

func TestDriverConfigIPv6Address(t *testing.T) {
	drv := driverConfig(config.MySQL{Host: "::1", Port: 3306}, nil)
	if drv.Addr != "[::1]:3306" {
		t.Errorf("Addr = %q, want [::1]:3306", drv.Addr)
	}
}

// TestDriverConfigCompressesResultSets: the chunks of a snapshot are nearly all the bytes binwave
// reads from the source, and rows of text compress several times over. On by default, and one setting
// away from off for a source that has no CPU to spare.
func TestDriverConfigCompressesResultSets(t *testing.T) {
	on := driverConfig(config.MySQL{Host: "h", Port: 3306, User: "u"}, nil)
	if !strings.Contains(on.FormatDSN(), "compress=true") {
		t.Errorf("the connection does not compress by default: %s", on.FormatDSN())
	}
	no := false
	off := driverConfig(config.MySQL{Host: "h", Port: 3306, User: "u", Compression: &no}, nil)
	if strings.Contains(off.FormatDSN(), "compress=true") {
		t.Errorf("compression: false was ignored: %s", off.FormatDSN())
	}
}
