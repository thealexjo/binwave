//go:build e2e

package e2e

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestPasswordsNeverAppearInLogs: a comparable tool logged credentials and passed them on command
// lines (issues 398, 1291, 1293). The MySQL library binwave uses logs its own configuration when a
// stream starts, so every log format and level is checked for the secret.
func TestPasswordsNeverAppearInLogs(t *testing.T) {
	t.Parallel()
	s := newStack(t)
	s.mustExec("CREATE TABLE binwave.sec (id INT PRIMARY KEY)")

	const secret = "S3cr3t-Pw-Zq9!xY"
	root := rootDB(t, s)
	for _, q := range []string{
		"CREATE USER 'leaky'@'%' IDENTIFIED BY '" + secret + "'",
		"GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON *.* TO 'leaky'@'%'",
	} {
		if _, err := root.Exec(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}

	for _, format := range []string{"json", "text"} {
		t.Run(format, func(t *testing.T) {
			cfg := s.writeConfig(settings{
				mysqlUser: "leaky", mysqlPassword: secret,
				logLevel: "debug", logFormat: format,
			})
			p := startBinwave(t, cfg)
			p.waitReady()
			s.mustExec("INSERT INTO binwave.sec VALUES (" + fmt.Sprint(time.Now().UnixNano()%1000000) + ")")
			time.Sleep(time.Second)
			p.term(30 * time.Second)

			logs := p.logs()
			if strings.Contains(logs, secret) {
				t.Errorf("the MySQL password appears in the %s logs:\n%s", format, grepLines(logs, secret))
			}
			for _, part := range []string{"S3cr3t", "Zq9!xY"} {
				if strings.Contains(logs, part) {
					t.Errorf("a fragment of the password %q appears in the %s logs", part, format)
				}
			}
		})
	}
}

func grepLines(s, needle string) string {
	var out []string
	for _, l := range strings.Split(s, "\n") {
		if strings.Contains(l, needle) {
			out = append(out, tail(l, 400))
		}
	}
	return strings.Join(out, "\n")
}
