package main

import (
	"bufio"
	"os"
	"strings"
	"testing"
)

// serviceSection returns the settings of the [Service] section of a unit file, every value of a
// setting in the order given.
func serviceSection(t *testing.T, path string) map[string][]string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	settings := map[string][]string{}
	section := ""
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		switch {
		case line == "" || strings.HasPrefix(line, "#"):
		case strings.HasPrefix(line, "["):
			section = line
		case section == "[Service]":
			key, value, _ := strings.Cut(line, "=")
			settings[key] = append(settings[key], value)
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return settings
}

// TestUnitFilesMatchTheProcess: the units start binwave the way the process expects it.
func TestUnitFilesMatchTheProcess(t *testing.T) {
	units := []struct {
		path, config string
	}{
		{"../../deploy/systemd/binwave.service", defaultConfigPath},
		{"../../deploy/systemd/binwave@.service", "/etc/binwave/%i.yaml"},
	}
	for _, u := range units {
		t.Run(u.path, func(t *testing.T) {
			s := serviceSection(t, u.path)
			// binwave handles only SIGTERM and SIGINT, so a reload signal would kill it.
			if r := s["ExecReload"]; len(r) > 0 {
				t.Errorf("ExecReload=%s, but binwave has no reload signal", r[0])
			}
			// systemd passes no environment of its own, and the config takes its secrets from it.
			want := "-" + strings.TrimSuffix(u.config, ".yaml") + ".env"
			if got := s["EnvironmentFile"]; len(got) != 1 || got[0] != want {
				t.Errorf("EnvironmentFile = %q, want %q next to the config", got, want)
			}
		})
	}
}
