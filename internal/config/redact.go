package config

import (
	"fmt"

	"go.yaml.in/yaml/v3"
)

const redacted = "******"

// Redacted returns a copy of the configuration with secrets masked, safe for logs and output.
func (c *Config) Redacted() *Config {
	cp := *c
	if cp.Source.MySQL.Password != "" {
		cp.Source.MySQL.Password = redacted
	}
	if cp.Target.ClickHouse.Password != "" {
		cp.Target.ClickHouse.Password = redacted
	}
	return &cp
}

// RedactedYAML renders the effective configuration with secrets masked.
func (c *Config) RedactedYAML() ([]byte, error) {
	out, err := yaml.Marshal(c.Redacted())
	if err != nil {
		return nil, fmt.Errorf("encode config: %w", err)
	}
	return out, nil
}
