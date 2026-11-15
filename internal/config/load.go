package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"

	"go.yaml.in/yaml/v3"
)

// EnvLookup resolves environment variables; os.LookupEnv in production.
type EnvLookup func(name string) (string, bool)

// Load reads, expands, decodes, defaults and validates the configuration file at path.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	return Parse(data, os.LookupEnv)
}

// ControlSocket reads the control socket path from the configuration file at path, for the commands
// that only talk to a running service: nothing else of the file is decoded or expanded, so they need
// none of the secrets the service is started with.
func ControlSocket(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read config: %w", err)
	}
	return ParseControlSocket(data, os.LookupEnv)
}

// ParseControlSocket is ControlSocket over the content of the file.
func ParseControlSocket(data []byte, env EnvLookup) (string, error) {
	var doc struct {
		Control struct {
			Socket string `yaml:"socket"`
		} `yaml:"control"`
	}
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return "", fmt.Errorf("parse config: %w", err)
	}
	socket, err := expandString(doc.Control.Socket, env)
	if err != nil {
		return "", fmt.Errorf("expand config: control.socket: %w", err)
	}
	if socket == "" {
		socket = DefaultControlSocket
	}
	v := &validator{}
	validateSocket(v, socket)
	if len(v.problems) > 0 {
		return "", &ValidationError{Problems: v.problems}
	}
	return socket, nil
}

// Parse decodes configuration from YAML, expanding ${VAR} references in scalar values,
// then applies defaults and validates the result. Unknown keys are rejected.
func Parse(data []byte, env EnvLookup) (*Config, error) {
	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if len(doc.Content) == 0 {
		return nil, errors.New("parse config: empty document")
	}

	if err := expandEnv(&doc, env); err != nil {
		return nil, fmt.Errorf("expand config: %w", err)
	}

	// Re-encode the expanded tree so it can be decoded strictly: yaml.Node.Decode
	// does not support rejecting unknown fields.
	expanded, err := yaml.Marshal(&doc)
	if err != nil {
		return nil, fmt.Errorf("encode expanded config: %w", err)
	}

	cfg := &Config{}
	dec := yaml.NewDecoder(bytes.NewReader(expanded))
	dec.KnownFields(true)
	if err := dec.Decode(cfg); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("decode config: %w", err)
	}

	cfg.applyDefaults(env)
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// envRef matches $${ (an escaped literal "${") or ${NAME}.
var envRef = regexp.MustCompile(`\$\$\{|\$\{([^}]*)\}`)

var envName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// expandEnv replaces ${NAME} in scalar values (never in keys). Unset variables are errors.
func expandEnv(node *yaml.Node, env EnvLookup) error {
	var errs []error

	var walk func(n *yaml.Node, isKey bool)
	walk = func(n *yaml.Node, isKey bool) {
		switch n.Kind {
		case yaml.ScalarNode:
			if isKey || !strings.Contains(n.Value, "${") {
				return
			}
			value, err := expandString(n.Value, env)
			if err != nil {
				errs = append(errs, fmt.Errorf("line %d: %w", n.Line, err))
				return
			}
			n.Value = value
			if n.Style == 0 {
				// Plain scalars get their type re-resolved from the expanded value,
				// so port: ${PORT} decodes as an integer. Quote to force a string.
				n.Tag = ""
			}
		case yaml.MappingNode:
			for i, child := range n.Content {
				walk(child, i%2 == 0)
			}
		default:
			for _, child := range n.Content {
				walk(child, false)
			}
		}
	}
	walk(node, false)

	return errors.Join(errs...)
}

func expandString(s string, env EnvLookup) (string, error) {
	var errs []error
	out := envRef.ReplaceAllStringFunc(s, func(match string) string {
		if match == "$${" {
			return "${"
		}
		name := match[2 : len(match)-1]
		if !envName.MatchString(name) {
			errs = append(errs, fmt.Errorf("invalid environment variable reference %q", match))
			return match
		}
		value, ok := env(name)
		if !ok {
			errs = append(errs, fmt.Errorf("environment variable %s is not set", name))
			return match
		}
		return value
	})
	return out, errors.Join(errs...)
}
