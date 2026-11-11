package config

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Config represents developer preferences loaded from .audienceshift.yaml / .audienceshift.yml.
type Config struct {
	Server   string            `yaml:"server"`
	Target   string            `yaml:"target"`
	TunnelID string            `yaml:"id"`
	Token    string            `yaml:"token"`
	Secret   string            `yaml:"secret"`
	Web      bool              `yaml:"web"`
	WebAddr  string            `yaml:"web_addr"`
	Headless bool              `yaml:"headless"`
	Secrets  map[string]string `yaml:"secrets"`
}

// DefaultConfig returns default configuration values.
func DefaultConfig() *Config {
	return &Config{
		Server:  "ws://localhost:8080/tunnel/connect",
		Target:  "http://localhost:8080",
		WebAddr: ":4040",
		Secrets: make(map[string]string),
	}
}

// FindConfigFile searches for .audienceshift.yaml or .audienceshift.yml in the current working directory.
func FindConfigFile() string {
	candidates := []string{
		".audienceshift.yaml",
		".audienceshift.yml",
		"audienceshift.yaml",
		"audienceshift.yml",
	}
	for _, f := range candidates {
		if fi, err := os.Stat(f); err == nil && !fi.IsDir() {
			return f
		}
	}
	return ""
}

// Load reads and parses a simple YAML config file without requiring heavy external dependencies.
// It parses top-level key-values and a nested 'secrets' block.
func Load(path string) (*Config, error) {
	cfg := DefaultConfig()
	if path == "" {
		return cfg, nil
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening config file: %w", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	inSecretsBlock := false

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// Handle indentation for nested secrets map
		rawLine := scanner.Text()
		if inSecretsBlock {
			if strings.HasPrefix(rawLine, " ") || strings.HasPrefix(rawLine, "\t") {
				parts := strings.SplitN(line, ":", 2)
				if len(parts) == 2 {
					k := strings.TrimSpace(parts[0])
					v := strings.Trim(strings.TrimSpace(parts[1]), `"'`)
					cfg.Secrets[k] = v
				}
				continue
			} else {
				inSecretsBlock = false
			}
		}

		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.ToLower(strings.TrimSpace(parts[0]))
		val := strings.Trim(strings.TrimSpace(parts[1]), `"'`)

		switch key {
		case "server":
			cfg.Server = val
		case "target":
			cfg.Target = val
		case "id", "tunnel_id":
			cfg.TunnelID = val
		case "token":
			cfg.Token = val
		case "secret":
			cfg.Secret = val
		case "web":
			b, _ := strconv.ParseBool(val)
			cfg.Web = b
		case "web_addr":
			cfg.WebAddr = val
		case "headless":
			b, _ := strconv.ParseBool(val)
			cfg.Headless = b
		case "secrets":
			inSecretsBlock = true
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("reading config: %w", err)
	}

	return cfg, nil
}
