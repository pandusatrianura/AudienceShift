package config

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// CustomPresetConfig defines user-specified webhook templates in .audienceshift.yaml.
type CustomPresetConfig struct {
	ID          string            `json:"id" yaml:"id"`
	Provider    string            `json:"provider" yaml:"provider"`
	Name        string            `json:"name" yaml:"name"`
	Description string            `json:"description" yaml:"description"`
	Method      string            `json:"method" yaml:"method"`
	Path        string            `json:"path" yaml:"path"`
	Headers     map[string]string `json:"headers" yaml:"headers"`
	Body        string            `json:"body" yaml:"body"`
}

// Config represents developer preferences loaded from .audienceshift.yaml / .audienceshift.yml.
type Config struct {
	Server        string               `yaml:"server"`
	Target        string               `yaml:"target"`
	TunnelID      string               `yaml:"id"`
	Token         string               `yaml:"token"`
	Secret        string               `yaml:"secret"`
	Web           bool                 `yaml:"web"`
	WebAddr       string               `yaml:"web_addr"`
	Headless      bool                 `yaml:"headless"`
	Secrets       map[string]string    `yaml:"secrets"`
	CustomPresets []CustomPresetConfig `yaml:"custom_presets"`
}

// DefaultConfig returns default configuration values.
func DefaultConfig() *Config {
	return &Config{
		Server:        "ws://localhost:8080/tunnel/connect",
		Target:        "http://localhost:8080",
		WebAddr:       ":4040",
		Secrets:       make(map[string]string),
		CustomPresets: make([]CustomPresetConfig, 0),
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
	inPresetsBlock := false
	var currentPreset *CustomPresetConfig

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		rawLine := scanner.Text()

		// Handle indentation for nested secrets map
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

		// Handle indentation for custom_presets list
		if inPresetsBlock {
			if strings.HasPrefix(rawLine, " ") || strings.HasPrefix(rawLine, "\t") || strings.HasPrefix(line, "-") {
				cleanLine := strings.TrimPrefix(line, "- ")
				parts := strings.SplitN(cleanLine, ":", 2)
				if len(parts) == 2 {
					subKey := strings.ToLower(strings.TrimSpace(parts[0]))
					subVal := strings.Trim(strings.TrimSpace(parts[1]), `"'`)
					if strings.HasPrefix(line, "-") || subKey == "id" {
						if currentPreset != nil {
							cfg.CustomPresets = append(cfg.CustomPresets, *currentPreset)
						}
						currentPreset = &CustomPresetConfig{Headers: make(map[string]string)}
					}
					if currentPreset != nil {
						switch subKey {
						case "id":
							currentPreset.ID = subVal
						case "provider":
							currentPreset.Provider = subVal
						case "name":
							currentPreset.Name = subVal
						case "description":
							currentPreset.Description = subVal
						case "method":
							currentPreset.Method = subVal
						case "path":
							currentPreset.Path = subVal
						case "body":
							currentPreset.Body = subVal
						}
					}
				}
				continue
			} else {
				if currentPreset != nil {
					cfg.CustomPresets = append(cfg.CustomPresets, *currentPreset)
					currentPreset = nil
				}
				inPresetsBlock = false
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
		case "custom_presets":
			inPresetsBlock = true
		}
	}

	if currentPreset != nil {
		cfg.CustomPresets = append(cfg.CustomPresets, *currentPreset)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("reading config: %w", err)
	}

	return cfg, nil
}
