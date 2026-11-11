package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/pandusatrianura/AudienceShift/internal/features/config"
)

func TestConfigLoadDefaults(t *testing.T) {
	cfg, err := config.Load("")
	if err != nil {
		t.Fatalf("unexpected error loading empty path: %v", err)
	}
	if cfg.Server != "ws://localhost:8080/tunnel/connect" {
		t.Errorf("unexpected default server: %s", cfg.Server)
	}
	if cfg.Target != "http://localhost:8080" {
		t.Errorf("unexpected default target: %s", cfg.Target)
	}
	if cfg.WebAddr != ":4040" {
		t.Errorf("unexpected default web addr: %s", cfg.WebAddr)
	}
}

func TestConfigLoadFile(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, ".audienceshift.yaml")

	content := `
# AudienceShift Configuration
server: wss://tunnel.mycompany.com/tunnel/connect
target: 3000
id: dev-alice
token: secret_token_xyz
secret: whsec_stripe_123
web: true
web_addr: :4045
headless: true

secrets:
  stripe: whsec_stripe_live
  midtrans: Mid-server-key-999
`
	if err := os.WriteFile(configPath, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write temp config: %v", err)
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		t.Fatalf("failed to load config file: %v", err)
	}

	if cfg.Server != "wss://tunnel.mycompany.com/tunnel/connect" {
		t.Errorf("unexpected server: %s", cfg.Server)
	}
	if cfg.Target != "3000" {
		t.Errorf("unexpected target: %s", cfg.Target)
	}
	if cfg.TunnelID != "dev-alice" {
		t.Errorf("unexpected tunnel id: %s", cfg.TunnelID)
	}
	if cfg.Token != "secret_token_xyz" {
		t.Errorf("unexpected token: %s", cfg.Token)
	}
	if cfg.Secret != "whsec_stripe_123" {
		t.Errorf("unexpected secret: %s", cfg.Secret)
	}
	if !cfg.Web {
		t.Errorf("expected web to be true")
	}
	if cfg.WebAddr != ":4045" {
		t.Errorf("unexpected web_addr: %s", cfg.WebAddr)
	}
	if !cfg.Headless {
		t.Errorf("expected headless to be true")
	}
	if cfg.Secrets["stripe"] != "whsec_stripe_live" {
		t.Errorf("unexpected stripe secret: %s", cfg.Secrets["stripe"])
	}
	if cfg.Secrets["midtrans"] != "Mid-server-key-999" {
		t.Errorf("unexpected midtrans secret: %s", cfg.Secrets["midtrans"])
	}
}
