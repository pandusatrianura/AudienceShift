package inspector_test

import (
	"strings"
	"testing"

	"github.com/pandusatrianura/AudienceShift/internal/features/inspector"
)

func TestExportJSONAndPostman(t *testing.T) {
	evt := inspector.NewWebhookEvent("test-tunnel", inspector.HTTPRequest{
		Method: "POST",
		Path:   "/webhooks/stripe",
		Headers: map[string][]string{
			"Authorization": {"Bearer secret_token_123"},
			"Content-Type":  {"application/json"},
		},
		Body: []byte(`{"id":"evt_123"}`),
	})
	evt.Provider = "stripe"

	events := []*inspector.WebhookEvent{evt}

	// 1. Test JSON Export masked
	jsonMasked, err := inspector.ExportJSON(events, false)
	if err != nil {
		t.Fatalf("ExportJSON failed: %v", err)
	}
	if strings.Contains(string(jsonMasked), "secret_token_123") {
		t.Errorf("expected sensitive token to be masked in default export!")
	}
	if !strings.Contains(string(jsonMasked), "Bearer ") || !strings.Contains(string(jsonMasked), "...") {
		t.Errorf("expected masked bearer token placeholder with ellipsis")
	}

	// 2. Test JSON Export unmasked
	jsonUnmasked, err := inspector.ExportJSON(events, true)
	if err != nil {
		t.Fatalf("ExportJSON unmasked failed: %v", err)
	}
	if !strings.Contains(string(jsonUnmasked), "secret_token_123") {
		t.Errorf("expected unmasked token when includeSecrets=true")
	}

	// 3. Test Postman Export
	postmanData, err := inspector.ExportPostman(events, "Test Collection")
	if err != nil {
		t.Fatalf("ExportPostman failed: %v", err)
	}
	if !strings.Contains(string(postmanData), `"schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json"`) {
		t.Errorf("expected valid Postman schema")
	}
	if !strings.Contains(string(postmanData), `{{base_url}}/webhooks/stripe`) {
		t.Errorf("expected Postman parameterized base URL")
	}
}
