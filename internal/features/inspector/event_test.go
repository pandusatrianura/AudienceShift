package inspector_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/pandusatrianura/AudienceShift/internal/features/inspector"
)

func TestGenerateID(t *testing.T) {
	id1 := inspector.GenerateID()
	id2 := inspector.GenerateID()

	if !strings.HasPrefix(id1, "evt_") {
		t.Fatalf("expected prefix 'evt_', got %q", id1)
	}
	if id1 == id2 {
		t.Fatalf("expected unique IDs, got identical %q", id1)
	}
}

func TestDetectProvider(t *testing.T) {
	tests := []struct {
		name     string
		headers  map[string][]string
		expected string
	}{
		{
			name:     "Stripe signature",
			headers:  map[string][]string{"Stripe-Signature": {"t=123,v1=abc"}},
			expected: "stripe",
		},
		{
			name:     "GitHub webhook",
			headers:  map[string][]string{"X-Hub-Signature-256": {"sha256=abc"}},
			expected: "github",
		},
		{
			name:     "Slack webhook",
			headers:  map[string][]string{"X-Slack-Signature": {"v0=abc"}},
			expected: "slack",
		},
		{
			name:     "Shopify webhook",
			headers:  map[string][]string{"X-Shopify-Hmac-Sha256": {"abc"}},
			expected: "shopify",
		},
		{
			name:     "Midtrans webhook",
			headers:  map[string][]string{"X-Midtrans-Id": {"trans_123"}},
			expected: "midtrans",
		},
		{
			name:     "Xendit webhook",
			headers:  map[string][]string{"X-Callback-Token": {"xnd_test_token"}},
			expected: "xendit",
		},
		{
			name:     "GitLab webhook",
			headers:  map[string][]string{"X-Gitlab-Event": {"Push Hook"}},
			expected: "gitlab",
		},
		{
			name:     "Discord webhook",
			headers:  map[string][]string{"X-Signature-Ed25519": {"ed_sig_123"}},
			expected: "discord",
		},
		{
			name:     "Twilio webhook",
			headers:  map[string][]string{"X-Twilio-Signature": {"tw_sig_123"}},
			expected: "twilio",
		},
		{
			name:     "Generic webhook",
			headers:  map[string][]string{"Content-Type": {"application/json"}},
			expected: "generic",
		},
		{
			name:     "Nil headers",
			headers:  nil,
			expected: "generic",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := inspector.DetectProvider(tt.headers)
			if got != tt.expected {
				t.Errorf("DetectProvider() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestHTTPRequestHelpers(t *testing.T) {
	req := inspector.HTTPRequest{
		Method: "POST",
		Path:   "/webhooks/stripe",
		Headers: map[string][]string{
			"Content-Type":     {"application/json; charset=utf-8"},
			"Stripe-Signature": {"sig_test_123"},
		},
		Body: []byte(`{"event":"charge.succeeded","amount":5000}`),
	}

	// Case-insensitive header lookup
	if got := req.Header("content-type"); got != "application/json; charset=utf-8" {
		t.Errorf("Header() case-insensitive failed: got %q", got)
	}
	if got := req.Header("non-existent"); got != "" {
		t.Errorf("Header() should return empty string for non-existent key, got %q", got)
	}

	// IsJSON check
	if !req.IsJSON() {
		t.Errorf("expected IsJSON() to be true")
	}

	// PrettyJSONBody
	pretty := req.PrettyJSONBody()
	if !strings.Contains(pretty, "\n") || !strings.Contains(pretty, "charge.succeeded") {
		t.Errorf("PrettyJSONBody failed to format JSON, got: %s", pretty)
	}

	// Non-JSON fallback
	nonJSONReq := inspector.HTTPRequest{Body: []byte("raw plain text")}
	if nonJSONReq.PrettyJSONBody() != "raw plain text" {
		t.Errorf("PrettyJSONBody should return raw string for non-JSON")
	}
}

func TestWebhookEventCloneAndReplay(t *testing.T) {
	original := inspector.NewWebhookEvent("tunnel-abc", inspector.HTTPRequest{
		Method: "POST",
		Path:   "/hook",
		Headers: map[string][]string{
			"X-Custom-Header": {"val1", "val2"},
		},
		Body:     []byte(`{"key":"value"}`),
		ClientIP: "1.2.3.4",
	})
	original.Response = inspector.HTTPResponse{
		StatusCode: 200,
		Headers: map[string][]string{
			"Content-Type": {"application/json"},
		},
		Body: []byte(`{"received":true}`),
	}

	clone := original.Clone()

	// Verify deep copy fields
	if clone.ID != original.ID {
		t.Errorf("clone should preserve the same ID")
	}
	if clone.TunnelID != original.TunnelID {
		t.Errorf("TunnelID mismatch")
	}

	// Mutate clone and assert original is unmodified
	clone.Request.Headers["X-Custom-Header"][0] = "mutated"
	clone.Request.Body[0] = 'X'
	clone.Response.Body[0] = 'Z'

	if original.Request.Headers["X-Custom-Header"][0] == "mutated" {
		t.Errorf("mutation leaked into original request headers")
	}
	if original.Request.Body[0] == 'X' {
		t.Errorf("mutation leaked into original request body")
	}
	if original.Response.Body[0] == 'Z' {
		t.Errorf("mutation leaked into original response body")
	}

	// Test CreateReplay
	replay := original.CreateReplay()
	if replay.ID == original.ID {
		t.Errorf("replay should have a new ID")
	}
	if replay.ReplayedFromID != original.ID {
		t.Errorf("replay ReplayedFromID should point to original ID")
	}
	if replay.Response.StatusCode != 0 {
		t.Errorf("replay response should be cleared")
	}
}

func TestJSONRoundTrip(t *testing.T) {
	evt := inspector.NewWebhookEvent("tun-1", inspector.HTTPRequest{
		Method: "POST",
		Path:   "/test",
		Body:   []byte(`{"msg":"hello"}`),
	})
	evt.Response = inspector.HTTPResponse{
		StatusCode: 201,
		Body:       []byte(`{"status":"created"}`),
	}

	data, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("failed to marshal event: %v", err)
	}

	var parsed inspector.WebhookEvent
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("failed to unmarshal event: %v", err)
	}

	if parsed.ID != evt.ID || parsed.Request.Method != "POST" || parsed.Response.StatusCode != 201 {
		t.Fatalf("parsed event differs from original: %+v", parsed)
	}
}

func TestToCURL(t *testing.T) {
	evt := inspector.NewWebhookEvent("tun-1", inspector.HTTPRequest{
		Method: "POST",
		Path:   "/webhooks/stripe",
		Headers: map[string][]string{
			"Content-Type":     {"application/json"},
			"Stripe-Signature": {"t=123,v1=abc"},
			"Content-Length":   {"45"},          // Should be omitted
			"Host":             {"example.com"}, // Should be omitted
		},
		Body: []byte(`{"id":"evt_123"}`),
	})

	curlCmd := evt.ToCURL("http://localhost:3000")

	if !strings.HasPrefix(curlCmd, `curl -X POST "http://localhost:3000/webhooks/stripe"`) {
		t.Errorf("unexpected curl prefix: %s", curlCmd)
	}
	if !strings.Contains(curlCmd, `-H "Stripe-Signature: t=123,v1=abc"`) {
		t.Errorf("expected Stripe-Signature header in curl: %s", curlCmd)
	}
	if strings.Contains(curlCmd, "Content-Length") || strings.Contains(curlCmd, "Host:") {
		t.Errorf("curl command contains hop-by-hop header: %s", curlCmd)
	}
	if !strings.Contains(curlCmd, `-d "{\"id\":\"evt_123\"}"`) {
		t.Errorf("expected body in curl: %s", curlCmd)
	}
}

func TestMaskedHeaders(t *testing.T) {
	req := inspector.HTTPRequest{
		Method: "POST",
		Path:   "/api/v1/webhook",
		Headers: map[string][]string{
			"Authorization":    {"Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"},
			"Cookie":           {"session=abcdefghijklmnopqrstuvwxyz"},
			"X-Api-Key":        {"short"},
			"X-Callback-Token": {"xnd_development_secret_token_12345"},
			"Content-Type":     {"application/json"},
			"Accept":           {"text/plain"},
		},
		Body: []byte(`{}`),
	}

	masked := req.MaskedHeaders()
	if masked == nil {
		t.Fatalf("expected non-nil masked headers")
	}

	// Authorization check
	auth := masked["Authorization"][0]
	if !strings.HasPrefix(auth, "Bearer ") || strings.Contains(auth, "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9") {
		t.Errorf("Authorization header not properly masked: got %q", auth)
	}

	// Cookie check
	cookie := masked["Cookie"][0]
	if cookie == "session=abcdefghijklmnopqrstuvwxyz" || !strings.Contains(cookie, "...") {
		t.Errorf("Cookie header not masked: got %q", cookie)
	}

	// Short token check (<= 8 chars)
	apiKey := masked["X-Api-Key"][0]
	if apiKey != "********" {
		t.Errorf("Short secret should be '********', got %q", apiKey)
	}

	// Callback token check
	cb := masked["X-Callback-Token"][0]
	if !strings.HasPrefix(cb, "xnd_") || !strings.HasSuffix(cb, "2345") || !strings.Contains(cb, "...") {
		t.Errorf("Callback token mask malformed: got %q", cb)
	}

	// Non-sensitive check
	if masked["Content-Type"][0] != "application/json" {
		t.Errorf("Non-sensitive header modified: got %q", masked["Content-Type"][0])
	}
	if masked["Accept"][0] != "text/plain" {
		t.Errorf("Non-sensitive header modified: got %q", masked["Accept"][0])
	}

	// Nil headers check
	nilReq := inspector.HTTPRequest{Headers: nil}
	if nilReq.MaskedHeaders() != nil {
		t.Errorf("expected nil for nil headers")
	}
}
