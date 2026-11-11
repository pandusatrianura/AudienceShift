package inspector

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// HTTPRequest represents a captured HTTP request forwarded to the local webhook handler.
type HTTPRequest struct {
	Method   string              `json:"method"`
	Path     string              `json:"path"`
	Query    string              `json:"query,omitempty"`
	Headers  map[string][]string `json:"headers"`
	Body     []byte              `json:"body"`
	ClientIP string              `json:"client_ip,omitempty"`
}

// Header returns the first value associated with the given key (case-insensitive).
func (r *HTTPRequest) Header(key string) string {
	if r.Headers == nil {
		return ""
	}
	for k, v := range r.Headers {
		if strings.EqualFold(k, key) && len(v) > 0 {
			return v[0]
		}
	}
	return ""
}

// IsJSON returns true if the Request Content-Type indicates JSON.
func (r *HTTPRequest) IsJSON() bool {
	ct := r.Header("Content-Type")
	return strings.Contains(strings.ToLower(ct), "application/json")
}

// MaskedHeaders returns a copy of headers where sensitive tokens are masked.
func (r *HTTPRequest) MaskedHeaders() map[string][]string {
	if r.Headers == nil {
		return nil
	}
	out := make(map[string][]string, len(r.Headers))
	for k, vv := range r.Headers {
		kLower := strings.ToLower(k)
		maskedList := make([]string, len(vv))
		for i, v := range vv {
			if isSensitiveHeader(kLower) {
				maskedList[i] = maskSecret(v)
			} else {
				maskedList[i] = v
			}
		}
		out[k] = maskedList
	}
	return out
}

func isSensitiveHeader(kLower string) bool {
	return strings.Contains(kLower, "authorization") ||
		strings.Contains(kLower, "cookie") ||
		strings.Contains(kLower, "token") ||
		strings.Contains(kLower, "secret") ||
		strings.Contains(kLower, "api-key") ||
		strings.Contains(kLower, "apikey") ||
		strings.Contains(kLower, "signature")
}

func maskSecret(s string) string {
	if len(s) <= 8 {
		return "********"
	}
	if strings.HasPrefix(strings.ToLower(s), "bearer ") {
		token := s[7:]
		if len(token) <= 8 {
			return "Bearer ********"
		}
		return fmt.Sprintf("Bearer %s...%s", token[:4], token[len(token)-3:])
	}
	return fmt.Sprintf("%s...%s", s[:4], s[len(s)-4:])
}

// HTTPResponse represents the response returned by the local application for a forwarded webhook.
type HTTPResponse struct {
	StatusCode int                 `json:"status_code"`
	Headers    map[string][]string `json:"headers,omitempty"`
	Body       []byte              `json:"body,omitempty"`
	Error      string              `json:"error,omitempty"`
}

// WebhookEvent represents an intercepted or replayed webhook lifecycle event.
type WebhookEvent struct {
	ID             string       `json:"id"`
	TunnelID       string       `json:"tunnel_id,omitempty"`
	Timestamp      time.Time    `json:"timestamp"`
	Provider       string       `json:"provider"` // "stripe", "github", "midtrans", "slack", "generic"
	Request        HTTPRequest  `json:"request"`
	Response       HTTPResponse `json:"response"`
	DurationMs     int64        `json:"duration_ms"`
	ReplayedFromID string       `json:"replayed_from_id,omitempty"`
}

// GenerateID produces a cryptographically random, collision-resistant event identifier.
func GenerateID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("evt_%d", time.Now().UnixNano())
	}
	return fmt.Sprintf("evt_%s", hex.EncodeToString(b))
}

// NewWebhookEvent constructs a new WebhookEvent with an auto-generated ID and timestamp.
func NewWebhookEvent(tunnelID string, req HTTPRequest) *WebhookEvent {
	return &WebhookEvent{
		ID:        GenerateID(),
		TunnelID:  tunnelID,
		Timestamp: time.Now().UTC(),
		Provider:  DetectProvider(req.Headers),
		Request:   req,
	}
}

// Clone creates a deep copy of the WebhookEvent with identical ID and properties.
func (e *WebhookEvent) Clone() *WebhookEvent {
	reqHeaders := make(map[string][]string, len(e.Request.Headers))
	for k, v := range e.Request.Headers {
		vals := make([]string, len(v))
		copy(vals, v)
		reqHeaders[k] = vals
	}

	reqBody := make([]byte, len(e.Request.Body))
	copy(reqBody, e.Request.Body)

	resHeaders := make(map[string][]string, len(e.Response.Headers))
	for k, v := range e.Response.Headers {
		vals := make([]string, len(v))
		copy(vals, v)
		resHeaders[k] = vals
	}

	resBody := make([]byte, len(e.Response.Body))
	copy(resBody, e.Response.Body)

	return &WebhookEvent{
		ID:        e.ID,
		TunnelID:  e.TunnelID,
		Timestamp: e.Timestamp,
		Provider:  e.Provider,
		Request: HTTPRequest{
			Method:   e.Request.Method,
			Path:     e.Request.Path,
			Query:    e.Request.Query,
			Headers:  reqHeaders,
			Body:     reqBody,
			ClientIP: e.Request.ClientIP,
		},
		Response: HTTPResponse{
			StatusCode: e.Response.StatusCode,
			Headers:    resHeaders,
			Body:       resBody,
			Error:      e.Response.Error,
		},
		DurationMs:     e.DurationMs,
		ReplayedFromID: e.ReplayedFromID,
	}
}

// CreateReplay constructs a new WebhookEvent based on this event with a new ID and replayed timestamp.
func (e *WebhookEvent) CreateReplay() *WebhookEvent {
	clone := e.Clone()
	clone.ID = GenerateID()
	clone.Timestamp = time.Now().UTC()
	clone.ReplayedFromID = e.ID
	clone.Response = HTTPResponse{}
	clone.DurationMs = 0
	return clone
}

// DetectProvider inspects incoming HTTP headers to identify popular webhook origins.
func DetectProvider(headers map[string][]string) string {
	if headers == nil {
		return "generic"
	}
	for k := range headers {
		keyLower := strings.ToLower(k)
		switch {
		case strings.HasPrefix(keyLower, "stripe-signature"):
			return "stripe"
		case strings.HasPrefix(keyLower, "x-hub-signature") || strings.HasPrefix(keyLower, "x-github-event"):
			return "github"
		case strings.HasPrefix(keyLower, "x-slack-signature"):
			return "slack"
		case strings.HasPrefix(keyLower, "x-shopify-hmac-sha256"):
			return "shopify"
		case strings.Contains(keyLower, "midtrans"):
			return "midtrans"
		case strings.HasPrefix(keyLower, "x-callback-token") || strings.Contains(keyLower, "xendit"):
			return "xendit"
		case strings.HasPrefix(keyLower, "x-gitlab-event") || strings.HasPrefix(keyLower, "x-gitlab-token"):
			return "gitlab"
		case strings.HasPrefix(keyLower, "x-signature-ed25519") || strings.HasPrefix(keyLower, "x-signature-timestamp"):
			return "discord"
		case strings.HasPrefix(keyLower, "x-twilio-signature"):
			return "twilio"
		}
	}
	return "generic"
}

// PrettyJSONBody returns an indented, human-readable representation of the body if valid JSON, or raw string.
func (r *HTTPRequest) PrettyJSONBody() string {
	if len(r.Body) == 0 {
		return ""
	}
	var out interface{}
	if err := json.Unmarshal(r.Body, &out); err == nil {
		indented, err := json.MarshalIndent(out, "", "  ")
		if err == nil {
			return string(indented)
		}
	}
	return string(r.Body)
}

// ToCURL formats the event's HTTP request as an executable cURL command.
func (e *WebhookEvent) ToCURL(targetBase string) string {
	if targetBase == "" {
		targetBase = "http://localhost:8080"
	}
	targetBase = strings.TrimRight(targetBase, "/")
	reqPath := e.Request.Path
	if !strings.HasPrefix(reqPath, "/") {
		reqPath = "/" + reqPath
	}
	fullURL := targetBase + reqPath

	var b strings.Builder
	b.WriteString(fmt.Sprintf("curl -X %s %q", e.Request.Method, fullURL))

	for k, vals := range e.Request.Headers {
		lower := strings.ToLower(k)
		if lower == "content-length" || lower == "connection" || lower == "host" || lower == "accept-encoding" {
			continue
		}
		for _, v := range vals {
			b.WriteString(fmt.Sprintf(" -H %q", fmt.Sprintf("%s: %s", k, v)))
		}
	}

	if len(e.Request.Body) > 0 {
		b.WriteString(fmt.Sprintf(" -d %q", string(e.Request.Body)))
	}

	return b.String()
}
