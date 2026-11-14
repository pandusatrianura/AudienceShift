package tunnel_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/pandusatrianura/AudienceShift/internal/features/inspector"
	"github.com/pandusatrianura/AudienceShift/internal/features/tunnel"
)

func TestTunnelFullLoop(t *testing.T) {
	// 1. Mock local application server
	receivedLocal := make(chan string, 1)
	localApp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		receivedLocal <- string(body)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Custom-App-Header", "audience-shift-verified")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"success","app":"received"}`))
	}))
	defer localApp.Close()

	// 2. Tunnel Server
	tunnelServer := tunnel.NewServer("tunnel.example.com", "secret-token")
	tsServer := httptest.NewServer(tunnelServer)
	defer tsServer.Close()

	// 3. Tunnel Client
	store := inspector.NewMemoryStore(10)
	bus := inspector.NewEventBus()
	defer bus.Close()

	eventCh, unsub := bus.Subscribe(10)
	defer unsub()

	// Convert http:// to ws://
	wsURL := "ws" + strings.TrimPrefix(tsServer.URL, "http") + tunnel.ConnectPath

	client := tunnel.NewClient(tunnel.ClientOptions{
		ServerURL:   wsURL,
		TunnelID:    "dev-user",
		AuthToken:   "secret-token",
		LocalTarget: localApp.URL,
		Store:       store,
		Bus:         bus,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = client.Start(ctx)
	}()
	defer client.Stop()

	// Give client connection a moment to establish
	time.Sleep(200 * time.Millisecond)

	// Verify server has 1 active tunnel
	if count := tunnelServer.ActiveCount(); count != 1 {
		t.Fatalf("expected 1 active tunnel on server, got %d", count)
	}

	// 4. Send external webhook request to tunnel server
	webhookReq, err := http.NewRequest("POST", tsServer.URL+"/webhooks/stripe", strings.NewReader(`{"event":"payment_intent.succeeded"}`))
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	webhookReq.Header.Set(tunnel.HeaderTunnelID, "dev-user")
	webhookReq.Header.Set("Content-Type", "application/json")
	webhookReq.Header.Set("Stripe-Signature", "t=12345,v1=abcdef")

	resp, err := http.DefaultClient.Do(webhookReq)
	if err != nil {
		t.Fatalf("webhook request failed: %v", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)

	// 5. Assertions
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK from local app via tunnel, got %d. Body: %s", resp.StatusCode, respBody)
	}

	if !strings.Contains(string(respBody), `"status":"success"`) {
		t.Fatalf("unexpected response body: %s", respBody)
	}

	if resp.Header.Get("X-Custom-App-Header") != "audience-shift-verified" {
		t.Fatalf("expected custom header from local app to be relayed back")
	}

	// Verify local app received exact body
	select {
	case body := <-receivedLocal:
		if !strings.Contains(body, "payment_intent.succeeded") {
			t.Fatalf("local app received incorrect body: %s", body)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for local app to receive webhook")
	}

	// Verify store captured the event
	if store.Count() != 1 {
		t.Fatalf("expected store to contain 1 event, got %d", store.Count())
	}
	events, _ := store.List(1)
	if events[0].Provider != "stripe" {
		t.Errorf("expected detected provider 'stripe', got %q", events[0].Provider)
	}
	if events[0].Response.StatusCode != 200 {
		t.Errorf("expected stored response status 200, got %d", events[0].Response.StatusCode)
	}

	// Verify EventBus published the event
	select {
	case evt := <-eventCh:
		if evt.ID != events[0].ID {
			t.Errorf("bus event ID mismatch")
		}
		if evt.Provider != "stripe" {
			t.Errorf("bus event provider mismatch")
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for event bus broadcast")
	}
}

func TestSubdomainExtraction(t *testing.T) {
	req, _ := http.NewRequest("GET", "http://example.com/test", nil)

	// Header extraction
	req.Header.Set(tunnel.HeaderTunnelID, "custom-id")
	if sub := tunnel.ExtractSubdomain(req, "example.com"); sub != "custom-id" {
		t.Errorf("expected header 'custom-id', got %q", sub)
	}
	req.Header.Del(tunnel.HeaderTunnelID)

	// Query param extraction
	req.URL.RawQuery = "tunnel=query-id"
	if sub := tunnel.ExtractSubdomain(req, "example.com"); sub != "query-id" {
		t.Errorf("expected query 'query-id', got %q", sub)
	}
	req.URL.RawQuery = ""

	// Subdomain extraction
	req.Host = "john.tunnel.example.com:8080"
	if sub := tunnel.ExtractSubdomain(req, "tunnel.example.com"); sub != "john" {
		t.Errorf("expected subdomain 'john', got %q", sub)
	}
}

func TestSubdomainHijackingProtection(t *testing.T) {
	tunnelServer := tunnel.NewServer("tunnel.example.com", "shared-token")
	tsServer := httptest.NewServer(tunnelServer)
	defer tsServer.Close()

	wsURL := "ws" + strings.TrimPrefix(tsServer.URL, "http") + tunnel.ConnectPath

	// Client 1 connects with id 'team-tunnel'
	client1 := tunnel.NewClient(tunnel.ClientOptions{
		ServerURL: wsURL,
		TunnelID:  "team-tunnel",
		AuthToken: "shared-token",
	})
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()

	go func() {
		_ = client1.Start(ctx1)
	}()
	defer client1.Stop()

	time.Sleep(200 * time.Millisecond)
	if tunnelServer.ActiveCount() != 1 {
		t.Fatalf("expected 1 active tunnel, got %d", tunnelServer.ActiveCount())
	}

	// Client 2 attempts to claim 'team-tunnel' with WRONG token
	client2 := tunnel.NewClient(tunnel.ClientOptions{
		ServerURL: wsURL,
		TunnelID:  "team-tunnel",
		AuthToken: "attacker-wrong-token",
	})
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	go func() {
		_ = client2.Start(ctx2)
	}()
	time.Sleep(300 * time.Millisecond)
	cancel2()
	client2.Stop()

	// Client 1's tunnel must STILL be alive and healthy!
	if _, ok := tunnelServer.GetTunnel("team-tunnel"); !ok {
		t.Fatalf("expected client 1's tunnel to remain active after attack attempt")
	}
}

func TestServerHealthz(t *testing.T) {
	srv := tunnel.NewServer("tunnel.example.com", "")
	ts := httptest.NewServer(srv)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatalf("failed to query /healthz: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK from /healthz, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), `"status":"healthy"`) {
		t.Errorf("expected healthz response to contain healthy status, got %s", body)
	}
}

func TestServerRateLimiter(t *testing.T) {
	srv := tunnel.NewServer("tunnel.example.com", "")
	srv.RateLimit = 2.0 // 2 req/sec
	srv.RateBurst = 2.0 // burst of 2

	// First 2 requests allowed
	if !srv.AllowRequest("test-sub") {
		t.Errorf("expected request 1 to be allowed")
	}
	if !srv.AllowRequest("test-sub") {
		t.Errorf("expected request 2 to be allowed")
	}

	// 3rd rapid request should be throttled
	if srv.AllowRequest("test-sub") {
		t.Errorf("expected request 3 to be rate limited (exceeding burst)")
	}

	// Different subdomain should have its own separate quota
	if !srv.AllowRequest("other-sub") {
		t.Errorf("expected other subdomain to be allowed independently")
	}
}
