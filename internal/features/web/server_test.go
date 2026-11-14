package web_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pandusatrianura/AudienceShift/internal/features/inspector"
	"github.com/pandusatrianura/AudienceShift/internal/features/replay"
	"github.com/pandusatrianura/AudienceShift/internal/features/web"
)

func TestWebServer(t *testing.T) {
	// Mock local app
	localApp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"received":true}`))
	}))
	defer localApp.Close()

	store := inspector.NewMemoryStore(10)
	bus := inspector.NewEventBus()
	defer bus.Close()
	replaySvc := replay.NewService(store, bus, localApp.URL)

	// Save test event
	evt := inspector.NewWebhookEvent("test-tun", inspector.HTTPRequest{
		Method: "POST",
		Path:   "/webhooks/github",
		Body:   []byte(`{"action":"opened"}`),
	})
	_ = store.Save(evt)

	webServer := web.NewServer(web.ServerOptions{
		TunnelID:    "test-tun",
		PublicURL:   "https://test-tun.tunnel.com",
		LocalTarget: localApp.URL,
		Store:       store,
		Bus:         bus,
		ReplaySvc:   replaySvc,
	})

	ts := httptest.NewServer(webServer)
	defer ts.Close()

	// 1. Static asset: index.html
	resp, err := http.Get(ts.URL + "/")
	if err != nil {
		t.Fatalf("failed to fetch index.html: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "AudienceShift") {
		t.Fatalf("expected index.html to contain 'AudienceShift', got: %s", body)
	}

	// 2. Status API
	resp, err = http.Get(ts.URL + "/api/status")
	if err != nil {
		t.Fatalf("failed to fetch /api/status: %v", err)
	}
	defer resp.Body.Close()
	var statusMap map[string]string
	_ = json.NewDecoder(resp.Body).Decode(&statusMap)
	if statusMap["tunnel_id"] != "test-tun" || statusMap["public_url"] != "https://test-tun.tunnel.com" {
		t.Fatalf("unexpected status response: %v", statusMap)
	}

	// 3. Events API
	resp, err = http.Get(ts.URL + "/api/events")
	if err != nil {
		t.Fatalf("failed to fetch /api/events: %v", err)
	}
	defer resp.Body.Close()
	var events []*inspector.WebhookEvent
	_ = json.NewDecoder(resp.Body).Decode(&events)
	if len(events) != 1 || events[0].ID != evt.ID {
		t.Fatalf("unexpected events list: %v", events)
	}

	// 4. Replay API
	replayPayload := map[string]string{
		"id": evt.ID,
	}
	jsonBytes, _ := json.Marshal(replayPayload)
	resp, err = http.Post(ts.URL+"/api/replay", "application/json", bytes.NewReader(jsonBytes))
	if err != nil {
		t.Fatalf("failed to trigger /api/replay: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK from /api/replay, got %d", resp.StatusCode)
	}

	// 5. Clear API
	resp, err = http.Post(ts.URL+"/api/events/clear", "application/json", nil)
	if err != nil {
		t.Fatalf("failed to clear events: %v", err)
	}
	defer resp.Body.Close()
	if store.Count() != 0 {
		t.Fatalf("expected store to be cleared, count is %d", store.Count())
	}
}

func TestWebServerPortFallback(t *testing.T) {
	// 1. Artificially occupy a port
	busyLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on test port: %v", err)
	}
	defer busyLn.Close()

	occupiedPort := busyLn.Addr().(*net.TCPAddr).Port

	webServer := web.NewServer(web.ServerOptions{
		TunnelID:    "port-test",
		PublicURL:   "https://port-test.tunnel.com",
		LocalTarget: "http://localhost:8080",
	})

	// 2. Instruct webServer to start on the occupied port
	ln, webURL, err := webServer.Start(fmt.Sprintf("127.0.0.1:%d", occupiedPort))
	if err != nil {
		t.Fatalf("Start should have fallen back to next available port, got err: %v", err)
	}
	defer ln.Close()

	boundPort := ln.Addr().(*net.TCPAddr).Port
	if boundPort == occupiedPort {
		t.Fatalf("expected boundPort to be different from occupiedPort %d, got %d", occupiedPort, boundPort)
	}

	expectedPrefix := fmt.Sprintf("http://127.0.0.1:%d", boundPort)
	if !strings.HasPrefix(webURL, expectedPrefix) {
		t.Fatalf("expected webURL to be %s, got %s", expectedPrefix, webURL)
	}

	// 3. Verify server is alive and responding
	resp, err := http.Get(webURL + "/api/status")
	if err != nil {
		t.Fatalf("failed to query fallen-back web server: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK from /api/status, got %d", resp.StatusCode)
	}
}

func TestWebServerExportAndComposer(t *testing.T) {
	localApp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"received":true}`))
	}))
	defer localApp.Close()

	store := inspector.NewMemoryStore(10)
	bus := inspector.NewEventBus()
	defer bus.Close()
	replaySvc := replay.NewService(store, bus, localApp.URL)

	evt := inspector.NewWebhookEvent("test-export", inspector.HTTPRequest{
		Method: "POST",
		Path:   "/webhooks/test",
		Body:   []byte(`{"test":true}`),
	})
	_ = store.Save(evt)

	webServer := web.NewServer(web.ServerOptions{
		TunnelID:    "test-export",
		PublicURL:   "https://test-export.tunnel.com",
		LocalTarget: localApp.URL,
		Store:       store,
		Bus:         bus,
		ReplaySvc:   replaySvc,
	})

	ts := httptest.NewServer(webServer)
	defer ts.Close()

	// 1. Test JSON Export
	resp, err := http.Get(ts.URL + "/api/events/export?format=json")
	if err != nil {
		t.Fatalf("failed to export json: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for JSON export, got %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "/webhooks/test") {
		t.Fatalf("expected exported json to contain webhook path")
	}

	// 2. Test Postman Export
	resp, err = http.Get(ts.URL + "/api/events/export?format=postman")
	if err != nil {
		t.Fatalf("failed to export postman: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for Postman export, got %d", resp.StatusCode)
	}
	body, _ = io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "https://schema.getpostman.com/json/collection/v2.1.0/collection.json") {
		t.Fatalf("expected valid Postman collection schema")
	}

	// 3. Test Composer Dispatch
	composerPayload := map[string]interface{}{
		"method": "POST",
		"path":   "/api/custom-test",
		"body":   `{"composer":true}`,
	}
	jsonBytes, _ := json.Marshal(composerPayload)
	resp, err = http.Post(ts.URL+"/api/composer/dispatch", "application/json", bytes.NewReader(jsonBytes))
	if err != nil {
		t.Fatalf("failed to dispatch composer: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK from composer dispatch, got %d", resp.StatusCode)
	}
	var resEvt inspector.WebhookEvent
	_ = json.NewDecoder(resp.Body).Decode(&resEvt)
	if resEvt.Response.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK in dispatched event response, got %d", resEvt.Response.StatusCode)
	}
}
