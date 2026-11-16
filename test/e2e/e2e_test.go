package e2e_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pandusatrianura/AudienceShift/internal/features/inspector"
	"github.com/pandusatrianura/AudienceShift/internal/features/replay"
	"github.com/pandusatrianura/AudienceShift/internal/features/storage"
	"github.com/pandusatrianura/AudienceShift/internal/features/synthesizer"
	"github.com/pandusatrianura/AudienceShift/internal/features/tunnel"
	"github.com/pandusatrianura/AudienceShift/internal/features/web"
)

func TestFullE2EWebhookLifecycle(t *testing.T) {
	// 1. Developer's Local Application Backend
	type receivedReq struct {
		Body      string
		Signature string
		Replayed  string
	}
	received := make(chan receivedReq, 5)

	localBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- receivedReq{
			Body:      string(body),
			Signature: r.Header.Get("Stripe-Signature"),
			Replayed:  r.Header.Get("X-AudienceShift-Replay"),
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"received":true,"backend_handled":true}`))
	}))
	defer localBackend.Close()

	// 2. Tunnel Server on VPS
	tunnelServer := tunnel.NewServer("my-tunnel.dev", "")
	vpsServer := httptest.NewServer(tunnelServer)
	defer vpsServer.Close()

	// 3. Client Studio setup
	store := inspector.NewMemoryStore(100)
	bus := inspector.NewEventBus()
	defer bus.Close()
	replaySvc := replay.NewService(store, bus, localBackend.URL)

	// Embedded Web Studio
	webStudio := web.NewServer(web.ServerOptions{
		TunnelID:    "alice",
		PublicURL:   "https://alice.my-tunnel.dev",
		LocalTarget: localBackend.URL,
		Store:       store,
		Bus:         bus,
		ReplaySvc:   replaySvc,
	})
	webServer := httptest.NewServer(webStudio)
	defer webServer.Close()

	// Connect Tunnel Client
	wsURL := "ws" + strings.TrimPrefix(vpsServer.URL, "http") + tunnel.ConnectPath
	client := tunnel.NewClient(tunnel.ClientOptions{
		ServerURL:   wsURL,
		TunnelID:    "alice",
		LocalTarget: localBackend.URL,
		Store:       store,
		Bus:         bus,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = client.Start(ctx)
	}()
	defer client.Stop()

	// Wait for tunnel connection
	time.Sleep(200 * time.Millisecond)

	// 4. External Webhook Delivery (e.g. from Stripe API)
	stripeSecret := "whsec_live_test_secret_999"
	initialPayload := []byte(`{"id":"evt_stripe_101","type":"charge.succeeded","amount":10000}`)
	initialSig := inspector.GenerateStripeSignature(initialPayload, stripeSecret, time.Now())

	req, err := http.NewRequest("POST", vpsServer.URL+"/api/v1/stripe/webhook", strings.NewReader(string(initialPayload)))
	if err != nil {
		t.Fatalf("failed to build webhook request: %v", err)
	}
	req.Header.Set(tunnel.HeaderTunnelID, "alice")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Stripe-Signature", initialSig)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("webhook request delivery failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK from local backend, got %d", resp.StatusCode)
	}

	// Assert local backend received original event
	select {
	case rec := <-received:
		if !strings.Contains(rec.Body, "evt_stripe_101") {
			t.Fatalf("local backend received unexpected body: %s", rec.Body)
		}
		if rec.Signature != initialSig {
			t.Fatalf("signature header mismatch")
		}
		if rec.Replayed == "true" {
			t.Fatalf("original request should not be marked replayed")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for local backend to receive webhook")
	}

	// 5. Verify Event stored in Studio
	if store.Count() != 1 {
		t.Fatalf("expected 1 event in store, got %d", store.Count())
	}
	events, _ := store.List(1)
	captured := events[0]
	if captured.Provider != "stripe" {
		t.Errorf("expected provider 'stripe', got %q", captured.Provider)
	}

	// 6. Mutate and Replay (Simulating test of refunded edge case)
	mutatedPayload := []byte(`{"id":"evt_stripe_101","type":"charge.refunded","amount":10000,"refunded":true}`)
	replayedEvt, err := replaySvc.ReplayByID(ctx, captured.ID, replay.MutationOptions{
		Body:          mutatedPayload,
		SigningSecret: stripeSecret, // Auto recalculate signature!
	})
	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}

	if replayedEvt.Response.StatusCode != 200 {
		t.Errorf("expected replayed response status 200, got %d", replayedEvt.Response.StatusCode)
	}

	// Assert local backend received mutated event with valid recalculated signature
	select {
	case rec := <-received:
		if !strings.Contains(rec.Body, "charge.refunded") {
			t.Fatalf("local backend did not receive mutated payload: %s", rec.Body)
		}
		if rec.Signature == initialSig {
			t.Fatalf("signature should have been recalculated for mutated payload")
		}
		if !inspector.VerifyStripeSignature([]byte(rec.Body), rec.Signature, stripeSecret, 1*time.Minute) {
			t.Fatalf("recalculated signature failed cryptographic validation!")
		}
		if rec.Replayed != "true" {
			t.Fatalf("replayed request should have X-AudienceShift-Replay header")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for replayed event on local backend")
	}

	// 7. Verify Web Studio API has both events
	webResp, err := http.Get(webServer.URL + "/api/events")
	if err != nil {
		t.Fatalf("failed to query web studio: %v", err)
	}
	defer webResp.Body.Close()

	var studioEvents []*inspector.WebhookEvent
	_ = jsonDecode(webResp.Body, &studioEvents)
	if len(studioEvents) != 2 {
		t.Fatalf("expected 2 events in web studio (original + replayed), got %d", len(studioEvents))
	}
}

func jsonDecode(r io.Reader, v interface{}) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

func TestMockReceiverLifecycle(t *testing.T) {
	// 1. Mock Local Developer Backend
	type receivedReq struct {
		Path     string
		Body     string
		MockHdr  string
		TokenHdr string
		Replayed string
	}
	received := make(chan receivedReq, 5)

	localBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- receivedReq{
			Path:     r.URL.Path,
			Body:     string(body),
			MockHdr:  r.Header.Get("X-AudienceShift-Mock"),
			TokenHdr: r.Header.Get("X-Callback-Token"),
			Replayed: r.Header.Get("X-AudienceShift-Replay"),
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"success","processed":true}`))
	}))
	defer localBackend.Close()

	// 2. Setup MemoryStore, EventBus, ReplayService
	store := inspector.NewMemoryStore(100)
	bus := inspector.NewEventBus()
	defer bus.Close()
	replaySvc := replay.NewService(store, bus, localBackend.URL)

	// 3. Mock Receiver HTTP Handler
	mockHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		bodyBytes, _ := io.ReadAll(io.LimitReader(r.Body, 10<<20))

		headers := make(map[string][]string, len(r.Header))
		for k, vv := range r.Header {
			headers[k] = vv
		}

		httpReq := inspector.HTTPRequest{
			Method:   r.Method,
			Path:     r.URL.Path,
			Query:    r.URL.RawQuery,
			Headers:  headers,
			Body:     bodyBytes,
			ClientIP: "127.0.0.1",
		}

		evt := inspector.NewWebhookEvent("mock-offline", httpReq)

		// Forward to localBackend
		destURL := strings.TrimRight(localBackend.URL, "/") + httpReq.Path
		targetReq, err := http.NewRequest(httpReq.Method, destURL, strings.NewReader(string(httpReq.Body)))
		if err != nil {
			http.Error(w, err.Error(), 502)
			return
		}
		for k, vv := range headers {
			lk := strings.ToLower(k)
			if lk == "content-length" || lk == "host" || lk == "connection" {
				continue
			}
			for _, v := range vv {
				targetReq.Header.Add(k, v)
			}
		}
		targetReq.Header.Set("X-AudienceShift-Mock", "true")

		resp, err := http.DefaultClient.Do(targetReq)
		if err != nil {
			http.Error(w, err.Error(), 502)
			return
		}
		defer resp.Body.Close()

		respBody, _ := io.ReadAll(resp.Body)
		evt.Response = inspector.HTTPResponse{
			StatusCode: resp.StatusCode,
			Headers:    resp.Header.Clone(),
			Body:       respBody,
		}
		evt.DurationMs = time.Since(start).Milliseconds()

		_ = store.Save(evt)
		bus.Publish(evt)

		w.WriteHeader(resp.StatusCode)
		_, _ = w.Write(respBody)
	})

	mockServer := httptest.NewServer(mockHandler)
	defer mockServer.Close()

	// 4. Send Webhook directly to Mock Server (e.g. Xendit invoice paid)
	payload := `{"id":"xnd_123","status":"PAID","amount":150000}`
	tokenVal := "xnd_live_secret_callback_token_9999"
	req, err := http.NewRequest("POST", mockServer.URL+"/api/v1/payments/xendit", strings.NewReader(payload))
	if err != nil {
		t.Fatalf("failed to build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Callback-Token", tokenVal)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("failed to send webhook to mock server: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", resp.StatusCode)
	}

	// 5. Verify local backend received forwarded request
	select {
	case rec := <-received:
		if rec.Path != "/api/v1/payments/xendit" {
			t.Errorf("expected path /api/v1/payments/xendit, got %s", rec.Path)
		}
		if !strings.Contains(rec.Body, "xnd_123") {
			t.Errorf("expected body to contain xnd_123, got %s", rec.Body)
		}
		if rec.MockHdr != "true" {
			t.Errorf("expected X-AudienceShift-Mock header to be true")
		}
		if rec.TokenHdr != tokenVal {
			t.Errorf("expected X-Callback-Token %q, got %q", tokenVal, rec.TokenHdr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for backend to receive forwarded webhook")
	}

	// 6. Verify Store & Inspector features
	if store.Count() != 1 {
		t.Fatalf("expected 1 event in store, got %d", store.Count())
	}
	events, _ := store.List(1)
	captured := events[0]

	if captured.Provider != "xendit" {
		t.Errorf("expected provider 'xendit', got %q", captured.Provider)
	}

	// Sensitive masking check
	masked := captured.Request.MaskedHeaders()
	if masked["X-Callback-Token"][0] == tokenVal || !strings.Contains(masked["X-Callback-Token"][0], "...") {
		t.Errorf("expected X-Callback-Token to be masked, got %q", masked["X-Callback-Token"][0])
	}

	// cURL generation check
	curlCmd := captured.ToCURL(localBackend.URL)
	if !strings.Contains(curlCmd, `curl -X POST "`+localBackend.URL+`/api/v1/payments/xendit"`) {
		t.Errorf("invalid cURL export: %s", curlCmd)
	}

	// 7. Verify Replay
	replayed, err := replaySvc.ReplayByID(context.Background(), captured.ID, replay.MutationOptions{})
	if err != nil {
		t.Fatalf("failed to replay event: %v", err)
	}
	if replayed.Response.StatusCode != 200 {
		t.Errorf("expected replay status 200, got %d", replayed.Response.StatusCode)
	}

	select {
	case rec := <-received:
		if rec.Replayed != "true" {
			t.Errorf("expected replayed request to have X-AudienceShift-Replay")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for replayed event on backend")
	}
}

func TestE2EComplianceAndExport(t *testing.T) {
	// 1. Backend with signature check
	secretKey := "whsec_test_secret_for_compliance_suite_12345"
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sig := r.Header.Get("Stripe-Signature")
		if sig == "" {
			http.Error(w, "missing signature", http.StatusUnauthorized)
			return
		}
		if !strings.Contains(sig, "v1=") {
			http.Error(w, "invalid signature format", http.StatusBadRequest)
			return
		}
		// If forged signature
		if strings.Contains(sig, "forged") {
			http.Error(w, "invalid signature", http.StatusUnauthorized)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"verified"}`))
	}))
	defer backend.Close()

	// 2. Test Store & Export
	store := inspector.NewMemoryStore(50)
	evt := inspector.NewWebhookEvent("test-tunnel", inspector.HTTPRequest{
		Method: "POST",
		Path:   "/webhooks/stripe",
		Body:   []byte(`{"id":"evt_123"}`),
	})
	_ = store.Save(evt)

	events, _ := store.List(10)
	jsonExport, err := inspector.ExportJSON(events, false)
	if err != nil {
		t.Fatalf("ExportJSON failed: %v", err)
	}
	if !strings.Contains(string(jsonExport), evt.ID) {
		t.Errorf("expected json export to contain event id %s", evt.ID)
	}

	postmanExport, err := inspector.ExportPostman(events, "E2E Test")
	if err != nil {
		t.Fatalf("ExportPostman failed: %v", err)
	}
	if !strings.Contains(string(postmanExport), "https://schema.getpostman.com/json/collection/v2.1.0/collection.json") {
		t.Errorf("expected postman schema in export")
	}

	// 3. Test Signature verification helpers
	body := []byte(`{"id":"evt_123"}`)
	validSig := inspector.GenerateStripeSignature(body, secretKey, time.Now())
	if !strings.Contains(validSig, "t=") || !strings.Contains(validSig, "v1=") {
		t.Errorf("generated signature does not conform to Stripe format")
	}
}

func TestE2E_BoltStore_DLQ_Synthesizer_Fanout(t *testing.T) {
	// 1. Temporary persistent BoltStore
	dir, err := os.MkdirTemp("", "e2e_bolt_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)
	dbPath := filepath.Join(dir, "e2e.db")

	store, err := storage.NewBoltStore(dbPath)
	if err != nil {
		t.Fatalf("failed to initialize BoltStore: %v", err)
	}
	defer store.Close()

	// 2. Mock Local Backend
	receivedCh := make(chan string, 5)
	localBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		receivedCh <- string(b)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"success":true}`))
	}))
	defer localBackend.Close()

	// 3. VPS Relay Server
	relayServer := tunnel.NewServer("fanout.internal", "")
	vps := httptest.NewServer(relayServer)
	defer vps.Close()

	bus := inspector.NewEventBus()
	defer bus.Close()
	replaySvc := replay.NewService(store, bus, localBackend.URL)

	// 4. Web Studio
	webStudio := web.NewServer(web.ServerOptions{
		TunnelID:    "sovereign-team",
		PublicURL:   "https://sovereign-team.fanout.internal",
		LocalTarget: localBackend.URL,
		Store:       store,
		Bus:         bus,
		ReplaySvc:   replaySvc,
	})
	webSrv := httptest.NewServer(webStudio)
	defer webSrv.Close()

	// 5. Connect 2 Client Sessions (Fanout)
	wsURL := "ws" + strings.TrimPrefix(vps.URL, "http") + tunnel.ConnectPath

	client1 := tunnel.NewClient(tunnel.ClientOptions{
		ServerURL:   wsURL,
		TunnelID:    "sovereign-team",
		LocalTarget: localBackend.URL,
		Store:       store,
		Bus:         bus,
	})
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	go func() { _ = client1.Start(ctx1) }()
	defer client1.Stop()

	client2 := tunnel.NewClient(tunnel.ClientOptions{
		ServerURL:   wsURL,
		TunnelID:    "sovereign-team",
		LocalTarget: localBackend.URL,
		Store:       store,
		Bus:         bus,
	})
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	go func() { _ = client2.Start(ctx2) }()
	defer client2.Stop()

	time.Sleep(200 * time.Millisecond)

	if count := relayServer.ActiveCount(); count != 2 {
		t.Fatalf("expected 2 connected fanout client sessions, got %d", count)
	}

	// 6. Deliver incoming webhook to VPS relay
	payloadJSON := `{"customer_id":"cus_12345","amount":5000,"currency":"usd","created_at":"2026-11-16T15:00:00Z"}`
	pubReq, err := http.NewRequest("POST", vps.URL+"/orders/webhook", strings.NewReader(payloadJSON))
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	pubReq.Header.Set(tunnel.HeaderTunnelID, "sovereign-team")
	pubReq.Header.Set("Content-Type", "application/json")

	pubResp, err := http.DefaultClient.Do(pubReq)
	if err != nil {
		t.Fatalf("webhook request delivery failed: %v", err)
	}
	defer pubResp.Body.Close()

	if pubResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", pubResp.StatusCode)
	}

	// Wait for delivery to local backend
	select {
	case body := <-receivedCh:
		if !strings.Contains(body, "cus_12345") {
			t.Errorf("unexpected body delivered: %s", body)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for local backend webhook delivery")
	}

	// 7. Verify Type Synthesizer API
	events, err := store.List(5)
	if err != nil || len(events) == 0 {
		t.Fatalf("expected at least 1 stored event in BoltDB, got %d", len(events))
	}
	targetEvt := events[0]

	synthResp, err := http.Get(webSrv.URL + "/api/events/types?id=" + targetEvt.ID)
	if err != nil {
		t.Fatalf("failed to query synthesizer API: %v", err)
	}
	defer synthResp.Body.Close()
	if synthResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK from synthesizer API, got %d", synthResp.StatusCode)
	}

	var synthTypes synthesizer.SynthesizedTypes
	_ = json.NewDecoder(synthResp.Body).Decode(&synthTypes)
	if !strings.Contains(synthTypes.Go, "CustomerID") || !strings.Contains(synthTypes.TypeScript, "customer_id") {
		t.Errorf("synthesizer output missing CustomerID field: %+v", synthTypes)
	}

	// 8. Test DLQ & Resend
	failedEvt := inspector.NewWebhookEvent("sovereign-team", inspector.HTTPRequest{
		Method: "POST",
		Path:   "/orders/failed",
		Body:   []byte(`{"error":"temporary_gateway_down"}`),
	})
	failedEvt.Response = inspector.HTTPResponse{
		StatusCode: 502,
		Error:      "bad gateway",
	}
	_ = store.Save(failedEvt)

	dlqResp, err := http.Get(webSrv.URL + "/api/dlq")
	if err != nil {
		t.Fatalf("failed to query DLQ API: %v", err)
	}
	defer dlqResp.Body.Close()
	var dlqList struct {
		Count int `json:"count"`
	}
	_ = json.NewDecoder(dlqResp.Body).Decode(&dlqList)
	if dlqList.Count != 1 {
		t.Errorf("expected 1 event in DLQ, got %d", dlqList.Count)
	}

	// Trigger DLQ Resend
	resendResp, err := http.Post(webSrv.URL+"/api/dlq/resend", "application/json", strings.NewReader(`{"limit":5,"interval_ms":10}`))
	if err != nil {
		t.Fatalf("failed to trigger DLQ resend: %v", err)
	}
	defer resendResp.Body.Close()
	var resendRes replay.DLQResendResult
	_ = json.NewDecoder(resendResp.Body).Decode(&resendRes)
	if resendRes.TotalProcessed != 1 || resendRes.SuccessCount != 1 {
		t.Errorf("expected 1 processed & 1 succeeded in DLQ resend, got %+v", resendRes)
	}

	// 9. Verify Persistence Across Reopen
	_ = store.Close()
	reopenedStore, err := storage.NewBoltStore(dbPath)
	if err != nil {
		t.Fatalf("failed to reopen BoltStore: %v", err)
	}
	defer reopenedStore.Close()

	if reopenedStore.Count() < 2 {
		t.Errorf("expected at least 2 events persisted across database restart, got %d", reopenedStore.Count())
	}
}
