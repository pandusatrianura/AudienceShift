package replay_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pandusatrianura/AudienceShift/internal/features/inspector"
	"github.com/pandusatrianura/AudienceShift/internal/features/replay"
)

func TestReplayServiceDirect(t *testing.T) {
	// Mock local app
	receivedCount := 0
	var lastHeaders http.Header
	var lastBody string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedCount++
		lastHeaders = r.Header.Clone()
		b, _ := io.ReadAll(r.Body)
		lastBody = string(b)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"received":true}`))
	}))
	defer server.Close()

	store := inspector.NewMemoryStore(10)
	bus := inspector.NewEventBus()
	defer bus.Close()

	svc := replay.NewService(store, bus, server.URL)

	// Original event
	original := inspector.NewWebhookEvent("tun-1", inspector.HTTPRequest{
		Method: "POST",
		Path:   "/webhooks/stripe",
		Headers: map[string][]string{
			"Content-Type":     {"application/json"},
			"Stripe-Signature": {"old_sig"},
		},
		Body: []byte(`{"id":"evt_123","status":"requires_payment"}`),
	})
	_ = store.Save(original)

	// 1. Simple replay without mutation
	replayed, err := svc.Replay(context.Background(), original, replay.MutationOptions{})
	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}

	if replayed.ID == original.ID {
		t.Errorf("replayed event should have distinct ID")
	}
	if replayed.ReplayedFromID != original.ID {
		t.Errorf("ReplayedFromID mismatch: got %q, want %q", replayed.ReplayedFromID, original.ID)
	}
	if replayed.Response.StatusCode != 200 {
		t.Errorf("expected replayed status 200, got %d", replayed.Response.StatusCode)
	}
	if lastHeaders.Get("X-AudienceShift-Replay") != "true" {
		t.Errorf("missing replay indicator header")
	}

	// 2. Replay with Mutation & Signature Re-signing
	secret := "whsec_stripe_test"
	mutatedBody := []byte(`{"id":"evt_123","status":"payment_intent.succeeded"}`)
	mutated, err := svc.Replay(context.Background(), original, replay.MutationOptions{
		Body:          mutatedBody,
		SigningSecret: secret,
	})
	if err != nil {
		t.Fatalf("Mutated replay failed: %v", err)
	}
	if mutated.Response.StatusCode != 200 {
		t.Errorf("expected mutated response status 200, got %d", mutated.Response.StatusCode)
	}

	if lastBody != string(mutatedBody) {
		t.Fatalf("local server received unexpected mutated body: %s", lastBody)
	}

	// Verify new Stripe-Signature on the replayed request
	newSig := lastHeaders.Get("Stripe-Signature")
	if newSig == "old_sig" || newSig == "" {
		t.Fatalf("expected updated Stripe-Signature, got %q", newSig)
	}
	if !inspector.VerifyStripeSignature(mutatedBody, newSig, secret, 1*time.Minute) {
		t.Fatalf("re-signed signature could not be verified by recipient")
	}

	// 3. ReplayByID
	replayedByID, err := svc.ReplayByID(context.Background(), original.ID, replay.MutationOptions{
		Path: "/webhooks/custom-endpoint",
	})
	if err != nil {
		t.Fatalf("ReplayByID failed: %v", err)
	}
	if replayedByID.Request.Path != "/webhooks/custom-endpoint" {
		t.Errorf("expected mutated path, got %q", replayedByID.Request.Path)
	}

	// Verify store has original + 3 replayed events = 4 events
	if store.Count() != 4 {
		t.Errorf("expected 4 events in store, got %d", store.Count())
	}

	// 4. Test Service Default Secret auto-re-signing (Issue 5 fix)
	svcWithSecret := replay.NewService(store, bus, server.URL, "whsec_default_secret")
	replayedAuto, err := svcWithSecret.Replay(context.Background(), original, replay.MutationOptions{})
	if err != nil {
		t.Fatalf("Replay with default secret failed: %v", err)
	}
	replayedSig := lastHeaders.Get("Stripe-Signature")
	if !inspector.VerifyStripeSignature(original.Request.Body, replayedSig, "whsec_default_secret", 1*time.Minute) {
		t.Fatalf("expected 1-key replay with default secret to refresh Stripe signature successfully")
	}
	if replayedAuto.Response.StatusCode != 200 {
		t.Errorf("expected 200 OK")
	}
}

func TestChaosSimulation(t *testing.T) {
	var callCount int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&callCount, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	store := inspector.NewMemoryStore(10)
	bus := inspector.NewEventBus()
	defer bus.Close()
	svc := replay.NewService(store, bus, server.URL)

	original := inspector.NewWebhookEvent("tun-1", inspector.HTTPRequest{
		Method: "POST",
		Path:   "/hook",
		Body:   []byte(`{"event":"test"}`),
	})

	// Test 1: Chaos Delay
	start := time.Now()
	_, err := svc.Replay(context.Background(), original, replay.MutationOptions{
		Chaos: replay.ChaosOptions{
			Delay: 100 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatalf("Replay with delay failed: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed < 90*time.Millisecond {
		t.Errorf("expected at least 90ms delay, got %v", elapsed)
	}

	// Test 2: Chaos Duplicate Delivery
	atomic.StoreInt32(&callCount, 0)
	_, err = svc.Replay(context.Background(), original, replay.MutationOptions{
		Chaos: replay.ChaosOptions{
			Duplicate: true,
		},
	})
	if err != nil {
		t.Fatalf("Replay with duplicate failed: %v", err)
	}

	// Wait briefly for concurrent duplicate goroutine to hit server
	time.Sleep(100 * time.Millisecond)
	if count := atomic.LoadInt32(&callCount); count != 2 {
		t.Errorf("expected 2 concurrent deliveries for duplicate chaos test, got %d", count)
	}
}

func TestReplayConnectionFailure(t *testing.T) {
	store := inspector.NewMemoryStore(10)
	bus := inspector.NewEventBus()
	defer bus.Close()

	// Dead target URL
	svc := replay.NewService(store, bus, "http://127.0.0.1:59999")

	original := inspector.NewWebhookEvent("tun-1", inspector.HTTPRequest{
		Method: "POST",
		Path:   "/hook",
		Body:   []byte(`{}`),
	})

	replayed, err := svc.Replay(context.Background(), original, replay.MutationOptions{})
	if err != nil {
		t.Fatalf("Replay returned unexpected error: %v", err)
	}

	if replayed.Response.StatusCode != http.StatusBadGateway {
		t.Errorf("expected 502 Bad Gateway on unreachable target, got %d", replayed.Response.StatusCode)
	}
	if !strings.Contains(replayed.Response.Error, "failed to connect") {
		t.Errorf("expected error message in response, got %q", replayed.Response.Error)
	}
}
