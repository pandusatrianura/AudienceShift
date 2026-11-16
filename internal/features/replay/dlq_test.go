package replay_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pandusatrianura/AudienceShift/internal/features/inspector"
	"github.com/pandusatrianura/AudienceShift/internal/features/replay"
)

func TestService_ResendDLQ(t *testing.T) {
	var requestCount int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := atomic.AddInt32(&requestCount, 1)
		if count%2 == 1 {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"recovered"}`))
		} else {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":"still failing"}`))
		}
	}))
	defer server.Close()

	store := inspector.NewMemoryStore(100)
	bus := inspector.NewEventBus()
	svc := replay.NewService(store, bus, server.URL)

	// Create 4 failed events
	for i := 0; i < 4; i++ {
		evt := inspector.NewWebhookEvent("tun_1", inspector.HTTPRequest{
			Method: "POST",
			Path:   "/webhook",
			Body:   []byte(`{"test":"dlq"}`),
		})
		evt.Response = inspector.HTTPResponse{
			StatusCode: 500,
			Error:      "internal server error",
		}
		_ = store.Save(evt)
	}

	failedList, err := store.ListFailed(10)
	if err != nil || len(failedList) != 4 {
		t.Fatalf("expected 4 failed events, got %d", len(failedList))
	}

	// Resend DLQ with 10ms pacing
	ctx := context.Background()
	res, err := svc.ResendDLQ(ctx, replay.DLQResendOptions{
		Limit:    10,
		Interval: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("ResendDLQ failed: %v", err)
	}

	if res.TotalProcessed != 4 {
		t.Errorf("expected 4 processed, got %d", res.TotalProcessed)
	}
	if res.SuccessCount != 2 {
		t.Errorf("expected 2 succeeded, got %d", res.SuccessCount)
	}
	if res.FailedCount != 2 {
		t.Errorf("expected 2 failed, got %d", res.FailedCount)
	}
	if len(res.ResentEventIDs) != 4 {
		t.Errorf("expected 4 resent IDs, got %d", len(res.ResentEventIDs))
	}
}

func TestService_ResendDLQ_Cancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	store := inspector.NewMemoryStore(100)
	bus := inspector.NewEventBus()
	svc := replay.NewService(store, bus, server.URL)

	for i := 0; i < 5; i++ {
		evt := inspector.NewWebhookEvent("tun_1", inspector.HTTPRequest{Method: "POST", Path: "/webhook"})
		evt.Response = inspector.HTTPResponse{StatusCode: 500}
		_ = store.Save(evt)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	res, err := svc.ResendDLQ(ctx, replay.DLQResendOptions{
		Limit:    10,
		Interval: 50 * time.Millisecond,
	})
	if err == nil {
		t.Errorf("expected context cancellation error, got nil")
	}
	if res != nil && res.TotalProcessed > 0 && res.SuccessCount > 0 {
		t.Errorf("expected no events to be successfully processed upon immediate cancellation")
	}
}
