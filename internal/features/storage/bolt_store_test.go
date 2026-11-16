package storage_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pandusatrianura/AudienceShift/internal/features/inspector"
	"github.com/pandusatrianura/AudienceShift/internal/features/storage"
)

func createTempDB(t *testing.T) (string, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "bolt_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	dbPath := filepath.Join(dir, "test.db")
	cleanup := func() {
		_ = os.RemoveAll(dir)
	}
	return dbPath, cleanup
}

func TestBoltStore_SaveAndGetByID(t *testing.T) {
	dbPath, cleanup := createTempDB(t)
	defer cleanup()

	store, err := storage.NewBoltStore(dbPath)
	if err != nil {
		t.Fatalf("failed to init BoltStore: %v", err)
	}
	defer store.Close()

	req := inspector.HTTPRequest{
		Method: "POST",
		Path:   "/webhooks/stripe",
		Body:   []byte(`{"event": "payment_intent.succeeded"}`),
	}
	evt := inspector.NewWebhookEvent("tun_1", req)
	evt.Response = inspector.HTTPResponse{StatusCode: 200, Body: []byte(`{"status":"ok"}`)}

	if err := store.Save(evt); err != nil {
		t.Fatalf("failed to save event: %v", err)
	}

	retrieved, err := store.GetByID(evt.ID)
	if err != nil {
		t.Fatalf("failed to get event by id: %v", err)
	}
	if retrieved.ID != evt.ID {
		t.Errorf("expected ID %s, got %s", evt.ID, retrieved.ID)
	}
	if string(retrieved.Request.Body) != string(req.Body) {
		t.Errorf("expected body %s, got %s", string(req.Body), string(retrieved.Request.Body))
	}
}

func TestBoltStore_ListChronological(t *testing.T) {
	dbPath, cleanup := createTempDB(t)
	defer cleanup()

	store, err := storage.NewBoltStore(dbPath)
	if err != nil {
		t.Fatalf("failed to init BoltStore: %v", err)
	}
	defer store.Close()

	now := time.Now()
	for i := 0; i < 5; i++ {
		evt := inspector.NewWebhookEvent("tun_1", inspector.HTTPRequest{Method: "POST", Path: "/test"})
		evt.Timestamp = now.Add(time.Duration(i) * time.Second)
		if err := store.Save(evt); err != nil {
			t.Fatalf("failed to save: %v", err)
		}
	}

	list, err := store.List(3)
	if err != nil {
		t.Fatalf("failed to list: %v", err)
	}
	if len(list) != 3 {
		t.Fatalf("expected 3 events, got %d", len(list))
	}
	// Verify descending order
	if list[0].Timestamp.Before(list[1].Timestamp) {
		t.Errorf("expected descending order: %v should be after %v", list[0].Timestamp, list[1].Timestamp)
	}
}

func TestBoltStore_ListFailed_DLQ(t *testing.T) {
	dbPath, cleanup := createTempDB(t)
	defer cleanup()

	store, err := storage.NewBoltStore(dbPath)
	if err != nil {
		t.Fatalf("failed to init BoltStore: %v", err)
	}
	defer store.Close()

	// 1 Successful event
	evtOk := inspector.NewWebhookEvent("tun_1", inspector.HTTPRequest{Method: "POST", Path: "/ok"})
	evtOk.Response = inspector.HTTPResponse{StatusCode: 200}
	_ = store.Save(evtOk)

	// 1 Failed with 500
	evtFail1 := inspector.NewWebhookEvent("tun_1", inspector.HTTPRequest{Method: "POST", Path: "/fail1"})
	evtFail1.Response = inspector.HTTPResponse{StatusCode: 500, Error: "internal server error"}
	_ = store.Save(evtFail1)

	// 1 Failed with 400
	evtFail2 := inspector.NewWebhookEvent("tun_1", inspector.HTTPRequest{Method: "POST", Path: "/fail2"})
	evtFail2.Response = inspector.HTTPResponse{StatusCode: 400}
	_ = store.Save(evtFail2)

	failed, err := store.ListFailed(10)
	if err != nil {
		t.Fatalf("failed to list failed: %v", err)
	}
	if len(failed) != 2 {
		t.Fatalf("expected 2 failed events in DLQ, got %d", len(failed))
	}

	// Update fail1 to success (e.g. replayed successfully)
	evtFail1.Response = inspector.HTTPResponse{StatusCode: 200, Error: ""}
	if err := store.Save(evtFail1); err != nil {
		t.Fatalf("failed to update event: %v", err)
	}

	failedAfter, err := store.ListFailed(10)
	if err != nil {
		t.Fatalf("failed to list failed after update: %v", err)
	}
	if len(failedAfter) != 1 {
		t.Fatalf("expected 1 failed event after update, got %d", len(failedAfter))
	}
}

func TestBoltStore_CapacityEviction(t *testing.T) {
	dbPath, cleanup := createTempDB(t)
	defer cleanup()

	// Cap at 3 events
	store, err := storage.NewBoltStore(dbPath, 3)
	if err != nil {
		t.Fatalf("failed to init BoltStore: %v", err)
	}
	defer store.Close()

	var firstID string
	now := time.Now()
	for i := 0; i < 5; i++ {
		evt := inspector.NewWebhookEvent("tun_1", inspector.HTTPRequest{Method: "POST", Path: "/test"})
		evt.Timestamp = now.Add(time.Duration(i) * time.Second)
		if i == 0 {
			firstID = evt.ID
		}
		_ = store.Save(evt)
	}

	if store.Count() > 3 {
		t.Errorf("expected count <= 3, got %d", store.Count())
	}

	// First event should have been pruned
	_, err = store.GetByID(firstID)
	if err != inspector.ErrEventNotFound {
		t.Errorf("expected ErrEventNotFound for evicted event, got %v", err)
	}
}

func TestBoltStore_PersistenceAcrossReopen(t *testing.T) {
	dbPath, cleanup := createTempDB(t)
	defer cleanup()

	store1, err := storage.NewBoltStore(dbPath)
	if err != nil {
		t.Fatalf("failed to init BoltStore: %v", err)
	}

	evt := inspector.NewWebhookEvent("tun_1", inspector.HTTPRequest{Method: "POST", Path: "/persistent"})
	evt.Response = inspector.HTTPResponse{StatusCode: 200}
	_ = store1.Save(evt)
	_ = store1.Close()

	// Reopen same database file
	store2, err := storage.NewBoltStore(dbPath)
	if err != nil {
		t.Fatalf("failed to reopen BoltStore: %v", err)
	}
	defer store2.Close()

	retrieved, err := store2.GetByID(evt.ID)
	if err != nil {
		t.Fatalf("failed to get event from reopened store: %v", err)
	}
	if retrieved.ID != evt.ID {
		t.Errorf("expected ID %s, got %s", evt.ID, retrieved.ID)
	}
	if store2.Count() != 1 {
		t.Errorf("expected count 1, got %d", store2.Count())
	}
}

func TestBoltStore_Clear(t *testing.T) {
	dbPath, cleanup := createTempDB(t)
	defer cleanup()

	store, err := storage.NewBoltStore(dbPath)
	if err != nil {
		t.Fatalf("failed to init BoltStore: %v", err)
	}
	defer store.Close()

	for i := 0; i < 5; i++ {
		evt := inspector.NewWebhookEvent("tun_1", inspector.HTTPRequest{Method: "POST", Path: "/clear-test"})
		_ = store.Save(evt)
	}
	if store.Count() != 5 {
		t.Fatalf("expected count 5, got %d", store.Count())
	}

	if err := store.Clear(); err != nil {
		t.Fatalf("failed to clear: %v", err)
	}

	if store.Count() != 0 {
		t.Errorf("expected count 0 after clear, got %d", store.Count())
	}
	list, err := store.List(10)
	if err != nil {
		t.Fatalf("failed to list after clear: %v", err)
	}
	if len(list) != 0 {
		t.Errorf("expected empty list after clear, got %d", len(list))
	}
}
