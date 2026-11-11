package inspector_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/pandusatrianura/AudienceShift/internal/features/inspector"
)

func TestMemoryStoreBasics(t *testing.T) {
	store := inspector.NewMemoryStore(3)

	if count := store.Count(); count != 0 {
		t.Fatalf("expected 0 events, got %d", count)
	}

	// Save nil event
	if err := store.Save(nil); err == nil {
		t.Fatal("expected error saving nil event")
	}

	// Save events
	evt1 := inspector.NewWebhookEvent("tun", inspector.HTTPRequest{Path: "/1"})
	evt2 := inspector.NewWebhookEvent("tun", inspector.HTTPRequest{Path: "/2"})
	evt3 := inspector.NewWebhookEvent("tun", inspector.HTTPRequest{Path: "/3"})

	_ = store.Save(evt1)
	_ = store.Save(evt2)
	_ = store.Save(evt3)

	if count := store.Count(); count != 3 {
		t.Fatalf("expected 3 events, got %d", count)
	}

	// List events (newest first)
	list, err := store.List(10)
	if err != nil {
		t.Fatalf("unexpected error listing events: %v", err)
	}
	if len(list) != 3 {
		t.Fatalf("expected 3 events, got %d", len(list))
	}
	if list[0].ID != evt3.ID || list[1].ID != evt2.ID || list[2].ID != evt1.ID {
		t.Errorf("list order incorrect, expected newest first")
	}

	// GetByID
	got, err := store.GetByID(evt2.ID)
	if err != nil || got.ID != evt2.ID {
		t.Fatalf("GetByID failed: %v", err)
	}

	// Get non-existent
	_, err = store.GetByID("invalid-id")
	if !errors.Is(err, inspector.ErrEventNotFound) {
		t.Fatalf("expected ErrEventNotFound, got %v", err)
	}

	// Eviction when exceeding capacity
	evt4 := inspector.NewWebhookEvent("tun", inspector.HTTPRequest{Path: "/4"})
	_ = store.Save(evt4)

	if count := store.Count(); count != 3 {
		t.Fatalf("expected capacity to remain 3, got %d", count)
	}

	// Evicted evt1 should be gone
	_, err = store.GetByID(evt1.ID)
	if !errors.Is(err, inspector.ErrEventNotFound) {
		t.Fatalf("expected oldest event evt1 to be evicted")
	}

	// Clear
	_ = store.Clear()
	if count := store.Count(); count != 0 {
		t.Fatalf("expected 0 events after clear, got %d", count)
	}
}

func TestMemoryStoreConcurrency(t *testing.T) {
	store := inspector.NewMemoryStore(100)
	var wg sync.WaitGroup

	// Concurrently save and list
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			evt := inspector.NewWebhookEvent("tun", inspector.HTTPRequest{
				Path: fmt.Sprintf("/path-%d", idx),
			})
			_ = store.Save(evt)
		}(i)
	}

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = store.List(10)
			_ = store.Count()
		}()
	}

	wg.Wait()

	if count := store.Count(); count != 50 {
		t.Fatalf("expected 50 events, got %d", count)
	}
}
