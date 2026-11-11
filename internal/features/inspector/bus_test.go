package inspector_test

import (
	"sync"
	"testing"
	"time"

	"github.com/pandusatrianura/AudienceShift/internal/features/inspector"
)

func TestEventBusPubSub(t *testing.T) {
	bus := inspector.NewEventBus()
	defer bus.Close()

	ch1, unsub1 := bus.Subscribe(10)
	defer unsub1()

	ch2, unsub2 := bus.Subscribe(10)

	evt := inspector.NewWebhookEvent("tun-1", inspector.HTTPRequest{
		Path: "/test-bus",
	})

	bus.Publish(evt)

	select {
	case received := <-ch1:
		if received.ID != evt.ID {
			t.Errorf("ch1 received wrong event ID: %s", received.ID)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("ch1 timed out waiting for event")
	}

	select {
	case received := <-ch2:
		if received.ID != evt.ID {
			t.Errorf("ch2 received wrong event ID: %s", received.ID)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("ch2 timed out waiting for event")
	}

	// Unsubscribe ch2
	unsub2()

	// Verify ch2 channel was closed
	_, ok := <-ch2
	if ok {
		t.Errorf("expected unsubscribed ch2 to be closed")
	}

	// Publish second event, ch1 should still receive it
	evt2 := inspector.NewWebhookEvent("tun-1", inspector.HTTPRequest{Path: "/test-2"})
	bus.Publish(evt2)

	select {
	case received := <-ch1:
		if received.ID != evt2.ID {
			t.Errorf("ch1 received wrong event ID: %s", received.ID)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("ch1 timed out waiting for event 2")
	}
}

func TestEventBusSlowConsumerProtection(t *testing.T) {
	bus := inspector.NewEventBus()
	defer bus.Close()

	// Tiny buffer of 1
	ch, unsub := bus.Subscribe(1)
	defer unsub()

	// Publish 3 events rapidly
	bus.Publish(inspector.NewWebhookEvent("tun", inspector.HTTPRequest{Path: "/1"}))
	bus.Publish(inspector.NewWebhookEvent("tun", inspector.HTTPRequest{Path: "/2"}))
	bus.Publish(inspector.NewWebhookEvent("tun", inspector.HTTPRequest{Path: "/3"}))

	// Should not block or hang
	select {
	case <-ch:
		// Consumed first event
	default:
		t.Fatal("expected at least one event in buffer")
	}
}

func TestEventBusConcurrency(t *testing.T) {
	bus := inspector.NewEventBus()
	defer bus.Close()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ch, unsub := bus.Subscribe(50)
			defer unsub()

			timeout := time.After(200 * time.Millisecond)
			for {
				select {
				case <-ch:
				case <-timeout:
					return
				}
			}
		}()
	}

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			bus.Publish(inspector.NewWebhookEvent("tun", inspector.HTTPRequest{Path: "/conc"}))
		}(i)
	}

	wg.Wait()
}
