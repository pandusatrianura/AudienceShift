package inspector

import (
	"sync"
)

// EventBus coordinates pub/sub distribution of live webhook events.
type EventBus struct {
	mu          sync.RWMutex
	subscribers map[int]chan *WebhookEvent
	nextID      int
	closed      bool
}

// NewEventBus creates a ready-to-use EventBus instance.
func NewEventBus() *EventBus {
	return &EventBus{
		subscribers: make(map[int]chan *WebhookEvent),
	}
}

// Subscribe registers a new listener and returns an event receive channel and an unsubscribe func.
func (b *EventBus) Subscribe(bufferSize int) (<-chan *WebhookEvent, func()) {
	if bufferSize <= 0 {
		bufferSize = 64
	}

	ch := make(chan *WebhookEvent, bufferSize)

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		close(ch)
		return ch, func() {}
	}

	id := b.nextID
	b.nextID++
	b.subscribers[id] = ch

	unsubscribe := func() {
		b.mu.Lock()
		defer b.mu.Unlock()

		if subscriberCh, ok := b.subscribers[id]; ok {
			delete(b.subscribers, id)
			close(subscriberCh)
		}
	}

	return ch, unsubscribe
}

// Publish broadcasts an event to all active subscribers without blocking publishers.
func (b *EventBus) Publish(event *WebhookEvent) {
	if event == nil {
		return
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.closed {
		return
	}

	for _, ch := range b.subscribers {
		select {
		case ch <- event.Clone():
		default:
			// Buffer full, drop for slow consumer to protect pipeline throughput
		}
	}
}

// Close unsubscribes all listeners and shuts down the bus.
func (b *EventBus) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return
	}
	b.closed = true

	for id, ch := range b.subscribers {
		delete(b.subscribers, id)
		close(ch)
	}
}
