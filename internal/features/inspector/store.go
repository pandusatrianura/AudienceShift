package inspector

import (
	"errors"
	"sync"
)

var (
	// ErrEventNotFound is returned when an event ID does not exist in the store.
	ErrEventNotFound = errors.New("event not found")
)

// EventStore abstracts the persistence mechanism for intercepted webhook events.
type EventStore interface {
	Save(event *WebhookEvent) error
	GetByID(id string) (*WebhookEvent, error)
	List(limit int) ([]*WebhookEvent, error)
	Clear() error
	Count() int
}

// MemoryStore provides a thread-safe circular in-memory buffer with bounded capacity.
type MemoryStore struct {
	mu       sync.RWMutex
	capacity int
	events   []*WebhookEvent
	lookup   map[string]*WebhookEvent
}

// NewMemoryStore initializes a new MemoryStore with the specified event capacity.
func NewMemoryStore(capacity int) *MemoryStore {
	if capacity <= 0 {
		capacity = 1000
	}
	return &MemoryStore{
		capacity: capacity,
		events:   make([]*WebhookEvent, 0, capacity),
		lookup:   make(map[string]*WebhookEvent, capacity),
	}
}

// Save stores or updates an event. If the store reaches capacity, the oldest event is evicted.
func (s *MemoryStore) Save(event *WebhookEvent) error {
	if event == nil {
		return errors.New("cannot save nil event")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// If event already exists in lookup, update it in place
	if _, exists := s.lookup[event.ID]; exists {
		s.lookup[event.ID] = event
		for i, e := range s.events {
			if e.ID == event.ID {
				s.events[i] = event
				break
			}
		}
		return nil
	}

	// Evict oldest if capacity is reached
	if len(s.events) >= s.capacity {
		oldest := s.events[0]
		delete(s.lookup, oldest.ID)
		s.events = s.events[1:]
	}

	s.events = append(s.events, event)
	s.lookup[event.ID] = event
	return nil
}

// GetByID retrieves a single event by its ID. Returns ErrEventNotFound if not present.
func (s *MemoryStore) GetByID(id string) (*WebhookEvent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	event, found := s.lookup[id]
	if !found {
		return nil, ErrEventNotFound
	}
	return event.Clone(), nil
}

// List returns the latest events up to the specified limit in descending chronological order (newest first).
func (s *MemoryStore) List(limit int) ([]*WebhookEvent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	total := len(s.events)
	if limit <= 0 || limit > total {
		limit = total
	}

	result := make([]*WebhookEvent, limit)
	for i := 0; i < limit; i++ {
		// Newest events are at the end of s.events
		result[i] = s.events[total-1-i].Clone()
	}

	return result, nil
}

// Clear flushes all stored events from memory.
func (s *MemoryStore) Clear() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.events = make([]*WebhookEvent, 0, s.capacity)
	s.lookup = make(map[string]*WebhookEvent, s.capacity)
	return nil
}

// Count returns the current number of events stored in memory.
func (s *MemoryStore) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.events)
}
