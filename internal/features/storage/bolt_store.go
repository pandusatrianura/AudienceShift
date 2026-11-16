package storage

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/pandusatrianura/AudienceShift/internal/features/inspector"
	"go.etcd.io/bbolt"
)

var (
	bucketEvents   = []byte("events")
	bucketTimeline = []byte("timeline")
	bucketDLQ      = []byte("dlq")
	bucketMeta     = []byte("meta")
	keyEventCount  = []byte("event_count")
)

// BoltStore is a thread-safe, pure-Go embedded persistent implementation of inspector.EventStore.
type BoltStore struct {
	mu       sync.RWMutex
	db       *bbolt.DB
	filePath string
	capacity int
}

// Compile-time check that BoltStore implements inspector.EventStore.
var _ inspector.EventStore = (*BoltStore)(nil)

// NewBoltStore initializes or opens an embedded BoltDB file at the specified path.
func NewBoltStore(path string, capacity ...int) (*BoltStore, error) {
	capVal := 5000
	if len(capacity) > 0 && capacity[0] > 0 {
		capVal = capacity[0]
	}

	dir := filepath.Dir(path)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("creating database directory: %w", err)
		}
	}

	db, err := bbolt.Open(path, 0600, &bbolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("opening bolt database at %s: %w", path, err)
	}

	// Initialize buckets
	err = db.Update(func(tx *bbolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(bucketEvents); err != nil {
			return fmt.Errorf("creating events bucket: %w", err)
		}
		if _, err := tx.CreateBucketIfNotExists(bucketTimeline); err != nil {
			return fmt.Errorf("creating timeline bucket: %w", err)
		}
		if _, err := tx.CreateBucketIfNotExists(bucketDLQ); err != nil {
			return fmt.Errorf("creating dlq bucket: %w", err)
		}
		bMeta, err := tx.CreateBucketIfNotExists(bucketMeta)
		if err != nil {
			return fmt.Errorf("creating meta bucket: %w", err)
		}
		if bMeta.Get(keyEventCount) == nil {
			if err := setEventCount(bMeta, 0); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("initializing buckets: %w", err)
	}

	return &BoltStore{
		db:       db,
		filePath: path,
		capacity: capVal,
	}, nil
}

func getEventCount(bMeta *bbolt.Bucket) int {
	data := bMeta.Get(keyEventCount)
	if len(data) < 8 {
		return 0
	}
	return int(binary.BigEndian.Uint64(data))
}

func setEventCount(bMeta *bbolt.Bucket, n int) error {
	var buf [8]byte
	if n < 0 {
		n = 0
	}
	binary.BigEndian.PutUint64(buf[:], uint64(n))
	return bMeta.Put(keyEventCount, buf[:])
}

// makeTimelineKey produces a sortable chronological key: <20-digit-nanos>_<event_id>
func makeTimelineKey(t time.Time, id string) []byte {
	return []byte(fmt.Sprintf("%020d_%s", t.UnixNano(), id))
}

// Save persists or updates an event in BoltDB.
func (s *BoltStore) Save(event *inspector.WebhookEvent) error {
	if event == nil {
		return errors.New("cannot save nil event")
	}

	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshaling event: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.db.Update(func(tx *bbolt.Tx) error {
		bEvents := tx.Bucket(bucketEvents)
		bTimeline := tx.Bucket(bucketTimeline)
		bDLQ := tx.Bucket(bucketDLQ)
		bMeta := tx.Bucket(bucketMeta)

		eventIDBytes := []byte(event.ID)
		timeKey := makeTimelineKey(event.Timestamp, event.ID)

		// Check if event already exists
		prevBytes := bEvents.Get(eventIDBytes)
		isNew := prevBytes == nil
		if prevBytes != nil {
			// If previously saved with different timestamp or failed status, update appropriately
			var prev inspector.WebhookEvent
			if err := json.Unmarshal(prevBytes, &prev); err == nil {
				prevTimeKey := makeTimelineKey(prev.Timestamp, prev.ID)
				if !bytes.Equal(prevTimeKey, timeKey) {
					_ = bTimeline.Delete(prevTimeKey)
					_ = bDLQ.Delete(prevTimeKey)
				}
				if prev.IsFailed() && !event.IsFailed() {
					_ = bDLQ.Delete(prevTimeKey)
					_ = bDLQ.Delete(timeKey)
				}
			}
		}

		// Save payload
		if err := bEvents.Put(eventIDBytes, data); err != nil {
			return fmt.Errorf("putting event: %w", err)
		}

		// Update timeline
		if err := bTimeline.Put(timeKey, eventIDBytes); err != nil {
			return fmt.Errorf("putting timeline entry: %w", err)
		}

		// Handle DLQ
		if event.IsFailed() {
			if err := bDLQ.Put(timeKey, eventIDBytes); err != nil {
				return fmt.Errorf("putting dlq entry: %w", err)
			}
		} else {
			_ = bDLQ.Delete(timeKey)
		}

		currentCount := getEventCount(bMeta)
		if isNew {
			currentCount++
		}

		// Enforce capacity limit
		if currentCount > s.capacity {
			c := bTimeline.Cursor()
			for currentCount > s.capacity {
				k, v := c.First()
				if k == nil {
					break
				}
				_ = bEvents.Delete(v)
				_ = bDLQ.Delete(k)
				_ = c.Delete()
				currentCount--
			}
		}

		return setEventCount(bMeta, currentCount)
	})
}

// GetByID retrieves a single event by its ID.
func (s *BoltStore) GetByID(id string) (*inspector.WebhookEvent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var event inspector.WebhookEvent
	err := s.db.View(func(tx *bbolt.Tx) error {
		bEvents := tx.Bucket(bucketEvents)
		data := bEvents.Get([]byte(id))
		if data == nil {
			return inspector.ErrEventNotFound
		}
		if err := json.Unmarshal(data, &event); err != nil {
			return fmt.Errorf("unmarshaling event %s: %w", id, err)
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return &event, nil
}

// List returns the latest events up to the specified limit in descending chronological order (newest first).
func (s *BoltStore) List(limit int) ([]*inspector.WebhookEvent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var events []*inspector.WebhookEvent

	err := s.db.View(func(tx *bbolt.Tx) error {
		bEvents := tx.Bucket(bucketEvents)
		bTimeline := tx.Bucket(bucketTimeline)

		c := bTimeline.Cursor()
		count := 0

		for k, v := c.Last(); k != nil; k, v = c.Prev() {
			data := bEvents.Get(v)
			if data == nil {
				continue
			}

			var evt inspector.WebhookEvent
			if err := json.Unmarshal(data, &evt); err == nil {
				events = append(events, &evt)
				count++
				if limit > 0 && count >= limit {
					break
				}
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return events, nil
}

// ListFailed returns up to limit failed events (HTTP 4xx/5xx or transport errors) in descending order.
func (s *BoltStore) ListFailed(limit int) ([]*inspector.WebhookEvent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var events []*inspector.WebhookEvent

	err := s.db.View(func(tx *bbolt.Tx) error {
		bEvents := tx.Bucket(bucketEvents)
		bDLQ := tx.Bucket(bucketDLQ)

		c := bDLQ.Cursor()
		count := 0

		for k, v := c.Last(); k != nil; k, v = c.Prev() {
			data := bEvents.Get(v)
			if data == nil {
				continue
			}

			var evt inspector.WebhookEvent
			if err := json.Unmarshal(data, &evt); err == nil {
				events = append(events, &evt)
				count++
				if limit > 0 && count >= limit {
					break
				}
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return events, nil
}

// Clear flushes all stored events from the database buckets.
func (s *BoltStore) Clear() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.db.Update(func(tx *bbolt.Tx) error {
		_ = tx.DeleteBucket(bucketEvents)
		_ = tx.DeleteBucket(bucketTimeline)
		_ = tx.DeleteBucket(bucketDLQ)
		_ = tx.DeleteBucket(bucketMeta)

		if _, err := tx.CreateBucketIfNotExists(bucketEvents); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(bucketTimeline); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(bucketDLQ); err != nil {
			return err
		}
		bMeta, err := tx.CreateBucketIfNotExists(bucketMeta)
		if err != nil {
			return err
		}
		return setEventCount(bMeta, 0)
	})
}

// Count returns the total number of events currently stored in the database.
func (s *BoltStore) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var count int
	_ = s.db.View(func(tx *bbolt.Tx) error {
		bMeta := tx.Bucket(bucketMeta)
		if bMeta != nil {
			count = getEventCount(bMeta)
		}
		return nil
	})
	return count
}

// Close gracefully closes the underlying Bolt database file.
func (s *BoltStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.db != nil {
		return s.db.Close()
	}
	return nil
}
