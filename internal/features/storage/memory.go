package storage

import (
	"context"
	"sync"
	"time"
)

type partitionBuffer struct {
	mu          sync.RWMutex
	capacity    int
	records     []Record
	startOffset int64
	nextOffset  int64
}

func newPartitionBuffer(capacity int) *partitionBuffer {
	if capacity <= 0 {
		capacity = 5000
	}
	return &partitionBuffer{
		capacity: capacity,
		records:  make([]Record, 0, capacity),
	}
}

func (pb *partitionBuffer) append(rec Record) Record {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	rec.Offset = pb.nextOffset
	if rec.Timestamp.IsZero() {
		rec.Timestamp = time.Now().UTC()
	}

	if len(pb.records) < pb.capacity {
		pb.records = append(pb.records, rec)
	} else {
		// Evict oldest by shifting or replacing
		copy(pb.records, pb.records[1:])
		pb.records[len(pb.records)-1] = rec
		pb.startOffset++
	}

	pb.nextOffset++
	return rec
}

func (pb *partitionBuffer) readFrom(startOffset int64, maxRecords int) []Record {
	pb.mu.RLock()
	defer pb.mu.RUnlock()

	if len(pb.records) == 0 || startOffset >= pb.nextOffset {
		return nil
	}

	// If requested offset is older than the oldest retained record, start from the earliest
	if startOffset < pb.startOffset {
		startOffset = pb.startOffset
	}

	idx := int(startOffset - pb.startOffset)
	if idx < 0 || idx >= len(pb.records) {
		return nil
	}

	endIdx := idx + maxRecords
	if endIdx > len(pb.records) {
		endIdx = len(pb.records)
	}

	result := make([]Record, endIdx-idx)
	copy(result, pb.records[idx:endIdx])
	return result
}

func (pb *partitionBuffer) watermarks() Watermarks {
	pb.mu.RLock()
	defer pb.mu.RUnlock()

	return Watermarks{
		Earliest: pb.startOffset,
		Latest:   pb.nextOffset,
	}
}

// MemoryEngine is an in-memory ring-buffer storage implementation.
type MemoryEngine struct {
	mu         sync.RWMutex
	capacity   int
	topics     map[string]map[int32]*partitionBuffer
	closed     bool
}

// NewMemoryEngine creates an in-memory storage engine with a per-partition capacity.
func NewMemoryEngine(perPartitionCapacity int) *MemoryEngine {
	return &MemoryEngine{
		capacity: perPartitionCapacity,
		topics:   make(map[string]map[int32]*partitionBuffer),
	}
}

func (m *MemoryEngine) getOrCreatePartition(topic string, partition int32) *partitionBuffer {
	m.mu.Lock()
	defer m.mu.Unlock()

	parts, exists := m.topics[topic]
	if !exists {
		parts = make(map[int32]*partitionBuffer)
		m.topics[topic] = parts
	}

	pb, exists := parts[partition]
	if !exists {
		pb = newPartitionBuffer(m.capacity)
		parts[partition] = pb
	}

	return pb
}

// Append writes a record to a topic partition and assigns a monotonic offset.
func (m *MemoryEngine) Append(ctx context.Context, topic string, partition int32, key, value []byte, headers []Header) (*Record, error) {
	pb := m.getOrCreatePartition(topic, partition)

	rec := Record{
		Topic:     topic,
		Partition: partition,
		Key:       key,
		Value:     value,
		Headers:   headers,
	}

	saved := pb.append(rec)
	return &saved, nil
}

// ReadFrom reads up to maxRecords starting from startOffset in topic partition.
func (m *MemoryEngine) ReadFrom(ctx context.Context, topic string, partition int32, startOffset int64, maxRecords int) ([]Record, error) {
	m.mu.RLock()
	parts, exists := m.topics[topic]
	if !exists {
		m.mu.RUnlock()
		return nil, ErrTopicNotFound
	}
	pb, exists := parts[partition]
	m.mu.RUnlock()

	if !exists {
		return nil, ErrPartitionNotFound
	}

	return pb.readFrom(startOffset, maxRecords), nil
}

// GetWatermarks returns the earliest available offset and the high-watermark offset.
func (m *MemoryEngine) GetWatermarks(ctx context.Context, topic string, partition int32) (Watermarks, error) {
	m.mu.RLock()
	parts, exists := m.topics[topic]
	if !exists {
		m.mu.RUnlock()
		return Watermarks{}, ErrTopicNotFound
	}
	pb, exists := parts[partition]
	m.mu.RUnlock()

	if !exists {
		return Watermarks{}, ErrPartitionNotFound
	}

	return pb.watermarks(), nil
}

// ListTopics returns a map of existing topic names and their partition counts.
func (m *MemoryEngine) ListTopics(ctx context.Context) (map[string]int32, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string]int32, len(m.topics))
	for topic, parts := range m.topics {
		result[topic] = int32(len(parts))
	}
	return result, nil
}

// Sync is a no-op for in-memory storage.
func (m *MemoryEngine) Sync() error {
	return nil
}

// Close shuts down the engine.
func (m *MemoryEngine) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

