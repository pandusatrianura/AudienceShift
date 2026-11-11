package storage

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrTopicNotFound is returned when a requested topic does not exist.
	ErrTopicNotFound = errors.New("topic not found")

	// ErrPartitionNotFound is returned when a requested partition does not exist.
	ErrPartitionNotFound = errors.New("partition not found")

	// ErrOffsetOutOfRange is returned when a requested offset is past the latest high watermark.
	ErrOffsetOutOfRange = errors.New("offset out of range")
)

// Header represents a key-value attribute attached to a record.
type Header struct {
	Key   string
	Value []byte
}

// Record represents a message stored in a partition.
type Record struct {
	Topic     string
	Partition int32
	Offset    int64
	Timestamp time.Time
	Key       []byte
	Value     []byte
	Headers   []Header
	IsDLQ     bool
}

// TopicPartition identifies a specific partition of a topic.
type TopicPartition struct {
	Topic     string
	Partition int32
}

// Watermarks defines the lowest (earliest available) and highest (next assignable) offsets in a partition.
type Watermarks struct {
	Earliest int64
	Latest   int64
}

// Engine defines the abstract interface for message storage engines.
type Engine interface {
	// Append writes a record to a topic partition and assigns a monotonic offset.
	Append(ctx context.Context, topic string, partition int32, key, value []byte, headers []Header) (*Record, error)

	// ReadFrom reads up to maxRecords starting from startOffset in topic partition.
	ReadFrom(ctx context.Context, topic string, partition int32, startOffset int64, maxRecords int) ([]Record, error)

	// GetWatermarks returns the earliest available offset and the high-watermark offset.
	GetWatermarks(ctx context.Context, topic string, partition int32) (Watermarks, error)

	// ListTopics returns a map of existing topic names and their partition counts.
	ListTopics(ctx context.Context) (map[string]int32, error)

	// Sync flushes pending data buffers to disk/storage.
	Sync() error

	// Close shuts down the engine and releases resources.
	Close() error
}

