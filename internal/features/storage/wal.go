package storage

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// DefaultMaxWALFileSize sets the default safety threshold (100 MB per partition) to avoid
// filling local developer hard disks.
const DefaultMaxWALFileSize int64 = 100 * 1024 * 1024

// ErrWALFileSizeLimitExceeded is returned when a WAL partition file reaches its maximum capacity.
var ErrWALFileSizeLimitExceeded = errors.New("wal file size limit exceeded")

type walIndexEntry struct {
	offset   int64
	position int64
	length   int32
}

type walPartition struct {
	mu          sync.RWMutex
	topic       string
	partition   int32
	file        *os.File
	filePath    string
	maxFileSize int64
	index       []walIndexEntry
	startOffset int64
	nextOffset  int64
}

func newWALPartition(dir, topic string, partition int32, maxFileSize int64) (*walPartition, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create data dir %s: %w", dir, err)
	}

	fileName := fmt.Sprintf("%s_%d.wal", topic, partition)
	filePath := filepath.Join(dir, fileName)

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("open wal file %s: %w", filePath, err)
	}

	if maxFileSize <= 0 {
		maxFileSize = DefaultMaxWALFileSize
	}

	wp := &walPartition{
		topic:       topic,
		partition:   partition,
		file:        file,
		filePath:    filePath,
		maxFileSize: maxFileSize,
		index:       make([]walIndexEntry, 0),
	}

	// Replay WAL index from disk
	if err := wp.recover(); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("recover wal %s: %w", filePath, err)
	}

	return wp, nil
}

func (wp *walPartition) recover() error {
	stat, err := wp.file.Stat()
	if err != nil {
		return err
	}
	if stat.Size() == 0 {
		return nil
	}

	_, err = wp.file.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	var pos int64 = 0
	var lenBuf [4]byte

	for pos < stat.Size() {
		// Guard against partial length prefix at EOF
		if pos+4 > stat.Size() {
			break
		}

		_, err := io.ReadFull(wp.file, lenBuf[:])
		if err != nil {
			break
		}
		recordLen := int32(binary.BigEndian.Uint32(lenBuf[:]))
		if recordLen <= 0 || pos+int64(recordLen+4) > stat.Size() {
			// Incomplete or corrupted record frame
			break
		}

		// Read offset
		var offsetBuf [8]byte
		_, err = io.ReadFull(wp.file, offsetBuf[:])
		if err != nil {
			break
		}
		offset := int64(binary.BigEndian.Uint64(offsetBuf[:]))

		wp.index = append(wp.index, walIndexEntry{
			offset:   offset,
			position: pos,
			length:   recordLen + 4,
		})

		if len(wp.index) == 1 {
			wp.startOffset = offset
		}
		wp.nextOffset = offset + 1

		// Advance to next record position
		pos += int64(recordLen + 4)
		_, _ = wp.file.Seek(pos, io.SeekStart)
	}

	// Truncate any incomplete or corrupted trailing bytes to preserve clean frame boundary
	if pos < stat.Size() {
		if err := wp.file.Truncate(pos); err != nil {
			return fmt.Errorf("truncate corrupted wal tail: %w", err)
		}
		_, _ = wp.file.Seek(pos, io.SeekStart)
	}

	return nil
}

func (wp *walPartition) append(key, value []byte, headers []Header) (*Record, error) {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	rec := Record{
		Topic:     wp.topic,
		Partition: wp.partition,
		Offset:    wp.nextOffset,
		Timestamp: time.Now().UTC(),
		Key:       key,
		Value:     value,
		Headers:   headers,
	}

	// Encode record binary:
	// [4-byte Len][8-byte Offset][8-byte TimeNano][4-byte KeyLen][Key][4-byte ValLen][Val][4-byte HeaderCount][Headers...]
	payloadLen := 8 + 8 + 4 + len(key) + 4 + len(value) + 4
	for _, h := range headers {
		payloadLen += 4 + len(h.Key) + 4 + len(h.Value)
	}

	buf := make([]byte, 4+payloadLen)
	binary.BigEndian.PutUint32(buf[0:4], uint32(payloadLen))
	binary.BigEndian.PutUint64(buf[4:12], uint64(rec.Offset))
	binary.BigEndian.PutUint64(buf[12:20], uint64(rec.Timestamp.UnixNano()))

	idx := 20
	// Key
	binary.BigEndian.PutUint32(buf[idx:idx+4], uint32(len(key)))
	idx += 4
	copy(buf[idx:idx+len(key)], key)
	idx += len(key)

	// Value
	binary.BigEndian.PutUint32(buf[idx:idx+4], uint32(len(value)))
	idx += 4
	copy(buf[idx:idx+len(value)], value)
	idx += len(value)

	// Headers
	binary.BigEndian.PutUint32(buf[idx:idx+4], uint32(len(headers)))
	idx += 4
	for _, h := range headers {
		binary.BigEndian.PutUint32(buf[idx:idx+4], uint32(len(h.Key)))
		idx += 4
		copy(buf[idx:idx+len(h.Key)], []byte(h.Key))
		idx += len(h.Key)

		binary.BigEndian.PutUint32(buf[idx:idx+4], uint32(len(h.Value)))
		idx += 4
		copy(buf[idx:idx+len(h.Value)], h.Value)
		idx += len(h.Value)
	}

	// Seek to end of file
	stat, err := wp.file.Stat()
	if err != nil {
		return nil, err
	}
	filePos := stat.Size()

	if wp.maxFileSize > 0 && filePos+int64(len(buf)) > wp.maxFileSize {
		return nil, fmt.Errorf("%w: partition %s_%d reached maximum size of %d bytes",
			ErrWALFileSizeLimitExceeded, wp.topic, wp.partition, wp.maxFileSize)
	}

	if _, err := wp.file.WriteAt(buf, filePos); err != nil {
		return nil, fmt.Errorf("write wal: %w", err)
	}

	wp.index = append(wp.index, walIndexEntry{
		offset:   rec.Offset,
		position: filePos,
		length:   int32(len(buf)),
	})

	if len(wp.index) == 1 {
		wp.startOffset = rec.Offset
	}
	wp.nextOffset++

	return &rec, nil
}

func (wp *walPartition) readFrom(startOffset int64, maxRecords int) ([]Record, error) {
	wp.mu.RLock()
	defer wp.mu.RUnlock()

	if len(wp.index) == 0 || startOffset >= wp.nextOffset {
		return nil, nil
	}

	if startOffset < wp.startOffset {
		startOffset = wp.startOffset
	}

	idx := int(startOffset - wp.startOffset)
	if idx < 0 || idx >= len(wp.index) {
		return nil, nil
	}

	end := idx + maxRecords
	if end > len(wp.index) {
		end = len(wp.index)
	}

	results := make([]Record, 0, end-idx)

	for i := idx; i < end; i++ {
		entry := wp.index[i]
		buf := make([]byte, entry.length)
		_, err := wp.file.ReadAt(buf, entry.position)
		if err != nil {
			break
		}

		offset := int64(binary.BigEndian.Uint64(buf[4:12]))
		timeNano := int64(binary.BigEndian.Uint64(buf[12:20]))
		pIdx := 20

		keyLen := int(binary.BigEndian.Uint32(buf[pIdx : pIdx+4]))
		pIdx += 4
		key := make([]byte, keyLen)
		copy(key, buf[pIdx:pIdx+keyLen])
		pIdx += keyLen

		valLen := int(binary.BigEndian.Uint32(buf[pIdx : pIdx+4]))
		pIdx += 4
		val := make([]byte, valLen)
		copy(val, buf[pIdx:pIdx+valLen])
		pIdx += valLen

		hCount := int(binary.BigEndian.Uint32(buf[pIdx : pIdx+4]))
		pIdx += 4
		var headers []Header
		if hCount > 0 {
			headers = make([]Header, hCount)
			for h := 0; h < hCount; h++ {
				kL := int(binary.BigEndian.Uint32(buf[pIdx : pIdx+4]))
				pIdx += 4
				hKey := string(buf[pIdx : pIdx+kL])
				pIdx += kL

				vL := int(binary.BigEndian.Uint32(buf[pIdx : pIdx+4]))
				pIdx += 4
				hVal := make([]byte, vL)
				copy(hVal, buf[pIdx:pIdx+vL])
				pIdx += vL

				headers[h] = Header{Key: hKey, Value: hVal}
			}
		}

		results = append(results, Record{
			Topic:     wp.topic,
			Partition: wp.partition,
			Offset:    offset,
			Timestamp: time.Unix(0, timeNano).UTC(),
			Key:       key,
			Value:     val,
			Headers:   headers,
		})
	}

	return results, nil
}

func (wp *walPartition) watermarks() Watermarks {
	wp.mu.RLock()
	defer wp.mu.RUnlock()

	return Watermarks{
		Earliest: wp.startOffset,
		Latest:   wp.nextOffset,
	}
}

func (wp *walPartition) sync() error {
	wp.mu.Lock()
	defer wp.mu.Unlock()
	if wp.file != nil {
		return wp.file.Sync()
	}
	return nil
}

func (wp *walPartition) close() error {
	wp.mu.Lock()
	defer wp.mu.Unlock()
	if wp.file != nil {
		return wp.file.Close()
	}
	return nil
}

// WALEngine is a persistent append-only Write-Ahead Log implementation of Engine.
type WALEngine struct {
	mu          sync.RWMutex
	dir         string
	maxFileSize int64
	partitions  map[string]map[int32]*walPartition
	closed      bool
}

// NewWALEngineWithLimit initializes a WAL engine storing partition logs in dataDir with custom file size cap.
func NewWALEngineWithLimit(dataDir string, maxFileSize int64) (*WALEngine, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("create wal dir: %w", err)
	}

	if maxFileSize <= 0 {
		maxFileSize = DefaultMaxWALFileSize
	}

	engine := &WALEngine{
		dir:         dataDir,
		maxFileSize: maxFileSize,
		partitions:  make(map[string]map[int32]*walPartition),
	}

	// Discover existing .wal files
	files, err := os.ReadDir(dataDir)
	if err == nil {
		for _, f := range files {
			if !f.IsDir() && strings.HasSuffix(f.Name(), ".wal") {
				base := strings.TrimSuffix(f.Name(), ".wal")
				idx := strings.LastIndex(base, "_")
				if idx > 0 {
					topic := base[:idx]
					var partID int32
					_, err := fmt.Sscanf(base[idx+1:], "%d", &partID)
					if err == nil {
						wp, err := newWALPartition(dataDir, topic, partID, maxFileSize)
						if err == nil {
							if engine.partitions[topic] == nil {
								engine.partitions[topic] = make(map[int32]*walPartition)
							}
							engine.partitions[topic][partID] = wp
						}
					}
				}
			}
		}
	}

	return engine, nil
}

// NewWALEngine initializes a WAL engine storing partition logs in dataDir.
func NewWALEngine(dataDir string) (*WALEngine, error) {
	return NewWALEngineWithLimit(dataDir, DefaultMaxWALFileSize)
}

func (w *WALEngine) getOrCreatePartition(topic string, partition int32) (*walPartition, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	parts, exists := w.partitions[topic]
	if !exists {
		parts = make(map[int32]*walPartition)
		w.partitions[topic] = parts
	}

	wp, exists := parts[partition]
	if !exists {
		var err error
		wp, err = newWALPartition(w.dir, topic, partition, w.maxFileSize)
		if err != nil {
			return nil, err
		}
		parts[partition] = wp
	}

	return wp, nil
}

// Append writes a record to the WAL partition.
func (w *WALEngine) Append(ctx context.Context, topic string, partition int32, key, value []byte, headers []Header) (*Record, error) {
	wp, err := w.getOrCreatePartition(topic, partition)
	if err != nil {
		return nil, err
	}
	return wp.append(key, value, headers)
}

// ReadFrom reads records starting from startOffset.
func (w *WALEngine) ReadFrom(ctx context.Context, topic string, partition int32, startOffset int64, maxRecords int) ([]Record, error) {
	w.mu.RLock()
	parts, exists := w.partitions[topic]
	if !exists {
		w.mu.RUnlock()
		return nil, ErrTopicNotFound
	}
	wp, exists := parts[partition]
	w.mu.RUnlock()

	if !exists {
		return nil, ErrPartitionNotFound
	}

	return wp.readFrom(startOffset, maxRecords)
}

// GetWatermarks returns earliest and latest offsets.
func (w *WALEngine) GetWatermarks(ctx context.Context, topic string, partition int32) (Watermarks, error) {
	w.mu.RLock()
	parts, exists := w.partitions[topic]
	if !exists {
		w.mu.RUnlock()
		return Watermarks{}, ErrTopicNotFound
	}
	wp, exists := parts[partition]
	w.mu.RUnlock()

	if !exists {
		return Watermarks{}, ErrPartitionNotFound
	}

	return wp.watermarks(), nil
}

// ListTopics returns a map of existing topic names and partition counts.
func (w *WALEngine) ListTopics(ctx context.Context) (map[string]int32, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	result := make(map[string]int32, len(w.partitions))
	for topic, parts := range w.partitions {
		result[topic] = int32(len(parts))
	}
	return result, nil
}

// Sync flushes all open WAL partition files to physical disk.
func (w *WALEngine) Sync() error {
	w.mu.RLock()
	defer w.mu.RUnlock()

	var firstErr error
	for _, parts := range w.partitions {
		for _, wp := range parts {
			if err := wp.sync(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// Close syncs and closes all open WAL file descriptors.
func (w *WALEngine) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.closed = true
	var firstErr error
	for _, parts := range w.partitions {
		for _, wp := range parts {
			_ = wp.sync()
			if err := wp.close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

