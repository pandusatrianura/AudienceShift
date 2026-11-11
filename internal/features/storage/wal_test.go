package storage_test

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/pandusatrianura/pocketqueue/internal/features/storage"
)

func TestWALEngine_PersistenceAndRecovery(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "pocketqueue-wal-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	ctx := context.Background()

	// 1. Initial engine session
	wal1, err := storage.NewWALEngine(tempDir)
	if err != nil {
		t.Fatalf("failed to create wal1: %v", err)
	}

	headers := []storage.Header{
		{Key: "env", Value: []byte("prod")},
		{Key: "source", Value: []byte("checkout")},
	}

	r1, err := wal1.Append(ctx, "orders", 0, []byte("k1"), []byte("v1"), headers)
	if err != nil || r1.Offset != 0 {
		t.Fatalf("wal1 append 1 failed: offset=%d err=%v", r1.Offset, err)
	}

	r2, err := wal1.Append(ctx, "orders", 0, []byte("k2"), []byte("v2"), nil)
	if err != nil || r2.Offset != 1 {
		t.Fatalf("wal1 append 2 failed: offset=%d err=%v", r2.Offset, err)
	}

	// Close session 1
	if err := wal1.Close(); err != nil {
		t.Fatalf("wal1 close failed: %v", err)
	}

	// 2. Open session 2 on same dir (Simulating process restart)
	wal2, err := storage.NewWALEngine(tempDir)
	if err != nil {
		t.Fatalf("failed to create wal2: %v", err)
	}
	defer wal2.Close()

	// Verify watermarks recovered
	wm, err := wal2.GetWatermarks(ctx, "orders", 0)
	if err != nil {
		t.Fatalf("wal2 get watermarks failed: %v", err)
	}
	if wm.Earliest != 0 || wm.Latest != 2 {
		t.Errorf("expected watermarks [0, 2], got [%d, %d]", wm.Earliest, wm.Latest)
	}

	// Read records back
	records, err := wal2.ReadFrom(ctx, "orders", 0, 0, 10)
	if err != nil {
		t.Fatalf("wal2 read failed: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 recovered records, got %d", len(records))
	}

	if string(records[0].Key) != "k1" || string(records[0].Value) != "v1" {
		t.Errorf("record 0 mismatch: %+v", records[0])
	}
	if len(records[0].Headers) != 2 || records[0].Headers[0].Key != "env" {
		t.Errorf("record 0 headers mismatch: %+v", records[0].Headers)
	}

	// Append 3rd record in session 2 to verify nextOffset continuity
	r3, err := wal2.Append(ctx, "orders", 0, []byte("k3"), []byte("v3"), nil)
	if err != nil || r3.Offset != 2 {
		t.Fatalf("wal2 append 3 failed: offset=%d err=%v", r3.Offset, err)
	}
}

func TestWALEngine_CorruptedTrailingRecovery(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "pocketqueue-wal-corrupt-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	ctx := context.Background()

	// 1. Session 1: write 2 valid records
	wal1, err := storage.NewWALEngine(tempDir)
	if err != nil {
		t.Fatalf("failed to create wal1: %v", err)
	}
	_, _ = wal1.Append(ctx, "events", 0, []byte("e1"), []byte("v1"), nil)
	_, _ = wal1.Append(ctx, "events", 0, []byte("e2"), []byte("v2"), nil)
	_ = wal1.Close()

	// 2. Corrupt the .wal file with a partial/garbage trailing frame
	walFilePath := tempDir + "/events_0.wal"
	file, err := os.OpenFile(walFilePath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("failed to open wal file for corruption: %v", err)
	}
	// Write 5 bytes of incomplete garbage
	_, _ = file.Write([]byte{0x00, 0x00, 0x01, 0x50, 0xFF})
	_ = file.Close()

	// 3. Session 2: recover with corrupted trailing bytes
	wal2, err := storage.NewWALEngine(tempDir)
	if err != nil {
		t.Fatalf("failed to recover wal with corrupt tail: %v", err)
	}
	defer wal2.Close()

	records, err := wal2.ReadFrom(ctx, "events", 0, 0, 10)
	if err != nil {
		t.Fatalf("read from recovered wal failed: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 valid recovered records, got %d", len(records))
	}
	if string(records[1].Key) != "e2" {
		t.Errorf("unexpected record 1 key: %s", records[1].Key)
	}

	// 4. Verify we can continue appending seamlessly
	r3, err := wal2.Append(ctx, "events", 0, []byte("e3"), []byte("v3"), nil)
	if err != nil {
		t.Fatalf("failed to append after recovery: %v", err)
	}
	if r3.Offset != 2 {
		t.Errorf("expected offset 2 for new record, got %d", r3.Offset)
	}
}

func TestWALEngine_Sync(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "pocketqueue-wal-sync-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	ctx := context.Background()
	wal, err := storage.NewWALEngine(tempDir)
	if err != nil {
		t.Fatalf("failed to create wal: %v", err)
	}
	defer wal.Close()

	for i := 0; i < 5; i++ {
		_, err := wal.Append(ctx, "sync-topic", 0, []byte("key"), []byte("value"), nil)
		if err != nil {
			t.Fatalf("append failed: %v", err)
		}
	}

	// Verify Sync commits without errors
	if err := wal.Sync(); err != nil {
		t.Fatalf("wal.Sync failed: %v", err)
	}
}

func TestWALEngine_FileSizeLimit(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "pocketqueue-wal-limit-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	ctx := context.Background()
	// Set very small limit: 120 bytes (each record with header is ~40-50 bytes)
	wal, err := storage.NewWALEngineWithLimit(tempDir, 120)
	if err != nil {
		t.Fatalf("failed to create wal: %v", err)
	}
	defer wal.Close()

	// Append record 1 (should succeed)
	_, err = wal.Append(ctx, "capped-topic", 0, []byte("key1"), []byte("val1"), nil)
	if err != nil {
		t.Fatalf("record 1 failed: %v", err)
	}

	// Append record 2 (should succeed)
	_, err = wal.Append(ctx, "capped-topic", 0, []byte("key2"), []byte("val2"), nil)
	if err != nil {
		t.Fatalf("record 2 failed: %v", err)
	}

	// Append record 3 (should exceed 120 byte limit and be rejected)
	_, err = wal.Append(ctx, "capped-topic", 0, []byte("key3-overflow"), []byte("val3-overflow-payload"), nil)
	if err == nil {
		t.Fatalf("expected error exceeding file size limit, got nil")
	}
	if !errors.Is(err, storage.ErrWALFileSizeLimitExceeded) {
		t.Errorf("expected ErrWALFileSizeLimitExceeded, got: %v", err)
	}
}
