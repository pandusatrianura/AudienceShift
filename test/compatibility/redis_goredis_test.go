package compatibility_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/pandusatrianura/pocketqueue/internal/features/broker"
	protocolredis "github.com/pandusatrianura/pocketqueue/internal/features/protocol/redis"
	"github.com/pandusatrianura/pocketqueue/internal/features/storage"
	"github.com/redis/go-redis/v9"
)

// setupRedisServer launches an in-memory PocketQueue Redis server on an ephemeral TCP port.
func setupRedisServer(t *testing.T) (*protocolredis.Server, *broker.Service, func()) {
	t.Helper()

	engine := storage.NewMemoryEngine(1000)
	bus := broker.NewEventBus(100)
	svc := broker.NewService(engine, bus, 1)

	server := protocolredis.NewServer("127.0.0.1:0", svc, nil)
	if err := server.Start(); err != nil {
		t.Fatalf("failed to start embedded redis server: %v", err)
	}

	cleanup := func() {
		_ = server.Close()
		_ = svc.Close()
	}

	return server, svc, cleanup
}

func TestGoRedis_Streams_Lifecycle(t *testing.T) {
	server, _, cleanup := setupRedisServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rdb := redis.NewClient(&redis.Options{
		Addr: server.Addr(),
	})
	defer rdb.Close()

	// 1. PING
	pong, err := rdb.Ping(ctx).Result()
	if err != nil {
		t.Fatalf("go-redis PING failed: %v", err)
	}
	if pong != "PONG" {
		t.Errorf("expected PONG, got %s", pong)
	}

	// 2. XADD
	stream := "telemetry-stream"
	id, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]interface{}{
			"sensor": "DHT22",
			"temp":   "23.8",
		},
	}).Result()
	if err != nil {
		t.Fatalf("go-redis XADD failed: %v", err)
	}
	if id != "0-0" {
		t.Errorf("expected stream ID 0-0, got %s", id)
	}

	// 3. XLEN
	length, err := rdb.XLen(ctx, stream).Result()
	if err != nil {
		t.Fatalf("go-redis XLEN failed: %v", err)
	}
	if length != 1 {
		t.Errorf("expected XLEN 1, got %d", length)
	}

	// 4. XRANGE
	entries, err := rdb.XRange(ctx, stream, "-", "+").Result()
	if err != nil {
		t.Fatalf("go-redis XRANGE failed: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].ID != "0-0" {
		t.Errorf("expected entry ID 0-0, got %s", entries[0].ID)
	}
	if entries[0].Values["sensor"] != "DHT22" || entries[0].Values["temp"] != "23.8" {
		t.Errorf("unexpected entry values: %+v", entries[0].Values)
	}
}

func TestGoRedis_Streams_XReadBlock(t *testing.T) {
	server, _, cleanup := setupRedisServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rdb := redis.NewClient(&redis.Options{
		Addr: server.Addr(),
	})
	defer rdb.Close()

	stream := "event-stream"

	// 1. Initial XADD so stream exists
	_, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]interface{}{"status": "init"},
	}).Result()
	if err != nil {
		t.Fatalf("initial XADD failed: %v", err)
	}

	// 2. Verify XREAD BLOCK timeout returns redis.Nil
	streams, err := rdb.XRead(ctx, &redis.XReadArgs{
		Streams: []string{stream, "$"},
		Block:   100 * time.Millisecond,
		Count:   1,
	}).Result()
	if !errors.Is(err, redis.Nil) {
		t.Fatalf("expected redis.Nil on timeout, got streams=%v err=%v", streams, err)
	}

	// 3. Verify XREAD BLOCK wake-up on live XADD
	var wg sync.WaitGroup
	var readResult []redis.XStream
	var readErr error

	wg.Add(1)
	go func() {
		defer wg.Done()
		readClient := redis.NewClient(&redis.Options{Addr: server.Addr()})
		defer readClient.Close()

		readResult, readErr = readClient.XRead(ctx, &redis.XReadArgs{
			Streams: []string{stream, "$"},
			Block:   2 * time.Second,
			Count:   1,
		}).Result()
	}()

	// Allow reader goroutine to establish connection and start blocking
	time.Sleep(100 * time.Millisecond)

	// Publish message to wake up reader
	_, err = rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]interface{}{"status": "live-alert"},
	}).Result()
	if err != nil {
		t.Fatalf("wake-up XADD failed: %v", err)
	}

	wg.Wait()

	if readErr != nil {
		t.Fatalf("blocking XREAD failed: %v", readErr)
	}
	if len(readResult) != 1 || len(readResult[0].Messages) != 1 {
		t.Fatalf("expected 1 message in readResult, got %+v", readResult)
	}

	msg := readResult[0].Messages[0]
	if msg.Values["status"] != "live-alert" {
		t.Errorf("expected status=live-alert, got %+v", msg.Values)
	}
}
