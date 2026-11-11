package redis_test

import (
	"bufio"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/pandusatrianura/pocketqueue/internal/features/broker"
	"github.com/pandusatrianura/pocketqueue/internal/features/protocol/redis"
	"github.com/pandusatrianura/pocketqueue/internal/features/storage"
)

func TestRedisServer_StreamsLifecycle(t *testing.T) {
	engine := storage.NewMemoryEngine(1000)
	bus := broker.NewEventBus(100)
	svc := broker.NewService(engine, bus, 1)

	server := redis.NewServer("127.0.0.1:0", svc, nil)
	if err := server.Start(); err != nil {
		t.Fatalf("failed to start redis server: %v", err)
	}
	defer server.Close()

	conn, err := net.DialTimeout("tcp", server.Addr(), 2*time.Second)
	if err != nil {
		t.Fatalf("failed to dial redis server: %v", err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

	sendCmd := func(cmd string) string {
		_, err := conn.Write([]byte(cmd))
		if err != nil {
			t.Fatalf("write error: %v", err)
		}
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("read line error: %v", err)
		}
		return strings.TrimRight(line, "\r\n")
	}

	// 1. PING
	pong := sendCmd("PING\r\n")
	if pong != "+PONG" {
		t.Errorf("expected +PONG, got %q", pong)
	}

	// 2. XADD
	// *5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$1\r\n*\r\n$4\r\ntemp\r\n$4\r\n25.5\r\n
	xaddResp := sendCmd("*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$1\r\n*\r\n$4\r\ntemp\r\n$4\r\n25.5\r\n")
	if xaddResp != "$3" { // bulk string header for "0-0"
		t.Errorf("expected $3 for stream ID length, got %q", xaddResp)
	}
	streamID, _ := reader.ReadString('\n')
	if strings.TrimRight(streamID, "\r\n") != "0-0" {
		t.Errorf("expected 0-0, got %q", streamID)
	}

	// 3. XLEN
	xlenResp := sendCmd("*2\r\n$4\r\nXLEN\r\n$8\r\nmystream\r\n")
	if xlenResp != ":1" {
		t.Errorf("expected :1 for stream length, got %q", xlenResp)
	}

	// 4. XRANGE
	xrangeResp := sendCmd("*4\r\n$6\r\nXRANGE\r\n$8\r\nmystream\r\n$1\r\n-\r\n$1\r\n+\r\n")
	if xrangeResp != "*1" {
		t.Errorf("expected *1 for xrange array, got %q", xrangeResp)
	}
}

func TestRedisServer_XReadBlockTimeout(t *testing.T) {
	engine := storage.NewMemoryEngine(100)
	bus := broker.NewEventBus(10)
	svc := broker.NewService(engine, bus, 1)

	server := redis.NewServer("127.0.0.1:0", svc, nil)
	if err := server.Start(); err != nil {
		t.Fatalf("failed to start redis server: %v", err)
	}
	defer server.Close()

	conn, err := net.DialTimeout("tcp", server.Addr(), 2*time.Second)
	if err != nil {
		t.Fatalf("failed to dial redis server: %v", err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

	// Send XREAD BLOCK 100 STREAMS mystream $
	start := time.Now()
	// *6\r\n$5\r\nXREAD\r\n$5\r\nBLOCK\r\n$3\r\n100\r\n$7\r\nSTREAMS\r\n$8\r\nmystream\r\n$1\r\n$\r\n
	cmd := "*6\r\n$5\r\nXREAD\r\n$5\r\nBLOCK\r\n$3\r\n100\r\n$7\r\nSTREAMS\r\n$8\r\nmystream\r\n$1\r\n$\r\n"
	if _, err := conn.Write([]byte(cmd)); err != nil {
		t.Fatalf("failed to write XREAD command: %v", err)
	}

	resp, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("failed to read XREAD response: %v", err)
	}
	elapsed := time.Since(start)

	if strings.TrimRight(resp, "\r\n") != "*-1" {
		t.Errorf("expected *-1 for timeout nil array, got %q", resp)
	}
	if elapsed < 80*time.Millisecond {
		t.Errorf("expected at least ~100ms wait, got %v", elapsed)
	}
}

func TestRedisServer_XReadBlockWakeup(t *testing.T) {
	engine := storage.NewMemoryEngine(100)
	bus := broker.NewEventBus(10)
	svc := broker.NewService(engine, bus, 1)

	server := redis.NewServer("127.0.0.1:0", svc, nil)
	if err := server.Start(); err != nil {
		t.Fatalf("failed to start redis server: %v", err)
	}
	defer server.Close()

	conn, err := net.DialTimeout("tcp", server.Addr(), 2*time.Second)
	if err != nil {
		t.Fatalf("failed to dial redis server: %v", err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

	// Start a background producer that publishes after 50ms
	go func() {
		time.Sleep(50 * time.Millisecond)
		pConn, err := net.DialTimeout("tcp", server.Addr(), 2*time.Second)
		if err != nil {
			return
		}
		defer pConn.Close()
		// XADD jobs * task email
		pCmd := "*5\r\n$4\r\nXADD\r\n$4\r\njobs\r\n$1\r\n*\r\n$4\r\ntask\r\n$5\r\nemail\r\n"
		_, _ = pConn.Write([]byte(pCmd))
	}()

	// Read with BLOCK 1500 STREAMS jobs $
	cmd := "*6\r\n$5\r\nXREAD\r\n$5\r\nBLOCK\r\n$4\r\n1500\r\n$7\r\nSTREAMS\r\n$4\r\njobs\r\n$1\r\n$\r\n"
	if _, err := conn.Write([]byte(cmd)); err != nil {
		t.Fatalf("failed to write XREAD command: %v", err)
	}

	resp, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("failed to read XREAD response: %v", err)
	}

	if strings.TrimRight(resp, "\r\n") != "*1" {
		t.Errorf("expected *1 for stream envelope array, got %q", resp)
	}
}

func TestRedisServer_PortInUse(t *testing.T) {
	engine := storage.NewMemoryEngine(10)
	bus := broker.NewEventBus(10)
	svc := broker.NewService(engine, bus, 1)

	server1 := redis.NewServer("127.0.0.1:0", svc, nil)
	if err := server1.Start(); err != nil {
		t.Fatalf("failed to start server1: %v", err)
	}
	defer server1.Close()

	// Try starting a second server on the exact same port
	server2 := redis.NewServer(server1.Addr(), svc, nil)
	err := server2.Start()
	if err == nil {
		server2.Close()
		t.Fatalf("expected port in use error, got nil")
	}

	if !errors.Is(err, redis.ErrPortInUse) {
		t.Errorf("expected error wrapping redis.ErrPortInUse, got: %v", err)
	}
}

func TestRedisServer_QuitAndConnectionClosure(t *testing.T) {
	engine := storage.NewMemoryEngine(10)
	bus := broker.NewEventBus(10)
	svc := broker.NewService(engine, bus, 1)

	server := redis.NewServer("127.0.0.1:0", svc, nil)
	if err := server.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer server.Close()

	conn, err := net.DialTimeout("tcp", server.Addr(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial server failed: %v", err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

	// Send QUIT command
	if _, err := conn.Write([]byte("QUIT\r\n")); err != nil {
		t.Fatalf("write QUIT failed: %v", err)
	}

	// Should receive +OK\r\n
	resp, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read QUIT response failed: %v", err)
	}
	if strings.TrimRight(resp, "\r\n") != "+OK" {
		t.Errorf("expected +OK, got %q", resp)
	}

	// Server must close connection promptly (next read returns EOF)
	_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	var dummy [1]byte
	_, readErr := reader.Read(dummy[:])
	if readErr == nil || !errors.Is(readErr, io.EOF) {
		t.Errorf("expected EOF after QUIT, got: %v", readErr)
	}
}

