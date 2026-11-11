package kafka_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/pandusatrianura/pocketqueue/internal/features/broker"
	"github.com/pandusatrianura/pocketqueue/internal/features/protocol/kafka"
	"github.com/pandusatrianura/pocketqueue/internal/features/storage"
)

func TestKafkaServer_TCPHandshakeAndProduceFetch(t *testing.T) {
	// 1. Setup broker & server
	engine := storage.NewMemoryEngine(1000)
	bus := broker.NewEventBus(100)
	svc := broker.NewService(engine, bus, 1)

	handler := kafka.NewHandler(svc, "127.0.0.1", 9092, nil)
	server := kafka.NewServer("127.0.0.1:0", handler, nil)

	if err := server.Start(); err != nil {
		t.Fatalf("failed to start kafka server: %v", err)
	}
	defer server.Close()

	// 2. Connect client via TCP
	conn, err := net.DialTimeout("tcp", server.Addr(), 2*time.Second)
	if err != nil {
		t.Fatalf("failed to dial server: %v", err)
	}
	defer conn.Close()

	// Helper to send frame and read response
	sendAndReceive := func(payload []byte) []byte {
		var lenBuf [4]byte
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(payload)))

		_, err := conn.Write(lenBuf[:])
		if err != nil {
			t.Fatalf("write len: %v", err)
		}
		_, err = conn.Write(payload)
		if err != nil {
			t.Fatalf("write payload: %v", err)
		}

		// Read response
		var respLenBuf [4]byte
		_, err = io.ReadFull(conn, respLenBuf[:])
		if err != nil {
			t.Fatalf("read resp len: %v", err)
		}
		respLen := int(binary.BigEndian.Uint32(respLenBuf[:]))
		respBytes := make([]byte, respLen)
		_, err = io.ReadFull(conn, respBytes)
		if err != nil {
			t.Fatalf("read resp payload: %v", err)
		}
		return respBytes
	}

	// 3. Test ApiVersions (Key 18)
	reqBuf := new(bytes.Buffer)
	enc := kafka.NewEncoder(reqBuf)
	enc.PutInt16(kafka.ApiKeyApiVersions)
	enc.PutInt16(3)       // version 3
	enc.PutInt32(777)     // correlation ID
	enc.PutString("unit-client")

	respBytes := sendAndReceive(reqBuf.Bytes())
	dec := kafka.NewDecoder(respBytes)

	corrId, _ := dec.GetInt32()
	if corrId != 777 {
		t.Errorf("expected correlation ID 777, got %d", corrId)
	}
	errCode, _ := dec.GetInt16()
	if errCode != 0 {
		t.Errorf("expected ApiVersions error code 0, got %d", errCode)
	}

	// 4. Test Produce (Key 0)
	prodBuf := new(bytes.Buffer)
	pEnc := kafka.NewEncoder(prodBuf)
	pEnc.PutInt16(kafka.ApiKeyProduce)
	pEnc.PutInt16(3)     // version 3
	pEnc.PutInt32(888)   // correlation ID
	pEnc.PutString("unit-client")

	// Produce body
	pEnc.PutNullableString(nil) // transactional_id
	pEnc.PutInt16(1)            // acks = 1
	pEnc.PutInt32(1500)         // timeout_ms = 1500

	// 1 topic
	pEnc.PutInt32(1)
	pEnc.PutString("orders")

	// 1 partition
	pEnc.PutInt32(1)
	pEnc.PutInt32(0) // partition 0

	// Encoded RecordBatch
	records := []kafka.RecordItem{
		{Key: []byte("ord-1"), Value: []byte(`{"status":"new"}`)},
	}
	batchBytes := kafka.EncodeRecordBatchV2(0, records)
	pEnc.PutBytes(batchBytes)

	prodRespBytes := sendAndReceive(prodBuf.Bytes())
	pDec := kafka.NewDecoder(prodRespBytes)

	pCorrId, _ := pDec.GetInt32()
	if pCorrId != 888 {
		t.Errorf("expected produce correlation ID 888, got %d", pCorrId)
	}
	topicsCount, _ := pDec.GetInt32()
	if topicsCount != 1 {
		t.Fatalf("expected 1 topic response, got %d", topicsCount)
	}
	tName, _ := pDec.GetString()
	if tName != "orders" {
		t.Errorf("expected topic 'orders', got %s", tName)
	}
	pCount, _ := pDec.GetInt32()
	if pCount != 1 {
		t.Fatalf("expected 1 partition response, got %d", pCount)
	}
	partId, _ := pDec.GetInt32()
	pErr, _ := pDec.GetInt16()
	baseOffset, _ := pDec.GetInt64()

	if partId != 0 || pErr != 0 || baseOffset != 0 {
		t.Errorf("unexpected produce result: part=%d, err=%d, baseOffset=%d", partId, pErr, baseOffset)
	}

	// 5. Test Fetch (Key 1)
	fetchBuf := new(bytes.Buffer)
	fEnc := kafka.NewEncoder(fetchBuf)
	fEnc.PutInt16(kafka.ApiKeyFetch)
	fEnc.PutInt16(4)     // version 4
	fEnc.PutInt32(999)   // correlation ID
	fEnc.PutString("unit-client")

	fEnc.PutInt32(-1)    // replica_id
	fEnc.PutInt32(500)   // max_wait_ms
	fEnc.PutInt32(1)     // min_bytes
	fEnc.PutInt32(65536) // max_bytes
	fEnc.PutInt8(0)      // isolation_level

	// 1 topic
	fEnc.PutInt32(1)
	fEnc.PutString("orders")

	// 1 partition
	fEnc.PutInt32(1)
	fEnc.PutInt32(0)     // partition 0
	fEnc.PutInt64(0)     // fetch_offset = 0
	fEnc.PutInt32(65536) // max_bytes

	fetchRespBytes := sendAndReceive(fetchBuf.Bytes())
	fDec := kafka.NewDecoder(fetchRespBytes)

	fCorrId, _ := fDec.GetInt32()
	if fCorrId != 999 {
		t.Errorf("expected fetch correlation ID 999, got %d", fCorrId)
	}
	_, _ = fDec.GetInt32() // throttle_time_ms
	fTopicsCount, _ := fDec.GetInt32()
	if fTopicsCount != 1 {
		t.Fatalf("expected 1 fetch topic response, got %d", fTopicsCount)
	}
	fTopicName, _ := fDec.GetString()
	if fTopicName != "orders" {
		t.Errorf("expected fetch topic 'orders', got %s", fTopicName)
	}
	fPartCount, _ := fDec.GetInt32()
	if fPartCount != 1 {
		t.Fatalf("expected 1 fetch partition response, got %d", fPartCount)
	}
	_, _ = fDec.GetInt32() // partition_id
	_, _ = fDec.GetInt16() // error_code
	_, _ = fDec.GetInt64() // high_watermark
	_, _ = fDec.GetInt64() // last_stable_offset
	_, _ = fDec.GetInt64() // log_start_offset
	_, _ = fDec.GetInt32() // aborted_transactions_count

	fetchedBatchBytes, err := fDec.GetBytes()
	if err != nil || len(fetchedBatchBytes) == 0 {
		t.Fatalf("expected non-empty record batch, got %d bytes (err: %v)", len(fetchedBatchBytes), err)
	}

	fetchedItems, err := kafka.DecodeRecordBatchV2(fetchedBatchBytes)
	if err != nil {
		t.Fatalf("failed to decode fetched record batch: %v", err)
	}

	if len(fetchedItems) != 1 {
		t.Fatalf("expected 1 fetched record, got %d", len(fetchedItems))
	}
	if string(fetchedItems[0].Key) != "ord-1" || string(fetchedItems[0].Value) != `{"status":"new"}` {
		t.Errorf("unexpected record contents: %+v", fetchedItems[0])
	}
}

func TestKafkaServer_RejectOversizedFrame(t *testing.T) {
	engine := storage.NewMemoryEngine(100)
	bus := broker.NewEventBus(10)
	svc := broker.NewService(engine, bus, 1)

	handler := kafka.NewHandler(svc, "127.0.0.1", 9092, nil)
	server := kafka.NewServer("127.0.0.1:0", handler, nil)

	if err := server.Start(); err != nil {
		t.Fatalf("failed to start kafka server: %v", err)
	}
	defer server.Close()

	conn, err := net.DialTimeout("tcp", server.Addr(), 2*time.Second)
	if err != nil {
		t.Fatalf("failed to dial server: %v", err)
	}
	defer conn.Close()

	// Send an oversized frame prefix (e.g. 0x7FFFFFFF = ~2GB)
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], 0x7FFFFFFF)
	if _, err := conn.Write(lenBuf[:]); err != nil {
		t.Fatalf("failed to write oversized frame length: %v", err)
	}

	// Server must close connection without hanging or panic
	_ = conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	var dummy [1]byte
	_, err = conn.Read(dummy[:])
	if err == nil {
		t.Errorf("expected connection to be closed by server, got nil error")
	}
}

func TestKafkaServer_ConsumerGroupHandshake(t *testing.T) {
	engine := storage.NewMemoryEngine(100)
	bus := broker.NewEventBus(10)
	svc := broker.NewService(engine, bus, 1)

	handler := kafka.NewHandler(svc, "127.0.0.1", 9092, nil)
	server := kafka.NewServer("127.0.0.1:0", handler, nil)

	if err := server.Start(); err != nil {
		t.Fatalf("failed to start kafka server: %v", err)
	}
	defer server.Close()

	conn, err := net.DialTimeout("tcp", server.Addr(), 2*time.Second)
	if err != nil {
		t.Fatalf("failed to dial server: %v", err)
	}
	defer conn.Close()

	sendAndRecv := func(payload []byte) []byte {
		var lenBuf [4]byte
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(payload)))
		if _, err := conn.Write(lenBuf[:]); err != nil {
			t.Fatalf("write len: %v", err)
		}
		if _, err := conn.Write(payload); err != nil {
			t.Fatalf("write payload: %v", err)
		}

		var respLenBuf [4]byte
		if _, err := io.ReadFull(conn, respLenBuf[:]); err != nil {
			t.Fatalf("read resp len: %v", err)
		}
		respLen := int(binary.BigEndian.Uint32(respLenBuf[:]))
		respBytes := make([]byte, respLen)
		if _, err := io.ReadFull(conn, respBytes); err != nil {
			t.Fatalf("read resp payload: %v", err)
		}
		return respBytes
	}

	// 1. JoinGroup (Key 11, Version 2)
	joinBuf := new(bytes.Buffer)
	jEnc := kafka.NewEncoder(joinBuf)
	jEnc.PutInt16(kafka.ApiKeyJoinGroup)
	jEnc.PutInt16(2)     // ApiVersion 2
	jEnc.PutInt32(1101)  // CorrelationId
	jEnc.PutString("test-client")
	// JoinGroup body
	jEnc.PutString("test-group")
	jEnc.PutInt32(30000) // session timeout
	jEnc.PutInt32(30000) // rebalance timeout
	jEnc.PutString("")   // memberId (empty requests new id)
	jEnc.PutString("consumer") // protocol type
	jEnc.PutInt32(1)     // 1 protocol
	jEnc.PutString("roundrobin")
	jEnc.PutBytes([]byte("dummy-meta"))

	joinRespBytes := sendAndRecv(joinBuf.Bytes())
	jDec := kafka.NewDecoder(joinRespBytes)

	corrId, _ := jDec.GetInt32()
	if corrId != 1101 {
		t.Errorf("expected correlation ID 1101, got %d", corrId)
	}
	_, _ = jDec.GetInt32() // throttle_time
	errCode, _ := jDec.GetInt16()
	if errCode != 0 {
		t.Fatalf("expected JoinGroup error code 0, got %d", errCode)
	}
	genId, _ := jDec.GetInt32()
	if genId != 1 {
		t.Errorf("expected generation ID 1, got %d", genId)
	}
	proto, _ := jDec.GetString()
	if proto != "roundrobin" {
		t.Errorf("expected protocol roundrobin, got %s", proto)
	}
	leaderId, _ := jDec.GetString()
	memberId, _ := jDec.GetString()
	if memberId == "" || leaderId != memberId {
		t.Errorf("expected non-empty leader/memberId match, leader: %s, member: %s", leaderId, memberId)
	}

	// 2. SyncGroup (Key 14, Version 1)
	syncBuf := new(bytes.Buffer)
	sEnc := kafka.NewEncoder(syncBuf)
	sEnc.PutInt16(kafka.ApiKeySyncGroup)
	sEnc.PutInt16(1)     // ApiVersion 1
	sEnc.PutInt32(1102)  // CorrelationId
	sEnc.PutString("test-client")
	// SyncGroup body
	sEnc.PutString("test-group")
	sEnc.PutInt32(genId)
	sEnc.PutString(memberId)
	sEnc.PutInt32(1)     // 1 assignment
	sEnc.PutString(memberId)
	sEnc.PutBytes(kafka.BuildDefaultMemberAssignment([]string{"orders"}, 1))

	syncRespBytes := sendAndRecv(syncBuf.Bytes())
	sDec := kafka.NewDecoder(syncRespBytes)

	sCorrId, _ := sDec.GetInt32()
	if sCorrId != 1102 {
		t.Errorf("expected correlation ID 1102, got %d", sCorrId)
	}
	_, _ = sDec.GetInt32() // throttle_time
	sErrCode, _ := sDec.GetInt16()
	if sErrCode != 0 {
		t.Fatalf("expected SyncGroup error code 0, got %d", sErrCode)
	}
	assignmentBytes, err := sDec.GetBytes()
	if err != nil || len(assignmentBytes) == 0 {
		t.Fatalf("expected assignment bytes, got len %d (err: %v)", len(assignmentBytes), err)
	}

	// 3. Heartbeat (Key 12, Version 1)
	hbBuf := new(bytes.Buffer)
	hEnc := kafka.NewEncoder(hbBuf)
	hEnc.PutInt16(kafka.ApiKeyHeartbeat)
	hEnc.PutInt16(1)
	hEnc.PutInt32(1103)
	hEnc.PutString("test-client")
	// Heartbeat body
	hEnc.PutString("test-group")
	hEnc.PutInt32(genId)
	hEnc.PutString(memberId)

	hbRespBytes := sendAndRecv(hbBuf.Bytes())
	hDec := kafka.NewDecoder(hbRespBytes)

	hCorrId, _ := hDec.GetInt32()
	if hCorrId != 1103 {
		t.Errorf("expected correlation ID 1103, got %d", hCorrId)
	}
	_, _ = hDec.GetInt32() // throttle_time
	hErrCode, _ := hDec.GetInt16()
	if hErrCode != 0 {
		t.Fatalf("expected Heartbeat error code 0, got %d", hErrCode)
	}
}

func TestKafkaServer_ProduceAcksZero(t *testing.T) {
	engine := storage.NewMemoryEngine(100)
	bus := broker.NewEventBus(10)
	svc := broker.NewService(engine, bus, 1)

	handler := kafka.NewHandler(svc, "127.0.0.1", 9092, nil)
	server := kafka.NewServer("127.0.0.1:0", handler, nil)

	if err := server.Start(); err != nil {
		t.Fatalf("failed to start kafka server: %v", err)
	}
	defer server.Close()

	conn, err := net.DialTimeout("tcp", server.Addr(), 2*time.Second)
	if err != nil {
		t.Fatalf("failed to dial server: %v", err)
	}
	defer conn.Close()

	// 1. Send Produce with acks = 0
	prodBuf := new(bytes.Buffer)
	pEnc := kafka.NewEncoder(prodBuf)
	pEnc.PutInt16(kafka.ApiKeyProduce)
	pEnc.PutInt16(3)     // version 3
	pEnc.PutInt32(9999)  // correlation ID
	pEnc.PutString("fire-forget-client")

	// Produce body
	pEnc.PutNullableString(nil) // transactional_id
	pEnc.PutInt16(0)            // acks = 0 (FIRE AND FORGET)
	pEnc.PutInt32(1000)         // timeout_ms

	pEnc.PutInt32(1)            // 1 topic
	pEnc.PutString("metrics")
	pEnc.PutInt32(1)            // 1 partition
	pEnc.PutInt32(0)            // partition 0

	batchBytes := kafka.EncodeRecordBatchV2(0, []kafka.RecordItem{
		{Key: []byte("cpu"), Value: []byte(`{"usage": 45}`)},
	})
	pEnc.PutBytes(batchBytes)

	// Write frame to server
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(prodBuf.Len()))
	_, _ = conn.Write(lenBuf[:])
	_, _ = conn.Write(prodBuf.Bytes())

	// Server should NOT send any response for acks == 0
	_ = conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	var dummy [1]byte
	n, readErr := conn.Read(dummy[:])
	if n > 0 {
		t.Errorf("expected 0 bytes returned for acks = 0, got %d bytes", n)
	}
	var netErr net.Error
	if readErr == nil || !errors.As(readErr, &netErr) || !netErr.Timeout() {
		t.Errorf("expected read timeout for acks = 0, got err: %v", readErr)
	}

	// 2. Verify record was still saved in the broker
	records, err := svc.Fetch(nil, "metrics", 0, 0, 10)
	if err != nil || len(records) != 1 {
		t.Fatalf("expected 1 record saved in topic, got len %d (err: %v)", len(records), err)
	}
	if string(records[0].Key) != "cpu" {
		t.Errorf("unexpected record key: %s", records[0].Key)
	}

	// 3. Verify connection remains healthy for subsequent requests
	reqBuf := new(bytes.Buffer)
	enc := kafka.NewEncoder(reqBuf)
	enc.PutInt16(kafka.ApiKeyApiVersions)
	enc.PutInt16(3)
	enc.PutInt32(12345) // correlation ID
	enc.PutString("client")

	binary.BigEndian.PutUint32(lenBuf[:], uint32(reqBuf.Len()))
	_, _ = conn.Write(lenBuf[:])
	_, _ = conn.Write(reqBuf.Bytes())

	var respLenBuf [4]byte
	_ = conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	if _, err := io.ReadFull(conn, respLenBuf[:]); err != nil {
		t.Fatalf("failed to read response length for follow-up request: %v", err)
	}
	respLen := int(binary.BigEndian.Uint32(respLenBuf[:]))
	respPayload := make([]byte, respLen)
	if _, err := io.ReadFull(conn, respPayload); err != nil {
		t.Fatalf("failed to read follow-up payload: %v", err)
	}

	dec := kafka.NewDecoder(respPayload)
	corrId, _ := dec.GetInt32()
	if corrId != 12345 {
		t.Errorf("expected correlation ID 12345, got %d", corrId)
	}
}

func TestKafkaServer_PortInUse(t *testing.T) {
	engine := storage.NewMemoryEngine(10)
	bus := broker.NewEventBus(10)
	svc := broker.NewService(engine, bus, 1)

	handler1 := kafka.NewHandler(svc, "127.0.0.1", 9092, nil)
	server1 := kafka.NewServer("127.0.0.1:0", handler1, nil)
	if err := server1.Start(); err != nil {
		t.Fatalf("failed to start server1: %v", err)
	}
	defer server1.Close()

	// Try starting a second server on the exact same port
	handler2 := kafka.NewHandler(svc, "127.0.0.1", 9092, nil)
	server2 := kafka.NewServer(server1.Addr(), handler2, nil)
	err := server2.Start()
	if err == nil {
		server2.Close()
		t.Fatalf("expected port in use error, got nil")
	}

	if !errors.Is(err, kafka.ErrPortInUse) {
		t.Errorf("expected error wrapping kafka.ErrPortInUse, got: %v", err)
	}
}

func TestKafkaServer_ProduceStorageErrorOnWALCap(t *testing.T) {
	tempDir := t.TempDir()
	// Set very small cap (50 bytes)
	engine, err := storage.NewWALEngineWithLimit(tempDir, 50)
	if err != nil {
		t.Fatalf("failed to create wal engine: %v", err)
	}
	defer engine.Close()

	bus := broker.NewEventBus(10)
	svc := broker.NewService(engine, bus, 1)

	handler := kafka.NewHandler(svc, "127.0.0.1", 9092, nil)
	server := kafka.NewServer("127.0.0.1:0", handler, nil)
	if err := server.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer server.Close()

	conn, err := net.DialTimeout("tcp", server.Addr(), 2*time.Second)
	if err != nil {
		t.Fatalf("failed to dial server: %v", err)
	}
	defer conn.Close()

	// Send Produce request exceeding 50 bytes WAL cap
	prodBuf := new(bytes.Buffer)
	pEnc := kafka.NewEncoder(prodBuf)
	pEnc.PutInt16(kafka.ApiKeyProduce)
	pEnc.PutInt16(3)    // version 3
	pEnc.PutInt32(7777) // correlation ID
	pEnc.PutString("storage-err-client")

	pEnc.PutNullableString(nil) // transactional_id
	pEnc.PutInt16(1)            // acks = 1
	pEnc.PutInt32(1000)         // timeout_ms

	pEnc.PutInt32(1)            // 1 topic
	pEnc.PutString("capped-topic")
	pEnc.PutInt32(1)            // 1 partition
	pEnc.PutInt32(0)            // partition 0

	// 100 bytes of data to exceed 50 byte cap
	bigData := make([]byte, 100)
	batchBytes := kafka.EncodeRecordBatchV2(0, []kafka.RecordItem{
		{Key: []byte("k"), Value: bigData},
	})
	pEnc.PutBytes(batchBytes)

	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(prodBuf.Len()))
	_, _ = conn.Write(lenBuf[:])
	_, _ = conn.Write(prodBuf.Bytes())

	// Read response
	var respLenBuf [4]byte
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.ReadFull(conn, respLenBuf[:]); err != nil {
		t.Fatalf("failed to read response length: %v", err)
	}
	respLen := int(binary.BigEndian.Uint32(respLenBuf[:]))
	respPayload := make([]byte, respLen)
	if _, err := io.ReadFull(conn, respPayload); err != nil {
		t.Fatalf("failed to read response payload: %v", err)
	}

	dec := kafka.NewDecoder(respPayload)
	corrId, _ := dec.GetInt32()
	if corrId != 7777 {
		t.Errorf("expected correlation ID 7777, got %d", corrId)
	}

	// Read ProduceResponse body
	topicsCount, _ := dec.GetInt32()
	if topicsCount != 1 {
		t.Fatalf("expected 1 topic response, got %d", topicsCount)
	}
	topicName, _ := dec.GetString()
	if topicName != "capped-topic" {
		t.Errorf("expected topic capped-topic, got %s", topicName)
	}
	partsCount, _ := dec.GetInt32()
	if partsCount != 1 {
		t.Fatalf("expected 1 partition response, got %d", partsCount)
	}
	partId, _ := dec.GetInt32()
	if partId != 0 {
		t.Errorf("expected partition 0, got %d", partId)
	}
	errCode, _ := dec.GetInt16()
	if errCode != kafka.ErrKafkaStorageError {
		t.Fatalf("expected ErrKafkaStorageError (56), got %d", errCode)
	}
}
