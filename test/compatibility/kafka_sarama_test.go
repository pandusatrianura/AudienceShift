package compatibility_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/pandusatrianura/pocketqueue/internal/features/broker"
	"github.com/pandusatrianura/pocketqueue/internal/features/protocol/kafka"
	"github.com/pandusatrianura/pocketqueue/internal/features/storage"
)

// setupKafkaServer launches an in-memory PocketQueue Kafka server on an ephemeral TCP port.
func setupKafkaServer(t *testing.T) (*kafka.Server, *broker.Service, func()) {
	t.Helper()

	engine := storage.NewMemoryEngine(1000)
	bus := broker.NewEventBus(100)
	svc := broker.NewService(engine, bus, 1)

	// Bind to dynamic/ephemeral port :0
	handler := kafka.NewHandler(svc, "127.0.0.1", 0, nil)
	server := kafka.NewServer("127.0.0.1:0", handler, nil)

	if err := server.Start(); err != nil {
		t.Fatalf("failed to start embedded kafka server: %v", err)
	}

	cleanup := func() {
		_ = server.Close()
		_ = svc.Close()
	}

	return server, svc, cleanup
}

func TestSarama_SyncProducer_ProduceAndConsume(t *testing.T) {
	server, _, cleanup := setupKafkaServer(t)
	defer cleanup()

	// Configure Sarama client
	config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Retry.Max = 3
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	brokerAddr := server.Addr()

	// 1. Initialize SyncProducer
	producer, err := sarama.NewSyncProducer([]string{brokerAddr}, config)
	if err != nil {
		t.Fatalf("failed to create sarama sync producer: %v", err)
	}
	defer producer.Close()

	topic := "sdk-orders"
	expectedKey := "order-999"
	expectedVal := `{"sku": "LAPTOP-01", "qty": 1, "price": 1299.99}`

	// 2. Publish a message
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(expectedKey),
		Value: sarama.StringEncoder(expectedVal),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		t.Fatalf("sarama SendMessage failed: %v", err)
	}

	if partition != 0 {
		t.Errorf("expected partition 0, got %d", partition)
	}
	if offset != 0 {
		t.Errorf("expected offset 0, got %d", offset)
	}

	// 3. Initialize Consumer
	consumer, err := sarama.NewConsumer([]string{brokerAddr}, config)
	if err != nil {
		t.Fatalf("failed to create sarama consumer: %v", err)
	}
	defer consumer.Close()

	// 4. Consume from partition
	partConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		t.Fatalf("failed to consume partition: %v", err)
	}
	defer partConsumer.Close()

	// 5. Verify record received
	select {
	case received := <-partConsumer.Messages():
		if string(received.Key) != expectedKey {
			t.Errorf("expected key %s, got %s", expectedKey, string(received.Key))
		}
		if string(received.Value) != expectedVal {
			t.Errorf("expected value %s, got %s", expectedVal, string(received.Value))
		}
		if received.Partition != 0 {
			t.Errorf("expected partition 0, got %d", received.Partition)
		}
		if received.Offset != 0 {
			t.Errorf("expected offset 0, got %d", received.Offset)
		}
	case err := <-partConsumer.Errors():
		t.Fatalf("consumer error: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for message in consumer")
	}
}

func TestSarama_MultiMessage_MonotonicOffsets(t *testing.T) {
	server, _, cleanup := setupKafkaServer(t)
	defer cleanup()

	config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForLocal

	producer, err := sarama.NewSyncProducer([]string{server.Addr()}, config)
	if err != nil {
		t.Fatalf("failed to create sarama sync producer: %v", err)
	}
	defer producer.Close()

	topic := "sdk-telemetry"
	count := 5

	for i := 0; i < count; i++ {
		msg := &sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder(fmt.Sprintf("device-%d", i)),
			Value: sarama.StringEncoder(fmt.Sprintf(`{"seq": %d, "temp": 24.%d}`, i, i)),
		}
		p, off, err := producer.SendMessage(msg)
		if err != nil {
			t.Fatalf("failed to send message %d: %v", i, err)
		}
		if int64(i) != off {
			t.Errorf("expected monotonic offset %d, got %d", i, off)
		}
		if p != 0 {
			t.Errorf("expected partition 0, got %d", p)
		}
	}

	// Consume all 5 messages
	consumer, err := sarama.NewConsumer([]string{server.Addr()}, config)
	if err != nil {
		t.Fatalf("failed to create consumer: %v", err)
	}
	defer consumer.Close()

	partConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		t.Fatalf("failed to consume partition: %v", err)
	}
	defer partConsumer.Close()

	for i := 0; i < count; i++ {
		select {
		case m := <-partConsumer.Messages():
			expectedKey := fmt.Sprintf("device-%d", i)
			if string(m.Key) != expectedKey {
				t.Errorf("msg %d: expected key %s, got %s", i, expectedKey, string(m.Key))
			}
			if m.Offset != int64(i) {
				t.Errorf("msg %d: expected offset %d, got %d", i, i, m.Offset)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for message %d", i)
		}
	}
}

type testGroupConsumerHandler struct {
	ready    chan bool
	received chan *sarama.ConsumerMessage
	once     sync.Once
}

func (h *testGroupConsumerHandler) Setup(sarama.ConsumerGroupSession) error {
	h.once.Do(func() {
		close(h.ready)
	})
	return nil
}

func (h *testGroupConsumerHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *testGroupConsumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		select {
		case h.received <- msg:
		default:
		}
		session.MarkMessage(msg, "")
	}
	return nil
}

func TestSarama_ConsumerGroup_Lifecycle(t *testing.T) {
	server, _, cleanup := setupKafkaServer(t)
	defer cleanup()

	config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Group.Heartbeat.Interval = 100 * time.Millisecond
	config.Consumer.Group.Session.Timeout = 1 * time.Second

	// Produce a message first
	producer, err := sarama.NewSyncProducer([]string{server.Addr()}, config)
	if err != nil {
		t.Fatalf("failed to create sync producer: %v", err)
	}
	topic := "sdk-group-orders"
	_, _, err = producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder("grp-key"),
		Value: sarama.StringEncoder("grp-val"),
	})
	if err != nil {
		t.Fatalf("failed to send message: %v", err)
	}
	_ = producer.Close()

	// Start consumer group
	group, err := sarama.NewConsumerGroup([]string{server.Addr()}, "sdk-order-processors", config)
	if err != nil {
		t.Fatalf("failed to create consumer group: %v", err)
	}
	defer group.Close()

	handler := &testGroupConsumerHandler{
		ready:    make(chan bool),
		received: make(chan *sarama.ConsumerMessage, 10),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errChan := make(chan error, 10)
	go func() {
		for err := range group.Errors() {
			errChan <- err
		}
	}()

	go func() {
		for {
			if err := group.Consume(ctx, []string{topic}, handler); err != nil {
				errChan <- err
				if ctx.Err() != nil {
					return
				}
				return
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()

	// Wait for consumer group setup & message consumption
	select {
	case err := <-errChan:
		t.Fatalf("consumer group error: %v", err)
	case <-handler.ready:
		// Group joined and session established
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for consumer group session setup")
	}

	select {
	case msg := <-handler.received:
		if string(msg.Key) != "grp-key" || string(msg.Value) != "grp-val" {
			t.Errorf("unexpected message: key=%s val=%s", string(msg.Key), string(msg.Value))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for message in consumer group")
	}

	cancel()
}

