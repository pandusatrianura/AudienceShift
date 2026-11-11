# ⚡ PocketQueue

> **The SQLite of Message Brokers.**  
> Ultra-lightweight (<5 MB single binary, <8 MB RAM), zero-dependency local message broker with an interactive Terminal UI (TUI), speaking drop-in **Kafka Wire Protocol** (`:9092`) and **Redis Streams RESP** (`:6379`).

[![Go Report Card](https://goreportcard.com/badge/github.com/pandusatrianura/pocketqueue)](https://goreportcard.com/report/github.com/pandusatrianura/pocketqueue)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Binary Size](https://img.shields.io/badge/Binary%20Size-4.96%20MB-success)](scripts/audit.ps1)
[![RAM Usage](https://img.shields.io/badge/Idle%20RAM-7.7%20MB-success)](scripts/audit.ps1)
[![Boot Time](https://img.shields.io/badge/Cold%20Start-2.1%20ms-success)](scripts/audit.ps1)

---

## 💡 Why PocketQueue?

Running Kafka, Zookeeper, or RabbitMQ on local dev machines consumes **1.5–3 GB of RAM**, burns CPU, drains battery, and requires complex Docker Compose setups just to test simple event-driven microservices.

**PocketQueue solves this with a 10x simpler model:**
- **Single executable binary (< 5 MB)**: No JVM, no Docker, no Erlang runtime, zero external dependencies (`CGO_ENABLED=0`).
- **Sub-8 MB RAM Footprint**: Run it in the background or right in your terminal with zero memory guilt.
- **Microsecond Cold Start (< 3 ms)**: Ready to accept connections almost instantaneously.
- **Drop-in Wire Compatibility**: Works with standard Kafka SDKs (port `9092`) and Redis Streams clients (port `6379`) without modifying your business code.
- **Interactive Terminal UI (TUI)**: Real-time event streaming, syntax-highlighted JSON inspector, and inline **Dead-Letter Queue (DLQ) Replay Studio** powered by Charm `bubbletea`.
- **Flexible Durability**: Runs in pure memory by default (zero disk cruft) or with persistent append-only Write-Ahead Logs via `--data-dir`.

---

## 📊 Benchmark & Comparison

| Metric / Feature | Kafka (Docker / KRaft) | Redpanda (Docker) | RabbitMQ | **PocketQueue** |
|---|---|---|---|---|
| **Memory (Idle)** | 1,200 – 2,000 MB | 500 – 1,000 MB | 250 – 500 MB | **< 8 MB** ⚡ |
| **Startup Latency** | 30 – 60 seconds | 5 – 10 seconds | 10 – 20 seconds | **~2 milliseconds** 🚀 |
| **Binary / Image Size** | > 800 MB (JVM + Image) | > 250 MB | > 300 MB | **< 5 MB** (Single Binary) |
| **External Dependencies** | Docker, JVM, KRaft | Docker, C++ runtimes | Docker, Erlang VM | **None** (Pure Go) |
| **Built-in TUI & Inspector** | ❌ (Requires AKHQ/Kafka UI) | ❌ (Requires Redpanda Console) | ❌ (Web UI only) | **✅ Native TUI (Charm)** |
| **In-Terminal DLQ Replay** | ❌ | ❌ | ❌ | **✅ Built-in (`r`/`e`)** |
| **Dual Wire Protocol** | Kafka only | Kafka only | AMQP / MQTT | **✅ Kafka + Redis Streams** |

---

## 🚀 Quick Start

### 1. Installation

#### Build from source:
```bash
git clone https://github.com/pandusatrianura/pocketqueue.git
cd pocketqueue
go build -ldflags="-s -w" -o bin/pocketqueue ./cmd/pocketqueue
```

### 2. Launch with Interactive TUI (Default)
Run PocketQueue directly in your terminal:
```bash
./bin/pocketqueue
```
*To launch with live simulated demo events streaming right away:*
```bash
./bin/pocketqueue --demo
```

### 3. Launch in Headless Daemon Mode (CI / Background)
```bash
./bin/pocketqueue --headless --port 9092 --redis-port 6379
```

---

## 📖 How to Use PocketQueue

### Workflow 1: Inner-Loop Development & Real-Time Inspection
1. **Start PocketQueue** in a dedicated terminal pane:
   ```bash
   ./bin/pocketqueue
   ```
2. **Point your application** to `localhost:9092` (Kafka) or `localhost:6379` (Redis):
   ```bash
   export KAFKA_BROKERS="localhost:9092"
   go run ./cmd/order-service
   ```
3. Events will stream into the **Live Stream** table in real time showing timestamp, topic name, partition, monotonic offset, key, and payload preview.
4. Press `Enter` to open the **Payload Inspector** and view formatted JSON, headers, and metadata.

### Workflow 2: Dead-Letter Queue (DLQ) Triage & Replay Studio
When a downstream consumer fails or a message is routed to DLQ:
1. In the TUI, navigate to the failed record marked with `🚨 <topic>.dlq`.
2. Press `r` or `e` to open the **Replay Studio**.
3. Fix the JSON payload directly inside the terminal (e.g. fix an invalid SKU or status field).
4. Press `Ctrl+S`: PocketQueue immediately republishes the repaired message back into the target topic, triggering your consumer again without having to restart your whole workflow.

### Workflow 3: Ad-Hoc CLI Publishing (No Client Code Needed)
Send mock events or trigger workers directly from your command line:
```bash
# Publish a JSON event to a topic
./bin/pocketqueue -topic orders -key ord-101 -value '{"customer": "diana", "amount": 149.99}'

# Target a custom broker port
./bin/pocketqueue -broker 127.0.0.1:9092 -topic notifications -value '{"alert": "high_cpu"}'
```

### Workflow 4: Dual-Protocol Interoperability
PocketQueue allows Kafka and Redis clients to communicate through the same broker:
- Microservice A publishes to Redis Streams via `redis-cli XADD orders * item "keyboard"`
- Microservice B consumes from Kafka topic `orders` at `localhost:9092`
- All events appear unified inside the same PocketQueue TUI!

---

## 🔌 Verified Client SDK Compatibility

PocketQueue is continuously tested against official production client libraries to guarantee drop-in compatibility with real microservices:

| Ecosystem | Client Library | Protocol | Tested Capabilities | Status |
|---|---|---|---|:---:|
| **Go** | [`IBM/sarama`](https://github.com/IBM/sarama) | Kafka `:9092` | `SyncProducer`, `Consumer`, `ConsumerGroup`, RecordBatch v2 | ✅ Verified |
| **Go** | [`redis/go-redis/v9`](https://github.com/redis/go-redis/v9) | Redis Streams `:6379` | `Ping`, `XAdd`, `XLen`, `XRange`, `XRead` (with `Block`) | ✅ Verified |

#### Go Example: Producing & Consuming with `IBM/sarama`
```go
config := sarama.NewConfig()
config.Version = sarama.V2_1_0_0
config.Producer.Return.Successes = true

producer, _ := sarama.NewSyncProducer([]string{"127.0.0.1:9092"}, config)
defer producer.Close()

// Publish message
producer.SendMessage(&sarama.ProducerMessage{
    Topic: "orders",
    Key:   sarama.StringEncoder("ord-001"),
    Value: sarama.StringEncoder(`{"status": "paid"}`),
})
```

#### Go Example: Appending & Blocking Read with `go-redis`
```go
rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
defer rdb.Close()

// Append to stream
rdb.XAdd(ctx, &redis.XAddArgs{
    Stream: "telemetry",
    Values: map[string]interface{}{"temp": "24.5"},
})

// Blocking read
entries, _ := rdb.XRead(ctx, &redis.XReadArgs{
    Streams: []string{"telemetry", "$"},
    Block:   5 * time.Second,
}).Result()
```

---

## ⚙️ How to Configure for Running Well

PocketQueue requires zero configuration by default, but provides fine-grained controls for different environments.

### CLI Flag Reference

| Flag | Default | Description | Recommended Usage |
|---|---|---|---|
| `--host` | `127.0.0.1` | Binding network host | Use `127.0.0.1` for local safety; use `0.0.0.0` inside Docker or LAN |
| `--port` | `9092` | Kafka wire protocol port | Standard Kafka port (`0` to disable Kafka) |
| `--redis-port` | `6379` | Redis Streams RESP port | Standard Redis port (`0` to disable Redis) |
| `--data-dir` | `""` (Empty) | Directory for WAL disk persistence | Set to `./data` to retain messages across reboots |
| `--headless` | `false` | Run as background daemon | Use in CI/CD, scripts, and Docker containers |
| `--demo` | `false` | Emit continuous realistic traffic | Perfect for UI demos, onboarding, and testing |
| `--max-messages` | `5000` | In-memory buffer size per partition | Increase to `20000` for high-throughput stress tests |
| `--log-level` | `info` | Log verbosity (`debug`, `info`, `warn`, `error`) | Use `debug` when diagnosing client handshakes |
| `--json-log` | `false` | Format logs as structured JSON | Recommended for CI/CD and container logs |

---

### Configuration Recipes

#### Recipe 1: Ephemeral Local Testing (Fastest, Zero Disk Cruft)
*Best for: Daily coding, unit tests, fast feedback loops.*
```bash
./bin/pocketqueue
```
- In-memory ring buffer keeps RAM below 8 MB.
- Microsecond message delivery.
- When you quit (`q`), all temporary state is discarded cleanly.

#### Recipe 2: Persistent State Across Restarts
*Best for: Testing app restarts, database syncs, or long-running local environments.*
```bash
./bin/pocketqueue --data-dir ./my-data
```
- Creates append-only Write-Ahead Logs (`.wal`) inside `./my-data`.
- Automatically restores all topics, messages, and offsets when restarted.

#### Recipe 3: Headless CI / CD Pipeline
*Best for: GitHub Actions, GitLab CI, or automated test runners.*
```bash
./bin/pocketqueue --headless --port 9092 --redis-port 0 &
```
- Starts silently in the background with zero TUI rendering overhead.
- Shuts down cleanly with `SIGTERM` / `SIGINT`.

#### Recipe 4: Docker Compose Drop-in Replacement
If your project uses Docker Compose, replace your heavy 1.5 GB Kafka container with PocketQueue:

```yaml
version: '3.8'

services:
  # Replace confluentinc/cp-kafka with lightweight PocketQueue:
  pocketqueue:
    image: golang:1.24-alpine
    working_dir: /app
    volumes:
      - ./:/app
    command: ./bin/pocketqueue --headless --host 0.0.0.0 --port 9092 --redis-port 6379
    ports:
      - "9092:9092"
      - "6379:6379"
```

---

### 💡 Best Practices & Performance Tips

1. **Port Conflicts**:
   - If port `9092` is already held by an old Docker Kafka container, either stop it (`docker stop <id>`) or run PocketQueue on a custom port:
     ```bash
     ./bin/pocketqueue --port 9094
     ```
2. **Terminal Rendering on Windows**:
   - PocketQueue uses modern ANSI escape codes. For the best visual experience, run in **Windows Terminal**, **VS Code Integrated Terminal**, or **Alacritty / WezTerm** with UTF-8 encoding enabled.
3. **High-Throughput Tuning**:
   - If your benchmark or producer emits > 5,000 messages/sec, run PocketQueue in `--headless` mode to dedicate 100% of CPU time to network I/O and storage rather than terminal UI rendering:
     ```bash
     ./bin/pocketqueue --headless --max-messages 20000
     ```

---

## ⌨️ Interactive Terminal UI (TUI) Keybindings

| Key | Action |
|---|---|
| `Tab` | Switch between **Live Stream** table and **Topics & DLQ** statistics |
| `↑` / `↓` (`k` / `j`) | Navigate message history |
| `Enter` | Open **Payload Inspector Modal** (auto-formats JSON and inspects headers) |
| `r` / `e` | Open **Replay Studio Modal** to edit JSON payload in-place |
| `Ctrl+S` | Submit and republish replayed message into topic |
| `Esc` | Close modal / return to stream |
| `q` / `Ctrl+C` | Graceful shutdown |

---

## 🔌 Client Usage Examples

### Connect via Kafka Clients (`localhost:9092`)

Any standard Kafka client connects out of the box with zero code changes:

#### Go (`franz-go` or `sarama`):
```go
client, err := kgo.NewClient(
    kgo.SeedBrokers("localhost:9092"),
    kgo.DefaultProduceTopic("orders"),
)
```

#### Python (`kafka-python` or `confluent-kafka`):
```python
from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
producer.send('orders', b'{"order_id": "ord-101", "amount": 99.5}')
```

#### Node.js (`kafkajs`):
```javascript
const { Kafka } = require('kafkajs');
const kafka = new Kafka({ brokers: ['localhost:9092'] });
const producer = kafka.producer();
await producer.send({
  topic: 'orders',
  messages: [{ key: 'ord-101', value: '{"status": "paid"}' }],
});
```

---

### Connect via Redis Clients (`localhost:6379`)

PocketQueue speaks native RESP for Redis Streams:

#### CLI (`redis-cli`):
```bash
# Add to stream
redis-cli XADD orders * customer "alice" amount "49.99"

# Inspect stream length
redis-cli XLEN orders

# Read from stream
redis-cli XRANGE orders - +
```

#### Python (`redis-py`):
```python
import redis
r = redis.Redis(host='localhost', port=6379)
r.xadd("orders", {"customer": "alice", "amount": "49.99"})
```

---

## 🏗️ Architecture & Extensibility

```
┌────────────────────────────────────────────────────────┐
│                   PocketQueue Binary                   │
├──────────────────────────┬─────────────────────────────┤
│   Kafka Protocol (:9092) │ Redis Streams RESP (:6379)  │
│  (ApiVersions, Metadata, │ (XADD, XREAD, XRANGE, XLEN) │
│    Produce, Fetch, DLQ)  │                             │
├──────────────────────────┴─────────────────────────────┤
│                 Broker Domain Service                  │
│       (Routing, Offsets, DLQ Tagging, Replay)          │
├──────────────────────────┬─────────────────────────────┤
│     EventBus (TUI)       │      Storage Engine         │
│  (Non-blocking channel)  │  (In-Memory / Append WAL)   │
└──────────────────────────┴─────────────────────────────┘
```

PocketQueue is designed following strict **I/O Isolation** and the vertical slice architecture:
- `internal/features/storage`: `storage.Engine` interface allows pluggable backends (In-Memory, WAL, or custom embedded DBs like Pebble/SQLite).
- `internal/features/broker`: Domain service managing topics, consumer groups, partition watermarks, and DLQs.
- `internal/features/protocol/kafka`: Zero-allocation binary wire protocol parser with CRC32-C Castagnoli checksums.
- `internal/features/protocol/redis`: RESP v2/v3 protocol engine for Redis Streams clients.
- `internal/features/tui`: Virtualized viewport and UI models with Charm Bubbletea.

---

## 🛡️ Engineering Doctrine & Triage Framework

PocketQueue enforces the **3-Tier Pre-Implementation Triage & Developer Tooling Integrity Framework** before adding any feature or refactoring code:
- **Hukum Rework Multiplier**: 15 minutes of structured failure-mode analysis saves 2–3 hours of downstream debugging.
- **Categorization Gate**:
  - 🔴 **Must Handle Now**: Non-negotiable blockers (TCP framing bounds, port clash guidance, graceful `file.Sync()` on SIGINT/SIGTERM).
  - 🟡 **Defensive Default**: Pragmatic fallbacks (dynamic partition cap of 16, WAL 100 MB file cap, 30 FPS UI render throttling).
  - 🟢 **Explicitly Ignored**: Consciously rejected complexities (no SASL/TLS, no Raft clustering, no 2-PC transactions) to keep the binary **< 15 MB** and RAM **< 25 MB**.

📖 Read the full architecture document: [docs/architecture/triage-framework.md](docs/architecture/triage-framework.md)

---

## 🛠️ Contributing

Contributions are welcome! Adding new capabilities is designed to be frictionless:
- **New Storage Engines**: Implement the `storage.Engine` interface (`Append`, `ReadFrom`, `GetWatermarks`, `ListTopics`).
- **New Protocols**: Implement a protocol listener calling `broker.Service`.
- **Payload Decoders**: Add custom decoders for Protobuf or Avro schema visualization in the TUI inspector.

### Running Tests:
```bash
go test -v ./...
```

### Running Resource Audit:
```powershell
powershell -ExecutionPolicy Bypass -File ./scripts/audit.ps1
```

---

## 📄 License

MIT License © 2026 Pandu Satrianura.
