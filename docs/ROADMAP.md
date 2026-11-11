# 🗺️ PocketQueue Milestone Roadmap

This roadmap outlines the evolution of PocketQueue while adhering strictly to the **10x Simpler & Lighter** constitution (< 15 MB binary, < 25 MB RAM idle, zero CGo).

---

## 🎯 Completed Milestones

### Milestone 1: Core Dual-Protocol Broker & TUI Studio ✅
- Single-binary pure Go architecture (`CGO_ENABLED=0`).
- Kafka Wire Protocol (`:9092`): ApiVersions, Metadata, Produce, Fetch.
- Redis Streams RESP (`:6379`): XADD, XREAD, XRANGE, XLEN.
- Pluggable Storage: In-Memory ring buffer & Write-Ahead Log (WAL).
- Charm Bubbletea TUI: Live stream viewport, JSON payload inspector, in-terminal DLQ Replay Studio (`r`/`e`).

### Milestone 2: Protocol Hardening & Resource Safeguards ✅
- **Pass 1: Resiliency & Protocol Foundations**
  - TCP framing length validation & OOM protection (16 MB cap).
  - Dynamic partition auto-expansion and non-negative partition guards.
  - Kafka consumer group protocol handshake stubs (`JoinGroup`, `SyncGroup`, `Heartbeat`, `LeaveGroup`).
  - Redis Streams `XREAD BLOCK` timeout (`*-1\r\n`) and non-blocking event-driven wakeups.
  - Bounded ring-buffer eviction clamping and corrupt trailing WAL frame auto-recovery.
  - TUI terminal resize protection (<40x10) and binary payload hex inspector.
- **Pass 2: Lifecycle & Resource Boundaries**
  - Kafka `acks = 0` fire-and-forget TCP socket preservation.
  - Cross-platform port collision fail-fast with guidance banner (`ErrPortInUse`).
  - Graceful `SIGINT`/`SIGTERM` OS signal shutdown with WAL `file.Sync()`.
  - Dynamic partition allocation cap (`MaxPartitionsPerTopic = 16`).
  - WAL file size cap safeguard (`100 MB` per partition).
  - Redis `QUIT` connection teardown and TUI ingress burst throttling ($\le 30$ FPS).
- **Architecture Doctrine**
  - Codified the **3-Tier Pre-Implementation Triage Framework** in `docs/architecture/triage-framework.md`.

### Milestone 3: Real-World Client SDK Compatibility Matrix (Go Ecosystem)
*Objective: Guarantee out-of-the-box drop-in compatibility with official production client libraries.*
- **Official Go Kafka Client (`github.com/IBM/sarama`)**:
  - `sarama.NewSyncProducer`: Verified message production, topic creation, partition routing, and monotonic base offsets.
  - `sarama.NewConsumer`: Verified partition subscription, API version negotiation, watermark resolution, and FIFO consumption.
  - `sarama.NewConsumerGroup`: Verified high-level consumer group rebalance flow (`FindCoordinator`, `JoinGroup`, `SyncGroup`, `Heartbeat`, `OffsetFetch`, `OffsetCommit`).
- **Official Go Redis Client (`github.com/redis/go-redis/v9`)**:
  - `rdb.Ping()`: Verified connection handshake and `HELLO` RESP protocol negotiation.
  - `rdb.XAdd()` & `rdb.XLen()` & `rdb.XRange()`: Verified standard Redis Streams ingestion, stream length query, and ID range retrieval.
  - `rdb.XRead()` with `Block`: Verified blocking read timeouts (`redis.Nil`) and event-driven live pub/sub wakeups.
- **Zero Binary Bloat Guarantee**:
  - All SDK dependencies are strictly test-scoped in `test/compatibility/` — binary size remains **5.02 MB** (budget < 15.0 MB).

---

## 🚀 Upcoming Milestones

### Milestone 3b: Multi-Language Client SDK Compatibility Test Matrix
*Objective: Expand client verification matrix across additional language runtimes.*
- **Python Ecosystem**:
  - `redis-py` (Python Redis Streams client)
  - `kafka-python` / `confluent-kafka`
- **Node.js / TypeScript Ecosystem**:
  - `ioredis` (Standard Node Redis client)
  - `kafkajs` (Popular Node Kafka library)

### Milestone 4: Multi-Arch Binary Packaging & Distribution
*Objective: Instant zero-install usage for developers across macOS, Linux, and Windows.*
- GitHub Releases automated workflow via GoReleaser.
- Stripped binaries for `darwin/arm64` (Apple Silicon), `darwin/amd64`, `linux/amd64`, `linux/arm64`, and `windows/amd64`.
- Homebrew formula (`brew install pocketqueue`) and Scoop manifest for Windows.

---

## 🛡️ Boundary Invariants (Always Enforced)
1. **Binary Size Ceiling**: `< 15.0 MB` (Current: `5.02 MB`).
2. **Idle Memory Ceiling**: `< 25.0 MB` (Current: `7.75 MB`).
3. **Cold Boot Ceiling**: `< 20.0 ms` (Current: `1.67 ms`).
4. **Zero External Runtime Dependencies**: Pure Go, zero CGo, zero JVM, zero Docker required for operation.

