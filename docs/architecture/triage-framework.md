# 🛡️ 3-Tier Pre-Implementation Triage & Developer Tooling Integrity Framework

> **Doctrine:** *"15 minutes of structured triage before writing code eliminates 2–3 hours of trial-and-error, prevents architectural rework, and guarantees local developer tooling integrity."*

---

## 1. Context & Motivation

PocketQueue is engineered to be **10x simpler and lighter** than industrial brokers (< 15 MB binary, < 25 MB RAM idle, zero runtime dependencies). Achieving this balance without sacrificing stability requires a disciplined engineering doctrine. 

When building developer tooling, jumping directly into implementation without mapping potential failure modes leads to two disastrous extremes:
1. **Cowboy Coding (Reckless Speed):** Developers or AI agents rush into implementation. Everything seems fast for the first hour, but the subsequent days are spent debugging desynchronized socket streams, goroutine leaks, corrupted page caches, and edge-case race conditions.
2. **Analysis Paralysis (Over-Engineering):** Engineers over-analyze every theoretical distributed systems failure (e.g., Paxos split-brain, multi-DC replication, enterprise SASL/SCRAM, 2-phase commit), ballooning binary size and complexity for a tool whose primary domain is the local laptop.

The **3-Tier Pre-Implementation Triage Framework** establishes a middle ground: a timeboxed, rigorous pre-flight gate that categorizes every potential issue *before a single line of production code is written*.

---

## 2. Hukum Rework Multiplier (Biaya Perbaikan)

Modifying system contracts, network framing, or concurrency boundaries *after* writing code and unit tests costs **5x to 10x more time and effort** than identifying them during planning.

### Real-World Case Study: Kafka `acks = 0` (Fire-and-Forget)
- **The Issue:** Many telemetry producers, log forwarders, and benchmark tools send Kafka Produce requests with `acks = 0` (expecting zero TCP response bytes).
- **The Rework Multiplier:** If we had implemented Produce assuming a response was always required, clients sending `acks = 0` would receive unsolicited TCP frames. This desynchronizes the client's internal correlation ID queue, causing cryptic client-side timeouts and hangs. Fixing this post-implementation would require refactoring the entire TCP connection loop and protocol handler.
- **The Pre-Flight Fix:** By identifying this edge case in pre-implementation triage, we placed a clean guardrail (`if req.Acks == 0 { return nil, nil }`) and skipped writing response frames in `server.go` on day one—requiring zero rework and zero architectural debt.

---

## 3. The 3-Tier Categorization Model

Before implementing any milestone, feature, or refactoring pass, all potential issues, edge cases, and gaps must be categorized into exactly three tiers:

```
┌────────────────────────────────────────────────────────────────────────┐
│                   PRE-IMPLEMENTATION TRIAGE FILTER                     │
├──────────────────────────┬─────────────────────────────┬───────────────┤
│  🔴 MUST HANDLE NOW      │   🟡 DEFENSIVE DEFAULT      │ 🟢 EXPLICITLY │
│       (Blockers)         │        (Edge Cases)         │    IGNORED    │
├──────────────────────────┼─────────────────────────────┼───────────────┤
│ • Socket desync          │ • Bounded ring buffers      │ • Multi-node  │
│ • Data corruption        │ • Dynamic partition caps    │ • SASL / TLS  │
│ • OOM attack vectors     │ • WAL file size caps        │ • Distributed │
│ • Port bind collisions   │ • TCP idle zombie cleanup   │   2PC trans.  │
│ • Ungraceful termination │ • TUI ingress throttling    │ • Custom RDB  │
└──────────────────────────┴─────────────────────────────┴───────────────┘
```

### 🔴 Must Handle Now (Blockers)
*Non-negotiable issues that, if ignored, corrupt system integrity, cause protocol desynchronization, or result in process crashes.*
- **Action:** Must be designed and enforced with explicit guardrails and unit tests in the initial implementation.
- **Examples in PocketQueue:**
  - **TCP Framing & OOM Guardrail:** Capping incoming frames to 16 MB so malicious or corrupted length headers cannot allocate gigabytes of RAM.
  - **Port Collision Fail-Fast:** Detecting `EADDRINUSE` / `WSAEADDRINUSE` at boot and exiting with clear human-friendly instructions instead of raw OS panics.
  - **Graceful OS Signal Shutdown:** Intercepting `SIGINT` / `SIGTERM` and executing `file.Sync()` on WAL partition descriptors so OS page caches are flushed before process exit.

### 🟡 Defensive Default (Edge Cases)
*Unlikely or abusive edge cases that can be safely neutralized with simple, bounded fallbacks without inventing complex architectures.*
- **Action:** Apply conservative limits, clamp values, set timeouts, or return safe defaults (KISS). Do not create complex subsystems.
- **Examples in PocketQueue:**
  - **Dynamic Partition Allocation Cap:** Cap partitions to `MaxPartitionsPerTopic = 16` per topic to prevent rogue loops from exhausting OS file descriptors.
  - **WAL File Size Safeguard:** Set a default 100 MB per partition cap (`DefaultMaxWALFileSize`) to prevent disk fill-up during accidental infinite benchmarks.
  - **TUI Ingress Throttling:** Throttle UI table renders to $\le 30$ FPS (batching 25 items) when message ingress exceeds 10,000 msg/sec, eliminating UI CPU churn while preserving lossless storage.
  - **Redis Idle Zombie Connection:** Detect `QUIT` or `io.EOF` and cleanly close connections, freeing socket goroutines.

### 🟢 Explicitly Ignored (Out of Scope)
*Features and complexities deliberately rejected to keep the binary `< 15 MB`, idle RAM `< 25 MB`, and codebase lightweight.*
- **Action:** Explicitly document the exclusion. Refuse requests to implement them unless the core mission of PocketQueue changes.
- **Exclusions in PocketQueue:**
  - **No SASL / SCRAM / TLS:** PocketQueue is an embedded localhost tool. SSL handshakes and auth add binary weight and runtime overhead with zero value for local development.
  - **No Distributed Clustering (Raft / KRaft):** Single binary, Broker ID `0`. No leader elections, no multi-node replication.
  - **No 2-Phase Commit Kafka Transactions:** At-least-once / at-most-once semantics are sufficient for local testing.
  - **No Separate Redis RDB / AOF Engine:** Redis Streams and Kafka share the exact same underlying unified storage engine (`MemoryEngine` or `WALEngine`).

---

## 4. Developer Tooling Integrity Principles

Developer tools operate under a higher bar of local environment etiquette:
1. **Never Brick the Host:** A developer broker must never exhaust file descriptors, fill up disk drives unannounced, or pin CPU cores when idle.
2. **Fail Fast with Guidance:** If port `9092` or `6379` is held by an existing Docker container, don't throw an unhandled panic. Suggest the exact command flag to fix it (`--port <alt>` or `--redis-port <alt>`).
3. **Clean Termination:** When a developer presses `Ctrl+C`, every open file descriptor must be flushed (`file.Sync()`) and closed.
4. **Instant Cold Boot:** Startup latency must remain under 20 ms (measured at ~2.1 ms) so test suites can start and stop the broker inside ephemeral unit test fixtures.

---

## 5. Pre-Implementation Quality Gate Checklist

Every developer or AI agent contributing to PocketQueue must complete this checklist before generating code:

- [ ] **Step 1: Failure Mode Brainstorm** — List all potential protocol violations, OS resource boundaries, and client SDK idiosyncrasies.
- [ ] **Step 2: 3-Tier Categorization** — Assign each item to 🔴 Must Handle Now, 🟡 Defensive Default, or 🟢 Explicitly Ignored.
- [ ] **Step 3: Sanity Check Against Budget** — Does any proposed solution introduce CGo, external network dependencies, or exceed memory/binary budgets?
- [ ] **Step 4: Formal Plan Review** — Document the plan in `artifacts/superpowers/plan.md` with explicit TDD verification commands.
- [ ] **Step 5: User Alignment** — Obtain explicit approval before modifying source code.

