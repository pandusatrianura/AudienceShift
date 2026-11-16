# AudienceShift 2.0: The Sovereign Webhook Platform

## Problem Statement
> **How Might We** transform AudienceShift from a personal reverse-tunnel and replay inspector into an indispensable, sovereign webhook platform that eliminates team coordination friction, automates developer type safety, and prevents lost webhooks—all within a single, zero-dependency Go binary?

---

## Recommended Direction: "The Sovereign Webhook Gateway & Developer Engine"

AudienceShift 2.0 expands the product into an all-in-one webhook engineering powerhouse designed for solo developers and high-growth engineering teams alike, without compromising the core ethos: **Pure Go, Zero External Databases, Single Static Binary, and Zero-NPM**.

### 1. Multi-Tenant Webhook Fanout (Team Broadcast)
In modern development teams, multiple engineers (e.g. backend, mobile, frontend, and staging environments) often need to observe or consume the exact same live webhook event (e.g. Stripe checkout completion or GitHub push event). Currently, SaaS dashboards only allow 1 webhook destination URL per event.
* **Mechanism:** The AudienceShift VPS Relay Server supports multi-subscriber connections to a single shared tunnel identifier (e.g. `team-payment.tunnel.company.com`).
* **Delivery:** When a webhook arrives at the relay ingress, it is concurrently broadcast across all active multiplexed Yamux client sessions in real time.
* **Non-Blocking:** A slow or stalled client does not block webhook delivery to other active team members.

### 2. Auto-Persistent Embedded Dead-Letter Queue (DLQ)
Webhooks are asynchronous and volatile. If a developer's local server crashes, laptop sleeps, or staging pod redeploys for 20 minutes, third-party webhooks can fail and become lost forever.
* **Automatic Local Storage:** AudienceShift automatically creates an embedded pure-Go persistent database (`.audienceshift.db`) in the project directory using a zero-CGO B-tree engine (such as `bbolt`).
* **DLQ & Bulk Replay:** Webhooks that fail local delivery (HTTP 500, 502, or connection refused) are routed into a local Dead-Letter Queue. When the backend service recovers, developers can trigger a 1-click **"⚡ Replay All Failed"** with configurable rate limits (e.g. 10 req/s) to avoid overwhelming the restored service.
* **Opt-Out & Customization:** Developers can supply `--memory-only` for transient sessions or `--db <custom-path>` to specify a designated database file.

### 3. 1-Click Go Struct & TypeScript Type Synthesizer
Hand-crafting DTO structs, TypeScript interfaces, and validation schemas from third-party vendor documentation is repetitive, error-prone, and slow.
* **Schema Inference:** AudienceShift sniffs real JSON payloads as they arrive and automatically infers property types, nullability, nested arrays, and objects.
* **Instant Export:**
  * **Go:** Generates clean Go structs with appropriate `json:"..."` tags.
  * **TypeScript:** Generates TypeScript interfaces (`.d.ts`) or ready-to-use Zod schemas (`z.object({...})`).
* **Ergonomics:** Accessible via hotkey `g` in the Terminal TUI and the dedicated **"Types"** tab in Web Studio.

---

## Key Assumptions to Validate

- [ ] **Assumption 1 (Team Fanout Usability):** Engineering teams prefer connecting multiple clients to a single shared tunnel endpoint over configuring separate webhook URLs for each engineer in provider dashboards.
  - *Validation Strategy:* Benchmark team workflow with 3 simultaneous client subscribers receiving simulated Stripe and Xendit webhooks.
- [ ] **Assumption 2 (Zero-CGO Persistence Overhead):** Pure-Go embedded storage (`bbolt`) maintains sub-millisecond write latencies and negligible memory overhead on $3–$5 VPS instances and modest developer laptops.
  - *Validation Strategy:* Concurrency stress test dispatching 5,000 requests to `.audienceshift.db` within a 512MB RAM environment.
- [ ] **Assumption 3 (Type Generation Fidelity):** Auto-inferred TypeScript interfaces and Go structs correctly resolve ambiguity (e.g. `int64` vs `float64`, optional fields, empty arrays) without requiring extensive developer manual correction.
  - *Validation Strategy:* Verify type synthesis against sample payloads from all 9 catalog providers (Stripe, Midtrans, GitHub, Xendit, Slack, Shopify, GitLab, Discord, Twilio).

---

## MVP Scope

### Phase 1: In Scope (What We Are Building)
1. **Embedded Persistence & DLQ:**
   - Auto-initialization of `.audienceshift.db` using pure-Go embedded storage (`bbolt`).
   - Persistent storage for event history and failed webhook DLQ.
   - CLI flags: `--memory-only` (opt-out) and `--db <path>`.
   - DLQ resend engine with controlled pacing.
2. **1-Click Type Synthesizer:**
   - Ingestion of arbitrary JSON payloads into inferred Go structs and TypeScript interfaces / Zod schemas.
   - Hotkey `g` in Terminal TUI and "Types" tab in Web Studio with 1-click clipboard copy.
3. **Multi-Client Webhook Fanout:**
   - Server relay support for multiple concurrent client listeners per tunnel subdomain with `fanout: true`.
   - Non-blocking broadcast across active client sessions.

---

## Not Doing (and Why)

- ❌ **External Database Infrastructure (PostgreSQL, Redis, RabbitMQ, Kafka):**
  - *Reason:* Compromises AudienceShift's core philosophy of being a zero-dependency, single-binary application. Everything must remain embedded and self-contained.
- ❌ **Hosted Multi-Tenant Cloud Platform (SaaS):**
  - *Reason:* AudienceShift is sovereign software. Cloud-hosting introduces billing, multi-tenant compliance, and privacy risks that divert focus from developer ownership.
- ❌ **Server-Side Payload Transformation Scripts (V8 / Lua Sandbox):**
  - *Reason:* Running untrusted user scripts on the VPS relay introduces Remote Code Execution (RCE) and sandbox escape vectors. Transformations belong safely on the developer's client machine.
- ❌ **Generic TCP/UDP Tunneling:**
  - *Reason:* AudienceShift is intentionally specialized for HTTP/WebSocket webhook inspection and replay. General-purpose TCP proxying dilutes product focus and introduces non-HTTP security abuse.

---

## Decisions & Open Questions

### Resolved Decisions:
- **Default Storage Policy:** Automatic persistent local database file `.audienceshift.db` created in the current working directory, with `--memory-only` available for ephemeral CI/CD pipelines or temporary testing.
- **Repository Archiving:** Approved and saved to `docs/ideas/sovereign-webhook-platform.md`.

### Technical Roadmap & Next Steps:
1. Define technical architecture and package structure for `internal/features/storage` (pure-Go embedded bbolt driver) and `internal/features/synthesizer` (Go & TS type generator).
2. Write formal implementation plan (`/superpowers-plan`) with phased verification commands.
