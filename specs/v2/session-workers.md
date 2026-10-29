# Session Workers Decision Record

## Status and scope

Status: accepted for a v1-only implementation. This document is normative for the v1 per-session worker divergence. Shared Session semantics remain governed by `specs/v2/session.md`; the reference commit below is read-only.

| Ref | SHA | Role |
| --- | --- | --- |
| `v1` | `fc21040893a2c53c13ec83cb5bacb8f139ca9fec` | Green baseline parent. |
| `rebaseline/green` | `a1f9457fa0c95a62c68a9c5daba482730f6f5f2d` | Current work base; 30/30 typecheck tasks pass. |
| `wip/parallel-refactor-checkpoint` | `b6ec8f1c6b35ac7d250c7f7c85a46bc37ba1702a` | Archived prior WIP; reapply source only. |
| `rebaseline/session-workers` | `58926fd8c2aad21c72b16588888c083768dd315d` | Earlier rebaseline; reference only. |
| upstream base | `upstream/v1` = `006d837c5022ea5278162307c04c78412cfae9f3` | The actual upstream base this v1 divergence builds on; read-only, not a merge target. |
| reference commit | `be242b5cd7097f327817c7e3252573a38489da32` (`be242b5cd7`) | Read-only upstream v1 reference; resolves in this clone and is an ancestor of HEAD and of the upstream base `006d837c50`, but is not a merged branch or merge target. |
| fetched upstream ref | `upstream/v2` = `4617210822bdbb31e5749751afbfb4584ccedac8` | The fetched upstream ref that is not the base being pinned, the reference commit, or a merge target. |

The actual upstream base is `upstream/v1` = `006d837c5022ea5278162307c04c78412cfae9f3`. The reference commit's 10-character prefix `be242b5cd7` resolves to `be242b5cd7097f327817c7e3252573a38489da32` in this clone and is an ancestor of HEAD and of the upstream base `006d837c50`; it is not a pending rebase target and not a merged branch. The fetched upstream ref this clone also has is `upstream/v2` = `4617210822bdbb31e5749751afbfb4584ccedac8`. Neither ref is a merge target. V1 diverges from the reference for two primary reasons: multicore CPU parallelism for Session execution, and crash containment per Session. Plugin isolation is secondary. Behavioral parity with the flag off, and with the reference semantics, is a constraint rather than a purpose.

The divergence has a kill criterion. Keep per-session workers only if the benchmark in D2 is met; otherwise switch to the in-process v2 model and delete the worker stack. D2 is the governing measurement, not a general preference for workers.

## Decisions

### D1 — The reference commit is read-only

The actual upstream base is `006d837c5022ea5278162307c04c78412cfae9f3` (`upstream/v1`). The reference commit's 10-character prefix `be242b5cd7` resolves to `be242b5cd7097f327817c7e3252573a38489da32` in this clone and is an ancestor of HEAD and of the upstream base `006d837c50`; it is not a pending rebase target and not a merged branch. The reference is read-only for semantics. Do not merge, cherry-pick, or copy worker code from it, and do not treat either ref as a merge target. Shared behavior is implemented in v1 terms and compared through the parity suite.

### D2 — Keep per-session workers only as a v1-only divergence

V1 keeps one lazily materialized worker per Session. The divergence is retained only while the benchmark passes:

- At 8 sessions, at least 25% of turn CPU is in Session-part JSON and tokenization.
- Throughput is 1.5–2x the in-process path.
- Turn p95 latency is within 20% of the in-process path.
- Cross-session leakage is zero.

If any condition is missed, switch to the in-process v2 model and delete the worker stack. The kill criterion is evaluated on recorded measurements, not on design intent.

### D3 — Purpose and priority

CPU parallelism and crash containment are primary. Plugin isolation is secondary. Parity is a constraint. A worker topology that improves isolation but misses the CPU or reliability gates is not justified.

### D4 — Build the graph once per worker

Build the Effect graph once per worker: 40 runnable nodes and 47 allowlisted nodes. Fix the per-run provide/scope behavior so a reused worker receives a fresh run scope without rebuilding the graph. Register cancellation before boot so a cancel that arrives during construction is observable. The earlier claim that 47 nodes were rebuilt per turn is incorrect and is replaced by this per-run scope/provide fix.

### D5 — No writer isolate

Delete the writer isolate. Each worker opens its own SQLite WAL connection and writes with `BEGIN IMMEDIATE` plus a bounded busy retry. The hub DB executor thread exists only to keep synchronous `bun:sqlite` calls off the event loop; it does not own ordering, caching, migrations, or database state. A hub connection is allowed only as a measured fallback, never as the default write path.

Cap running workers at the CPU-core count by default. Queue work at the cap rather than oversubscribing. The queue is scheduling state, not a second admission model.

### D6 — Table ownership

Tables with a `session_id` foreign key are worker-writable. Every other table is hub-only. Hub-only explicitly includes migrations, schema version, cross-session config/auth/project state, permission and question state, and leases. A generated allowlist test derives the split from the schema; a table that gains or loses `session_id` without updating the allowlist fails the gate.

### D7 — Schema gate

A session worker never runs DDL, never completes a migration, and never marks schema state. Every process that owns a hub or the core `Database` layer may migrate, serialized by a cross-process lease: one row in a sidecar SQLite file (`<database>.schema-gate`) acquired, renewed, and released in `BEGIN IMMEDIATE`, carrying an owner and a deadline so a crashed holder expires instead of blocking forever. The migrating process holds the lease for the whole migrate, verify, and mark sequence and renews it on a fixed interval; a lost lease aborts the work before its final commit. A waiter polls without blocking its thread and does not give up while the lease it observes is still live, so its wait outlasts one lease TTL plus a renewal interval. The schema version is `PRAGMA user_version` plus a hash of migration content. A hub with live workers refuses to migrate, bumps the version only under the lease, then opens the spawn gate. A worker whose observed version differs refuses with a typed `SchemaMismatch`; it does not fall back, run DDL, or mark the schema itself. Before any worker is admitted, the hub also verifies that `bun:sqlite` and `node:sqlite` report the same `sqlite_version()` and `PRAGMA compile_options`, and refuses worker mode otherwise.

### D8 — Worker crash contract

A crash produces a `WorkerCrashed` payload:

```ts
{
  sessionID: string
  workerID: string
  phase: "boot" | "materialize" | "stream" | "tool" | "commit"
  lastCommittedMessageID: string | null
  inFlightCallIDs: readonly string[]
  retryable: false
  reason: string
}
```

Persist and return the same payload. Never reuse a dead worker. A child whose parent dies receives `ParentCrashed` and is cancelled. Test every phase, including a crash after the commit has become durable.

### D9 — Auto-resume only from an unambiguous attempt

Auto-resume follows the v2 durable attempt model and is allowed only when the tool ledger shows that no side-effecting tool started. Resume the model, stream, and commit phases from the last committed assistant message. Cap automatic resumes at 10 per run. Cancellation never resumes.

### D10 — Tool-call ledger

The `tool_call` ledger has `unique(call_id, session)`. A call is marked started before its effect begins and finished in the same transaction as its result. An orphaned `started` row is `indeterminate`: it is first-class state, it is never auto-retried, and no side-effecting tool is auto-re-executed. Recovery may report indeterminate work; it may not guess.

### D11 — Boot cancellation

The boot sequence is permit, then materialize. A cancel during boot marks the generation, interrupts construction, terminates any partial isolate, releases the permit exactly once, starts no run, and returns:

```ts
{ mode: "cancelled", winner: "cancel", phase: "boot" }
```

No message is appended. A graph that is ready but not started takes the same path. Death during abort is a cancel win plus telemetry. Cancellation is idempotent, and a permit-count test proves the permit is not leaked or double-released.

### D12 — Sequences and ownership

There is no global or sender ordinal. `event_sequence` is keyed by `aggregate_id`; each aggregate's cursor is `seq`, and `event` has `unique(aggregate_id, seq)`. A new append reserves a contiguous block inside `BEGIN IMMEDIATE` with a single upsert-returning statement (`INSERT … ON CONFLICT(aggregate_id) DO UPDATE … RETURNING seq`, the block form used by both the Bus and EventV2), so a failed commit never leaves a gap. `event_sequence.owner_id` is the v1 sync owner and is never rewritten by a worker. Worker ownership is a separate generation lease in `event_sequence.worker_owner_id`, renewed on every write and by the background heartbeat (`worker_heartbeat_at`, `worker_expires_at`); an idle generation must not self-fence while it is alive. Reclaim is owner-scoped and atomic in one `BEGIN IMMEDIATE`, conditional on the observed owner and sequence, refuses a live lease unless its owner process is provably dead, and happens only after verifying that no commit advanced the sequence. PID-based death detection is valid only under the assumption that every hub process that can write or reclaim the same durable database shares a PID namespace. `isDeadWorkerOwner` treats `process.kill(pid, 0)` returning `ESRCH` as proof of death; in a different PID namespace (for example a host and a container sharing the database) `ESRCH` can describe a live owner, so that probe is not proof of death and reclaim must defer to lease expiry. The encoded PID is the hub process that spawned the isolate, and the owner string also carries a host scope (hostname, PID namespace, and boot identity) so that a process in another namespace never treats the bare PID as proof of death: the PID probe applies only when the scope matches this process, and any other owner is reclaimed by lease expiry. A stale generation's next write is fenced by the owner mismatch. Projectors are database-only and run in the committing process inside the same transaction.

### D13 — Bus API

Export `publish(events)` with in-transaction projectors, `project`, and `claim`/`reclaim`. Keep `commitDurableEvent` internal. Workers receive a Bus backed by their own connection. The Bus is the write contract; there is no separate writer RPC.

### D14 — Database and durable log are the only truth

The database and durable log are the only authoritative state. No authoritative prompt state lives in memory. Permission and question state is hub-owned and is never written by a worker. `StateCache` is an advisory, version-fenced cache keyed by `(aggregate_id, seq)`; the hub constructs it, invalidates an aggregate when a durable event arrives, and clears it on shutdown. Production never reads or writes payload entries through it (`get`/`set`); only tests do.

### D15 — Worker transport

Send JSON strings over `postMessage`. Decode exactly once with Effect Schema at the port boundary. Reject messages above 4 MiB. There is no build-hash verification: the hub and worker ship in one compiled binary, so they cannot mismatch.

### D16 — Large durable payloads

`event_blob` is keyed by `(aggregate_id, seq)` with `type`, `hash`, `size`, and `file_path`, written in the same transaction as the event row. The inline threshold is `DEFAULT_INLINE_PAYLOAD_LIMIT_BYTES` = `DEFAULT_BLOB_LIMIT_BYTES` (4 MiB) minus the core Bus's `ENVELOPE_HEADROOM_BYTES` (64 KiB). The egress frame owns a separate, independent `ENVELOPE_HEADROOM_BYTES` constant in `frame.ts`; tests pin the two together by asserting `DEFAULT_INLINE_PAYLOAD_LIMIT_BYTES === MAX_INLINE_EVENT_BYTES`, not by sharing one constant. Payloads at or below that threshold stay inline in `event.data`; a larger payload is written first to a content-addressed file under the blob directory, the `event_blob` row stores `hash`/`size` with `file_path` set, and `event.data` stores the `{ __eventBlob, size }` reference. `event_blob.data` remains only for rows written by the older inline-blob schema. `event_blob.hash` is indexed because GC probes references across every aggregate. The worker publishes oversized bodies through its own Bus; the egress frame carries a small `{ __eventBlobPosition, frameBytes }` reference for a body that does not fit its envelope. The hub resolves that reference with its off-loop SQL executor: it reads the `event_blob` row by position, validates hash and path through the core Bus, and reads the file, falling back to the committed inline `event.data` row when no blob row exists. Workers do not fetch blobs through the Bus for delivery. GC deletes the row in the committing transaction and unlinks a file only when no row for that hash remains, so a concurrent publisher's file is never removed.

### D17 — Deltas and resync

Deltas are advisory, coalesced, and droppable under a bounded per-session queue. Durable events are never dropped. Each subscription has a monotonic delta sequence. A gap produces `Resync { aggregateID, seq }` followed by a snapshot at the last durable sequence. Acks are never a correctness barrier.

### D18 — Run barrier

The run barrier is per Session. It waits for durable commits, projectors, tool settlement, and the background-promotion decision. It does not wait for delta acks. The barrier is bounded at 5 seconds; expiry produces `RunBarrierTimeout` and no next run starts. Cancellation wins over barrier completion.

### D19 — Hook routing table

`WORKER_HOOK_ROUTES` is hardcoded and keyed off the 14 actually invoked hooks (a 14-entry table; `permission.ask` is declared-only). Every invoked hook runs on the hub: the worker forwards it over the control port and applies the result to the caller, including `command.execute.before`, both compaction hooks and `experimental.text.complete`. Autocontinue crosses as the model's identity and the hub rebuilds `model` and `provider`, because the provider entry carries functions and credentials. An unclassified hook is fatal at startup and names the plugin that declared it. There is no silent skip and no fallback. Do not add a new plugin declaration API to hide an unsupported route.

### D20 — One tool routing point

Route tools at one point: local worker or hub. Delete `broker/*` and the port/client/layer nodes. MCP and LSP run on the hub with the Session working directory and context. Workers never spawn MCP or LSP processes. V1 already runs LSP hub-only outside the worker path; preserve that parity. Do not delete LSP.

### D21 — Permissions, questions, and cancellation

The hub owns permission and question state. Interruption uses interruptible fibers and a per-request cancel frame. Worker disposal performs cleanup. Cooperative cancel returns the last assistant message; hard terminate occurs after a typed grace period. A parent cancel cascades to children only.

### D22 — Subagents and resume

Port the v2 `SubagentJob.start`, `background`, and `notify` semantics plus `Session.resume`. Start is process-local. Background persists only when it has a `notificationID`. Suspended Sessions resume with an attempt cap. Restore foreground-to-background promotion.

### D23 — Effect and module boundaries

Convert each module to Effect as it is touched; do not perform a standalone rewrite. There is no `packages/session-transport` package; do not target a package that does not exist. Split the hub into transport, plugin-tool routing, production wiring, and core, with an import-boundary test. Move code upstream behind `Foo.Service` and update `AGENTS.md`; do not mix conventions. Exit criterion: no new Promise service internals in changed modules.

### D24 — Comment purge

Run the repository-wide comment purge only after the re-baseline, as package-scoped comment-only commits. Record the rationale in this spec first. Delete `.parallel-implement` plan references from source. Treat roughly 1% as a guideline, not a CI lint.

### D25 — Flag and rollout

There are two flag states: off and on. Startup validates configured plugins. Infrastructure failures are typed; there is no fallback. The kill switch affects new Sessions only; existing Sessions drain. Remove the flag one release after full parity is green. Keep the kill switch for two releases. Status exposes mode and disabled reason.

### D26 — Parity suite

Run both modes across the matrix: database memory/file, plugins none/supported/unsupported, crash none/boot/stream/tool/commit. Parity means identical user-visible event and status sequences and normalized database rows. Zero skips are allowed except the listed worker-only cases. Run a PR subset and the full suite nightly. The full suite is the release gate.

## Invariants

| Invariant | Enforced by |
| --- | --- |
| Database plus durable log is the only truth. | D14 |
| Durable events are never dropped, contiguous per aggregate, and owner-fenced. | D12, D17 |
| Tools are never re-executed automatically. | D9, D10 |
| Hooks are never silently skipped. | D19 |
| One execution mode per Session. | D25 |
| No build-hash handshake; one binary cannot mismatch. | D15 |
| Workers never run DDL or mark schema state; migration is serialized by the sidecar gate. | D6, D7 |
| The barrier is commit-based, not delta-ack-based. | D18 |
| Deltas may be dropped; durable events may not. | D17 |
| Transport is JSON plus Schema with a 4 MiB bound. | D15 |
| The graph is built once per worker; cancel is registered before boot. | D4, D11 |
| One tool routing point; MCP/LSP on the hub. | D20 |
| Effect conventions and intentional naming hold in changed modules. | D23 |
| Comments stay in this spec, not in code commentary. | D24 |

## Superseded and retained plan steps

The old 118-step plan lives in `.parallel-implement/multicore-session-parallelism.md`. Its writer-isolate, broker, and egress-frame steps are not executable against the green baseline.

| Old steps | Disposition |
| --- | --- |
| 53–56, 61, 66–68 | Superseded writer-isolate internals. Replaced by D5, D7, D12, D17. |
| 71–73, 77–84 | Superseded broker and cross-worker job-registry design. Replaced by D20 and D22. |
| 70, 74–79 | Egress frame/ack design rewritten. Replaced by D15–D18. |
| 57–60, 62–65, 69 | Retained in re-homed form: table ownership, owner fencing, capped queue, crash generations, cache invalidation, resume idempotency, refactor, Bus egress, and per-aggregate sequence. |

Landed on the green baseline: layer/graph, dispatch/runtime, and test-infra. Archived on `wip/parallel-refactor-checkpoint`: hub-wiring, control bus, plugin, session-root, schema/sdk/tui, broker/egress, and writer-db. The green baseline is v1 plus layer/dispatch/runtime/test-infra, with the hub reverted to v1 plus adaptations.

## Open risks

| Risk | Owner | Measurement | Trigger |
| --- | --- | --- | --- |
| Per-worker WAL contention and busy retries under 8 sessions. | `writer-db` | Busy-retry count, commit p95, failed-commit rate. | Retry budget exhausted or p95 regression beyond D2. |
| Synchronous SQLite on the hub event loop. | `writer-db`, `hub-wiring` | Event-loop gap p95 under load. | Any sustained gap from direct hub writes. |
| CPU benchmark miss. | `dispatch-lifecycle` | Turn CPU share in Session-part JSON/tokenization, throughput, turn p95. | Any D2 condition missed; delete worker stack. |
| Cross-session leakage. | `test-infra` | Leakage suite across 8 concurrent Sessions. | Any cross-session row, event, or cache hit. |
| Crash after commit duplicates effects. | `dispatch-lifecycle` | Phase matrix including commit-after-durable. | Any replayed side effect or reused dead worker. |
| Schema gate race during spawn. | `writer-db` | Zero-live-worker migration test and `SchemaMismatch` test. | Worker starts against mixed schema. |
| Hook route drift. | `plugin` | Source-derived 14-entry route test; startup fatal test. | New trigger without a route. |
| Delta gap or resync storm. | `control-bus` | Resync count and bounded-queue overflow. | Resync loop or dropped durable event. |
| Worker cap memory and spawn-gate queue growth. | `dispatch-lifecycle` | RSS at CPU-core cap; spawn-gate wait time. | OOM or unbounded waiter growth. |
| Archived hub work does not apply cleanly. | `hub-wiring` | Reapply conflict count and hub test pass. | Conflict in authority or transport wiring. |
| Flag parity gap at release. | `test-infra` | Parity-matrix failures. | Any non-listed skip or row mismatch. |

## Acceptance criteria

1. **CPU benchmark gate.** At 8 sessions, Session-part JSON/tokenization consumes at least 25% of turn CPU; throughput is 1.5–2x in-process; turn p95 is within 20% of in-process; cross-session leakage is zero. Record the profile and JSON results. If missed, execute the D2 kill criterion.
2. **Parity suite.** Both modes across DB memory/file, plugins none/supported/unsupported, and crash none/boot/stream/tool/commit. User-visible event and status sequences are identical; normalized DB rows are identical. Zero skips except the listed worker-only cases. The PR subset passes on every change; the full matrix is the release gate.
3. **Green baseline.** `rebaseline/green` at `a1f9457f` remains green: locked `bun turbo typecheck` is 30/30, and the baseline targeted suite is 639 pass, 1 skip, 0 fail. New tests do not regress these gates.
4. **DB ownership.** No writer isolate or writer RPC remains. Each worker uses a WAL connection; hub DDL is gated by zero live workers; the table allowlist is generated; `SchemaMismatch` and busy-retry tests pass.
5. **Crash and idempotency.** `WorkerCrashed` covers all phases including commit-after-durable; the tool ledger marks orphans indeterminate and never auto-replays; auto-resume is capped at 10; boot cancel releases one permit and appends no message.
6. **Transport.** JSON strings cross `postMessage`; Schema decodes once; the 4 MiB test passes; deltas resync without dropping durables; the 5-second run barrier times out without starting the next run.
7. **Tools and plugins.** One tool routing point; broker and port/client/layer nodes are gone; MCP/LSP run on the hub; the 14-entry hook route table forwards every invoked hook to the hub and fails startup on unclassified hooks; permission/question state stays hub-owned.
8. **Effect and structure.** Hub boundaries have an import-boundary test; changed modules expose `Foo.Service` and follow Effect naming; no new Promise service internals.
9. **Rollout.** Two flag states only; kill switch drains existing Sessions and blocks new ones; status exposes mode and disabled reason; flag removal waits one release after full parity green; kill switch remains two releases.
10. **Comments.** Purge only after re-baseline, in package-scoped comment-only commits, with this spec as the rationale; no `.parallel-implement` or plan references remain in source; 1% remains a guideline.

## Glossary

- **worker** — A Bun worker isolate that executes one Session's run and owns its WAL connection.
- **hub** — The main process. It owns HTTP/SSE, plugins, MCP/LSP, permission and question state, schema migration, and authority state.
- **writer isolate** — The superseded single writer process. It is removed by D5 and is not a fallback.
- **durable event** — A committed event row with a contiguous per-aggregate sequence. It is replayable and never dropped.
- **delta** — An advisory live fragment. It may be coalesced or dropped and never advances a durable cursor.
- **barrier** — The per-Session boundary after durable commits, projectors, tool settlement, and background-promotion decisions.
- **boot phase** — The permit-held startup interval from worker creation to ready handshake, and the phase name used by `WorkerCrashed` and boot cancellation.
- **owner_id** — The worker-generation lease that fences commits and permits atomic reclaim after in-flight commits are ruled out.
