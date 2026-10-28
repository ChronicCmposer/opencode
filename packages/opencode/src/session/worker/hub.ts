// The pure hub.
//
// The hub is the composition of three landed layers:
//   * the per-session `Dispatcher` — one worker per session, reused across turns
//     and idle-reaped, with the active key→worker map;
//   * the executor pool — workers whose run-state machine lives inside
//     the isolate (`execute.ts`);
//   * the cooperative cancel path — `cancelWithGrace`.
//
// It owns NO session execution: every session runs in a worker isolate, the hub
// only schedules and forwards control. `SessionHubService` is the Effect service
// `run-state.ts` delegates to when `experimentalSessionWorkers` is on; the
// pool-spawning layer node (`makeSessionHubNode`) provides it and tears the pool
// down with the layer scope.

import { Context } from "effect"
import path from "node:path"
import { InstanceRef } from "@/effect/instance-ref"
import { SessionStatus } from "@/session/status"
import { Permission } from "@/permission"
import { Question } from "@/question"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { createEgressReceiver } from "@/session/egress/receiver"
import { type DurableCommit } from "@/session/egress/mesh"
import { durableAggregateMetaOf, isEventBlobReference, type EventBlobPosition, type EventV2Event, type EventsAckPayload } from "./frame"
import { encodeFrame, resyncWithPositionFrame, type DurableResyncPayload } from "@/session/egress/frame"
import { type LiveEventPublisher } from "@/event-v2-bridge"
import type { EventV2 } from "@opencode-ai/core/event"
import { Durable } from "@opencode-ai/schema/durable-event-manifest"
import { Bus, StateCache } from "@opencode-ai/core/bus"
import type { SessionID } from "@/session/schema"
import { type DbExecutor } from "./db-executor"
import type { DispatchOrigin } from "@/session/origin"
import { defined } from "@/util/object"
import { createDispatcher, type CancellationConfig, type Dispatcher, type DispatcherOptions } from "./dispatch"
import { createLifecycleTracker, type LifecycleTracker } from "@/session/ledger/lifecycle"
import { decideAutoResume, type AutoResumeDecision, type IndeterminateCall } from "@/session/ledger/resume"
import { RUN_BARRIER_TIMEOUT_MS, RunBarrierTimeout, createRunBarrier, type RunBarrier, type RunBarrierPhases, type RunBarrierPromotionOutcome, type RunBarrierRun } from "@/session/run-barrier"
import { createSessionDispatch, type ExecutorWorkerPool, type SessionDispatch, type SessionExecutionResult } from "./execute"
import { type CancelOutcome } from "./cancel"
import { type PidRegistry, type PidReport } from "./pid-registry"
import type { HubProxyHandlers, HubProxyStreamHandler } from "./hub-proxy-wire"
import { serveHubProxy } from "./hub-proxy-wire"
import { errorTagOf } from "./port"
import { noopWorkerLogger, workerLogMessage, type WorkerLogger } from "./log"
import { Session } from "@/session/session"
import type { SessionPrompt } from "@/session/prompt"
import type { ShellInput } from "@/session/prompt"
import { isWorkerCrashPhase, recordAsync } from "@/session/ledger/crash"
import { BuildMismatch, resolveBuildHash } from "./build-hash"

import { createCancelReporter, type CancelReport } from "./cancel-report"
import {
  createWorkerPortRegistry,
  type WorkerPortBackends,
  type WorkerPortBackendsSource,
  type WorkerPortRegistry,
  type WorkerTransferredPorts,
} from "./worker-ports"

/**
 * Advisory hub cache. Entries are keyed by `(aggregateID, seq)` and fenced by the
 * cache's state version; it is never authoritative. The durable database and log
 * remain the only truth (D14).
 */
export type HubTruthCache = StateCache<unknown>

/** The invalidator a durable event source calls after a commit. */
export interface HubCacheInvalidation {
  invalidate(aggregateID: string): void
}

/** The five crash phases a worker reports (D8). */
export const WORKER_CRASH_PHASES = ["boot", "materialize", "stream", "tool", "commit"] as const
export type WorkerCrashPhase = (typeof WORKER_CRASH_PHASES)[number]

const WORKER_CRASH_PHASE_SET: ReadonlySet<string> = new Set(WORKER_CRASH_PHASES)

/**
 * The D8 crash payload a worker or dispatcher supplies to the hub. It is the
 * persistence contract, not a hub-invented summary: every field comes from the
 * crashing generation.
 */
export interface WorkerCrashed {
  readonly sessionID: string
  readonly workerID: number
  readonly phase: WorkerCrashPhase
  readonly lastCommittedMessageID: string | null
  readonly inFlightCallIDs: readonly string[]
  readonly retryable: false
  readonly reason: string
}

/** Parse an untrusted crash payload at the boundary. */
export function isWorkerCrashed(value: unknown): value is WorkerCrashed {
  if (typeof value !== "object" || value === null) return false
  const payload = value as Partial<WorkerCrashed>
  return (
    typeof payload.sessionID === "string" &&
    typeof payload.workerID === "number" &&
    Number.isInteger(payload.workerID) &&
    payload.workerID >= 0 &&
    typeof payload.phase === "string" &&
    WORKER_CRASH_PHASE_SET.has(payload.phase) &&
    (typeof payload.lastCommittedMessageID === "string" || payload.lastCommittedMessageID === null) &&
    Array.isArray(payload.inFlightCallIDs) &&
    payload.inFlightCallIDs.every((id) => typeof id === "string") &&
    payload.retryable === false &&
    typeof payload.reason === "string"
  )
}

/**
 * Read the typed `WorkerCrashed` payload a crash error carries. The shape may be
 * attached as `workerCrashed`, `crash`, or on the cause; no payload means the crash
 * contract is absent, so the hub must not fabricate one.
 */
export function workerCrashedFrom(error: unknown): WorkerCrashed | undefined {
  if (typeof error !== "object" || error === null) return undefined
  const candidate = error as {
    readonly workerCrashed?: unknown
    readonly crash?: unknown
    readonly payload?: unknown
    readonly cause?: unknown
  }
  if (isWorkerCrashed(candidate.workerCrashed)) return candidate.workerCrashed
  if (isWorkerCrashed(candidate.crash)) return candidate.crash
  const cause = workerCrashedFrom(candidate.cause)
  if (cause !== undefined) return cause
  // The dispatcher exposes the synthesized D8 payload as a getter.
  return isWorkerCrashed(candidate.payload) ? candidate.payload : undefined
}

/**
 * A lifecycle report whose target is not the reporting worker's own session.
 * Lifecycle truth is hub-owned and a worker may only report the session it is
 * bound to; a foreign id is a scope violation, never a silent no-op (D6/D21).
 */
export class ForeignLifecycleReportError extends Error {
  readonly _tag = "ForeignLifecycleReportError"
  constructor(
    readonly workerID: number,
    readonly boundSessionID: SessionID | undefined,
    readonly reportedSessionID: string,
  ) {
    super(
      `worker ${workerID} is bound to ${boundSessionID ?? "no session"}; ` +
        `refusing a lifecycle report for ${reportedSessionID}`,
    )
    this.name = "ForeignLifecycleReportError"
  }
}


/** The hub surface `run-state.ts` consumes when the rollout flag is on. */
export interface SessionHub {
  readonly dispatcher: Dispatcher
  /**
   * The hub-observed run lifecycle truth (last committed message, in-flight tool
   * calls, barrier phases). The tool/commit paths feed it; the crash path reads
   * it synchronously and auto-resume applies its policy.
   */
  readonly lifecycle: LifecycleTracker
  /** The per-session run barrier the next run waits on. */
  readonly barrier: RunBarrier
  /**
   * The advisory hub cache. Never authoritative: the durable database and log
   * remain the only truth (D14). It is empty on construction (a restart starts
   * cold) and cleared on shutdown.
   */
  readonly cache?: HubTruthCache
  /**
   * The session a worker is currently bound to (the dispatcher's key→worker map,
   * inverted). A worker isolate serves exactly one session at a time, so this is
   * the resolver the hub-proxy and live-event paths use to reach the worker's own
   * instance context instead of latching the first dispatched one.
   */
  sessionOfWorker(workerID: number): SessionID | undefined
  /**
   * Serve the worker's transferred egress ports against the hub's
   * authorities. Returns a disposer; idempotent per worker. No-op for a port the
   * hub has no backend for. Worker writes never cross a port: the isolate owns
   * its own connection.
   */
  attachWorkerPorts(workerID: number, ports: WorkerTransferredPorts): () => void
  /**
   * Serve the three hub-proxy authorities (`SessionStatus` / `Permission` /
   * `Question`) over a worker's control port. Returns a disposer; idempotent per
   * worker; no-op when the hub has no hub-proxy handlers.
   */
  attachHubProxyPort(workerID: number, port: MessagePort): () => void
  /**
   * Retire one generation of a session's worker that the pool replaces in place
   * (a mid-session model switch). Runs the same hub-side retirement as the
   * dispatcher path so the old generation's in-flight tool calls, ports, pending
   * permission/question requests and durable lease never outlive it.
   */
  releaseGeneration(sessionID: SessionID, workerID?: number): Promise<void>
  /** Dispatch `sessionID` to a worker and resolve with its result (admission-aware). */
  ensureRunning(sessionID: SessionID, origin?: DispatchOrigin): Promise<SessionExecutionResult>
  /**
   * Dispatch a shell turn. `shell` is the serialized `ShellInput` the worker
   * replays through `SessionPrompt.shell`; absent keeps the plain startShell path.
   */
  startShell(sessionID: SessionID, shell?: ShellInput): Promise<SessionExecutionResult>
  assertNotBusy(sessionID: SessionID): Promise<boolean>
  /** The UI-stop path: cooperative checkpoint, then terminate after the grace window. */
  cancel(sessionID: SessionID): Promise<CancelOutcome | undefined>
  /**
   * Cancel a session and its subagent cascade, returning the requested session's
   * outcome AND every cascaded descendant with whether its worker published the
   * terminal idle. `run-state.ts` consumes this to write the fallback idle
   * exactly once per cancelled session (a cooperative settle publishes its own;
   * a queued or grace-terminated one does not).
   */
  cancelWithReport(sessionID: SessionID): Promise<CancelReport>
  cancelAll(): Promise<void>
  terminateAll(): Promise<void>
}

export class SessionHubService extends Context.Service<SessionHubService, SessionHub>()("@opencode/SessionHub") {}

/**
 * Cancellation wiring, either pre-built or derived from the materialised pool
 * (the production case needs the pool to look up a worker's `cancelPort`).
 */
export type CancellationSource = CancellationConfig | ((pool: ExecutorWorkerPool) => CancellationConfig)

export interface SessionHubOptions {
  /** The executor pool; production workers spawn on the first dispatch, not at boot. */
  readonly pool: ExecutorWorkerPool
  /**
   * Hub-resident authorities for the worker egress ports and the
   * hub-proxy control port. When present, the hub serves transferred worker
   * ports; the production init advertises `capabilities.sessionWorkerPorts`.
   */
  readonly ports?: WorkerPortBackendsSource
  /**
   * Invoked synchronously at the start of every `ensureRunning`/`startShell`,
   * before any async work, with the session being dispatched. Used by the
   * production hub to capture the caller's ambient `InstanceRef` and runtime
   * context (the `attach` pattern), and to remember which session's persisted
   * model the worker init should resolve.
   */
  readonly onDispatch?: (sessionID: SessionID) => void
  /**
   * Resolve a session's subagent depth (`metadata.depth`) for the dispatch path.
   * Depth 0 is a foreground root; a positive depth takes bounded child headroom
   * so a parent awaiting a `task` child never deadlocks the running-worker cap.
   * Values are clamped to a safe non-negative integer; absent/malformed is 0.
   */
  readonly depthOf?: (sessionID: SessionID) => number | undefined
  readonly cancellation?: CancellationSource
  /** Release hub-side state for one retired generation (crash/reap/cancel/model switch). */
  readonly onSessionReleased?: (sessionID: SessionID) => void
  /**
   * Idle-reap window forwarded to the dispatcher. Absent uses the dispatcher's
   * env/default resolution; tests inject a short window.
   */
  readonly reapMs?: number
  /** Scheduler seam for the dispatcher's idle-reap timer (tests). */
  readonly schedule?: DispatcherOptions["schedule"]
  /**
   * Per-session durable-write barrier: the dispatcher holds a
   * completed run's slot until this resolves, so the next run can never overtake
   * the prior run's durable egress. Absent means a released slot is immediate.
   */
  readonly writeBarrier?: DispatcherOptions["writeBarrier"]
  /**
   * Live-only event publisher. When present, the hub republishes worker events
   * from their port and publishes `session.cancelled` after a cancel.
   */
  readonly events?: LiveEventPublisher
  /**
   * Resolve the live publisher for a specific session, so `session.cancelled`
   * carries THAT session's location instead of a first-dispatch one. Falls back
   * to {@link events} when absent.
   */
  readonly eventPublisherFor?: (sessionID: SessionID) => LiveEventPublisher | undefined
  /**
   * Invoked at the start of every cancel, before the grace window, for the session
   * and each descendant. The production layer aborts in-flight hub-executed plugin
   * tools here; a plain hub without it simply skips that best-effort unwind.
   */
  readonly onCancelRequested?: (sessionID: SessionID) => void
  /**
   * Receive a worker's `{type:"pid-report"}` frame from its control
   * port. `workerID` is the authoritative owner (the port association), not the
   * worker's self-reported id. The production hub mirrors each entry into its
   * `PidRegistry`; absent means pid reports are ignored (a pure hub, no reaper).
   */
  readonly onPidReport?: (workerID: number, report: PidReport) => void
  /**
   * Off-loop fallback for hub-side SQL. The executor owns no ordering, cache,
   * migration, or schema policy; callers own every query (D5).
   */
  readonly db?: DbExecutor
  /** Advisory cache, never authoritative. Defaults to a fresh, empty cache. */
  readonly cache?: HubTruthCache
  /**
   * Persist/surface the typed D8 payload a crashed generation reports. When the
   * crash error carries no payload the hub logs the missing contract and does not
   * fabricate one.
   */
  readonly onWorkerCrashed?: (payload: WorkerCrashed) => void | Promise<void>
  /**
   * The run lifecycle truth shared with the tool/commit paths. Absent creates a
   * fresh bounded tracker owned by this hub.
   */
  readonly lifecycle?: LifecycleTracker
  /** The run barrier. Absent creates the production 5 s barrier. */
  readonly barrier?: RunBarrier
  /**
   * Caller-supplied barrier phases. A TEST SEAM only: production reports real
   * observations to the barrier's run handle instead.
   */
  readonly barrierPhases?: (sessionID: SessionID) => RunBarrierPhases
  /**
   * The background-promotion decision the hub observed for a session at run end.
   * The production layer derives it from its worker-job mirror; a hub with no
   * mirror observed no job (`no-job`).
   */
  readonly promotionOutcome?: (sessionID: SessionID) => RunBarrierPromotionOutcome
  /**
   * Re-dispatch after an allowed auto-resume. The production layer re-enters
   * `ensureRunning`; absent means the caller still observes the decision but
   * owns the replay.
   */
  readonly resumeRun?: (
    decision: Extract<AutoResumeDecision, { resume: true }>,
    payload: WorkerCrashed,
  ) => void | Promise<void>
  /**
   * Upper bound (ms) on the dispatcher's asynchronous resume decision. A decision
   * not proven within the budget settles as a typed refusal (`indeterminate`), so
   * a stuck ledger read can never park the next dispatch. Omitted or <= 0
   * disables the bound (a caller that drives the decision by hand).
   */
  readonly resumeDecisionTimeoutMs?: number
  /**
   * An async boot gate awaited before the first dispatch (the production hub's
   * off-loop schema ensure). Absent means no gate (a pure in-memory hub).
   * The gate is only awaited on the admission path, never at construction, so
   * building a hub stays synchronous.
   */
  readonly bootGate?: () => Promise<void>
  /** Optional structured logger seam; absent defaults to a no-op (flag-off behavior). */
  readonly log?: WorkerLogger
}

// ---------------------------------------------------------------------------
// Hub events sink (spec Q15/Q16/Q22): worker EventV2 frames → hub event surface.
// ---------------------------------------------------------------------------

/**
 * The canonical `session.cancelled` event type, sourced from the schema definition
 * the hub publishes after a cancel resolves.
 */
export const SESSION_CANCELLED_TYPE = SessionV1.Event.Cancelled.type

/**
 * Recover an over-frame durable event body by committed position. The body is
 * usually in the shared D16 blob, but a body just under the inline bound lives
 * in `event.data` instead (the envelope, not the body, exceeded the frame):
 * the resolver falls back to that committed row. `undefined` means neither
 * source exists at that position, which the events port surfaces as a typed
 * resync — never a dropped event or a raw reference. A corrupt row fails loud
 * (`Bus.InvalidDurableEvent`) and is also surfaced as a resync.
 */
export type EventBlobResolver = (
  position: EventBlobPosition,
  event: { readonly type: string },
) => Promise<Record<string, unknown> | undefined>

export interface EventBlobResolverOptions {
  /**
   * Async SQL over the same database file the worker wrote. Production passes
   * the hub's off-loop executor, so resolution never runs SQLite on the hub
   * main thread; tests pass a real connection adapter.
   */
  readonly query: (sql: string, params: ReadonlyArray<unknown>) => Promise<ReadonlyArray<Record<string, unknown>>>
  /** `dirname(dbFile)/event-blobs`, shared with the worker Bus and EventV2. */
  readonly blobDir: string
}

/** The committed inline body at one aggregate position, when no blob row exists. */
const INLINE_EVENT_QUERY = "SELECT data FROM event WHERE aggregate_id = ? AND seq = ?"

/**
 * Parse a committed inline `event.data` value. A JSON string is decoded; an
 * already-decoded record is accepted as-is. A leftover blob reference (or any
 * non-object) is not a body, so the caller keeps the typed-resync path.
 */
function inlineEventDataOf(row: Record<string, unknown> | undefined): Record<string, unknown> | undefined {
  if (row === undefined) return undefined
  const data = row["data"]
  const decoded = typeof data === "string" ? JSON.parse(data) : data
  if (typeof decoded !== "object" || decoded === null || Array.isArray(decoded)) return undefined
  const record = decoded as Record<string, unknown>
  return Bus.isBlobReference(record) ? undefined : record
}

/**
 * Build the production body resolver: first the `event_blob` row by position,
 * decoded and hash/path-validated by the core Bus (the SQL comes from
 * `Bus.blobRowQuery`, the same query `Bus.loadBlobRows` issues). When no blob
 * row exists, the body was stored inline, so the committed `event.data` row is
 * the authority. A position with neither source is `undefined`.
 */
export function createEventBlobResolver(options: EventBlobResolverOptions): EventBlobResolver {
  const blob: Bus.BlobStore = {
    dir: options.blobDir,
    limitBytes: Bus.DEFAULT_BLOB_LIMIT_BYTES,
    supportsTable: true,
  }
  return async (position, event) => {
    const query = Bus.blobRowQuery(position.aggregateID, [position.seq])
    const rows = await options.query(query.sql, query.params)
    const row = Bus.blobRowsBySeq(rows).get(position.seq)
    if (row !== undefined) {
      return Bus.resolveBlobRow(
        { type: event.type, aggregateID: position.aggregateID, seq: position.seq },
        row,
        blob,
      )
    }
    const inline = await options.query(INLINE_EVENT_QUERY, [position.aggregateID, position.seq])
    return inlineEventDataOf(inline[0])
  }
}

export interface HubEventSinkOptions {
  /** Live-only publisher the sink republishes each event through. */
  readonly publisher: LiveEventPublisher
  /** Advisory cache invalidated when a durable event reaches the hub. */
  readonly cache?: HubCacheInvalidation
  /** Resolve a durable event's aggregate id; defaults to its `sessionID`. */
  readonly aggregateOf?: (event: EventV2Event) => string | undefined
  /**
   * Resolve an oversized durable body before republishing. `serveEventsPort`
   * awaits this for a `{ __eventBlobPosition, frameBytes }` payload; a missing
   * row or a resolution failure is surfaced as a typed resync.
   */
  readonly resolveBlob?: EventBlobResolver
  /**
   * Read the committed events in a missing durable range so a gap is healed on
   * the live surface instead of only counted. Production supplies a reader over
   * the hub's off-loop SQL executor; absent means a gap is resync-only.
   */
  readonly readDurable?: DurableHealReader
}

/** One committed durable event read back while healing a gap. */
export interface DurableHealEntry {
  readonly seq: number
  readonly event: EventV2Event
}

/**
 * Read committed events in the bounded range `(after, upTo]`, ordered by
 * sequence. The reader is async by contract so production can run SQLite on the
 * hub's executor thread and never block the hub event loop.
 */
export type DurableHealReader = (
  aggregateID: string,
  after: number,
  upTo: number,
) => Promise<readonly DurableHealEntry[]>

/** The gap a sink heals; `after` defaults to the last position it published. */
export interface DurableHealPosition {
  readonly aggregateID: string
  readonly seq: number
  readonly after?: number
}

export interface DurableHealReaderOptions {
  /** Async SQL over the same database file the worker wrote. */
  readonly query: (sql: string, params: ReadonlyArray<unknown>) => Promise<ReadonlyArray<Record<string, unknown>>>
  /** Maximum rows per page; the sink keeps reading pages until the gap is filled. */
  readonly pageSize?: number
}

const DURABLE_HEAL_SQL =
  "SELECT id, type, seq, data FROM event WHERE aggregate_id = ? AND seq > ? AND seq <= ? ORDER BY seq ASC LIMIT ?"

const DEFAULT_HEAL_PAGE_SIZE = 512

/** Decode a persisted `event.data` value; SQLite stores it as JSON text. */
function decodeHealRowData(value: unknown): unknown {
  if (typeof value !== "string") return value
  try {
    return JSON.parse(value)
  } catch (error) {
    throw new Error(`invalid persisted event data: ${error instanceof Error ? error.message : String(error)}`)
  }
}

/**
 * Build the production gap-heal reader over the durable `event` log. Each read
 * is bounded by `pageSize`; the sink iterates pages so healing is both ordered
 * and memory-bounded regardless of how large the missed range is.
 */
export function createDurableHealReader(options: DurableHealReaderOptions): DurableHealReader {
  const pageSize = options.pageSize ?? DEFAULT_HEAL_PAGE_SIZE
  if (!Number.isInteger(pageSize) || pageSize < 1) {
    throw new Error(`invalid durable heal page size: ${pageSize} (expected a positive integer)`)
  }
  return async (aggregateID, after, upTo) => {
    if (typeof aggregateID !== "string" || aggregateID.length === 0) {
      throw new Error(`invalid durable heal aggregateID: ${JSON.stringify(aggregateID)}`)
    }
    if (!Number.isInteger(after) || after < -1 || !Number.isInteger(upTo)) {
      throw new Error(`invalid durable heal range: (${JSON.stringify(after)}, ${JSON.stringify(upTo)}]`)
    }
    if (upTo <= after) return []
    const rows = await options.query(DURABLE_HEAL_SQL, [aggregateID, after, upTo, pageSize])
    return rows.map((row, index) => {
      if (typeof row["id"] !== "string") throw new Error(`invalid durable heal row ${index}: id must be a string`)
      if (typeof row["type"] !== "string") throw new Error(`invalid durable heal row ${index}: type must be a string`)
      if (typeof row["seq"] !== "number" || !Number.isInteger(row["seq"])) {
        throw new Error(`invalid durable heal row ${index}: seq must be an integer`)
      }
      if (!("data" in row)) throw new Error(`invalid durable heal row ${index}: data is missing`)
      return {
        seq: row["seq"],
        event: { id: row["id"], type: row["type"], properties: decodeHealRowData(row["data"]) },
      }
    })
  }
}

/** The aggregate an event belongs to, when its payload identifies one. */
export function aggregateIDOfEvent(event: EventV2Event): string | undefined {
  if (typeof event.properties !== "object" || event.properties === null) return undefined
  const properties = event.properties as Record<string, unknown>
  for (const field of ["sessionID", "aggregateID", "aggregate_id"]) {
    const value = properties[field]
    if (typeof value === "string" && value.length > 0) return value
  }
  return undefined
}

/**
 * A worker's events-port sink. Each event is republished live-only — never
 * re-committed, because a durable worker event already committed in its isolate.
 * `end`/`resync`/`ack` only update flow-control state: the sink owns no buffering,
 * so the overflow/resync policy lives with the receiver that owns backpressure.
 */
export interface HubEventSink {
  /**
   * Republish one worker event live-only. `durable` marks an event whose commit
   * already happened in the worker, so the hub cache must drop the aggregate.
   * `aggregateID`/`seq` are the frame's committed identity; when present they are
   * authoritative for cache invalidation (the payload is not re-parsed to guess).
   * A durable event with no `seq` is malformed: its commit still fences the
   * aggregate, so it is never downgraded to a live-only delta.
   */
  event(
    event: EventV2Event,
    options?: {
      readonly durable?: boolean
      readonly aggregateID?: string
      readonly seq?: number
    },
  ): void
  /** The worker signalled end-of-stream. */
  end(): void
  /**
   * Heal a durable gap by reading `(after, seq)` from the committed log and
   * republishing it live. Awaited by `serveEventsPort` so the gapped frame and
   * every later delivery stay ordered behind the healed range.
   */
  readonly heal?: (position: DurableHealPosition) => void | Promise<void>
  /** A resync: a delta was dropped or a durable sequence gapped; durable events continue. */
  resync(payload?: DurableResyncPayload): void
  /** A delivery acknowledgement. */
  ack(payload?: EventsAckPayload): void
  /** Whether the worker signalled end-of-stream. */
  readonly ended: boolean
  /** Number of resyncs surfaced (received and overflow). */
  readonly resyncs: number
  /** Number of `ack` frames observed. */
  readonly acks: number
  /**
   * Resolve an oversized durable body by committed position. `serveEventsPort`
   * awaits it before republishing; absent means a position reference is healed
   * by a typed resync instead of ever crossing to the live publisher.
   */
  readonly resolveBlob?: EventBlobResolver
}

/**
 * The live event plus the durable identity that committed in the worker. The
 * shared `LiveEvent` seam gains the field when the event bridge adopts it; the
 * cast keeps the hub half compiling and forwards the metadata instead of
 * dropping it on the floor.
 */
type DurableLiveEvent = Parameters<LiveEventPublisher["publish"]>[0] & {
  readonly durable?: { readonly aggregateID: string; readonly seq: number; readonly version: number }
}

function publishWithDurable(publisher: LiveEventPublisher, event: DurableLiveEvent): void {
  ;(publisher.publish as (event: DurableLiveEvent) => void)(event)
}

export function createHubEventSink(options: HubEventSinkOptions): HubEventSink {
  const { publisher, cache } = options
  const aggregateOf = options.aggregateOf ?? aggregateIDOfEvent
  const readDurable = options.readDurable
  const lastDurable = new Map<string, number>()
  let ended = false
  let resyncs = 0
  let acks = 0

  const publishDurable = (event: EventV2Event, aggregateID: string, seq: number): void => {
    publishWithDurable(publisher, {
      id: event.id,
      type: event.type,
      data: event.properties,
      durable: {
        aggregateID,
        seq,
        // The position is authoritative; only the schema version comes from the
        // shipped manifest.
        version: Durable.get(event.type)?.durable?.version ?? 1,
      },
    })
    cache?.invalidate(aggregateID)
    lastDurable.set(aggregateID, Math.max(seq, lastDurable.get(aggregateID) ?? -1))
  }

  /** Recover a healed row's body, falling back to the committed blob/inline row. */
  const healBodyOf = async (
    event: EventV2Event,
    aggregateID: string,
    seq: number,
  ): Promise<unknown | undefined> => {
    const properties = event.properties
    if (isEventBlobReference(properties)) {
      return options.resolveBlob?.(properties.__eventBlobPosition, { type: event.type })
    }
    if (!Bus.isBlobReference(properties)) return properties
    return options.resolveBlob?.({ aggregateID, seq }, { type: event.type })
  }

  /**
   * Read the bounded missing range page by page and publish each committed event
   * in order. The graft deliberately stops at `seq - 1`: the gapped frame at
   * `seq` is delivered by the normal port path exactly once.
   */
  const healRange = async (position: DurableHealPosition): Promise<void> => {
    if (readDurable === undefined) return
    const upTo = position.seq - 1
    let cursor = position.after ?? lastDurable.get(position.aggregateID) ?? -1
    while (cursor < upTo) {
      let page: readonly DurableHealEntry[]
      try {
        page = await readDurable(position.aggregateID, cursor, upTo)
      } catch {
        // A failed read is recoverable by the worker's own resync; never throw
        // into the ordered port chain.
        return
      }
      if (page.length === 0) return
      const before = cursor
      for (const entry of page) {
        if (entry.seq <= cursor || entry.seq > upTo) continue
        const body = await healBodyOf(entry.event, position.aggregateID, entry.seq)
        if (body === undefined) return
        publishDurable({ ...entry.event, properties: body }, position.aggregateID, entry.seq)
        cursor = entry.seq
      }
      // A reader that made no progress cannot fill the gap; stop rather than spin.
      if (cursor === before) return
    }
  }

  const heal = (position: DurableHealPosition): void | Promise<void> => {
    resyncs += 1
    if (readDurable === undefined) return
    return healRange(position)
  }

  return {
    ...(options.resolveBlob === undefined ? {} : { resolveBlob: options.resolveBlob }),
    heal,
    event(event, meta) {
      // Defense in depth: `serveEventsPort` resolves a position reference before
      // calling here. A reference that still arrives (a direct caller, or a hub
      // with no blob resolver) must never reach the live publisher as raw data;
      // a typed resync is the only safe outcome.
      if (isEventBlobReference(event.properties)) {
        resyncs += 1
        return
      }
      // The event crosses verbatim as `{ id, type, properties }`. A durable frame
      // additionally carries the committed `(aggregateID, seq)` so the hub's
      // `sync` envelope can identify the exact durable position; the payload is
      // never re-committed here (the worker's commit is authoritative).
      const aggregateID = meta?.durable === true ? (meta.aggregateID ?? aggregateOf(event)) : undefined
      if (aggregateID === undefined) {
        publisher.publish({ id: event.id, type: event.type, data: event.properties })
        return
      }
      if (meta?.seq === undefined) {
        // A durable event with no committed position is malformed, but its commit
        // still happened: fence the aggregate rather than treating it as a delta.
        cache?.invalidate(aggregateID)
        publisher.publish({ id: event.id, type: event.type, data: event.properties })
        return
      }
      publishDurable(event, aggregateID, meta.seq)
    },
    end() {
      ended = true
    },
    resync(payload) {
      resyncs += 1
      // A worker-reported resync may carry the exact position it lost. Heal it
      // the same way as a receiver gap; the counter above already recorded it.
      if (readDurable !== undefined && payload?.aggregateID !== undefined && payload.seq !== undefined) {
        void healRange({ aggregateID: payload.aggregateID, seq: payload.seq })
      }
    },
    ack() {
      acks += 1
    },
    get ended() {
      return ended
    },
    get resyncs() {
      return resyncs
    },
    get acks() {
      return acks
    },
  }
}

/**
 * The production durable-egress barrier. `execute` awaits it after a run reply,
 * so the dispatcher cannot hand the worker its next run before the run's durable
 * events reached the hub.
 *
 * The worker's events sender posts one `ack` after a batch that carried durable
 * frames; the hub receives the durable frames and the ack on the SAME port, in
 * order. The barrier therefore resolves once the hub has acknowledged every
 * durable frame it has observed for the session. A bounded miss (a dead port
 * that can never ack) resolves with a warning instead of wedging the dispatcher
 * forever; the durable row and log remain the authority.
 */
export interface EgressDeliveryBarrier {
  /** One durable frame reached the hub's events port for `sessionID`. */
  observeDurable(sessionID: SessionID): void
  /**
   * The worker acknowledged `count` durable hand-offs for `sessionID`. An ack
   * closes the CURRENT batch: a count larger than the durables observed since the
   * previous ack (a resynced reference that never reached the live sink) settles
   * what was observed and is never carried forward as credit.
   */
  observeAck(sessionID: SessionID, count?: number): void
  /** Resolve once every durable frame observed for the current run is acknowledged. */
  wait(sessionID: SessionID): Promise<void>
  /** Drop the current run's progress; `wait` also resets implicitly when it settles. */
  reset(sessionID: SessionID): void
}

export interface EgressDeliveryBarrierOptions {
  /** Bound on a missing ack; defaults to {@link DEFAULT_EGRESS_BARRIER_TIMEOUT_MS}. */
  readonly timeoutMs?: number
  readonly log?: WorkerLogger
}

export const DEFAULT_EGRESS_BARRIER_TIMEOUT_MS = 5_000

interface EgressProgress {
  /** Durables observed since the last ack (the current sender batch). */
  outstanding: number
  /** Diagnostics for the current run; reset when the run settles. */
  observed: number
  acked: number
  waiters: Set<() => void>
  timer: ReturnType<typeof setTimeout> | undefined
}

export function createEgressDeliveryBarrier(options: EgressDeliveryBarrierOptions = {}): EgressDeliveryBarrier {
  const timeoutMs = options.timeoutMs ?? DEFAULT_EGRESS_BARRIER_TIMEOUT_MS
  if (!Number.isInteger(timeoutMs) || timeoutMs < 1) {
    throw new Error(`invalid egress barrier timeout: ${timeoutMs} (expected a positive integer)`)
  }
  const log = options.log ?? noopWorkerLogger
  const progress = new Map<SessionID, EgressProgress>()

  const progressOf = (sessionID: SessionID): EgressProgress => {
    const existing = progress.get(sessionID)
    if (existing !== undefined) return existing
    const created: EgressProgress = { outstanding: 0, observed: 0, acked: 0, waiters: new Set(), timer: undefined }
    progress.set(sessionID, created)
    return created
  }

  /** Clear one run's progress and release its waiters. */
  const clear = (state: EgressProgress): void => {
    if (state.timer !== undefined) {
      clearTimeout(state.timer)
      state.timer = undefined
    }
    state.outstanding = 0
    state.observed = 0
    state.acked = 0
    for (const waiter of state.waiters) waiter()
    state.waiters.clear()
  }

  /** Release every waiter once the current batch is fully acknowledged. */
  const settle = (state: EgressProgress): void => {
    if (state.outstanding > 0) return
    clear(state)
  }

  return {
    observeDurable(sessionID) {
      const state = progressOf(sessionID)
      state.outstanding += 1
      state.observed += 1
    },
    observeAck(sessionID, count = 1) {
      // An ack is authoritative only for a non-negative integer count. A
      // malformed count cannot inflate progress; it is ignored (fail closed).
      if (!Number.isInteger(count) || count < 0) return
      const state = progressOf(sessionID)
      state.acked += count
      // Correlation, not cumulative credit: only durables actually observed in
      // this batch can be settled, and any extra ack count is discarded.
      state.outstanding = Math.max(0, state.outstanding - count)
      settle(state)
    },
    wait(sessionID) {
      const state = progressOf(sessionID)
      if (state.outstanding === 0) return Promise.resolve()
      return new Promise<void>((resolve) => {
        state.waiters.add(resolve)
        state.timer ??= setTimeout(() => {
          state.timer = undefined
          log(workerLogMessage("durable egress ack missing"), {
            level: "WARN",
            sessionID,
            outstanding: state.outstanding,
            observed: state.observed,
            acked: state.acked,
            timeoutMs,
          })
          clear(state)
        }, timeoutMs)
      })
    },
    reset(sessionID) {
      const state = progress.get(sessionID)
      if (state !== undefined) clear(state)
    },
  }
}

/**
 * Serve a worker's events port with the shared egress receiver (length-prefixed
 * `WorkerFrameEnvelope` frames, backpressure-aware), routing every decoded frame
 * to `sink`. A malformed frame fails loud: the receiver closes the port.
 *
 * A durable event identified by its aggregate `(aggregateID, seq)` is additionally
 * handed to `onDurable` — the hub's cross-isolate mesh commit path, so another
 * worker's `events.durable()` subscribers wake. The live republish is unchanged:
 * the mesh is an additive wakeup channel, never a re-commit. A per-aggregate
 * sequence gap is surfaced as a resync (and sent back to the worker) instead of
 * failing the port, so the stream heals from the worker's DB snapshot.
 *
 * An oversized durable body arrives as `{ __eventBlobPosition, frameBytes }`.
 * When the sink has a {@link EventBlobResolver}, the body is recovered from the
 * shared blob BEFORE either the live publisher or the mesh sees it; a missing
 * row (or a corrupt one) surfaces a typed resync. Deliveries stay ordered: once
 * a reference is resolving, later frames — including the batch's ack — queue
 * behind it.
 */
export function serveEventsPort(
  port: MessagePort,
  sink: HubEventSink,
  onError?: (error: Error) => void,
  onDurable?: (commit: DurableCommit) => void,
): () => void {
  let closed = false
  let tail: Promise<void> | undefined
  /** Run `task` now when nothing is pending, else append it to the delivery chain. */
  const ordered = (task: () => void | Promise<void>): void => {
    if (tail === undefined) {
      void task()
      return
    }
    enqueue(task)
  }
  const enqueue = (task: () => void | Promise<void>): void => {
    const next = (tail ?? Promise.resolve()).then(task).then(
      () => undefined,
      () => undefined,
    )
    tail = next
    void next.then(() => {
      if (tail === next) tail = undefined
    })
  }
  const deliver = (event: EventV2Event, meta: { readonly aggregateID: string; readonly seq: number } | undefined): void => {
    if (closed) return
    sink.event(event, {
      durable: meta !== undefined,
      ...(meta === undefined ? {} : { aggregateID: meta.aggregateID, seq: meta.seq }),
    })
    if (meta === undefined || onDurable === undefined) return
    onDurable({ meta, event })
  }
  const deliverReference = (
    event: EventV2Event,
    position: EventBlobPosition,
    meta: { readonly aggregateID: string; readonly seq: number } | undefined,
  ): void => {
    const resolver = sink.resolveBlob
    if (resolver === undefined) {
      ordered(() => sink.resync({ aggregateID: position.aggregateID, seq: position.seq }))
      return
    }
    enqueue(async () => {
      let data: Record<string, unknown> | undefined
      try {
        data = await resolver(position, { type: event.type })
      } catch {
        // A missing/corrupt row is recoverable from the worker's DB snapshot.
        data = undefined
      }
      if (data === undefined) {
        sink.resync({ aggregateID: position.aggregateID, seq: position.seq })
        return
      }
      deliver({ ...event, properties: data }, meta)
    })
  }
  const receiver = createEgressReceiver({
    port,
    onFrame: (frame) => {
      if (frame.kind !== "event") return
      // The canonical durability test (present only when BOTH `aggregateID` and
      // `seq` are set), shared with the receiver and fan-out so the three never
      // drift on exactly which frames are durable.
      const meta = durableAggregateMetaOf(frame)
      const event = frame.payload.event
      if (isEventBlobReference(event.properties)) {
        deliverReference(event, event.properties.__eventBlobPosition, meta)
        return
      }
      ordered(() => deliver(event, meta))
    },
    // A worker that skipped a durable sequence is healed from the committed log
    // (when the sink has a reader) and asked over the same bidirectional events
    // port to re-read its own aggregate. Healing is awaited on the delivery chain
    // so the gapped frame and the frames after it cannot overtake it.
    onDurableGap: (got, last) => {
      enqueue(async () => {
        if (closed) return
        if (sink.heal !== undefined) {
          await sink.heal({ aggregateID: got.aggregateID, seq: got.seq, after: last.seq })
        } else {
          sink.resync({ aggregateID: got.aggregateID, seq: got.seq })
        }
        if (closed) return
        try {
          port.postMessage(encodeFrame(resyncWithPositionFrame({ aggregateID: got.aggregateID, seq: got.seq })))
        } catch (error) {
          // The peer may already be gone; the sink counter still records the gap.
          onError?.(error instanceof Error ? error : new Error(String(error)))
        }
      })
    },
    onEnd: () => ordered(() => sink.end()),
    onResync: (payload) => ordered(() => sink.resync(payload)),
    onAck: (payload) => ordered(() => sink.ack(payload)),
    ...(onError === undefined ? {} : { onError }),
  })
  return () => {
    closed = true
    receiver.close()
  }
}


/**
 * Mark one retired generation's lease reclaimable, scoped to its exact
 * `worker_owner_id`. A successor's live lease is never touched, so a retirement
 * that lands after a reclaim cannot strand the new generation.
 */
export async function releaseRetiredWorkerLease(
  db: DbExecutor,
  sessionID: SessionID,
  retiredOwnerID: string,
): Promise<boolean> {
  if (typeof retiredOwnerID !== "string" || retiredOwnerID.length === 0) return false
  const result = await db.run(
    "UPDATE event_sequence SET worker_expires_at = 0 WHERE aggregate_id = ? AND worker_owner_id = ? AND worker_expires_at IS NOT NULL",
    [sessionID, retiredOwnerID],
  )
  return result.changes > 0
}

/** Compose the dispatcher + executor pool + cooperative cancel into one hub. */
/**
 * Upper bound on a dispatch depth carried from `metadata.depth`. Nesting beyond
 * this is already refused by the subagent limit; the clamp only keeps a
 * malformed/forged value from growing the cap's per-depth budget map without
 * bound.
 */
const MAX_DISPATCH_DEPTH = 64

/** Parse a dispatch depth at the boundary: finite, non-negative, bounded. */
export function boundedDispatchDepth(depth: number | undefined): number {
  if (depth === undefined || !Number.isFinite(depth)) return 0
  return Math.min(Math.max(0, Math.trunc(depth)), MAX_DISPATCH_DEPTH)
}

/**
 * The default budget for the dispatcher's asynchronous resume decision. Long
 * enough for one off-loop ledger read on a busy database, short enough that a
 * wedged executor cannot hold the session's next dispatch open.
 */
export const DEFAULT_RESUME_DECISION_TIMEOUT_MS = 2_000

/**
 * Bound an async resume decision. The decision function owns its own failures
 * (the hub's ledger read already falls back to the in-memory tracker); this
 * wrapper only guarantees the dispatcher observes SOME decision within the
 * budget. A timeout refuses as `indeterminate` — the safe answer when the
 * durable `started` rows cannot be read, because a lost read must never
 * auto-resume a side-effecting call.
 */
export function boundedResumeDecision(
  decide: () => Promise<AutoResumeDecision>,
  timeoutMs: number | undefined,
  onTimeout: () => AutoResumeDecision,
): Promise<AutoResumeDecision> {
  const work = Promise.resolve().then(decide)
  if (timeoutMs === undefined || !Number.isFinite(timeoutMs) || timeoutMs <= 0) return work
  return new Promise((resolve, reject) => {
    let settled = false
    const finish = (settle: () => void) => {
      if (settled) return
      settled = true
      clearTimeout(timer)
      settle()
    }
    const timer = setTimeout(() => finish(() => resolve(onTimeout())), timeoutMs)
    work.then(
      (decision) => finish(() => resolve(decision)),
      (error) => finish(() => reject(error)),
    )
  })
}

export function createSessionHub(options: SessionHubOptions): SessionHub {
  const log = options.log ?? noopWorkerLogger
  const cache = options.cache ?? new StateCache<unknown>()
  const db = options.db
  // Persist every typed crash through the hub's off-loop executor unless the
  // caller supplied its own sink; a hub with no executor has no sink. The
  // payload returned to the caller is the same value (no sync hub SQL here).
  const onWorkerCrashed =
    options.onWorkerCrashed ??
    (db === undefined ? undefined : (payload: WorkerCrashed) => recordAsync(db, payload).then(() => undefined))

  const surfaceWorkerCrash = async (error: unknown, sessionID: SessionID): Promise<void> => {
    const payload = workerCrashedFrom(error)
    if (payload === undefined) {
      if (errorTagOf(error) === "WorkerCrashedError") {
        log(workerLogMessage("worker crash payload missing"), {
          level: "ERROR",
          sessionID,
          expected: {
            sessionID,
            workerID: "number",
            phase: WORKER_CRASH_PHASES.join(" | "),
            lastCommittedMessageID: "string | null",
            inFlightCallIDs: "readonly string[]",
            retryable: false,
            reason: "string",
          },
        })
      }
      return
    }
    log(workerLogMessage("worker crashed"), {
      level: "ERROR",
      sessionID: payload.sessionID,
      workerID: payload.workerID,
      phase: payload.phase,
      retryable: payload.retryable,
      reason: payload.reason,
    })
    if (onWorkerCrashed === undefined) return
    try {
      await onWorkerCrashed(payload)
    } catch (sinkError) {
      // Persistence failure must never replace the crash the caller observes.
      log(workerLogMessage("worker crash persistence failed"), {
        level: "ERROR",
        sessionID: payload.sessionID,
        error: sinkError instanceof Error ? sinkError.message : String(sinkError),
      })
    }
  }
  // The port authorities are built on the first attach (never at construction):
  // a value is used as-is; a source function is invoked once and memoised.
  const portSource = options.ports
  let resolvedBackends: WorkerPortBackends | undefined
  let backendsResolved = false
  const portBackends = (): WorkerPortBackends | undefined => {
    if (portSource === undefined) return undefined
    if (typeof portSource !== "function") return portSource
    if (!backendsResolved) {
      resolvedBackends = portSource()
      backendsResolved = true
    }
    return resolvedBackends
  }
  // The backends to dispose at teardown, or `undefined` when none exist: a value
  // source is disposed directly; a function source is disposed only if it was
  // actually built (a lazy source that never attached owns no resources).
  const disposables = (): WorkerPortBackends | undefined => {
    if (typeof portSource !== "function") return portSource
    return backendsResolved ? resolvedBackends : undefined
  }
  // Assigned once the pool exists; read lazily by the routing closures below.
  let dispatcher: Dispatcher
  // Assigned once the registry exists (below); read lazily by the attach thunks.
  let ports: WorkerPortRegistry

  ports = createWorkerPortRegistry({
    log,
    backends: portBackends,
    hubProxyHandlersFor,
    hubProxyStreamingFor,
    onPidReport: options.onPidReport,
  })

  /**
   * The served hub-proxy handler map for ONE worker: the caller's authorities,
   * built per attach so a worker reads its OWN session context. A static
   * `hubProxy` remains the fallback for a plain test hub.
   */
  function hubProxyHandlersFor(workerID: number): HubProxyHandlers | undefined {
    const backends = portBackends()
    const handlers = backends?.hubProxyFor?.(workerID) ?? backends?.hubProxy
    if (handlers === undefined) return undefined
    // The hub owns the lifecycle truth. Every report is bound to the session the
    // REPORTING worker is registered to: a worker cannot move another session's
    // commit/tool/phase state by naming it over the control port.
    const boundSessionOf = (sessionID: unknown): SessionID => {
      const reported = String(sessionID)
      const bound = dispatcher.registry.sessionsOn(workerID)[0]
      if (bound === undefined || reported !== bound) {
        throw new ForeignLifecycleReportError(workerID, bound, reported)
      }
      return bound
    }
    // Serving these on every control port (not only the production authority
    // map) means a worker's real prompt/tool path reports commit/tool/phase
    // into the same tracker the crash path reads.
    return {
      ...handlers,
      "lifecycle.commit": (sessionID, messageID) => {
        const bound = boundSessionOf(sessionID)
        lifecycle.commit(bound, String(messageID))
        lifecycle.phase(bound, "commit")
        // The durable commit is the real `commits` report. The projectors run
        // inside that same commit transaction (verified by projection), so the
        // one observation is also the projector report.
        const run = activeRuns.get(bound)
        run?.commit(String(messageID))
        run?.projector("session-row")
      },
      "lifecycle.phase": (sessionID, phase) => {
        const bound = boundSessionOf(sessionID)
        if (isWorkerCrashPhase(phase)) lifecycle.phase(bound, phase)
      },
      "lifecycle.toolStarted": (sessionID, callID, sideEffecting) => {
        const bound = boundSessionOf(sessionID)
        lifecycle.toolStarted(bound, String(callID), sideEffecting !== false)
        lifecycle.phase(bound, "tool")
      },
      "lifecycle.toolFinished": (sessionID, callID) => {
        const bound = boundSessionOf(sessionID)
        lifecycle.toolFinished(bound, String(callID))
        // `tools` is observed only when the LAST in-flight call settles; a
        // single finished call among several does not drain the phase.
        if (lifecycle.indeterminate(bound).length === 0) activeRuns.get(bound)?.tool(String(callID))
      },
    }
  }

  /**
   * The streaming half of a worker's control port. It cannot be mixed into
   * `handlers` (a method is served by one or the other), so it rides the separate
   * `streaming` map `serveHubProxy` accepts. A source backend may specialize by
   * worker; a static `hubProxyStreaming` is the fallback.
   */
  function hubProxyStreamingFor(workerID: number): Readonly<Record<string, HubProxyStreamHandler>> | undefined {
    const backends = portBackends()
    return backends?.hubProxyStreamingFor?.(workerID) ?? backends?.hubProxyStreaming
  }

  const pool = options.pool
  const resolvedCancellation =
    typeof options.cancellation === "function" ? options.cancellation(pool) : options.cancellation
  // Every terminal cancel mode the dispatcher reports, keyed by session id. The
  // dispatcher cascades children-first but discards a descendant's own outcome,
  // so the reporter records the modes and reconstructs which descendants still
  // need a fallback idle (see `cancelWithReport`).
  // The child edge is synchronous by contract: the dispatcher reads it on the
  // crash/timeout path, where a `Session.children` query would block. The
  // production layer supplies its spawn-observed index; a caller may override
  // it (tests, or a caller-driven tree walk) through `cancellation.childrenOf`.
  const childrenOf = resolvedCancellation?.childrenOf ?? (() => [])
  const cancelReporter = createCancelReporter({
    dispatcher: () => dispatcher,
    childrenOf,
    events: options.events,
    ...defined({ eventPublisherFor: options.eventPublisherFor, onCancelRequested: options.onCancelRequested }),
    log,
  })
  const baseOnCancelled = resolvedCancellation?.onCancelled
  const cancellation: CancellationConfig | undefined =
    resolvedCancellation === undefined
      ? undefined
      : {
          ...resolvedCancellation,
          childrenOf,
          onCancelled: (event) => {
            cancelReporter.recordCancelled(event.sessionID, event.mode)
            baseOnCancelled?.(event)
          },
        }
  const lifecycle = options.lifecycle ?? createLifecycleTracker()
  const resumeDecisionTimeoutMs = options.resumeDecisionTimeoutMs ?? DEFAULT_RESUME_DECISION_TIMEOUT_MS
  const awaitBootGate = options.bootGate ?? (() => Promise.resolve())
  const barrier = options.barrier ?? createRunBarrier()
  // A TEST SEAM only. Production omits it and reports observations to the
  // per-run handle opened by `barrier.beginRun` below.
  const barrierPhases = options.barrierPhases
  const promotionOutcome =
    options.promotionOutcome ?? ((_sessionID: SessionID): RunBarrierPromotionOutcome => "no-job")
  // The report handle for each session's current run. A late report is rejected
  // by the barrier's epoch check, so a superseded run cannot settle its successor.
  const activeRuns = new Map<SessionID, RunBarrierRun>()

  /**
   * The dispatcher holds a completed run's slot until the barrier observes all
   * four phases. Nothing is settled by assumption: `commits`/`projectors` come
   * from the bound commit reporter, `tools` from the bound tool reporter (or the
   * explicit zero-tool decision here), and `promotion` from the decision the hub
   * actually observed. A phase with no report keeps the barrier closed until its
   * typed timeout; cancel still wins.
   */
  const runBarrier = async (sessionID: SessionID): Promise<void> => {
    await options.writeBarrier?.(sessionID)
    const run = activeRuns.get(sessionID)
    if (run !== undefined) {
      // A run that observed no tool call is settled explicitly. A started call
      // that never finished is NOT settled: the missing report times the barrier
      // out instead of letting the next run overtake it.
      if (run.missing().includes("tools") && lifecycle.indeterminate(sessionID).length === 0) run.noTools()
      // A run can end without a durable commit: a shell turn, a loop that exits
      // before its first model step, or a compaction stop. The commit-side
      // phases are settled as an explicit `none` (never a fabricated commit), so
      // the next run is admitted instead of waiting out the 5 s bound and then
      // refusing every prompt until the idle reaper retires the worker.
      if (run.missing().includes("commits")) run.noCommit()
      run.promotion(promotionOutcome(sessionID))
    }
    try {
      if (barrierPhases === undefined) await barrier.wait(sessionID)
      else await barrier.wait(sessionID, barrierPhases(sessionID))
    } finally {
      activeRuns.delete(sessionID)
    }
  }

  /**
   * Sessions whose owning generation was retired (crash, hard terminate, idle
   * reap). Its in-flight tool calls can never settle, so the next admitted run
   * releases them instead of letting them time out its barrier. Only a lost
   * generation grants that release: a live run's calls always hold the barrier.
   */
  const lostGenerations = new Set<SessionID>()

  /**
   * A compiled build mismatch is permanent for this entry: respawning the same
   * bundle crash-loops. The typed error is kept until the resolved build hash
   * changes, so a retry before a rebuild fails fast with the same typed cause.
   */
  const blockedBuilds = new Map<SessionID, BuildMismatch>()
  const assertBuildAdmissible = (sessionID: SessionID): void => {
    const blocked = blockedBuilds.get(sessionID)
    if (blocked === undefined) return
    if (blocked.expected !== resolveBuildHash().hash) {
      blockedBuilds.delete(sessionID)
      return
    }
    throw blocked
  }

  /**
   * A retired generation can never run `dispose()` (crash, hard terminate), so
   * its worker lease would otherwise stay live until the 30 s TTL and the
   * immediate successor's `Bus.reclaim` would fail with `Bus.LeaseConflict`.
   * Retirement is the authoritative "this generation is dead" signal, so mark
   * its lease expired here. The update is scoped to the retired generation's
   * exact `worker_owner_id`; a successor that already claimed the aggregate must
   * never have its live lease zeroed.
   */
  const retiredLeaseReleases = new Map<SessionID, Promise<void>>()

  const queueRetiredLeaseRelease = (sessionID: SessionID, task: () => Promise<void>): Promise<void> => {
    const previous = retiredLeaseReleases.get(sessionID) ?? Promise.resolve()
    const next = previous.then(task).catch((error: unknown) => {
      log(workerLogMessage("retired lease release failed"), {
        level: "WARN",
        sessionID,
        error: error instanceof Error ? error.message : String(error),
      })
    })
    retiredLeaseReleases.set(sessionID, next)
    void next.finally(() => {
      if (retiredLeaseReleases.get(sessionID) === next) retiredLeaseReleases.delete(sessionID)
    })
    return next
  }

  /**
   * Release the retired generation's lease. The `workerID` prefix is its stable
   * generation identity (`worker-<id>-<pid>-<uuid>`); the exact owner is read
   * from the row and passed to the scoped update so a successor's lease is
   * untouched even if it raced the retirement.
   */
  const releaseRetiredLease = (sessionID: SessionID, workerID?: number): Promise<void> => {
    if (db === undefined || workerID === undefined) return Promise.resolve()
    const generationPrefix = `worker-${workerID}-`
    return queueRetiredLeaseRelease(sessionID, async () => {
      const rows = await db.query("SELECT worker_owner_id FROM event_sequence WHERE aggregate_id = ?", [sessionID])
      const owner = rows[0]?.worker_owner_id
      if (typeof owner !== "string" || !owner.startsWith(generationPrefix)) return
      await releaseRetiredWorkerLease(db, sessionID, owner)
    })
  }

  /** Successor admission waits for the retired generation's release to land. */
  const awaitRetiredLeaseRelease = (sessionID: SessionID): Promise<void> =>
    retiredLeaseReleases.get(sessionID) ?? Promise.resolve()

  /**
   * The one hub-side retirement for a worker generation. Both the dispatcher's
   * `onSessionReleased` (crash/reap/cancel) and the pool's in-place generation
   * replacement (a mid-session model switch) funnel through here, so every
   * retired generation releases the same state exactly once.
   */
  const releaseGeneration = async (sessionID: SessionID, workerID?: number): Promise<void> => {
    // This generation can never settle another phase: the next run must not
    // inherit its in-flight calls.
    lostGenerations.add(sessionID)
    try {
      // Retirement disposes this worker's hub-side port servers immediately,
      // rather than leaving them alive until shutdown.
      if (workerID !== undefined) ports.disposeWorker(workerID)
      await options.onSessionReleased?.(sessionID)
    } finally {
      // The lease release is the retirement's contract, not a best-effort
      // follow-up: even a throwing port disposer or release hook must not let a
      // successor boot against the old generation's live lease.
      await releaseRetiredLease(sessionID, workerID)
    }
  }

  /**
   * A timed-out barrier refuses the next run while its worker is still alive
   * (the run may still settle). Once that worker has been retired — a crash or a
   * hard terminate — no fiber can ever settle the phases, so the stale barrier is
   * abandoned instead of wedging the Session forever with `RunBarrierTimeout`.
   */
  const admitBarrier = (sessionID: SessionID): void => {
    const state = barrier.state(sessionID)
    if (state === "timed-out") {
      if (dispatcher.registry.worker(sessionID) === undefined) {
        lostGenerations.add(sessionID)
        barrier.abandon(sessionID)
        return
      }
      throw new RunBarrierTimeout(sessionID, RUN_BARRIER_TIMEOUT_MS)
    }
    if (state === "cancelled" || state === "failed") barrier.reset(sessionID)
  }

  dispatcher = createDispatcher({
    pool,
    crashState: (sessionID) => lifecycle.crashState(sessionID),
    recordCrash: (payload) => lifecycle.observeCrash(payload),
    resumeDecision: (payload) => {
      // A compiled build mismatch is permanent for this entry: respawning the
      // same bundle would crash-loop. The dispatcher carries the typed cause on
      // the in-memory payload, so no string matching is involved; keep the
      // session blocked until the resolved build changes.
      const cause = (payload as { readonly cause?: unknown }).cause
      if (cause instanceof BuildMismatch) {
        blockedBuilds.set(payload.sessionID as SessionID, cause)
        return { resume: false, sessionID: payload.sessionID, phase: payload.phase, reason: "phase" }
      }
      const decide = (indeterminate: readonly IndeterminateCall[]) =>
        decideAutoResume({
          sessionID: payload.sessionID,
          phase: payload.phase,
          attempts: lifecycle.attempts(payload.sessionID),
          cancelled: lifecycle.cancelled(payload.sessionID),
          lastCommittedMessageID: payload.lastCommittedMessageID,
          indeterminate,
        })
      // The durable `tool_call` rows are the crash truth. The tracker is only a
      // mirror: a worker can die between its durable write and the report, and a
      // finished row whose report was lost must not falsely block resume.
      if (db === undefined) return decide(lifecycle.indeterminate(payload.sessionID))
      return boundedResumeDecision(
        () =>
          db
            .query(
              "SELECT call_id, side_effecting FROM tool_call WHERE session_id = ? AND state = 'started' ORDER BY time_started, call_id",
              [payload.sessionID],
            )
            .then((rows) =>
              decide(
                rows.map((row) => ({
                  callID: String(row.call_id),
                  sideEffecting: Number(row.side_effecting) === 1,
                })),
              ),
            )
            .catch(() => decide(lifecycle.indeterminate(payload.sessionID))),
        resumeDecisionTimeoutMs,
        () => {
          log(workerLogMessage("resume decision timed out"), {
            level: "WARN",
            sessionID: payload.sessionID,
            timeoutMs: resumeDecisionTimeoutMs,
          })
          return {
            resume: false,
            sessionID: payload.sessionID,
            phase: payload.phase,
            reason: "indeterminate",
          }
        },
      )
    },
    onAutoResume: (decision, payload) => {
      // Cancel always wins: a decision computed before the cancel but resolved
      // after it must never dispatch a successor run.
      if (lifecycle.cancelled(decision.sessionID)) {
        log(workerLogMessage("auto-resume refused after cancel"), {
          level: "INFO",
          sessionID: decision.sessionID,
        })
        return
      }
      lifecycle.recordAttempt(decision.sessionID)
      if (options.resumeRun !== undefined) {
        // The dispatcher settles the original crashed caller from this promise,
        // so its failure must reach that caller instead of being swallowed into
        // a generic "no session result recorded".
        return options.resumeRun(decision, payload)
      }
      // No explicit resume owner: run the successor detached. The dispatcher
      // cannot observe this run's outcome without replacing the crashed caller's
      // typed `WorkerCrashedError` with it, which would hide the crash. Keep the
      // crash authoritative for this caller; a deliberate replay wires
      // `resumeRun` above and can forward its outcome instead.
      void dispatch
        .ensureRunning(decision.sessionID as SessionID)
        .then(() => lifecycle.resetAttempts(decision.sessionID))
        .catch((error: unknown) => {
          log(workerLogMessage("auto-resume failed"), {
            level: "WARN",
            sessionID: decision.sessionID,
            error: error instanceof Error ? error.message : String(error),
          })
        })
      return undefined
    },
    writeBarrier: runBarrier,
    assertAdmissible: assertBuildAdmissible,
    onRunStart: (sessionID) => {
      // A lost generation's calls can never settle. Release them into the OLD
      // phases first (so the fresh `tools` phase below is not pre-resolved and
      // still gates calls started during this run), then start the generation.
      if (lostGenerations.delete(sessionID)) {
        for (const call of lifecycle.indeterminate(sessionID)) lifecycle.toolFinished(sessionID, call.callID)
      }
      lifecycle.beginRun(sessionID)
      // A run whose report set cannot be opened still runs; its barrier then
      // fails typed rather than wedging the dispatcher with an unhandled throw.
      try {
        activeRuns.set(sessionID, barrier.beginRun(sessionID))
      } catch (error) {
        activeRuns.delete(sessionID)
        log(workerLogMessage("run barrier begin failed"), {
          level: "WARN",
          sessionID,
          error: error instanceof Error ? error.message : String(error),
        })
      }
    },
    onSessionReleased: async (sessionID, workerID) => {
      await releaseGeneration(sessionID, workerID)
    },
    ...defined({
      cancellation,
      reapMs: options.reapMs,
      schedule: options.schedule,
      log,
    }),
  })
  // Depth is carried explicitly on the run (`metadata.depth` -> Session.spawn).
  // The production hub resolves it per session; a plain hub without the seam is a
  // foreground root (0). Clamp so malformed metadata cannot grow the cap's depth
  // map without bound or claim an absurd level.
  const dispatch: SessionDispatch = createSessionDispatch({ dispatcher, pool })
  const onDispatch = options.onDispatch
  const resolveDepth = (sessionID: SessionID): number =>
    options.depthOf === undefined ? 0 : boundedDispatchDepth(options.depthOf(sessionID))

  return {
    dispatcher,
    cache,
    // The inverted key→worker map: a worker's session, or undefined when idle.
    sessionOfWorker: (workerID) => dispatcher.registry.sessionsOn(workerID)[0],
    attachWorkerPorts: ports.attachWorkerPorts,
    attachHubProxyPort: ports.attachHubProxyPort,
    releaseGeneration,
    lifecycle,
    barrier,
    ensureRunning: async (sessionID, origin) => {
      // Capture the caller's ambient context synchronously; awaiting first would
      // resume outside this fiber and lose it.
      onDispatch?.(sessionID)
      // The off-loop schema gate must complete before any isolate can spawn.
      await awaitBootGate()
      admitBarrier(sessionID)
      assertBuildAdmissible(sessionID)
      // A retired generation's release must land before its successor boots and
      // claims the aggregate; otherwise the successor can be admitted against a
      // still-live lease.
      await awaitRetiredLeaseRelease(sessionID)
      try {
        const result = await dispatch.ensureRunning(sessionID, resolveDepth(sessionID), origin)
        blockedBuilds.delete(sessionID)
        lifecycle.resetAttempts(sessionID)
        return result
      } catch (error) {
        await surfaceWorkerCrash(error, sessionID)
        throw error
      }
    },
    startShell: async (sessionID, shell) => {
      onDispatch?.(sessionID)
      await awaitBootGate()
      admitBarrier(sessionID)
      assertBuildAdmissible(sessionID)
      await awaitRetiredLeaseRelease(sessionID)
      try {
        const result = await dispatch.startShell(sessionID, shell, resolveDepth(sessionID))
        blockedBuilds.delete(sessionID)
        lifecycle.resetAttempts(sessionID)
        return result
      } catch (error) {
        await surfaceWorkerCrash(error, sessionID)
        throw error
      }
    },
    assertNotBusy: async (sessionID) => {
      const workerID = dispatcher.registry.entry(sessionID)?.workerID
      const worker = workerID === undefined ? undefined : pool.byID(workerID)
      // A proven-dead isolate is idle even while its run is still registered:
      // the registry reservation cannot outlive the worker it describes.
      if (worker !== undefined && !worker.alive) return false
      // An admitted run is busy even before its worker materializes: a run queued
      // in `cap.acquire` has no isolate to probe, so the registry reservation is
      // the only truth. Reporting idle there would let a second prompt mutate a
      // session whose run is still waiting for a slot.
      if (dispatcher.registry.get(sessionID) !== undefined) return true
      // Only a proven-dead isolate is idle. A live worker that cannot answer the
      // probe (timeout, broken control channel) must fail closed: the caller
      // must not treat an unproven channel as permission to mutate the session.
      if (worker === undefined) return false
      return worker.assertNotBusy(sessionID)
    },
    cancel: async (sessionID) => {
      lifecycle.cancel(sessionID)
      barrier.cancel(sessionID)
      return (await cancelReporter.cancelWithReport(sessionID)).outcome
    },
    cancelWithReport: async (sessionID) => {
      lifecycle.cancel(sessionID)
      barrier.cancel(sessionID)
      return cancelReporter.cancelWithReport(sessionID)
    },
    cancelAll: async () => {
      for (const entry of dispatcher.registry.entries()) {
        lifecycle.cancel(entry.sessionID)
        barrier.cancel(entry.sessionID)
      }
      return dispatch.cancelAll()
    },
    async terminateAll() {
      ports.disposeAll()
      await dispatcher.terminateAll()
      cache.clear()
      // Only tear down backends we actually built (a source never attached to is
      // never constructed, so no isolate is spawned just to dispose it).
      const backends = disposables()
      await backends?.dispose?.()
      await db?.close().catch(() => undefined)
    },
  }
}

export * from "./cancel-report"
export * from "./hub-proxy-authorities"
export * from "./production-hub"

// `worker-pools` and `worker-ports` both model the service idiom, so star-exporting
// both makes `Service`, `layer`, and `node` ambiguous (TS2308). Re-export every
// public symbol explicitly and qualify the colliding names by module; the
// `WorkerPools` / `WorkerPorts` namespaces still expose them unqualified
// (`WorkerPools.Service`, `WorkerPorts.layer`, ...).
export {
  type BunExecutorTransportOptions,
  type BunExecutorWorkerPoolOptions,
  DEFAULT_RUN_IDLE_TIMEOUT_MS,
  DEFAULT_SPAWN_GATE_TIMEOUT_MS,
  DEFAULT_TRANSPORT_BUFFER_CAPACITY,
  type DescribedSessionWorkerEntry,
  type FromPoolOptions,
  type IdleBoundedExecutorWorkerPool,
  type IdleIsolateCap,
  type IdleIsolateCapOptions,
  type IdleWorkerEntry,
  type Interface,
  type ProductionPoolFactory,
  type ProductionWorkerPoolOptions,
  SESSION_WORKER_ENTRY,
  Service as WorkerPoolsService,
  type SessionBoundSpawner,
  type SessionWorkerEntrySource,
  type SessionWorkerInitSource,
  type WorkerCap,
  type WorkerGeneration,
  type WorkerPoolSinks,
  WorkerPools,
  type WorkerPoolsStats,
  createBunExecutorTransport,
  createBunExecutorWorkerPool,
  createIdleIsolateCap,
  createProductionWorkerPool,
  createSessionBoundExecutorWorker,
  createWorkerCap,
  describeSessionWorkerEntry,
  fromPool,
  layer as WorkerPoolsLayer,
  liveSessionWorkerCount,
  logRunIdleTimeoutWarning,
  makeProduction,
  node as WorkerPoolsNode,
  resolveIdleWorkerCapacity,
  resolveRunIdleTimeoutMs,
  resolveSessionWorkerEntry,
  runIdleTimeoutWarning,
  spawnBunExecutorWorker,
  withSessionWorkerPorts,
  workerShutdownStats,
} from "./worker-pools"

export {
  Service as WorkerPortsService,
  type WorkerPortBackends,
  type WorkerPortBackendsSource,
  WorkerPorts,
  type WorkerPortRegistry,
  type WorkerPortRegistryOptions,
  type WorkerPortsDiagnostics,
  type WorkerPortsOptions,
  type WorkerTransferredPorts,
  createWorkerPortRegistry,
  drainEgressPort,
  isPidReport,
  layer as WorkerPortsLayer,
  make,
  node as WorkerPortsNode,
  servePidReportPort,
} from "./worker-ports"

export * as SessionHubModule from "./hub"
