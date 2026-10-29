// The pure hub.

import { Cause, Context, Effect, Exit, Fiber, Schema } from "effect"
import path from "node:path"
import { InstanceRef } from "../../effect/instance-ref"
import { SessionStatus } from "../status"
import { Permission } from "../../permission"
import { Question } from "../../question"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { createEgressPort } from "../egress/receiver"
import { type DurableCommit } from "../egress/mesh"
import {
  durableAggregateMetaOf,
  isEventBlobReference,
  type EventBlobPosition,
  type EventV2Event,
  type EventsAckPayload,
} from "./frame"
import { encodeFrame, resyncWithPositionFrame, type DurableResyncPayload } from "../egress/frame"
import { type LiveEventPublisher } from "../../event-v2-bridge"
import type { EventV2 } from "@opencode-ai/core/event"
import { Durable } from "@opencode-ai/schema/durable-event-manifest"
import { DurableLog, StateCache } from "@opencode-ai/core/durable-log"
import type { SessionID } from "../schema"
import { type DbExecutor } from "./db-executor"
import type { DispatchOrigin } from "../origin"
import { defined } from "../../util/object"
import {
  createDispatcher,
  type CancellationConfig,
  type CommitOutcome,
  type Dispatcher,
  type DispatcherOptions,
} from "./dispatch"
import { createLifecycleTracker, type LifecycleTracker } from "../ledger/lifecycle"
import { decideAutoResume, type AutoResumeDecision, type IndeterminateCall } from "../ledger/resume"
import {
  RUN_BARRIER_TIMEOUT_MS,
  RunBarrier,
  RunBarrierTimeout,
  type RunBarrierPromotionOutcome,
  type RunBarrierRun,
} from "../run-barrier"
import {
  createSessionDispatch,
  type ExecutorWorkerPool,
  type SessionDispatch,
  type SessionExecutionResult,
} from "./execute"
import { type CancelOutcome } from "./cancel"
import { type PidRegistry, type PidReport } from "./pid-registry"
import type { HubProxyHandlers, HubProxyStreamHandler } from "./hub-proxy-wire"
import { serveHubProxy } from "./hub-proxy-wire"
import { errorTagOf } from "./port"
import type { EffectBridge } from "@/effect/bridge"
import { workerLogMessage } from "./worker-logging"
import { Session } from "../session"
import type { SessionPrompt } from "../prompt"
import type { ShellInput } from "../prompt"
import { isWorkerCrashPhase, recordAsync } from "../ledger/crash"

import { createCancelReporter, type CancelReport } from "./cancel-report"
import {
  makeAttachments,
  type Attachments,
  type WorkerPortBackends,
  type WorkerPortBackendsSource,
  type WorkerPorts as WorkerPortsInterface,
  type WorkerTransferredPorts,
} from "./worker-ports"

/** Advisory hub cache. */
export type HubTruthCache = StateCache<unknown>

/** The invalidator a durable event source calls after a commit. */
export interface HubCacheInvalidation {
  invalidate(aggregateID: string): void
}

/** The five crash phases a worker reports (D8). */
export const WORKER_CRASH_PHASES = ["boot", "materialize", "stream", "tool", "commit"] as const
export type WorkerCrashPhase = (typeof WORKER_CRASH_PHASES)[number]

const WORKER_CRASH_PHASE_SET: ReadonlySet<string> = new Set(WORKER_CRASH_PHASES)

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

/** Read the typed `WorkerCrashed` payload a crash error carries. */
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
  return isWorkerCrashed(candidate.payload) ? candidate.payload : undefined
}

/** A lifecycle report whose target is not the reporting worker's own session. */
export class ForeignLifecycleReportError extends Schema.TaggedErrorClass<ForeignLifecycleReportError>(
  "ForeignLifecycleReportError",
)("WorkerHub.ForeignLifecycleReport", {
  workerID: Schema.Number,
  boundSessionID: Schema.optional(Schema.String),
  reportedSessionID: Schema.String,
}) {
  constructor(
    override readonly workerID: number,
    override readonly boundSessionID: SessionID | undefined,
    override readonly reportedSessionID: string,
  ) {
    super({ workerID, boundSessionID, reportedSessionID })
    this.name = "ForeignLifecycleReportError"
  }
  override get message() {
    return (
      `worker ${this.workerID} is bound to ${this.boundSessionID ?? "no session"}; ` +
      `refusing a lifecycle report for ${this.reportedSessionID}`
    )
  }
}

/** The hub surface `run-state.ts` consumes when the rollout flag is on. */
export interface SessionHub {
  readonly dispatcher: Dispatcher
  /** The hub-observed run lifecycle truth (last committed message, in-flight tool calls, barrier phases). */
  readonly lifecycle: LifecycleTracker
  /** The per-session run barrier the next run waits on. */
  readonly barrier: RunBarrier.Interface
  /** The advisory hub cache. */
  readonly cache?: HubTruthCache
  /** The session a worker is currently bound to (the dispatcher's key→worker map, inverted). */
  sessionOfWorker(workerID: number): SessionID | undefined
  /** Serve the worker's transferred egress ports against the hub's authorities. */
  attachWorkerPorts(workerID: number, ports: WorkerTransferredPorts): () => void
  /** Serve the three hub-proxy authorities (`SessionStatus` / `Permission` / `Question`) over a worker's control port. */
  attachHubProxyPort(workerID: number, port: MessagePort): () => void
  /** Retire one generation of a session's worker that the pool replaces in place (a mid-session model switch). */
  releaseGeneration(sessionID: SessionID, workerID?: number): Promise<void>
  /** Dispatch `sessionID` to a worker and resolve with its result (admission-aware). */
  ensureRunning(sessionID: SessionID, origin?: DispatchOrigin): Promise<SessionExecutionResult>
  /** Effect-native ensureRunning consumed by `SessionRunState`. */
  ensureRunningEffect(sessionID: SessionID, origin?: DispatchOrigin): Effect.Effect<SessionExecutionResult, unknown>
  /** Dispatch a shell turn. */
  startShell(sessionID: SessionID, shell?: ShellInput): Promise<SessionExecutionResult>
  assertNotBusy(sessionID: SessionID): Promise<boolean>
  /** The UI-stop path: cooperative checkpoint, then terminate after the grace window. */
  cancel(sessionID: SessionID): Promise<CancelOutcome | undefined>
  /** Cancel a session and its subagent cascade, returning the requested session's outcome AND every cascaded descendant with whether its worker published. */
  cancelWithReport(sessionID: SessionID): Promise<CancelReport>
  cancelAll(): Promise<void>
  terminateAll(): Promise<void>
}

export class SessionHubService extends Context.Service<SessionHubService, SessionHub>()("@opencode/SessionHub") {}

/** Cancellation wiring, either pre-built or derived from the materialised pool (the production case needs the pool to look up a worker's `cancelPort`). */
export type CancellationSource = CancellationConfig | ((pool: ExecutorWorkerPool) => CancellationConfig)

/** The hub's per-worker handler seams. */
export interface WorkerPortSeams {
  readonly hubProxyHandlersFor: (workerID: number) => HubProxyHandlers | undefined
  readonly hubProxyStreamingFor: (workerID: number) => Readonly<Record<string, HubProxyStreamHandler>> | undefined
  readonly onPidReport?: (workerID: number, report: PidReport) => void
}

export interface SessionHubOptions {
  /** The executor pool; production workers spawn on the first dispatch, not at boot. */
  readonly pool: ExecutorWorkerPool
  /** Hub-resident authorities for the worker egress ports and the hub-proxy control port. */
  readonly ports?: WorkerPortBackendsSource
  readonly workerPorts?: WorkerPortsInterface
  /** Receives the hub's per-worker handler seams so a service constructed before the hub can serve the same lifecycle-bound handlers as the private. */
  readonly bindWorkerPortSeams?: (seams: WorkerPortSeams) => void
  /** Invoked synchronously at the start of every `ensureRunning`/`startShell`, before any async work, with the session being dispatched. */
  readonly onDispatch?: (sessionID: SessionID) => void
  /** Resolve a session's subagent depth (`metadata.depth`) for the dispatch path. */
  readonly depthOf?: (sessionID: SessionID) => number | undefined
  readonly cancellation?: CancellationSource
  /** Release one retired generation's hub-owned resources (ports, pending asks bound to the worker, lease). */
  readonly onGenerationReleased?: (sessionID: SessionID, workerID?: number) => unknown
  /** Deadline for the retired generation's lease UPDATE only. */
  readonly retiredLeaseReleaseTimeoutMs?: number
  /** Release one session's hub-owned state when the session actually leaves the hub (reap, delete, explicit shutdown). */
  readonly onSessionReleased?: (sessionID: SessionID) => unknown
  /** Idle-reap window forwarded to the dispatcher. */
  readonly reapMs?: number
  /** Scheduler seam for the dispatcher's idle-reap timer (tests). */
  readonly schedule?: DispatcherOptions["schedule"]
  /** Per-session durable-write barrier: the dispatcher holds a completed run's slot until this resolves. */
  readonly writeBarrier?: DispatcherOptions["writeBarrier"]
  /** Live-only event publisher. */
  readonly events?: LiveEventPublisher
  /** Resolve the live publisher for a specific session, so `session.cancelled` carries THAT session's location instead of a first-dispatch one. */
  readonly eventPublisherFor?: (sessionID: SessionID) => LiveEventPublisher | undefined
  /** Invoked at the start of every cancel, before the grace window, for the session and each descendant. */
  readonly onCancelRequested?: (sessionID: SessionID) => void
  /** Receive a worker's `{type:"pid-report"}` frame from its control port. */
  readonly onPidReport?: (workerID: number, report: PidReport) => void
  /** Off-loop fallback for hub-side SQL. */
  readonly db?: DbExecutor
  /** Advisory cache, never authoritative. Defaults to a fresh, empty cache. */
  readonly cache?: HubTruthCache
  readonly onWorkerCrashed?: (payload: WorkerCrashed) => void | Promise<void>
  /** The run lifecycle truth shared with the tool/commit paths. */
  readonly lifecycle?: LifecycleTracker
  /** The run barrier. */
  readonly barrier: RunBarrier.Interface
  /** The background-promotion decision the hub observed for a session at run end. */
  readonly promotionOutcome?: (sessionID: SessionID) => RunBarrierPromotionOutcome
  /** Re-dispatch after an allowed auto-resume. */
  readonly resumeRun?: (
    decision: Extract<AutoResumeDecision, { resume: true }>,
    payload: WorkerCrashed,
  ) => void | Promise<void>
  /** Upper bound (ms) on the dispatcher's asynchronous resume decision. */
  readonly resumeDecisionTimeoutMs?: number
  /** An async boot gate awaited before the first dispatch (the production hub's off-loop schema ensure). */
  readonly bootGate?: () => Promise<void>
  /** Optional structured logger seam; absent defaults to a no-op (flag-off behavior). */
  readonly bridge?: EffectBridge.Shape
  /** Layer-scoped runtime for the Promise-shaped hub callbacks; absent uses the default runtime. */
  readonly runPromise?: <A, E>(effect: Effect.Effect<A, E>) => Promise<A>
}

/** The canonical `session.cancelled` event type, sourced from the schema definition the hub publishes after a cancel resolves. */
export const SESSION_CANCELLED_TYPE = SessionV1.Event.Cancelled.type

/** Recover an over-frame durable event body by committed position. */
export type EventBlobResolver = (
  position: EventBlobPosition,
  event: { readonly type: string },
) => Promise<Record<string, unknown> | undefined>

export interface EventBlobResolverOptions {
  /** Async SQL over the same database file the worker wrote. */
  readonly query: (sql: string, params: ReadonlyArray<unknown>) => Promise<ReadonlyArray<Record<string, unknown>>>
  /** `dirname(dbFile)/event-blobs`, shared with the worker DurableLog and EventV2. */
  readonly blobDir: string
}

/** The committed inline body at one aggregate position, when no blob row exists. */
const INLINE_EVENT_QUERY = "SELECT data FROM event WHERE aggregate_id = ? AND seq = ?"

/** Parse a committed inline `event.data` value. */
function inlineEventDataOf(row: Record<string, unknown> | undefined): Record<string, unknown> | undefined {
  if (row === undefined) return undefined
  const data = row["data"]
  const decoded = typeof data === "string" ? JSON.parse(data) : data
  if (typeof decoded !== "object" || decoded === null || Array.isArray(decoded)) return undefined
  const record = decoded as Record<string, unknown>
  return DurableLog.isBlobReference(record) ? undefined : record
}

/** Build the production body resolver: first the `event_blob` row by position. */
export function createEventBlobResolver(options: EventBlobResolverOptions): EventBlobResolver {
  const blob: DurableLog.BlobStore = {
    dir: options.blobDir,
    limitBytes: DurableLog.DEFAULT_BLOB_LIMIT_BYTES,
    supportsTable: true,
  }
  return async (position, event) => {
    const query = DurableLog.blobRowQuery(position.aggregateID, [position.seq])
    const rows = await options.query(query.sql, query.params)
    const row = DurableLog.blobRowsBySeq(rows).get(position.seq)
    if (row !== undefined) {
      return DurableLog.resolveBlobRow(
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
  /** Resolve an oversized durable body before republishing. */
  readonly resolveBlob?: EventBlobResolver
  /** Read the committed events in a missing durable range so a gap is healed on the live surface instead of only counted. */
  readonly readDurable?: DurableHealReader
}

/** One committed durable event read back while healing a gap. */
export interface DurableHealEntry {
  readonly seq: number
  readonly event: EventV2Event
}

/** Read committed events in the bounded range `(after, upTo]`, ordered by sequence. */
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
  /** Also publish `seq` itself: the position was lost, not merely gapped. */
  readonly inclusive?: boolean
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

/** Build the production gap-heal reader over the durable `event` log. */
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

/** A worker's events-port sink. */
export interface HubEventSink {
  /** Republish one worker event live-only. */
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
  /** Heal a durable gap by reading `(after, seq)` from the committed log and republishing it live. */
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
  /** Resolve an oversized durable body by committed position. */
  readonly resolveBlob?: EventBlobResolver
}

/** The live event plus the durable identity that committed in the worker. */
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
        // The position is authoritative; only the schema version comes from the shipped manifest.
        version: Durable.get(event.type)?.durable?.version ?? 1,
      },
    })
    cache?.invalidate(aggregateID)
    lastDurable.set(aggregateID, Math.max(seq, lastDurable.get(aggregateID) ?? -1))
  }

  /** Recover a healed row's body, falling back to the committed blob/inline row. */
  const healBodyOf = async (event: EventV2Event, aggregateID: string, seq: number): Promise<unknown | undefined> => {
    const properties = event.properties
    if (isEventBlobReference(properties)) {
      return options.resolveBlob?.(properties.__eventBlobPosition, { type: event.type })
    }
    if (!DurableLog.isBlobReference(properties)) return properties
    return options.resolveBlob?.({ aggregateID, seq }, { type: event.type })
  }

  /** Read the bounded missing range page by page and publish each committed event in order. */
  const healRange = async (position: DurableHealPosition): Promise<void> => {
    if (readDurable === undefined) return
    const upTo = position.inclusive === true ? position.seq : position.seq - 1
    let cursor = position.after ?? lastDurable.get(position.aggregateID) ?? -1
    while (cursor < upTo) {
      let page: readonly DurableHealEntry[]
      try {
        page = await readDurable(position.aggregateID, cursor, upTo)
      } catch {
        // A failed read is recoverable by the worker's own resync; never throw into the ordered port chain.
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

  // Every heal runs behind the previous one, so a worker-reported resync and a receiver gap never publish the same aggregate concurrently.
  let healChain: Promise<void> = Promise.resolve()
  const serializedHeal = (position: DurableHealPosition): Promise<void> => {
    const next = healChain.then(() => healRange(position))
    healChain = next.catch(() => {})
    return next
  }

  const heal = (position: DurableHealPosition): void | Promise<void> => {
    resyncs += 1
    if (readDurable === undefined) return
    return serializedHeal(position)
  }

  return {
    ...(options.resolveBlob === undefined ? {} : { resolveBlob: options.resolveBlob }),
    heal,
    event(event, meta) {
      // Defense in depth: `serveEventsPort` resolves a position reference before calling here.
      if (isEventBlobReference(event.properties)) {
        resyncs += 1
        return
      }
      // The event crosses verbatim as `{ id, type, properties }`.
      const aggregateID = meta?.durable === true ? (meta.aggregateID ?? aggregateOf(event)) : undefined
      if (aggregateID === undefined) {
        publisher.publish({ id: event.id, type: event.type, data: event.properties })
        return
      }
      if (meta?.seq === undefined) {
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
      if (readDurable !== undefined && payload?.aggregateID !== undefined && payload.seq !== undefined) {
        void serializedHeal({ aggregateID: payload.aggregateID, seq: payload.seq, inclusive: true }).catch(() => {})
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

/** The production durable-egress barrier. */
export interface EgressDeliveryBarrier {
  /** One durable frame reached the hub's events port for `sessionID`. */
  observeDurable(sessionID: SessionID): void
  /** The worker acknowledged `count` durable hand-offs for `sessionID`. */
  observeAck(sessionID: SessionID, count?: number): void
  /** Resolve once every durable frame observed for the current run is acknowledged. */
  wait(sessionID: SessionID, expected?: number): Promise<void>
  /** Drop the current run's progress; `wait` also resets implicitly when it settles. */
  reset(sessionID: SessionID): void
  /** Forget the session entirely when it leaves the hub. */
  release(sessionID: SessionID): void
}

export interface EgressDeliveryBarrierOptions {
  /** Bound on a missing ack; defaults to {@link DEFAULT_EGRESS_BARRIER_TIMEOUT_MS}. */
  readonly timeoutMs?: number
  readonly bridge?: EffectBridge.Shape
}

export const DEFAULT_EGRESS_BARRIER_TIMEOUT_MS = 5_000

interface EgressWaiter {
  readonly expected: number | undefined
  readonly resolve: () => void
}

interface EgressProgress {
  /** Durables observed since the last ack (the current sender batch). */
  outstanding: number
  /** Diagnostics for the current run; reset when the run settles. */
  observed: number
  acked: number
  waiters: Set<EgressWaiter>
  timer: ReturnType<typeof setTimeout> | undefined
}

export function createEgressDeliveryBarrier(options: EgressDeliveryBarrierOptions = {}): EgressDeliveryBarrier {
  const timeoutMs = options.timeoutMs ?? DEFAULT_EGRESS_BARRIER_TIMEOUT_MS
  if (!Number.isInteger(timeoutMs) || timeoutMs < 1) {
    throw new Error(`invalid egress barrier timeout: ${timeoutMs} (expected a positive integer)`)
  }
  const bridge = options.bridge
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
    for (const waiter of state.waiters) waiter.resolve()
    state.waiters.clear()
  }

  const satisfied = (state: EgressProgress, expected: number | undefined): boolean =>
    state.outstanding === 0 && state.acked >= (expected ?? 0)

  /** Spend the acknowledgements a settled wait accounted for. */
  const consume = (state: EgressProgress, expected: number | undefined): void => {
    state.observed = 0
    state.acked = expected === undefined ? 0 : state.acked - expected
  }

  /** Release every waiter whose batch is now fully acknowledged. */
  const settle = (state: EgressProgress): void => {
    for (const waiter of state.waiters) {
      if (!satisfied(state, waiter.expected)) continue
      state.waiters.delete(waiter)
      consume(state, waiter.expected)
      waiter.resolve()
    }
    if (state.waiters.size > 0 || state.timer === undefined) return
    clearTimeout(state.timer)
    state.timer = undefined
  }

  return {
    observeDurable(sessionID) {
      const state = progressOf(sessionID)
      state.outstanding += 1
      state.observed += 1
    },
    observeAck(sessionID, count = 1) {
      // An ack is authoritative only for a non-negative integer count.
      if (!Number.isInteger(count) || count < 0) return
      const state = progressOf(sessionID)
      state.acked += count
      // Correlation, not cumulative credit: only durables actually observed in this batch can be settled, and any extra ack count is discarded.
      state.outstanding = Math.max(0, state.outstanding - count)
      settle(state)
    },
    wait(sessionID, expected) {
      const state = progressOf(sessionID)
      if (satisfied(state, expected)) {
        consume(state, expected)
        return Promise.resolve()
      }
      return new Promise<void>((resolve) => {
        state.waiters.add({ expected, resolve })
        state.timer ??= setTimeout(() => {
          state.timer = undefined
          void bridge?.fork(
            Effect.logWarning(workerLogMessage("durable egress ack missing"), {
              sessionID,
              outstanding: state.outstanding,
              observed: state.observed,
              acked: state.acked,
              expected,
              timeoutMs,
            }),
          )
          clear(state)
        }, timeoutMs)
      })
    },
    reset(sessionID) {
      const state = progress.get(sessionID)
      if (state !== undefined) clear(state)
    },
    release(sessionID) {
      const state = progress.get(sessionID)
      if (state === undefined) return
      clear(state)
      progress.delete(sessionID)
    },
  }
}

/** Serve a worker's events port with the shared egress receiver (length-prefixed `WorkerFrameEnvelope` frames, backpressure-aware). */
export function serveEventsPort(
  port: MessagePort,
  sink: HubEventSink,
  onError?: (error: Error) => void,
  onDurable?: (commit: DurableCommit) => void,
  bridge?: EffectBridge.Shape,
): () => void {
  let closed = false
  /** Ask the worker to re-read its own log; a dead peer only loses the request. */
  const requestResync = (payload: DurableResyncPayload): void => {
    if (closed) return
    try {
      port.postMessage(encodeFrame(resyncWithPositionFrame(payload)))
    } catch (error) {
      onError?.(error instanceof Error ? error : new Error(String(error)))
    }
  }
  const deliver = (
    event: EventV2Event,
    meta: { readonly aggregateID: string; readonly seq: number } | undefined,
  ): void => {
    if (closed) return
    sink.event(event, {
      durable: meta !== undefined,
      ...(meta === undefined ? {} : { aggregateID: meta.aggregateID, seq: meta.seq }),
    })
    if (meta === undefined || onDurable === undefined) return
    onDurable({ meta, event })
  }
  const connection = createEgressPort(port, {
    onFrame: (frame) => {
      if (frame.kind !== "event") return
      // The canonical durability test (present only when BOTH `aggregateID` and `seq` are set).
      const meta = durableAggregateMetaOf(frame)
      const event = frame.payload.event
      if (!isEventBlobReference(event.properties)) {
        deliver(event, meta)
        return
      }
      // Resolve the oversized body BEFORE the live publisher or the mesh sees it.
      const position = event.properties.__eventBlobPosition
      const resolver = sink.resolveBlob
      if (resolver === undefined) {
        sink.resync({ aggregateID: position.aggregateID, seq: position.seq })
        requestResync({ aggregateID: position.aggregateID, seq: position.seq })
        return
      }
      return Effect.gen(function* () {
        const data = yield* Effect.tryPromise({
          try: () => resolver(position, { type: event.type }),
          catch: (error) => error,
        }).pipe(Effect.catch(() => Effect.succeed(undefined)))
        if (closed) return
        if (data === undefined) {
          sink.resync({ aggregateID: position.aggregateID, seq: position.seq })
          requestResync({ aggregateID: position.aggregateID, seq: position.seq })
          return
        }
        deliver({ ...event, properties: data }, meta)
      })
    },
    // A worker that skipped a durable sequence is healed from the committed log (when the sink has a reader) and asked over the same bidirectional events.
    onDurableGap: (got, last) =>
      Effect.gen(function* () {
        if (closed) return
        const heal = sink.heal
        if (heal === undefined) {
          sink.resync({ aggregateID: got.aggregateID, seq: got.seq })
        } else {
          const healed = yield* Effect.tryPromise({
            try: () => Promise.resolve(heal({ aggregateID: got.aggregateID, seq: got.seq, after: last.seq })),
            catch: (error) => error,
          }).pipe(
            Effect.as(true),
            Effect.catch(() => Effect.succeed(false)),
          )
          // A failed heal leaves the resync request unsent, exactly as the old delivery chain did: the sink still records the gap via `heal`.
          if (!healed) return
        }
        requestResync({ aggregateID: got.aggregateID, seq: got.seq })
      }),
    onFrameRejected: (error) => {
      onError?.(error)
      requestResync({})
    },
    onEnd: () => sink.end(),
    onResync: (payload) => sink.resync(payload),
    onAck: (payload) => sink.ack(payload),
    ...(onError === undefined ? {} : { onError }),
  })
  // `createEgressPort` reports every fatal callback throw/defect or failing handler through `onError` (and `lastError`) exactly once before its program.
  const program = connection.program.pipe(Effect.catch(() => Effect.void))
  // Share the hub's service runtime when it supplies one; direct callers/tests fork the identical program without an extra runtime per attach site.
  const forked = forkEventsProgram(program, bridge)
  return () => {
    closed = true
    connection.close()
    if (forked.owned) {
      forked.fiber.interruptUnsafe()
      return
    }
    void bridge?.promise(Fiber.interrupt(forked.fiber))
  }
}

/** Fork the long-lived events stream on the hub's bridge when it can host one. */
function forkEventsProgram(
  program: Effect.Effect<void, never>,
  bridge: EffectBridge.Shape | undefined,
): { readonly fiber: Fiber.Fiber<void, never>; readonly owned: boolean } {
  if (bridge !== undefined) {
    try {
      return { fiber: bridge.fork(program), owned: false }
    } catch {}
  }
  return { fiber: Effect.runFork(program), owned: true }
}

/** Mark one retired generation's lease reclaimable, scoped to its exact `worker_owner_id`. */
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
/** Upper bound on a dispatch depth carried from `metadata.depth`. */
const MAX_DISPATCH_DEPTH = 64

/** Parse a dispatch depth at the boundary: finite, non-negative, bounded. */
export function boundedDispatchDepth(depth: number | undefined): number {
  if (depth === undefined || !Number.isFinite(depth)) return 0
  return Math.min(Math.max(0, Math.trunc(depth)), MAX_DISPATCH_DEPTH)
}

/** The default budget for the dispatcher's asynchronous resume decision. */
export const DEFAULT_RESUME_DECISION_TIMEOUT_MS = 2_000

/** Bound an async resume decision. */
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
  const bridge = options.bridge
  const runPromise = options.runPromise ?? Effect.runPromise
  const cache = options.cache ?? new StateCache<unknown>()
  const db = options.db
  // Persist every typed crash through the hub's off-loop executor unless the caller supplied its own sink; a hub with no executor has no sink.
  const onWorkerCrashed =
    options.onWorkerCrashed ??
    (db === undefined ? undefined : (payload: WorkerCrashed) => recordAsync(db, payload).then(() => undefined))

  const surfaceWorkerCrash = async (error: unknown, sessionID: SessionID): Promise<void> => {
    const payload = workerCrashedFrom(error)
    if (payload === undefined) {
      if (errorTagOf(error) === "WorkerCrashedError") {
        void bridge?.fork(
          Effect.logError(workerLogMessage("worker crash payload missing"), {
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
          }),
        )
      }
      return
    }
    void bridge?.fork(
      Effect.logError(workerLogMessage("worker crashed"), {
        sessionID: payload.sessionID,
        workerID: payload.workerID,
        phase: payload.phase,
        retryable: payload.retryable,
        reason: payload.reason,
      }),
    )
    if (onWorkerCrashed === undefined) return
    try {
      await onWorkerCrashed(payload)
    } catch (sinkError) {
      // Persistence failure must never replace the crash the caller observes.
      void bridge?.fork(
        Effect.logError(workerLogMessage("worker crash persistence failed"), {
          sessionID: payload.sessionID,
          error: sinkError instanceof Error ? sinkError.message : String(sinkError),
        }),
      )
    }
  }
  // The port authorities are built on the first attach (never at construction): a value is used as-is; a source function is invoked once and memoised.
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
  // The backends to dispose at teardown, or `undefined` when none exist: a value source is disposed directly.
  const disposables = (): WorkerPortBackends | undefined => {
    if (typeof portSource !== "function") return portSource
    return backendsResolved ? resolvedBackends : undefined
  }
  let dispatcher: Dispatcher
  let ports: Attachments

  // A pre-built service owns the port servers, but the hub still owns the per-worker handler seams (they bind reports to the reporting worker's session).
  const portService = options.workerPorts
  if (portService !== undefined) {
    options.bindWorkerPortSeams?.({
      hubProxyHandlersFor,
      hubProxyStreamingFor,
      ...(options.onPidReport === undefined ? {} : { onPidReport: options.onPidReport }),
    })
  }

  const serviceAttachments = portService?.attachments
  ports =
    serviceAttachments === undefined
      ? makeAttachments({
          ...(bridge === undefined ? {} : { bridge }),
          backends: portBackends,
          hubProxyHandlersFor,
          hubProxyStreamingFor,
          onPidReport: options.onPidReport,
        })
      : {
          ...serviceAttachments,
          attachWorkerPorts: (workerID, transferred) => {
            portBackends()
            return serviceAttachments.attachWorkerPorts(workerID, transferred)
          },
          attachHubProxyPort: (workerID, port) => {
            portBackends()
            return serviceAttachments.attachHubProxyPort(workerID, port)
          },
        }

  /** The served hub-proxy handler map for ONE worker: the caller's authorities, built per attach so a worker reads its OWN session context. */
  function hubProxyHandlersFor(workerID: number): HubProxyHandlers | undefined {
    const backends = portBackends()
    const handlers = backends?.hubProxyFor?.(workerID) ?? backends?.hubProxy
    if (handlers === undefined) return undefined
    // The hub owns the lifecycle truth.
    const boundSessionOf = (sessionID: unknown): SessionID => {
      const reported = String(sessionID)
      const bound = dispatcher.registry.sessionsOn(workerID)[0]
      if (bound === undefined || reported !== bound) {
        throw new ForeignLifecycleReportError(workerID, bound, reported)
      }
      return bound
    }
    return {
      ...handlers,
      "lifecycle.commit": (sessionID, messageID) => {
        const bound = boundSessionOf(sessionID)
        lifecycle.commit(bound, String(messageID))
        lifecycle.phase(bound, "commit")
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
        if (lifecycle.indeterminate(bound).length === 0) activeRuns.get(bound)?.tool(String(callID))
      },
    }
  }

  /** The streaming half of a worker's control port. */
  function hubProxyStreamingFor(workerID: number): Readonly<Record<string, HubProxyStreamHandler>> | undefined {
    const backends = portBackends()
    return backends?.hubProxyStreamingFor?.(workerID) ?? backends?.hubProxyStreaming
  }

  const pool = options.pool
  const resolvedCancellation =
    typeof options.cancellation === "function" ? options.cancellation(pool) : options.cancellation
  // Every terminal cancel mode the dispatcher reports, keyed by session id.
  const childrenOf = resolvedCancellation?.childrenOf ?? (() => [])
  const cancelReporter = createCancelReporter({
    dispatcher: () => dispatcher,
    childrenOf,
    events: options.events,
    ...defined({ eventPublisherFor: options.eventPublisherFor, onCancelRequested: options.onCancelRequested }),
    ...(bridge === undefined ? {} : { bridge }),
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
  const barrier = options.barrier
  const promotionOutcome = options.promotionOutcome ?? ((_sessionID: SessionID): RunBarrierPromotionOutcome => "no-job")
  // The report handle for each session's current run.
  const activeRuns = new Map<SessionID, RunBarrierRun>()
  /** The authoritative commit outcome of each session's last finished run, set before its active-run record is deleted. */
  const commitOutcomes = new Map<SessionID, CommitOutcome>()

  /** The dispatcher holds a completed run's slot until the barrier observes all four phases. */
  const runBarrier = async (sessionID: SessionID, carried?: CommitOutcome): Promise<void> => {
    await options.writeBarrier?.(sessionID)
    const run = activeRuns.get(sessionID)
    let committed: string | null = carried?.committed ?? null
    if (run !== undefined) {
      // A run that observed no tool call is settled explicitly.
      if (run.missing().includes("tools") && lifecycle.indeterminate(sessionID).length === 0) run.noTools()
      const observed = run.commitOutcome()
      if (observed !== undefined) committed = observed.committed
      // The commit-side phases settle from the carried outcome.
      if (run.missing().includes("commits")) {
        if (committed === null) run.noCommit()
        else run.commit(committed)
      }
      run.promotion(promotionOutcome(sessionID))
    }
    // Read the authoritative outcome BEFORE deleting the active-run record; the run `done` reply returns exactly this value.
    commitOutcomes.set(sessionID, { committed })
    try {
      await runPromise(barrier.wait(sessionID))
    } finally {
      activeRuns.delete(sessionID)
    }
  }

  /** Sessions whose owning generation was retired (crash, hard terminate, idle reap). */
  const lostGenerations = new Set<SessionID>()

  /** A retired generation can never run `dispose()` (crash, hard terminate). */
  const retiredLeaseReleases = new Map<SessionID, Promise<void>>()
  /** The exact `worker_owner_id` remembered for each session's CURRENT generation. */
  const leaseOwnerBySession = new Map<SessionID, string>()
  const retiredLeaseReleaseTimeoutMs = options.retiredLeaseReleaseTimeoutMs ?? 5_000

  const queueRetiredLeaseRelease = (sessionID: SessionID, task: () => Promise<void>): Promise<void> => {
    const previous = retiredLeaseReleases.get(sessionID) ?? Promise.resolve()
    const next = previous.then(task).catch((error: unknown) => {
      void bridge?.fork(
        Effect.logWarning(workerLogMessage("retired lease release failed"), {
          sessionID,
          error: error instanceof Error ? error.message : String(error),
        }),
      )
    })
    retiredLeaseReleases.set(sessionID, next)
    void next.finally(() => {
      if (retiredLeaseReleases.get(sessionID) === next) retiredLeaseReleases.delete(sessionID)
    })
    return next
  }

  /** Read the EXACT `worker_owner_id` a generation holds for this session. */
  const captureRetiredLeaseOwner = async (sessionID: SessionID, workerID?: number): Promise<string | undefined> => {
    if (db === undefined || workerID === undefined) return undefined
    const generationPrefix = `worker-${workerID}-`
    try {
      const rows = await db.query("SELECT worker_owner_id FROM event_sequence WHERE aggregate_id = ?", [sessionID])
      const owner = rows[0]?.worker_owner_id
      return typeof owner === "string" && owner.startsWith(generationPrefix) ? owner : undefined
    } catch (error) {
      void bridge?.fork(
        Effect.logWarning(workerLogMessage("worker lease owner read failed"), {
          sessionID,
          error: error instanceof Error ? error.message : String(error),
        }),
      )
      return undefined
    }
  }

  /** Zero the retired generation's lease with a conditional update on the exact owner captured at retirement start. */
  const releaseRetiredLease = (sessionID: SessionID, retiredOwnerID: string | undefined): Promise<void> => {
    if (db === undefined || retiredOwnerID === undefined) return Promise.resolve()
    return queueRetiredLeaseRelease(sessionID, async () => {
      await releaseRetiredWorkerLease(db, sessionID, retiredOwnerID)
    })
  }

  /** Remember the exact owner the CURRENT generation holds after its successful dispatch. */
  const rememberLeaseOwner = async (sessionID: SessionID): Promise<void> => {
    const workerID = dispatcher.registry.entry(sessionID)?.workerID
    if (db === undefined || workerID === undefined) return
    const owner = await captureRetiredLeaseOwner(sessionID, workerID)
    if (owner !== undefined) leaseOwnerBySession.set(sessionID, owner)
  }

  /** Successor admission waits for the retired generation's release to land. */
  const awaitRetiredLeaseRelease = (sessionID: SessionID): Promise<void> =>
    retiredLeaseReleases.get(sessionID) ?? Promise.resolve()

  /** Bound ONLY the retired lease UPDATE. */
  const awaitBoundedRetiredLeaseRelease = (sessionID: SessionID, release: Promise<void>): Promise<void> => {
    const timeoutMs = retiredLeaseReleaseTimeoutMs
    if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) return release
    return new Promise<void>((resolve) => {
      let settled = false
      const timer = setTimeout(() => {
        if (settled) return
        settled = true
        void bridge?.fork(
          Effect.logWarning(workerLogMessage("retired lease release timed out"), { sessionID, timeoutMs }),
        )
        resolve()
      }, timeoutMs)
      timer.unref?.()
      void release.finally(() => {
        if (settled) return
        settled = true
        clearTimeout(timer)
        resolve()
      })
    })
  }

  /** Release the hub resources owned by ONE worker generation: its ports, the pending asks bound to that worker. */
  const releaseGenerationResources = async (
    sessionID: SessionID,
    workerID: number | undefined,
    releaseOptions: { readonly lost?: boolean },
    hook: ((sessionID: SessionID, workerID?: number) => unknown) | undefined,
  ): Promise<void> => {
    if (releaseOptions.lost !== false) lostGenerations.add(sessionID)
    // (1) Invoke the hook synchronously.
    let hookResult: PromiseLike<unknown> | undefined
    let hookError: unknown
    let hookThrew = false
    try {
      const released = hook?.(sessionID, workerID)
      if (released !== undefined && typeof (released as PromiseLike<unknown>).then === "function") {
        hookResult = released as PromiseLike<unknown>
      }
    } catch (error) {
      hookThrew = true
      hookError = error
    }
    // (2) Resolve the exact retired owner before any successor can be admitted.
    const storedOwner = workerID === undefined ? undefined : leaseOwnerBySession.get(sessionID)
    const generationPrefix = workerID === undefined ? undefined : `worker-${workerID}-`
    const exactStoredOwner =
      storedOwner !== undefined && generationPrefix !== undefined && storedOwner.startsWith(generationPrefix)
        ? storedOwner
        : undefined
    if (storedOwner !== undefined) leaseOwnerBySession.delete(sessionID)
    const retiredOwnerID = exactStoredOwner ?? (await captureRetiredLeaseOwner(sessionID, workerID))
    // (3) Dispose the old generation's ports and let the hook's async half land, then bound ONLY the lease UPDATE.
    let workError: unknown
    let workFailed = false
    try {
      if (workerID !== undefined) ports.disposeWorker(workerID)
      if (hookResult !== undefined) await hookResult
    } catch (error) {
      workFailed = true
      workError = error
    }
    await awaitBoundedRetiredLeaseRelease(sessionID, releaseRetiredLease(sessionID, retiredOwnerID))
    if (workFailed) throw workError
    if (hookThrew) throw hookError
  }

  /** The public generation release used by callers that retire a single generation directly. */
  const releaseGeneration = (sessionID: SessionID, workerID?: number): Promise<void> =>
    releaseGenerationResources(
      sessionID,
      workerID,
      { lost: true },
      options.onGenerationReleased ?? options.onSessionReleased,
    )

  /** A timed-out barrier refuses the next run while its worker is still alive (the run may still settle). */
  const admitBarrier = Effect.fn("SessionHub.admitBarrier")(function* (sessionID: SessionID) {
    const state = barrier.state(sessionID)
    if (state === "timed-out") {
      if (dispatcher.registry.worker(sessionID) === undefined) {
        lostGenerations.add(sessionID)
        return yield* barrier.abandon(sessionID)
      }
      return yield* new RunBarrierTimeout({ sessionID, timeoutMs: RUN_BARRIER_TIMEOUT_MS })
    }
    if (state === "cancelled" || state === "failed") yield* barrier.reset(sessionID)
  })

  dispatcher = createDispatcher({
    pool,
    crashState: (sessionID) => lifecycle.crashState(sessionID),
    recordCrash: (payload) => lifecycle.observeCrash(payload),
    resumeDecision: (payload) => {
      const decide = (indeterminate: readonly IndeterminateCall[]) =>
        decideAutoResume({
          sessionID: payload.sessionID,
          phase: payload.phase,
          attempts: lifecycle.attempts(payload.sessionID),
          cancelled: lifecycle.cancelled(payload.sessionID),
          lastCommittedMessageID: payload.lastCommittedMessageID,
          indeterminate,
        })
      // The durable `tool_call` rows are the crash truth.
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
          void bridge?.fork(
            Effect.logWarning(workerLogMessage("resume decision timed out"), {
              sessionID: payload.sessionID,
              timeoutMs: resumeDecisionTimeoutMs,
            }),
          )
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
      // Cancel always wins: a decision computed before the cancel but resolved after it must never dispatch a successor run.
      if (lifecycle.cancelled(decision.sessionID)) {
        void bridge?.fork(
          Effect.logInfo(workerLogMessage("auto-resume refused after cancel"), { sessionID: decision.sessionID }),
        )
        return
      }
      lifecycle.recordAttempt(decision.sessionID)
      if (options.resumeRun !== undefined) {
        // The dispatcher settles the original crashed caller from this promise.
        return options.resumeRun(decision, payload)
      }
      // No explicit resume owner: run the successor detached.
      void dispatch
        .ensureRunning(decision.sessionID as SessionID)
        .then(() => lifecycle.resetAttempts(decision.sessionID))
        .catch((error: unknown) => {
          void bridge?.fork(
            Effect.logWarning(workerLogMessage("auto-resume failed"), {
              sessionID: decision.sessionID,
              error: error instanceof Error ? error.message : String(error),
            }),
          )
        })
      return undefined
    },
    writeBarrier: runBarrier,
    commitOutcome: (sessionID) => commitOutcomes.get(sessionID),
    onRunStart: (sessionID) => {
      // A lost generation's calls can never settle.
      if (lostGenerations.delete(sessionID)) {
        for (const call of lifecycle.indeterminate(sessionID)) lifecycle.toolFinished(sessionID, call.callID)
      }
      commitOutcomes.delete(sessionID)
      lifecycle.beginRun(sessionID)
      // Synchronous dispatcher callback: reporters need the run registered before it returns.
      const begun = Effect.runSyncExit(barrier.beginRun(sessionID))
      if (Exit.isSuccess(begun)) {
        activeRuns.set(sessionID, begun.value)
      } else {
        activeRuns.delete(sessionID)
        void bridge?.fork(
          Effect.logWarning(workerLogMessage("run barrier begin failed"), {
            sessionID,
            error: Cause.pretty(begun.cause),
          }),
        )
      }
    },
    onGenerationRetired: async (sessionID, workerID) => {
      // A model switch replaces one generation but keeps the session live, so only the generation-scoped hook runs (never the session release).
      await releaseGenerationResources(sessionID, workerID, { lost: false }, options.onGenerationReleased)
    },
    onSessionReleased: async (sessionID, workerID) => {
      await releaseGenerationResources(sessionID, workerID, { lost: true }, options.onGenerationReleased)
      leaseOwnerBySession.delete(sessionID)
      await options.onSessionReleased?.(sessionID)
    },
    ...defined({
      cancellation,
      reapMs: options.reapMs,
      schedule: options.schedule,
      ...(bridge === undefined ? {} : { bridge }),
    }),
  })
  // Depth is carried explicitly on the run (`metadata.depth` -> Session.spawn).
  const dispatch: SessionDispatch = createSessionDispatch({
    dispatcher,
    pool,
    commitOutcome: (sessionID) => commitOutcomes.get(sessionID),
  })
  const onDispatch = options.onDispatch
  const resolveDepth = (sessionID: SessionID): number =>
    options.depthOf === undefined ? 0 : boundedDispatchDepth(options.depthOf(sessionID))

  return {
    dispatcher,
    cache,
    sessionOfWorker: (workerID) => dispatcher.registry.sessionsOn(workerID)[0],
    attachWorkerPorts: ports.attachWorkerPorts,
    attachHubProxyPort: ports.attachHubProxyPort,
    releaseGeneration,
    lifecycle,
    barrier,
    ensureRunningEffect: (sessionID, origin) =>
      Effect.gen(function* () {
        // Capture the caller's ambient context synchronously; awaiting first would resume outside this fiber and lose it.
        onDispatch?.(sessionID)
        // The off-loop schema gate must complete before any isolate can spawn.
        yield* Effect.tryPromise({ try: () => awaitBootGate(), catch: (error) => error })
        yield* admitBarrier(sessionID)
        // A retired generation's release must land before its successor boots and claims the aggregate.
        yield* Effect.tryPromise({
          try: () => awaitRetiredLeaseRelease(sessionID),
          catch: (error) => error,
        })
        const result = yield* dispatch.ensureRunningEffect(sessionID, resolveDepth(sessionID), origin)
        lifecycle.resetAttempts(sessionID)
        // Record the owner this generation claimed, so a later retirement can release exactly it without re-reading a possibly-successor-owned row.
        yield* Effect.tryPromise({ try: () => rememberLeaseOwner(sessionID), catch: (error) => error })
        return result
      }).pipe(Effect.tapError((error) => Effect.promise(() => surfaceWorkerCrash(error, sessionID)))),
    ensureRunning: async (sessionID, origin) => {
      // Capture the caller's ambient context synchronously; awaiting first would resume outside this fiber and lose it.
      onDispatch?.(sessionID)
      // The off-loop schema gate must complete before any isolate can spawn.
      await awaitBootGate()
      await runPromise(admitBarrier(sessionID))
      // A retired generation's release must land before its successor boots and claims the aggregate.
      await awaitRetiredLeaseRelease(sessionID)
      try {
        const result = await dispatch.ensureRunning(sessionID, resolveDepth(sessionID), origin)
        lifecycle.resetAttempts(sessionID)
        // Record the owner this generation claimed, so a later retirement can release exactly it without re-reading a possibly-successor-owned row.
        await rememberLeaseOwner(sessionID)
        return result
      } catch (error) {
        await surfaceWorkerCrash(error, sessionID)
        throw error
      }
    },
    startShell: async (sessionID, shell) => {
      onDispatch?.(sessionID)
      await awaitBootGate()
      await runPromise(admitBarrier(sessionID))
      await awaitRetiredLeaseRelease(sessionID)
      try {
        const result = await dispatch.startShell(sessionID, shell, resolveDepth(sessionID))
        lifecycle.resetAttempts(sessionID)
        await rememberLeaseOwner(sessionID)
        return result
      } catch (error) {
        await surfaceWorkerCrash(error, sessionID)
        throw error
      }
    },
    assertNotBusy: async (sessionID) => {
      const workerID = dispatcher.registry.entry(sessionID)?.workerID
      const worker = workerID === undefined ? undefined : pool.byID(workerID)
      // A proven-dead isolate is idle even while its run is still registered: the registry reservation cannot outlive the worker it describes.
      if (worker !== undefined && !worker.alive) return false
      // An admitted run is busy even before its worker materializes: a run queued in `cap.acquire` has no isolate to probe.
      if (dispatcher.registry.get(sessionID) !== undefined) return true
      // Only a proven-dead isolate is idle.
      if (worker === undefined) return false
      return worker.assertNotBusy(sessionID)
    },
    cancel: async (sessionID) => {
      lifecycle.cancel(sessionID)
      await runPromise(barrier.cancel(sessionID))
      return (await cancelReporter.cancelWithReport(sessionID)).outcome
    },
    cancelWithReport: async (sessionID) => {
      lifecycle.cancel(sessionID)
      await runPromise(barrier.cancel(sessionID))
      return cancelReporter.cancelWithReport(sessionID)
    },
    cancelAll: async () => {
      for (const entry of dispatcher.registry.entries()) {
        lifecycle.cancel(entry.sessionID)
        await runPromise(barrier.cancel(entry.sessionID))
      }
      return dispatch.cancelAll()
    },
    async terminateAll() {
      ports.disposeAll()
      await dispatcher.terminateAll()
      cache.clear()
      // Only tear down backends we actually built (a source never attached to is never constructed, so no isolate is spawned just to dispose it).
      const backends = disposables()
      await backends?.dispose?.()
      await db?.close().catch(() => undefined)
    },
  }
}

export * from "./cancel-report"
export * from "./hub-proxy-authorities"
export * from "./production-hub"

export {
  type BunExecutorTransportOptions,
  type BunExecutorWorkerPoolOptions,
  DEFAULT_RUN_IDLE_TIMEOUT_MS,
  DEFAULT_SPAWN_GATE_TIMEOUT_MS,
  DEFAULT_TRANSPORT_BUFFER_CAPACITY,
  type DescribedSessionWorkerEntry,
  type IdleBoundedExecutorWorkerPool,
  type IdleIsolateCap,
  type IdleIsolateCapOptions,
  type IdleWorkerEntry,
  type Interface,
  type ProductionPoolFactory,
  type ProductionWorkerPoolOptions,
  SESSION_WORKER_ENTRY,
  type SpawnGateHandle,
  type SpawnGateOptions,
  type SpawnOutcome,
  type SpawnPermit,
  SpawnGateAbortedError,
  SpawnGateTimeoutError,
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
  createWorkerCap,
  describeSessionWorkerEntry,
  layer as WorkerPoolsLayer,
  liveSessionWorkerCount,
  logRunIdleTimeoutWarning,
  makeProduction,
  makeSpawnGate,
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
  type WorkerPortsDiagnostics,
  type WorkerPortsOptions,
  type WorkerTransferredPorts,
  drainEgressPort,
  isPidReport,
  layer as WorkerPortsLayer,
  make,
  node as WorkerPortsNode,
  servePidReportPort,
} from "./worker-ports"

export * as SessionHub from "./hub"
