// Phase 7 — writer client (plan tasks 7.5, 7.16).
//
// Hub-side handle to the writer Worker. It owns two behaviours the plan pins down:
//
//   * At-most-once crash semantics (7.16): when the writer isolate exits, every
//     in-flight request is rejected — never auto-requeued — so no duplicate write or
//     sequence is ever produced. A subsequent call respawns the writer with a bumped
//     generation counter.
//   * Generation-scoped identity: the client stamps each spawn with an incrementing
//     `generation`, delivered to the isolate via `workerData` and echoed in its
//     `ready` frame, so survivors can detect a stale writer.

import { mergeEnv } from "@/session/broker/env"
import {
  createRequestChannel,
  portFromMessagePort,
  resolveWorkerEntry,
  servePort,
  type FrameChannel,
  type RequestChannel,
} from "@/session/worker/port"
import {
  type WorkerWriterPublishResult,
  type WorkerWriterReply,
  type WorkerWriterRequest,
} from "@/session/worker/writer-client"
import { isRecord } from "@/session/worker/frame"
import { defined } from "@/util/object"
import type { StorageReadResult } from "./core"
import {
  ProtocolError,
  parseQueryMethod,
  parseWriterEventValue,
  parseWriterResponse,
  writerErrorInfo,
  type QueryMethod,
  type WriterEvent,
  type WriterFailure,
  type WriterRequest,
  type WriterResponse,
  type WriterResult,
} from "./protocol"

export interface PublishResult {
  readonly seq: number | null
  readonly committed: boolean
  /** Egress ordinal; `null` for frames that carry no durable event. */
  readonly ordinal: number | null
  readonly idempotent: boolean
}

export class WriterCrashedError extends Error {
  readonly tag = "WRITER_CRASHED"
  constructor(message: string) {
    super(message)
    this.name = "WRITER_CRASHED"
  }
}

/** A ready/request deadline elapsed; carries a tag so callers can branch on it. */
export class WriterTimeoutError extends Error {
  readonly tag = "WRITER_TIMEOUT"
  constructor(message: string) {
    super(message)
    this.name = "WRITER_TIMEOUT"
  }
}

/** A typed failure returned by the writer isolate (e.g. `SQLITE_BUSY_EXHAUSTED`). */
export class WriterRemoteError extends Error {
  readonly tag: string
  /** Native SQLite code carried across the port, when the failure had one. */
  readonly code?: string | number
  constructor(tag: string, message: string, code?: string | number) {
    super(message)
    this.name = tag
    this.tag = tag
    if (code !== undefined) this.code = code
  }
}

export interface WriterClientOptions {
  /** Worker entry; defaults to the production writer worker. */
  readonly entry?: string | URL
  /** Environment overlay; defaults to the current process environment. */
  readonly env?: Record<string, string | undefined>
  /** Request/readiness timeout in ms; defaults to 10000. */
  readonly timeoutMs?: number
  /** Extra `workerData` merged into each spawn (generation is always set). */
  readonly workerData?: Record<string, unknown>
  readonly onCrash?: (error: WriterCrashedError) => void
}

/** Per-query cache toggle. */
export interface QueryOptions {
  readonly cache?: boolean
}

/** Options for a durable/non-durable publish. */
export interface PublishOptions {
  readonly durable: boolean
  readonly ownerID?: string
  readonly strictOwner?: boolean
  readonly commitId?: string
}

/** Options for an idempotent replay. */
export interface ReplayOptions {
  readonly ownerID?: string
  readonly strictOwner?: boolean
}

const DEFAULT_TIMEOUT_MS = 10_000

declare global {
  /**
   * Compile-time path to the bundled writer isolate, injected by `script/build.ts`
   * (mirrors `OPENCODE_SESSION_WORKER_PATH`). Absent in dev/test, where the
   * source-relative isolate is used instead.
   */
  const OPENCODE_WRITER_WORKER_PATH: string | undefined
}

/**
 * Resolve the writer isolate entry: the runtime env override, else the build-time
 * define, else the source-relative isolate. The packaged binary cannot resolve the
 * source path (`new URL("./worker.ts", import.meta.url)` becomes
 * `/$bunfs/root/worker.ts`), so this mirrors `resolveSessionWorkerEntry` to load
 * the bundled worker. Pure so the resolution is testable without re-importing.
 */
export function resolveWriterWorkerEntry(
  options: { readonly env?: Record<string, string | undefined>; readonly definedPath?: string } = {},
): string | URL {
  return resolveWorkerEntry({
    env: options.env,
    definedPath:
      options.definedPath ??
      (typeof OPENCODE_WRITER_WORKER_PATH !== "undefined" ? OPENCODE_WRITER_WORKER_PATH : undefined),
    envVar: "OPENCODE_WRITER_WORKER_PATH",
    fallback: new URL("./worker.ts", import.meta.url),
  })
}

/** `Omit` over a union collapses to common keys; distribute so each frame keeps its fields. */
type DistributiveOmit<T, K extends PropertyKey> = T extends unknown ? Omit<T, K> : never
type WriterFrame = DistributiveOmit<WriterRequest, "id">

/**
 * True for frames that only read: a timeout on one of these can safely reject
 * the caller without dropping the writer generation. Mutations, transaction
 * control, admin statements and storage writes are all fatal-on-timeout.
 */
function isReadOnlyFrame(frame: WriterFrame): boolean {
  switch (frame.type) {
    case "query":
      return frame.method === "all" || frame.method === "values"
    case "storage.read":
    case "storage.list":
      return true
    default:
      return false
  }
}

export class WriterClient {
  private readonly entry: string | URL
  private readonly env: Record<string, string>
  private readonly timeoutMs: number
  private readonly baseWorkerData: Record<string, unknown>
  private readonly onCrash: ((error: WriterCrashedError) => void) | undefined

  private worker: Worker | undefined
  /**
   * The request/response channel over the writer port. Correlation, timeouts and
   * dispose live in the shared `createRequestChannel`; this class supplies the
   * frame codec, the per-call post target and the crash/ready handling.
   */
  private readonly requests: RequestChannel<WriterRequest | WriterResponse, WriterResult | WriterFailure, number>
  /** The channel's inbound sink; set when `createRequestChannel` subscribes. */
  private inboundHandler: ((frame: WriterRequest | WriterResponse) => void) | undefined
  /**
   * The spawn the next post targets. Set synchronously around `request`, so a
   * re-open handshake can target a captured spawn while a frame from a sibling
   * call cannot observe a torn-down `this.worker`.
   */
  private postTarget: Worker | undefined
  private nextId = 1
  private generationValue = 0
  private alive = false
  private starting = false
  private closed = false
  private readyPromise: Promise<void> | undefined
  private readyReject: ((error: unknown) => void) | undefined
  /** Connection config replayed on every (re)spawn so a fresh isolate reopens the same database. */
  private openFilename: string | undefined
  /** File-KV backend replayed on every (re)spawn alongside the database. */
  private storageDir: string | undefined

  constructor(options: WriterClientOptions = {}) {
    // Honour the caller's env overlay rather than the import-time process env.
    this.entry = options.entry ?? resolveWriterWorkerEntry({ env: options.env })
    this.env = mergeEnv(options.env)
    this.timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS
    this.baseWorkerData = options.workerData ?? {}
    this.onCrash = options.onCrash

    // The port facade: outbound frames post to the per-call target; inbound
    // responses are emitted to the channel's single subscriber. The target is
    // mutated synchronously around each `request`, so a reply can never race it.
    const channel: FrameChannel<WriterRequest | WriterResponse> = {
      post: (frame) => {
        const target = this.postTarget
        if (target === undefined) throw new WriterCrashedError("writer is not running")
        target.postMessage(frame)
      },
      onMessage: (handler) => {
        this.inboundHandler = handler
        return () => {
          this.inboundHandler = undefined
        }
      },
    }
    this.requests = createRequestChannel<WriterRequest | WriterResponse, WriterResult | WriterFailure, number>(
      channel,
      {
        timeoutMs: this.timeoutMs,
        isReply: (message): message is WriterResult | WriterFailure =>
          message.type === "result" || message.type === "failure",
        idOf: (reply) => reply.id,
        nextId: () => this.nextId++,
        disposedError: () => new WriterCrashedError("writer client is closed"),
        timeoutError: (id, timeoutMs) =>
          new WriterTimeoutError(`writer request ${id} timed out after ${timeoutMs}ms`),
      },
    )
  }

  get generation(): number {
    return this.generationValue
  }

  get isAlive(): boolean {
    return this.alive
  }

  // -- lifecycle ------------------------------------------------------------

  private ensure(): Promise<void> {
    if (this.closed) return Promise.reject(new WriterCrashedError("writer client is closed"))
    if ((this.alive || this.starting) && this.readyPromise !== undefined) return this.readyPromise
    return this.spawn()
  }

  private spawn(): Promise<void> {
    const generation = ++this.generationValue
    const worker = new Worker(this.entry, {
      env: this.env,
      workerData: { ...this.baseWorkerData, generation },
      // Born named `ocw` so the isolate is observable from creation, not only
      // once its own entry self-names.
      name: "ocw",
    } as ConstructorParameters<typeof Worker>[1])
    this.worker = worker
    this.alive = false
    this.starting = true

    const ready = new Promise<void>((resolve, reject) => {
      this.readyReject = reject
      const timer = setTimeout(() => {
        if (worker !== this.worker) return
        // A worker that never became ready is an orphan: drop it before giving up.
        reject(new WriterTimeoutError(`writer worker did not become ready within ${this.timeoutMs}ms`))
        void worker.terminate()
        this.crash(new WriterCrashedError(`writer worker did not become ready within ${this.timeoutMs}ms`))
      }, this.timeoutMs)

      const becomeReady = async (echoedGeneration: number): Promise<void> => {
        if (echoedGeneration !== generation) {
          reject(
            new WriterCrashedError(
              `writer ready generation ${echoedGeneration} does not match spawn generation ${generation}`,
            ),
          )
          void worker.terminate()
          this.crash(new WriterCrashedError("writer generation mismatch"))
          return
        }
        try {
          // A fresh isolate owns no connection: restore the replayed state before
          // admitting any other caller. Target the captured spawn, never
          // `this.worker`, which a crash could have replaced mid-await.
          if (this.openFilename !== undefined) {
            await this.postTo<{ opened: boolean }>(worker, { type: "admin.open", filename: this.openFilename })
          }
          if (this.storageDir !== undefined) {
            await this.postTo<unknown>(worker, { type: "admin.openStorage", dir: this.storageDir })
          }
        } catch (error) {
          // Reopen failed: never mark the worker alive, so callers respawn instead
          // of posting into a half-initialised isolate. Only crash the generation
          // that actually failed.
          if (worker === this.worker) {
            this.crash(new WriterCrashedError("writer failed to restore state after respawn"))
          }
          reject(error instanceof Error ? error : new WriterCrashedError(String(error)))
          return
        }
        // The ready echo belongs to the captured spawn: never mark a replacement
        // (or a torn-down) worker alive on this generation's behalf.
        if (worker !== this.worker) {
          reject(new WriterCrashedError("writer was replaced before it became ready"))
          return
        }
        this.alive = true
        resolve()
      }

      worker.onmessage = (event: MessageEvent) => {
        if (worker !== this.worker) return
        let message: WriterResponse
        try {
          message = parseWriterResponse(event.data)
        } catch (error) {
          // A worker we cannot understand is unusable: drop it before crashing.
          void worker.terminate()
          this.crash(new WriterCrashedError(error instanceof Error ? error.message : String(error)))
          return
        }
        if (message.type === "ready") {
          clearTimeout(timer)
          void becomeReady(message.generation)
          return
        }
        if (message.id === -1) {
          // The isolate could not parse a frame we sent — a protocol violation.
          // Treat it as fatal for this worker: reject every in-flight request and
          // drop it.
          const detail = message.type === "failure" ? message.error.tag : "unknown"
          void worker.terminate()
          this.crash(new WriterCrashedError(`writer rejected a malformed frame (${detail})`))
          return
        }
        // A correlated reply: hand it to the shared channel, which resolves the
        // matching pending request. `id: -1` and `ready` are handled above.
        this.inboundHandler?.(message)
      }
      worker.onerror = (event: ErrorEvent) => {
        if (worker !== this.worker) return
        this.crash(new WriterCrashedError(`writer worker error: ${event.message}`))
      }
      worker.addEventListener("close", () => {
        if (worker !== this.worker) return
        this.crash(new WriterCrashedError("writer worker exited"))
      })
    })

    ready.catch(() => undefined).finally(() => {
      this.starting = false
    })
    this.readyPromise = ready
    return ready
  }

  /**
   * Tear the worker down: reject the ready promise and every in-flight request, and
   * forget the worker so the next call respawns. Never requeues (at-most-once).
   *
   * A crash is reported for every generation, including one that dies during
   * startup before it was ever marked alive, so a supervisor sees the crash it
   * otherwise would have missed. A deliberate `close()` is not a crash and does
   * not notify.
   */
  private crash(error: WriterCrashedError, options: { readonly notify?: boolean } = {}): void {
    this.alive = false
    this.starting = false
    this.worker = undefined
    const rejectReady = this.readyReject
    this.readyReject = undefined
    this.readyPromise = undefined
    rejectReady?.(error)
    // Reject every in-flight request (never requeue) while leaving the channel
    // usable, so the next call respawns against it.
    this.requests.fail(error)
    if (options.notify ?? true) this.onCrash?.(error)
  }

  async close(): Promise<void> {
    if (this.closed) return
    this.closed = true
    const worker = this.worker
    this.crash(new WriterCrashedError("writer client is closed"), { notify: false })
    // Permanently retire the channel so no later call can post into a torn-down client.
    this.requests.dispose()
    if (worker !== undefined) await worker.terminate()
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.close()
  }

  // -- request plumbing -----------------------------------------------------

  private async request<T>(frame: WriterFrame): Promise<T> {
    await this.ensure()
    return this.send<T>(frame)
  }

  /** Post a frame to the live isolate; the caller has already awaited `ensure()`. */
  private send<T>(frame: WriterFrame): Promise<T> {
    if (this.worker === undefined || !this.alive) return Promise.reject(new WriterCrashedError("writer is not running"))
    return this.post<T>(frame, { fatalOnTimeout: !isReadOnlyFrame(frame) })
  }

  /**
   * Post a frame without the liveness gate, for the respawn re-open handshake.
   *
   * `fatalOnTimeout` is for mutating and transaction frames: a timed-out write
   * may still execute inside the isolate, so the whole generation is dropped
   * rather than risk committing a write the caller never observed. Read frames
   * simply reject the caller and keep the worker.
   */
  private post<T>(frame: WriterFrame, options: { readonly fatalOnTimeout?: boolean } = {}): Promise<T> {
    return this.postTo<T>(this.worker, frame).catch((error: unknown) => {
      // A timed-out mutation may still execute inside the isolate, so the whole
      // generation is dropped rather than risk committing a write the caller
      // never observed. Reads simply reject and keep the worker.
      if (options.fatalOnTimeout === true && error instanceof WriterTimeoutError) {
        void this.worker?.terminate()
        this.crash(new WriterCrashedError(error.message))
      }
      throw error
    })
  }

  /** Post to one captured spawn, so a reopen cannot land on a replacement worker. */
  private postTo<T>(target: Worker | undefined, frame: WriterFrame): Promise<T> {
    if (target === undefined) return Promise.reject(new WriterCrashedError("writer is not running"))
    this.postTarget = target
    try {
      return this.requests.request((id) => ({ ...frame, id })).then((reply) => {
        if (reply.type === "result") return reply.value as T
        throw new WriterRemoteError(reply.error.tag, reply.error.message, reply.error.code)
      })
    } finally {
      // `request` posted synchronously above, so the target is no longer needed.
      this.postTarget = undefined
    }
  }

  // -- typed API ------------------------------------------------------------

  async open(filename: string): Promise<{ opened: boolean; filename: string }> {
    // Record before issuing: if the isolate crashes after opening but before
    // replying, a respawn must still replay this open rather than come up with no
    // connection.
    this.openFilename = filename
    return this.request<{ opened: boolean; filename: string }>({ type: "admin.open", filename })
  }

  pragma(sql: string): Promise<void> {
    return this.request({ type: "admin.pragma", sql })
  }

  migrate(statements: ReadonlyArray<string>): Promise<void> {
    return this.request({ type: "admin.migrate", statements })
  }

  checkpoint(): Promise<void> {
    return this.request({ type: "admin.checkpoint" })
  }

  query(
    method: QueryMethod,
    sql: string,
    params: ReadonlyArray<unknown> = [],
    options: QueryOptions = {},
  ): Promise<unknown> {
    return this.request({
      type: "query",
      method,
      sql,
      params,
      ...defined({ cache: options.cache }),
    })
  }

  register(projectorId: string, eventType: string, sql: string): Promise<void> {
    return this.request({ type: "register", projectorId, eventType, sql })
  }

  begin(): Promise<string> {
    return this.request({ type: "tx.begin" })
  }

  txQuery(
    handle: string,
    method: QueryMethod,
    sql: string,
    params: ReadonlyArray<unknown> = [],
  ): Promise<unknown> {
    return this.request({ type: "tx.query", handle, method, sql, params })
  }

  commit(handle: string): Promise<void> {
    return this.request({ type: "tx.commit", handle })
  }

  rollback(handle: string): Promise<void> {
    return this.request({ type: "tx.rollback", handle })
  }

  publish(event: WriterEvent, options: PublishOptions): Promise<PublishResult> {
    return this.request({
      type: "publish",
      event,
      durable: options.durable,
      ...defined({ ownerID: options.ownerID, strictOwner: options.strictOwner, commitId: options.commitId }),
    })
  }

  claim(aggregateID: string, ownerID: string): Promise<void> {
    return this.request({ type: "claim", aggregateID, ownerID })
  }

  steal(aggregateID: string, ownerID: string): Promise<void> {
    return this.request({ type: "steal", aggregateID, ownerID })
  }

  remove(aggregateID: string): Promise<void> {
    return this.request({ type: "remove", aggregateID })
  }

  // -- file-KV storage (plan 7.13) -----------------------------------------

  async openStorage(dir: string): Promise<void> {
    // Record before issuing, for the same mid-open crash reason as `open`.
    this.storageDir = dir
    await this.request({ type: "admin.openStorage", dir })
  }

  storageRead(key: ReadonlyArray<string>): Promise<StorageReadResult> {
    return this.request({ type: "storage.read", key })
  }

  storageWrite(key: ReadonlyArray<string>, content: unknown): Promise<void> {
    return this.request({ type: "storage.write", key, content })
  }

  storageRemove(key: ReadonlyArray<string>): Promise<void> {
    return this.request({ type: "storage.remove", key })
  }

  storageList(prefix: ReadonlyArray<string>): Promise<ReadonlyArray<ReadonlyArray<string>>> {
    return this.request({ type: "storage.list", prefix })
  }

  replay(event: WriterEvent, seq: number, options: ReplayOptions = {}): Promise<PublishResult> {
    return this.request({
      type: "replay",
      event,
      seq,
      ...defined({ ownerID: options.ownerID, strictOwner: options.strictOwner }),
    })
  }
}

export function createWriterClient(options: WriterClientOptions = {}): WriterClient {
  return new WriterClient(options)
}

// ---------------------------------------------------------------------------
// Step 17 — hub side of the worker writer port.
// ---------------------------------------------------------------------------

/**
 * The slice of the hub-owned writer a worker port server needs. `WriterClient`
 * satisfies it structurally: a worker `publish` is a durable publish, so it
 * returns `{ seq, committed }`, and the four transaction primitives relay the
 * reserved-handle protocol verbatim.
 */
export interface HubWriter {
  readonly query: (method: QueryMethod, sql: string, params: readonly unknown[]) => Promise<unknown>
  readonly publish: (
    event: WriterEvent,
    options: { readonly durable: boolean },
  ) => Promise<{ readonly seq: number | null; readonly committed: boolean }>
  readonly begin: () => Promise<string>
  readonly txQuery: (handle: string, method: QueryMethod, sql: string, params: readonly unknown[]) => Promise<unknown>
  readonly commit: (handle: string) => Promise<void>
  readonly rollback: (handle: string) => Promise<void>
}

/** The error reply plus the optional native code the worker classifier consumes. */
type WriterErrorReply = Extract<WorkerWriterReply, { readonly ok: "error" }> & { readonly code?: string | number }

/** Parse a required string field of an inbound worker frame; fail loud otherwise. */
function requestString(record: Record<string, unknown>, field: string): string {
  const value = record[field]
  if (typeof value !== "string") {
    throw new ProtocolError(`field "${field}" must be a string (got ${JSON.stringify(value)})`)
  }
  return value
}

/** Parse the optional positional `params` of an inbound worker frame. */
function requestParams(record: Record<string, unknown>): readonly unknown[] | undefined {
  const value = record["params"]
  if (value === undefined) return undefined
  if (!Array.isArray(value)) throw new ProtocolError(`field "params" must be an array (got ${JSON.stringify(value)})`)
  return value
}

/**
 * Parse an untrusted worker writer frame into a trusted request, failing loud. The
 * port is the trust boundary: a frame that is not a well-formed request must never
 * be trusted field by field, and a non-request is a protocol violation.
 */
export function parseWorkerWriterRequest(input: unknown): WorkerWriterRequest {
  if (!isRecord(input)) throw new ProtocolError(`writer frame must be an object (got ${JSON.stringify(input)})`)
  if (input["kind"] !== "request") {
    throw new ProtocolError(`writer frame must have kind "request" (got ${JSON.stringify(input["kind"])})`)
  }
  const id = input["id"]
  if (typeof id !== "number" || !Number.isInteger(id)) {
    throw new ProtocolError(`field "id" must be an integer (got ${JSON.stringify(id)})`)
  }
  const op = input["op"]
  switch (op) {
    case "query": {
      const params = requestParams(input)
      return {
        kind: "request",
        id,
        op,
        method: requestString(input, "method"),
        sql: requestString(input, "sql"),
        ...defined({ params }),
      }
    }
    case "publish":
      // The event is validated here, so the server hands an already-trusted shape
      // to the hub writer rather than trusting a bare `event` field.
      return { kind: "request", id, op, event: parseWriterEventValue(input["event"]) }
    case "tx.begin":
      return { kind: "request", id, op }
    case "tx.query": {
      const params = requestParams(input)
      return {
        kind: "request",
        id,
        op,
        handle: requestString(input, "handle"),
        method: requestString(input, "method"),
        sql: requestString(input, "sql"),
        ...defined({ params }),
      }
    }
    case "tx.commit":
      return { kind: "request", id, op, handle: requestString(input, "handle") }
    case "tx.rollback":
      return { kind: "request", id, op, handle: requestString(input, "handle") }
    default:
      throw new ProtocolError(`unknown writer request op ${JSON.stringify(op)}`)
  }
}

/**
 * Hub side: serve worker writer requests on `port` against `hubWriter`. Returns a
 * disposer. Inbound frames are parsed at this boundary; a reply to a closed port is
 * swallowed (the worker is gone); a rejected request becomes a typed error reply,
 * never an unhandled rejection.
 */
export function serveWriterPort(hubWriter: HubWriter, port: MessagePort): () => void {
  const channel = portFromMessagePort<unknown>(port)

  const reply = (payload: WorkerWriterReply): void => {
    try {
      channel.post(payload)
    } catch {
      // Drop the reply: the port is gone.
    }
  }
  const replyError = (id: number, error: unknown): void => {
    // `writerErrorInfo` forwards the native SQLite code so the worker-side
    // `classifySqliteError` can map a lock onto `LockTimeoutError` rather than
    // `UnknownError`.
    const payload: WriterErrorReply = { kind: "reply", id, ok: "error", ...writerErrorInfo(error) }
    reply(payload)
  }

  return servePort(
    channel,
    parseWorkerWriterRequest,
    (request) => executeWorkerWriterRequest(hubWriter, request),
    (raw, outcome) => {
      if (!outcome.ok) {
        // Correlate by the parsed id, or by the raw id when the frame was unparseable.
        if (outcome.request !== undefined) replyError(outcome.request.id, outcome.error)
        else if (isRecord(raw) && typeof raw["id"] === "number") replyError(raw["id"], outcome.error)
        return
      }
      reply(replyForWorkerWriter(outcome.request, outcome.value))
    },
  )
}

/** Dispatch one parsed worker writer request to the hub writer; returns its raw value. */
function executeWorkerWriterRequest(hubWriter: HubWriter, request: WorkerWriterRequest): unknown {
  switch (request.op) {
    case "query":
      return hubWriter.query(parseQueryMethod(request.method), request.sql, request.params ?? [])
    case "publish":
      return hubWriter.publish(parseWriterEventValue(request.event), { durable: true })
    case "tx.begin":
      return hubWriter.begin()
    case "tx.query":
      return hubWriter.txQuery(request.handle, parseQueryMethod(request.method), request.sql, request.params ?? [])
    case "tx.commit":
      return hubWriter.commit(request.handle)
    case "tx.rollback":
      return hubWriter.rollback(request.handle)
  }
}

/** Encode a dispatched value as the op-specific success reply. */
function replyForWorkerWriter(request: WorkerWriterRequest, value: unknown): WorkerWriterReply {
  switch (request.op) {
    case "query":
      return { kind: "reply", id: request.id, ok: "query", value: value ?? null }
    case "publish":
      return { kind: "reply", id: request.id, ok: "publish", result: value as WorkerWriterPublishResult }
    case "tx.begin":
      return { kind: "reply", id: request.id, ok: "tx.begin", handle: value as string }
    case "tx.query":
      return { kind: "reply", id: request.id, ok: "tx.query", value: value ?? null }
    case "tx.commit":
      return { kind: "reply", id: request.id, ok: "tx.commit" }
    case "tx.rollback":
      return { kind: "reply", id: request.id, ok: "tx.rollback" }
  }
}

export * as WriterClientModule from "./client"
