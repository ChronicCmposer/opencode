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
import { messageOf, portFromMessagePort } from "@/session/worker/port"
import {
  writerErrorTag,
  type WorkerWriterEvent,
  type WorkerWriterFrame,
  type WorkerWriterReply,
} from "@/session/worker/writer-client"
import { defined } from "@/util/object"
import type { StorageReadResult } from "./core"
import {
  ProtocolError,
  parseWriterResponse,
  type QueryMethod,
  type WriterEvent,
  type WriterRequest,
  type WriterResponse,
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
  constructor(tag: string, message: string) {
    super(message)
    this.name = tag
    this.tag = tag
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

interface Pending {
  readonly resolve: (value: unknown) => void
  readonly reject: (error: unknown) => void
  readonly timer: ReturnType<typeof setTimeout>
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
  const defined =
    options.definedPath ??
    (typeof OPENCODE_WRITER_WORKER_PATH !== "undefined" ? OPENCODE_WRITER_WORKER_PATH : undefined)
  const fromEnv = (options.env ?? process.env).OPENCODE_WRITER_WORKER_PATH
  const resolved = fromEnv ?? defined
  if (resolved === undefined || resolved === "") return new URL("./worker.ts", import.meta.url)
  return resolved
}

const DEFAULT_ENTRY = resolveWriterWorkerEntry()

/** `Omit` over a union collapses to common keys; distribute so each frame keeps its fields. */
type DistributiveOmit<T, K extends PropertyKey> = T extends unknown ? Omit<T, K> : never
type WriterFrame = DistributiveOmit<WriterRequest, "id">

export class WriterClient {
  private readonly entry: string | URL
  private readonly env: Record<string, string>
  private readonly timeoutMs: number
  private readonly baseWorkerData: Record<string, unknown>
  private readonly onCrash: ((error: WriterCrashedError) => void) | undefined

  private worker: Worker | undefined
  private readonly pending = new Map<number, Pending>()
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
    this.entry = options.entry ?? DEFAULT_ENTRY
    this.env = mergeEnv(options.env)
    this.timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS
    this.baseWorkerData = options.workerData ?? {}
    this.onCrash = options.onCrash
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
          // admitting any other caller.
          if (this.openFilename !== undefined) {
            await this.post<{ opened: boolean }>({ type: "admin.open", filename: this.openFilename })
          }
          if (this.storageDir !== undefined) {
            await this.post<unknown>({ type: "admin.openStorage", dir: this.storageDir })
          }
        } catch (error) {
          // Reopen failed: never mark the worker alive, so callers respawn instead
          // of posting into a half-initialised isolate.
          this.alive = false
          reject(error instanceof Error ? error : new WriterCrashedError(String(error)))
          this.crash(new WriterCrashedError("writer failed to restore state after respawn"))
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
        this.handleResponse(message)
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

  private handleResponse(message: WriterResponse): void {
    if (message.type === "ready") return
    if (message.id === -1) {
      // The isolate could not parse a frame we sent — a protocol violation. Treat it
      // as fatal for this worker: reject every in-flight request and drop it.
      const detail = message.type === "failure" ? message.error.tag : "unknown"
      void this.worker?.terminate()
      this.crash(new WriterCrashedError(`writer rejected a malformed frame (${detail})`))
      return
    }
    const pending = this.pending.get(message.id)
    if (pending === undefined) return
    this.pending.delete(message.id)
    clearTimeout(pending.timer)
    if (message.type === "result") pending.resolve(message.value)
    else pending.reject(new WriterRemoteError(message.error.tag, message.error.message))
  }

  /**
   * Tear the worker down: reject the ready promise and every in-flight request, and
   * forget the worker so the next call respawns. Never requeues (at-most-once).
   */
  private crash(error: WriterCrashedError): void {
    const wasAlive = this.alive
    this.alive = false
    this.starting = false
    this.worker = undefined
    const rejectReady = this.readyReject
    this.readyReject = undefined
    this.readyPromise = undefined
    rejectReady?.(error)
    for (const pending of this.pending.values()) {
      clearTimeout(pending.timer)
      pending.reject(error)
    }
    this.pending.clear()
    if (wasAlive) this.onCrash?.(error)
  }

  async close(): Promise<void> {
    if (this.closed) return
    this.closed = true
    const worker = this.worker
    this.crash(new WriterCrashedError("writer client is closed"))
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
    return this.post<T>(frame)
  }

  /** Post a frame without the liveness gate, for the respawn re-open handshake. */
  private post<T>(frame: WriterFrame): Promise<T> {
    const worker = this.worker
    if (worker === undefined) return Promise.reject(new WriterCrashedError("writer is not running"))
    return new Promise<T>((resolve, reject) => {
      const id = this.nextId++
      const timer = setTimeout(() => {
        this.pending.delete(id)
        reject(new WriterTimeoutError(`writer request ${id} timed out after ${this.timeoutMs}ms`))
      }, this.timeoutMs)
      this.pending.set(id, { resolve: resolve as (value: unknown) => void, reject, timer })
      worker.postMessage({ ...frame, id })
    })
  }

  // -- typed API ------------------------------------------------------------

  async open(filename: string): Promise<{ opened: boolean; filename: string }> {
    const result = await this.request<{ opened: boolean; filename: string }>({ type: "admin.open", filename })
    this.openFilename = filename
    return result
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
    await this.request({ type: "admin.openStorage", dir })
    this.storageDir = dir
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

const QUERY_METHODS: ReadonlySet<string> = new Set<QueryMethod>(["all", "values", "run", "exec"])

/** Parse a wire method string into the trusted `QueryMethod`; fail loud otherwise. */
function toQueryMethod(method: string): QueryMethod {
  if (!QUERY_METHODS.has(method)) {
    throw new ProtocolError(`unknown query method "${method}"; expected all, values, run or exec`)
  }
  return method as QueryMethod
}

/** Parse a wire event into a trusted `WriterEvent`; `data` must be a record. */
function toWriterEvent(event: WorkerWriterEvent): WriterEvent {
  const data = event.data
  if (typeof data !== "object" || data === null || Array.isArray(data)) {
    throw new ProtocolError(`event.data must be an object (got ${JSON.stringify(data)})`)
  }
  return { id: event.id, type: event.type, aggregateID: event.aggregateID, data: data as Record<string, unknown> }
}

/**
 * Hub side: serve worker writer requests on `port` against `hubWriter`. Returns a
 * disposer. A reply to a closed port is swallowed (the worker is gone); a rejected
 * request becomes a typed error reply, never an unhandled rejection.
 */
export function serveWriterPort(hubWriter: HubWriter, port: MessagePort): () => void {
  const channel = portFromMessagePort<WorkerWriterFrame>(port)
  return channel.onMessage((frame) => {
    if (frame.kind !== "request") return
    const id = frame.id
    const reply = (payload: WorkerWriterReply): void => {
      try {
        channel.post(payload)
      } catch {
        // Drop the reply: the port is gone.
      }
    }
    const replyError = (error: unknown): void =>
      reply({ kind: "reply", id, ok: "error", tag: writerErrorTag(error), message: messageOf(error) })
    try {
      switch (frame.op) {
        case "query":
          hubWriter.query(toQueryMethod(frame.method), frame.sql, frame.params ?? []).then(
            (value) => reply({ kind: "reply", id, ok: "query", value: value ?? null }),
            replyError,
          )
          return
        case "publish":
          hubWriter.publish(toWriterEvent(frame.event), { durable: true }).then(
            (result) => reply({ kind: "reply", id, ok: "publish", result }),
            replyError,
          )
          return
        case "tx.begin":
          hubWriter.begin().then(
            (handle) => reply({ kind: "reply", id, ok: "tx.begin", handle }),
            replyError,
          )
          return
        case "tx.query":
          hubWriter.txQuery(frame.handle, toQueryMethod(frame.method), frame.sql, frame.params ?? []).then(
            (value) => reply({ kind: "reply", id, ok: "tx.query", value: value ?? null }),
            replyError,
          )
          return
        case "tx.commit":
          hubWriter.commit(frame.handle).then(
            () => reply({ kind: "reply", id, ok: "tx.commit" }),
            replyError,
          )
          return
        case "tx.rollback":
          hubWriter.rollback(frame.handle).then(
            () => reply({ kind: "reply", id, ok: "tx.rollback" }),
            replyError,
          )
          return
        default: {
          const unknown: never = frame
          replyError(new Error(`unknown writer request: ${JSON.stringify(unknown)}`))
        }
      }
    } catch (error) {
      replyError(error)
    }
  })
}

export * as WriterClientModule from "./client"
