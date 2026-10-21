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

import type { StorageReadResult } from "./core"
import type { QueryMethod, WriterEvent, WriterRequest, WriterResponse } from "./protocol"

export interface PublishResult {
  readonly seq: number | null
  readonly committed: boolean
  readonly ordinal: number
  readonly idempotent: boolean
}

export class WriterCrashedError extends Error {
  readonly tag = "WRITER_CRASHED"
  constructor(message: string) {
    super(message)
    this.name = "WRITER_CRASHED"
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

interface Pending {
  readonly resolve: (value: unknown) => void
  readonly reject: (error: unknown) => void
  readonly timer: ReturnType<typeof setTimeout>
}

const DEFAULT_ENTRY = new URL("./worker.ts", import.meta.url)
const DEFAULT_TIMEOUT_MS = 10_000

/** `Omit` over a union collapses to common keys; distribute so each frame keeps its fields. */
type DistributiveOmit<T, K extends PropertyKey> = T extends unknown ? Omit<T, K> : never
type WriterFrame = DistributiveOmit<WriterRequest, "id">

function mergeEnv(overlay?: Record<string, string | undefined>): Record<string, string> {
  const env: Record<string, string> = {}
  for (const [key, value] of Object.entries(process.env)) if (value !== undefined) env[key] = value
  for (const [key, value] of Object.entries(overlay ?? {})) {
    if (value === undefined) delete env[key]
    else env[key] = value
  }
  return env
}

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
  /** Connection config replayed on every (re)spawn so a fresh isolate reopens the same database. */
  private openFilename: string | undefined

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
    if (this.alive && this.readyPromise !== undefined) return this.readyPromise
    if (this.starting && this.readyPromise !== undefined) return this.readyPromise
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
      const timer = setTimeout(() => {
        reject(new WriterCrashedError(`writer worker did not become ready within ${this.timeoutMs}ms`))
      }, this.timeoutMs)

      const becomeReady = () => {
        this.alive = true
        const filename = this.openFilename
        if (filename === undefined) {
          resolve()
          return
        }
        // A fresh isolate owns no connection: reopen the same database before
        // callers are admitted.
        this.send<{ opened: boolean }>({ type: "admin.open", filename }).then(
          () => resolve(),
          (error) => reject(error instanceof Error ? error : new WriterCrashedError(String(error))),
        )
      }

      worker.onmessage = (event: MessageEvent) => {
        const message = event.data as WriterResponse
        if (message.type === "ready") {
          clearTimeout(timer)
          becomeReady()
          return
        }
        this.handleResponse(message)
      }
      worker.onerror = (event: ErrorEvent) => this.crash(new WriterCrashedError(`writer worker error: ${event.message}`))
      worker.addEventListener("close", () => this.crash(new WriterCrashedError("writer worker exited")))
    })

    ready.catch(() => undefined).finally(() => {
      this.starting = false
    })
    this.readyPromise = ready
    return ready
  }

  private handleResponse(message: WriterResponse): void {
    if (message.type === "ready") return
    const pending = this.pending.get(message.id)
    if (pending === undefined) return
    this.pending.delete(message.id)
    clearTimeout(pending.timer)
    if (message.type === "result") pending.resolve(message.value)
    else pending.reject(new WriterRemoteError(message.error.tag, message.error.message))
  }

  private crash(error: WriterCrashedError): void {
    const wasAlive = this.alive
    this.alive = false
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
    this.worker = undefined
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
    const worker = this.worker
    if (worker === undefined || !this.alive) return Promise.reject(new WriterCrashedError("writer is not running"))
    return new Promise<T>((resolve, reject) => {
      const id = this.nextId++
      const timer = setTimeout(() => {
        this.pending.delete(id)
        reject(new Error(`writer request ${id} timed out after ${this.timeoutMs}ms`))
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
    options: { readonly cache?: boolean } = {},
  ): Promise<unknown> {
    return this.request({
      type: "query",
      method,
      sql,
      params,
      ...(options.cache === undefined ? {} : { cache: options.cache }),
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

  publish(
    event: WriterEvent,
    options: { readonly durable: boolean; readonly ownerID?: string; readonly strictOwner?: boolean; readonly commitId?: string },
  ): Promise<PublishResult> {
    return this.request({
      type: "publish",
      event,
      durable: options.durable,
      ...(options.ownerID === undefined ? {} : { ownerID: options.ownerID }),
      ...(options.strictOwner === undefined ? {} : { strictOwner: options.strictOwner }),
      ...(options.commitId === undefined ? {} : { commitId: options.commitId }),
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

  openStorage(dir: string): Promise<void> {
    return this.request({ type: "admin.openStorage", dir })
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

  replay(
    event: WriterEvent,
    seq: number,
    options: { readonly ownerID?: string; readonly strictOwner?: boolean } = {},
  ): Promise<PublishResult> {
    return this.request({
      type: "replay",
      event,
      seq,
      ...(options.ownerID === undefined ? {} : { ownerID: options.ownerID }),
      ...(options.strictOwner === undefined ? {} : { strictOwner: options.strictOwner }),
    })
  }
}

export function createWriterClient(options: WriterClientOptions = {}): WriterClient {
  return new WriterClient(options)
}

export * as WriterClientModule from "./client"
