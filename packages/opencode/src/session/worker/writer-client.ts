// Step 16 — worker-side writer client over a `MessagePort`.
//
// The hub owns the single `WriterClient` (`session/writer/client.ts`), which in turn
// owns the sole SQLite connection. A session worker must not spawn a nested writer;
// instead it forwards the reduced `WriterClientHandle` (`query` / `publish`) over a
// dedicated port to the hub, which serves it with `serveWriterPort` (step 17).
//
// Frames are JSON-safe discriminated objects with a `kind` discriminant, so both
// ends switch exhaustively instead of probing for `op`/`ok`/`type`. Requests carry
// a numeric `id` correlated by the client; replies echo it. No closures cross the
// port.

import { defined } from "@/util/object"
import type { WriterClientHandle } from "./layer"
import { messageOf, portFromMessagePort } from "./port"

/** A durable event as it crosses the port; `data` is validated at the server boundary. */
export interface WorkerWriterEvent {
  readonly id: string
  readonly type: string
  readonly aggregateID: string
  readonly data: unknown
}

export type WorkerWriterRequest =
  | {
      readonly kind: "request"
      readonly id: number
      readonly op: "query"
      readonly method: string
      readonly sql: string
      readonly params?: readonly unknown[]
    }
  | { readonly kind: "request"; readonly id: number; readonly op: "publish"; readonly event: WorkerWriterEvent }
  // Reserved-handle transaction family: the worker pins a transaction, runs
  // statements against its opaque handle, then commits or rolls back. The handle
  // is minted by the hub-owned writer and echoed verbatim.
  | { readonly kind: "request"; readonly id: number; readonly op: "tx.begin" }
  | {
      readonly kind: "request"
      readonly id: number
      readonly op: "tx.query"
      readonly handle: string
      readonly method: string
      readonly sql: string
      readonly params?: readonly unknown[]
    }
  | { readonly kind: "request"; readonly id: number; readonly op: "tx.commit"; readonly handle: string }
  | { readonly kind: "request"; readonly id: number; readonly op: "tx.rollback"; readonly handle: string }

export interface WorkerWriterPublishResult {
  readonly seq: number | null
  readonly committed: boolean
}

export type WorkerWriterReply =
  | { readonly kind: "reply"; readonly id: number; readonly ok: "query"; readonly value: unknown }
  | { readonly kind: "reply"; readonly id: number; readonly ok: "publish"; readonly result: WorkerWriterPublishResult }
  | { readonly kind: "reply"; readonly id: number; readonly ok: "tx.begin"; readonly handle: string }
  | { readonly kind: "reply"; readonly id: number; readonly ok: "tx.query"; readonly value: unknown }
  | { readonly kind: "reply"; readonly id: number; readonly ok: "tx.commit" }
  | { readonly kind: "reply"; readonly id: number; readonly ok: "tx.rollback" }
  | { readonly kind: "reply"; readonly id: number; readonly ok: "error"; readonly tag: string; readonly message: string }

export type WorkerWriterFrame = WorkerWriterRequest | WorkerWriterReply

/** A typed failure returned across the port (mirrors `WriterRemoteError`). */
export class WorkerWriterError extends Error {
  readonly tag: string
  constructor(tag: string, message: string) {
    super(message)
    this.name = tag
    this.tag = tag
  }
}

/**
 * The worker-side handle: the `WriterClientHandle` seam plus lifecycle control.
 *
 * The transaction primitives are declared here (not only on `WriterClientHandle`)
 * so this module owns the full worker-visible shape: the hub `WriterClient` and
 * the worker relay agree on a `Promise`-based, opaque-string-handle API. The
 * `WriterTxId` semantics (begin → txQuery* → commit | rollback, typed failure)
 * are preserved; only the identifier representation (`string` handle, matching
 * the hub) and the async style (`Promise`) differ from the plan's illustrative
 * `Effect`/`number` sketch.
 */
export interface WorkerWriterClient extends WriterClientHandle {
  /** Pin a transaction and return its opaque handle (e.g. `"tx-1"`). */
  begin(): Promise<string>
  /** Run a statement against the pinned transaction; rejects on a typed failure. */
  txQuery(handle: string, method: string, sql: string, params?: readonly unknown[]): Promise<unknown>
  /** Commit the pinned transaction; the handle is spent. */
  commit(handle: string): Promise<void>
  /** Roll back the pinned transaction, discarding its writes. */
  rollback(handle: string): Promise<void>
  dispose(): void
}

export interface CreateWorkerWriterClientOptions {
  /**
   * Default per-request timeout. When set, a request with no reply within the
   * window rejects and its pending entry is removed, so a lost reply never leaks
   * a promise.
   */
  readonly timeoutMs?: number
}

interface PendingRequest {
  readonly resolve: (reply: WorkerWriterReply) => void
  readonly reject: (error: unknown) => void
  readonly timer: ReturnType<typeof setTimeout> | undefined
}

const DISPOSED_MESSAGE = "worker writer client disposed"

/**
 * Narrow a reply to the expected success shape, converting an error reply (or a
 * mismatched success) into a typed `WorkerWriterError`. The port is the trust
 * boundary: once narrowed, the caller reads fields without re-checking.
 */
function expectReply<K extends WorkerWriterReply["ok"]>(
  reply: WorkerWriterReply,
  expected: K,
): Extract<WorkerWriterReply, { readonly ok: K }> {
  if (reply.ok === "error") throw new WorkerWriterError(reply.tag, reply.message)
  if (reply.ok !== expected) {
    throw new WorkerWriterError("PROTOCOL", `expected a "${expected}" reply, received "${reply.ok}"`)
  }
  return reply as Extract<WorkerWriterReply, { readonly ok: K }>
}

/** Worker side: a thin `WriterClientHandle` over `port`. */
export function createWorkerWriterClient(
  port: MessagePort,
  options: CreateWorkerWriterClientOptions = {},
): WorkerWriterClient {
  const channel = portFromMessagePort<WorkerWriterFrame>(port)
  const defaultTimeoutMs = options.timeoutMs
  let nextId = 1
  let disposed = false
  const pending = new Map<number, PendingRequest>()

  const unsubscribe = channel.onMessage((frame) => {
    // Only replies are addressed to the client; a request frame is a protocol
    // violation we ignore rather than serve (the worker never owns the writer).
    if (frame.kind !== "reply") return
    const waiter = pending.get(frame.id)
    if (waiter === undefined) return
    pending.delete(frame.id)
    if (waiter.timer !== undefined) clearTimeout(waiter.timer)
    waiter.resolve(frame)
  })

  const request = (build: (id: number) => WorkerWriterRequest): Promise<WorkerWriterReply> => {
    if (disposed) return Promise.reject(new WorkerWriterError("DISPOSED", DISPOSED_MESSAGE))
    const id = nextId++
    return new Promise<WorkerWriterReply>((resolve, reject) => {
      const timer =
        defaultTimeoutMs === undefined
          ? undefined
          : setTimeout(() => {
              pending.delete(id)
              reject(new WorkerWriterError("TIMEOUT", `worker writer request timed out after ${defaultTimeoutMs}ms`))
            }, defaultTimeoutMs)
      pending.set(id, { resolve, reject, timer })
      try {
        channel.post(build(id))
      } catch (error) {
        pending.delete(id)
        if (timer !== undefined) clearTimeout(timer)
        reject(error)
      }
    })
  }

  return {
    async query(method, sql, params) {
      const reply = await request((id) => ({
        kind: "request",
        id,
        op: "query",
        method,
        sql,
        ...defined({ params }),
      }))
      return expectReply(reply, "query").value
    },

    async publish(event) {
      const reply = await request((id) => ({ kind: "request", id, op: "publish", event }))
      return expectReply(reply, "publish").result
    },

    async begin() {
      return expectReply(await request((id) => ({ kind: "request", id, op: "tx.begin" })), "tx.begin").handle
    },

    async txQuery(handle, method, sql, params) {
      const reply = await request((id) => ({
        kind: "request",
        id,
        op: "tx.query",
        handle,
        method,
        sql,
        ...defined({ params }),
      }))
      return expectReply(reply, "tx.query").value
    },

    async commit(handle) {
      expectReply(await request((id) => ({ kind: "request", id, op: "tx.commit", handle })), "tx.commit")
    },

    async rollback(handle) {
      expectReply(await request((id) => ({ kind: "request", id, op: "tx.rollback", handle })), "tx.rollback")
    },

    dispose() {
      if (disposed) return
      disposed = true
      unsubscribe()
      for (const waiter of pending.values()) {
        if (waiter.timer !== undefined) clearTimeout(waiter.timer)
        waiter.reject(new WorkerWriterError("DISPOSED", DISPOSED_MESSAGE))
      }
      pending.clear()
    },
  }
}

/** Coerce an unknown thrown value to a frame-safe error tag. */
export function writerErrorTag(error: unknown): string {
  if (typeof error === "object" && error !== null && "tag" in error) {
    const tag = (error as { tag: unknown }).tag
    if (typeof tag === "string") return tag
  }
  return "WRITER_ERROR"
}

export * as WorkerWriterClientModule from "./writer-client"
