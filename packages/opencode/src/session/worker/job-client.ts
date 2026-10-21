// Phase 9.2/9.3/9.4 — worker thin client over the hub job registry
// (plan tasks 9.2/9.3/9.4; decision lines 54, 64).
//
// The hub registry (`job-registry.ts`) is the single authority. Each worker
// connects one `JobPort` (structurally a `MessagePort` / the egress control
// link). This module is the wire in between:
//
//   * `serveJobRegistry(registry, port)` — hub side: route request frames to the
//     registry and reply; the hub owns the one registry for every worker.
//   * `createJobClient(port)` — worker side: a thin client whose `wait`/`cancel`
//     satisfy the `BackgroundJobClientHandle` seam in `layer.ts`, plus the wider
//     `start`/`extend`/`complete`/`fail` surface a worker needs to report runs.
//   * `createResultRouter(portForSession)` — the hub's `routeResult` seam: push a
//     settled child's result to the worker hosting its `parentID` (9.4).
//
// Frames are JSON-safe discriminated objects. Requests carry a numeric `id`
// correlated by the client; replies echo it; `childResult` is an unsolicited
// hub→worker push keyed by `parentID`. No closures cross the port.

import { defined } from "@/util/object"
import type { JobInfo, JobRegistry, JobWaitResult, StartJobInput } from "./job-registry"
import { messageOf, portFromMessagePort } from "./port"

/**
 * Every frame carries a `kind` discriminant, so both ends switch exhaustively
 * instead of probing for `op`/`ok`/`type`.
 */
export type JobRequest =
  | { readonly kind: "request"; readonly id: number; readonly op: "start"; readonly input: StartJobInput }
  | { readonly kind: "request"; readonly id: number; readonly op: "extend"; readonly jobID: string }
  | {
      readonly kind: "request"
      readonly id: number
      readonly op: "complete"
      readonly jobID: string
      readonly output: string
      readonly sequence?: number
    }
  | { readonly kind: "request"; readonly id: number; readonly op: "fail"; readonly jobID: string; readonly error: string }
  | { readonly kind: "request"; readonly id: number; readonly op: "wait"; readonly jobID: string; readonly timeout?: number }
  | { readonly kind: "request"; readonly id: number; readonly op: "waitForPromotion"; readonly jobID: string }
  | { readonly kind: "request"; readonly id: number; readonly op: "promote"; readonly jobID: string }
  | { readonly kind: "request"; readonly id: number; readonly op: "get"; readonly jobID: string }
  | { readonly kind: "request"; readonly id: number; readonly op: "list" }
  | { readonly kind: "request"; readonly id: number; readonly op: "cancel"; readonly jobID: string }

export type JobReply =
  | { readonly kind: "reply"; readonly id: number; readonly ok: "info"; readonly info?: JobInfo }
  | { readonly kind: "reply"; readonly id: number; readonly ok: "wait"; readonly result: JobWaitResult }
  | { readonly kind: "reply"; readonly id: number; readonly ok: "list"; readonly infos: JobInfo[] }
  | { readonly kind: "reply"; readonly id: number; readonly ok: "boolean"; readonly value: boolean }
  | { readonly kind: "reply"; readonly id: number; readonly ok: "error"; readonly message: string }

/** An unsolicited hub→worker push: a settled child routed to its parent. */
export type JobPush = {
  readonly kind: "push"
  readonly type: "childResult"
  readonly parentID: string
  readonly result: JobInfo
}

export type JobFrame = JobRequest | JobReply | JobPush

/** The slice of a control channel the job protocol needs. */
export interface JobPort {
  post(frame: JobFrame): void
  onMessage(handler: (frame: JobFrame) => void): () => void
}

/**
 * Adapt a real `MessagePort` (the worker's dedicated control link) to `JobPort`.
 * Mirrors `cancel.ts`'s `cancelPortFromMessagePort`, so the hub can serve the
 * registry over a transferred port and the worker can drive the client.
 */
export function jobPortFromMessagePort(port: MessagePort): JobPort {
  const channel = portFromMessagePort<JobFrame>(port)
  return { post: channel.post, onMessage: channel.onMessage }
}

/** Hub side: serve registry requests from one worker port. Returns a disposer. */
export function serveJobRegistry(registry: JobRegistry, port: JobPort): () => void {
  return port.onMessage((frame) => {
    if (frame.kind !== "request") return
    const id = frame.id
    // The port can be closed (worker gone) between frames; a failed reply is
    // not a registry error, so it is swallowed rather than crashing the server.
    const reply = (payload: JobReply): void => {
      try {
        port.post(payload)
      } catch {
        // Drop the reply: the port is gone.
      }
    }
    const replyError = (error: unknown): void => reply({ kind: "reply", id, ok: "error", message: messageOf(error) })
    try {
      switch (frame.op) {
        case "start":
          reply({ kind: "reply", id, ok: "info", info: registry.start(frame.input) })
          break
        case "extend":
          reply({ kind: "reply", id, ok: "boolean", value: registry.extend(frame.jobID) })
          break
        case "complete":
          reply({ kind: "reply", id, ok: "info", info: registry.complete(frame.jobID, frame.output, frame.sequence) })
          break
        case "fail":
          reply({ kind: "reply", id, ok: "info", info: registry.fail(frame.jobID, frame.error) })
          break
        case "promote":
          reply({ kind: "reply", id, ok: "info", info: registry.promote(frame.jobID) })
          break
        case "get":
          reply({ kind: "reply", id, ok: "info", info: registry.get(frame.jobID) })
          break
        case "list":
          reply({ kind: "reply", id, ok: "list", infos: registry.list() })
          break
        case "cancel":
          reply({ kind: "reply", id, ok: "info", info: registry.cancel(frame.jobID) })
          break
        case "wait":
          registry
            .wait({ id: frame.jobID, ...defined({ timeout: frame.timeout }) })
            .then((result) => reply({ kind: "reply", id, ok: "wait", result }), replyError)
          break
        case "waitForPromotion":
          registry.waitForPromotion(frame.jobID).then(
            (info) => reply({ kind: "reply", id, ok: "info", info }),
            replyError,
          )
          break
        default: {
          const unknown: never = frame
          replyError(new Error(`unknown job request: ${JSON.stringify(unknown)}`))
        }
      }
    } catch (error) {
      replyError(error)
    }
  })
}

export interface JobClient {
  start(input: StartJobInput): Promise<JobInfo>
  extend(id: string): Promise<boolean>
  complete(id: string, output: string, sequence?: number): Promise<JobInfo | undefined>
  fail(id: string, error: unknown): Promise<JobInfo | undefined>
  wait(id: string, timeoutMs?: number): Promise<JobWaitResult>
  waitForPromotion(id: string): Promise<JobInfo>
  promote(id: string): Promise<JobInfo | undefined>
  get(id: string): Promise<JobInfo | undefined>
  list(): Promise<JobInfo[]>
  cancel(id: string): Promise<void>
  onChildResult(handler: (result: JobInfo) => void): () => void
  dispose(): void
}

export interface CreateJobClientOptions {
  /**
   * Default per-request timeout. When set, a request with no reply within the
   * window rejects and its pending entry is removed (the client never leaks a
   * promise waiting on a lost reply).
   */
  readonly timeoutMs?: number
}

interface PendingRequest {
  readonly resolve: (frame: JobReply) => void
  readonly reject: (error: unknown) => void
  readonly timer: ReturnType<typeof setTimeout> | undefined
}

const DISPOSED_MESSAGE = "job client disposed"

/** Worker side: a thin client over `port`. */
export function createJobClient(port: JobPort, options: CreateJobClientOptions = {}): JobClient {
  const defaultTimeoutMs = options.timeoutMs
  let nextId = 1
  let disposed = false
  const pending = new Map<number, PendingRequest>()
  const childHandlers = new Set<(result: JobInfo) => void>()

  const unsubscribe = port.onMessage((frame) => {
    switch (frame.kind) {
      case "push":
        // Only `childResult` is defined today; switch keeps future pushes explicit.
        if (frame.type === "childResult") for (const handler of childHandlers) handler(frame.result)
        return
      case "request":
        // A worker client never serves requests.
        return
      case "reply": {
        const waiter = pending.get(frame.id)
        if (waiter === undefined) return
        pending.delete(frame.id)
        if (waiter.timer !== undefined) clearTimeout(waiter.timer)
        if (frame.ok === "error") waiter.reject(new Error(frame.message))
        else waiter.resolve(frame)
        return
      }
    }
  })

  const request = (build: (id: number) => JobRequest, timeoutMs = defaultTimeoutMs): Promise<JobReply> => {
    if (disposed) return Promise.reject(new Error(DISPOSED_MESSAGE))
    const id = nextId++
    return new Promise<JobReply>((resolve, reject) => {
      const timer =
        timeoutMs === undefined
          ? undefined
          : setTimeout(() => {
              pending.delete(id)
              reject(new Error(`job request timed out after ${timeoutMs}ms`))
            }, timeoutMs)
      pending.set(id, { resolve, reject, timer })
      try {
        port.post(build(id))
      } catch (error) {
        pending.delete(id)
        if (timer !== undefined) clearTimeout(timer)
        reject(error)
      }
    })
  }

  const infoOf = (frame: JobReply): JobInfo | undefined => (frame.ok === "info" ? frame.info : undefined)

  return {
    async start(input) {
      const info = infoOf(await request((id) => ({ kind: "request", id, op: "start", input })))
      if (info === undefined) throw new Error("job.start returned no info")
      return info
    },
    async extend(id) {
      const frame = await request((rid) => ({ kind: "request", id: rid, op: "extend", jobID: id }))
      return frame.ok === "boolean" && frame.value
    },
    async complete(id, output, sequence) {
      return infoOf(
        await request((rid) => ({
          kind: "request",
          id: rid,
          op: "complete",
          jobID: id,
          output,
          ...defined({ sequence }),
        })),
      )
    },
    async fail(id, error) {
      return infoOf(
        await request((rid) => ({ kind: "request", id: rid, op: "fail", jobID: id, error: messageOf(error) })),
      )
    },
    async wait(id, timeoutMs) {
      const frame = await request((rid) => ({
        kind: "request",
        id: rid,
        op: "wait",
        jobID: id,
        ...defined({ timeout: timeoutMs }),
      }))
      if (frame.ok === "wait") return frame.result
      if (frame.ok === "info") return { ...defined({ info: frame.info }), timedOut: false }
      return { timedOut: false }
    },
    async waitForPromotion(id) {
      const info = infoOf(
        await request((rid) => ({ kind: "request", id: rid, op: "waitForPromotion", jobID: id })),
      )
      if (info === undefined) throw new Error("job.waitForPromotion returned no info")
      return info
    },
    async promote(id) {
      return infoOf(await request((rid) => ({ kind: "request", id: rid, op: "promote", jobID: id })))
    },
    async get(id) {
      return infoOf(await request((rid) => ({ kind: "request", id: rid, op: "get", jobID: id })))
    },
    async list() {
      const frame = await request((id) => ({ kind: "request", id, op: "list" }))
      return frame.ok === "list" ? frame.infos : []
    },
    async cancel(id) {
      await request((rid) => ({ kind: "request", id: rid, op: "cancel", jobID: id }))
    },
    onChildResult(handler) {
      if (disposed) return () => {}
      childHandlers.add(handler)
      return () => childHandlers.delete(handler)
    },
    dispose() {
      if (disposed) return
      disposed = true
      unsubscribe()
      for (const waiter of pending.values()) {
        if (waiter.timer !== undefined) clearTimeout(waiter.timer)
        waiter.reject(new Error(DISPOSED_MESSAGE))
      }
      pending.clear()
      childHandlers.clear()
    },
  }
}

/**
 * The hub's `routeResult` seam (9.4): resolve the worker hosting `parentID` and
 * push the child's settled info to it. Workers with no parent connection are
 * silently dropped (the parent may already have finished).
 */
export function createResultRouter(
  portForSession: (sessionID: string) => JobPort | undefined,
): (parentID: string, result: JobInfo) => void {
  return (parentID, result) => {
    portForSession(parentID)?.post({ kind: "push", type: "childResult", parentID, result })
  }
}

export * as WorkerJobClient from "./job-client"
