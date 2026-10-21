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

import type { JobInfo, JobRegistry, JobWaitResult, StartJobInput } from "./job-registry"

export type JobRequest =
  | { readonly id: number; readonly op: "start"; readonly input: StartJobInput }
  | { readonly id: number; readonly op: "extend"; readonly jobID: string }
  | {
      readonly id: number
      readonly op: "complete"
      readonly jobID: string
      readonly output: string
      readonly sequence?: number
    }
  | { readonly id: number; readonly op: "fail"; readonly jobID: string; readonly error: string }
  | { readonly id: number; readonly op: "wait"; readonly jobID: string; readonly timeout?: number }
  | { readonly id: number; readonly op: "waitForPromotion"; readonly jobID: string }
  | { readonly id: number; readonly op: "promote"; readonly jobID: string }
  | { readonly id: number; readonly op: "get"; readonly jobID: string }
  | { readonly id: number; readonly op: "list" }
  | { readonly id: number; readonly op: "cancel"; readonly jobID: string }

export type JobReply =
  | { readonly id: number; readonly ok: "info"; readonly info?: JobInfo }
  | { readonly id: number; readonly ok: "wait"; readonly result: JobWaitResult }
  | { readonly id: number; readonly ok: "list"; readonly infos: JobInfo[] }
  | { readonly id: number; readonly ok: "boolean"; readonly value: boolean }
  | { readonly id: number; readonly ok: "error"; readonly message: string }

/** An unsolicited hub→worker push: a settled child routed to its parent. */
export type JobPush = { readonly type: "childResult"; readonly parentID: string; readonly result: JobInfo }

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
  return {
    post: (frame) => port.postMessage(frame),
    onMessage: (handler) => {
      const listener = (event: MessageEvent) => handler(event.data as JobFrame)
      port.addEventListener("message", listener)
      port.start()
      return () => port.removeEventListener("message", listener)
    },
  }
}

const messageOf = (error: unknown): string => (error instanceof Error ? error.message : String(error))

/** Hub side: serve registry requests from one worker port. Returns a disposer. */
export function serveJobRegistry(registry: JobRegistry, port: JobPort): () => void {
  return port.onMessage((frame) => {
    if (!("op" in frame)) return
    const id = frame.id
    const reply = (payload: JobReply): void => port.post(payload)
    try {
      switch (frame.op) {
        case "start":
          reply({ id, ok: "info", info: registry.start(frame.input) })
          break
        case "extend":
          reply({ id, ok: "boolean", value: registry.extend(frame.jobID) })
          break
        case "complete":
          reply({ id, ok: "info", info: registry.complete(frame.jobID, frame.output, frame.sequence) })
          break
        case "fail":
          reply({ id, ok: "info", info: registry.fail(frame.jobID, frame.error) })
          break
        case "promote":
          reply({ id, ok: "info", info: registry.promote(frame.jobID) })
          break
        case "get":
          reply({ id, ok: "info", info: registry.get(frame.jobID) })
          break
        case "list":
          reply({ id, ok: "list", infos: registry.list() })
          break
        case "cancel":
          reply({ id, ok: "info", info: registry.cancel(frame.jobID) })
          break
        case "wait":
          void registry
            .wait({ id: frame.jobID, ...(frame.timeout === undefined ? {} : { timeout: frame.timeout }) })
            .then((result) => reply({ id, ok: "wait", result }))
          break
        case "waitForPromotion":
          void registry.waitForPromotion(frame.jobID).then((info) => reply({ id, ok: "info", info }))
          break
        default:
          reply({ id, ok: "error", message: "unknown job request" })
      }
    } catch (error) {
      reply({ id, ok: "error", message: messageOf(error) })
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

/** Worker side: a thin client over `port`. */
export function createJobClient(port: JobPort): JobClient {
  let nextId = 1
  const pending = new Map<number, { readonly resolve: (frame: JobReply) => void; readonly reject: (error: unknown) => void }>()
  const childHandlers = new Set<(result: JobInfo) => void>()

  const unsubscribe = port.onMessage((frame) => {
    if ("type" in frame) {
      if (frame.type === "childResult") for (const handler of childHandlers) handler(frame.result)
      return
    }
    if (!("ok" in frame)) return
    const waiter = pending.get(frame.id)
    if (waiter === undefined) return
    pending.delete(frame.id)
    if (frame.ok === "error") waiter.reject(new Error(frame.message))
    else waiter.resolve(frame)
  })

  const request = (build: (id: number) => JobRequest): Promise<JobReply> => {
    const id = nextId++
    return new Promise<JobReply>((resolve, reject) => {
      pending.set(id, { resolve, reject })
      port.post(build(id))
    })
  }

  const infoOf = (frame: JobReply): JobInfo | undefined => (frame.ok === "info" ? frame.info : undefined)

  return {
    async start(input) {
      const info = infoOf(await request((id) => ({ id, op: "start", input })))
      if (info === undefined) throw new Error("job.start returned no info")
      return info
    },
    async extend(id) {
      const frame = await request((rid) => ({ id: rid, op: "extend", jobID: id }))
      return frame.ok === "boolean" && frame.value
    },
    async complete(id, output, sequence) {
      return infoOf(
        await request((rid) => ({
          id: rid,
          op: "complete",
          jobID: id,
          output,
          ...(sequence === undefined ? {} : { sequence }),
        })),
      )
    },
    async fail(id, error) {
      return infoOf(await request((rid) => ({ id: rid, op: "fail", jobID: id, error: messageOf(error) })))
    },
    async wait(id, timeoutMs) {
      const frame = await request((rid) => ({
        id: rid,
        op: "wait",
        jobID: id,
        ...(timeoutMs === undefined ? {} : { timeout: timeoutMs }),
      }))
      if (frame.ok === "wait") return frame.result
      if (frame.ok === "info") return { ...(frame.info === undefined ? {} : { info: frame.info }), timedOut: false }
      return { timedOut: false }
    },
    async waitForPromotion(id) {
      const info = infoOf(await request((rid) => ({ id: rid, op: "waitForPromotion", jobID: id })))
      if (info === undefined) throw new Error("job.waitForPromotion returned no info")
      return info
    },
    async promote(id) {
      return infoOf(await request((rid) => ({ id: rid, op: "promote", jobID: id })))
    },
    async get(id) {
      return infoOf(await request((rid) => ({ id: rid, op: "get", jobID: id })))
    },
    async list() {
      const frame = await request((id) => ({ id, op: "list" }))
      return frame.ok === "list" ? frame.infos : []
    },
    async cancel(id) {
      await request((rid) => ({ id: rid, op: "cancel", jobID: id }))
    },
    onChildResult(handler) {
      childHandlers.add(handler)
      return () => childHandlers.delete(handler)
    },
    dispose() {
      unsubscribe()
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
    portForSession(parentID)?.post({ type: "childResult", parentID, result })
  }
}

export * as WorkerJobClient from "./job-client"
