// Phase 9.2/9.4/9.6 — hub-hosted cross-worker job registry
// (plan tasks 9.2/9.4/9.6; decisions lines 53, 54, 64).
//
// `BackgroundJob` is process-local (`src/background/job.ts`), so a child session
// running on another worker cannot be awaited from the parent's worker. This
// module makes the HUB the single authority: every worker registers a job,
// reports its terminal result and waits — all against one in-memory registry
// that never crosses an isolate.
//
// The registry deliberately mirrors the core `BackgroundJob` `Info`/`Interface`
// shape (`started_at` / `completed_at` / `status` / `output` / `error`) so the
// worker-side thin client (`job-client.ts`) is a drop-in replacement. It adds
// only the cross-worker fields Phase 9 needs: `parentID`, `workerID` and `depth`,
// plus a `parentID → children` index (decision line 53) and a terminal-result
// routing seam (`routeResult`) the hub wires to the parent worker's control port
// (9.4).
//
// Pure and deterministic: `now` and `schedule` are injectable, so the registry
// is tested directly with no threads and no model APIs.

/** A `BackgroundJob` status; identical to the core union. */
export type JobStatus = "running" | "completed" | "error" | "cancelled"

export interface JobInfo {
  id: string
  type: string
  title?: string
  status: JobStatus
  started_at: number
  completed_at?: number
  output?: string
  error?: string
  metadata?: Record<string, unknown>
  /** The subagent parent session; absent for a root/foreground job. */
  parentID?: string
  /** The worker currently hosting the job (informational). */
  workerID?: number
  /** Nesting depth: 0 = foreground/root, ≥1 = subagent. */
  depth: number
}

export interface StartJobInput {
  readonly id: string
  readonly type: string
  readonly title?: string
  readonly metadata?: Record<string, unknown>
  readonly parentID?: string
  readonly workerID?: number
  readonly depth?: number
}

export interface JobWaitResult {
  readonly info?: JobInfo
  readonly timedOut: boolean
}

/** Timer seam (mirrors `dispatch.ts`/`cancel.ts`); defaults to `setTimeout`. */
export type JobScheduler = (delayMs: number, run: () => void) => () => void

export interface JobRegistryOptions {
  readonly now?: () => number
  readonly schedule?: JobScheduler
  /** Deliver a settled child's info to the worker hosting `parentID` (9.4). */
  readonly routeResult?: (parentID: string, result: JobInfo) => void
}

export interface JobRegistry {
  readonly size: number
  /** Register a running job; a running id joins the existing job. */
  start(input: StartJobInput): JobInfo
  /** Join a running job (reserve another completion); `false` when not running. */
  extend(id: string): boolean
  /** Report a successful run; the last `sequence` wins when joined. */
  complete(id: string, output: string, sequence?: number): JobInfo | undefined
  /** Report a failed run. */
  fail(id: string, error: unknown): JobInfo | undefined
  wait(input: { readonly id: string; readonly timeout?: number }): Promise<JobWaitResult>
  waitForPromotion(id: string): Promise<JobInfo>
  promote(id: string): JobInfo | undefined
  get(id: string): JobInfo | undefined
  list(): JobInfo[]
  cancel(id: string): JobInfo | undefined
  /** Record that `childID` is a subagent of `parentID` (9.4/9.6). */
  linkChild(parentID: string, childID: string): void
  childrenOf(parentID: string): readonly string[]
  parentOf(childID: string): string | undefined
}

interface Active {
  info: JobInfo
  pending: number
  next: number
  output: { sequence: number; text: string } | undefined
  readonly waiters: Set<(result: JobWaitResult) => void>
  readonly promoteWaiters: Set<(info: JobInfo) => void>
}

const defaultSchedule: JobScheduler = (delayMs, run) => {
  const timer = setTimeout(run, delayMs)
  ;(timer as { unref?: () => void }).unref?.()
  return () => clearTimeout(timer)
}

const messageOf = (error: unknown): string => (error instanceof Error ? error.message : String(error))

export function createJobRegistry(options: JobRegistryOptions = {}): JobRegistry {
  const now = options.now ?? (() => Date.now())
  const schedule = options.schedule ?? defaultSchedule
  const routeResult = options.routeResult
  const jobs = new Map<string, Active>()
  const childrenByParent = new Map<string, Set<string>>()
  const parentByChild = new Map<string, string>()

  const snapshot = (active: Active): JobInfo => ({
    ...active.info,
    ...(active.info.metadata ? { metadata: { ...active.info.metadata } } : {}),
  })

  const resolveWaiters = (active: Active, result: JobWaitResult): void => {
    for (const waiter of active.waiters) waiter(result)
    active.waiters.clear()
    for (const waiter of active.promoteWaiters) waiter(snapshot(active))
    active.promoteWaiters.clear()
  }

  /** Settle a running job once; wake every waiter and route a child's result. */
  const settle = (active: Active, status: JobStatus, fields: Partial<JobInfo>): JobInfo => {
    active.info = { ...active.info, ...fields, status, completed_at: now() }
    const info = snapshot(active)
    resolveWaiters(active, { info, timedOut: false })
    if (routeResult && info.parentID !== undefined) routeResult(info.parentID, info)
    return info
  }

  const start = (input: StartJobInput): JobInfo => {
    const existing = jobs.get(input.id)
    if (existing?.info.status === "running") return snapshot(existing)

    const active: Active = {
      info: {
        id: input.id,
        type: input.type,
        ...(input.title === undefined ? {} : { title: input.title }),
        ...(input.metadata === undefined ? {} : { metadata: input.metadata }),
        ...(input.parentID === undefined ? {} : { parentID: input.parentID }),
        ...(input.workerID === undefined ? {} : { workerID: input.workerID }),
        status: "running",
        started_at: now(),
        depth: input.depth ?? 0,
      },
      pending: 1,
      next: 1,
      output: undefined,
      waiters: new Set(),
      promoteWaiters: new Set(),
    }
    jobs.set(input.id, active)
    if (input.parentID !== undefined) linkChild(input.parentID, input.id)
    return snapshot(active)
  }

  const extend = (id: string): boolean => {
    const active = jobs.get(id)
    if (active === undefined || active.info.status !== "running") return false
    active.pending += 1
    active.next += 1
    return true
  }

  const complete = (id: string, output: string, sequence?: number): JobInfo | undefined => {
    const active = jobs.get(id)
    if (active === undefined) return undefined
    if (active.info.status !== "running") return snapshot(active)

    const seq = sequence ?? active.next - 1
    const latest = active.output === undefined || seq > active.output.sequence ? { sequence: seq, text: output } : active.output
    active.output = latest

    const pending = active.pending - 1
    if (pending > 0) {
      active.pending = pending
      return snapshot(active)
    }
    return settle(active, "completed", { output: latest.text })
  }

  const fail = (id: string, error: unknown): JobInfo | undefined => {
    const active = jobs.get(id)
    if (active === undefined) return undefined
    if (active.info.status !== "running") return snapshot(active)
    return settle(active, "error", { error: messageOf(error) })
  }

  const cancel = (id: string): JobInfo | undefined => {
    const active = jobs.get(id)
    if (active === undefined) return undefined
    if (active.info.status !== "running") return snapshot(active)
    return settle(active, "cancelled", {})
  }

  const wait = (input: { readonly id: string; readonly timeout?: number }): Promise<JobWaitResult> =>
    new Promise<JobWaitResult>((resolve) => {
      const active = jobs.get(input.id)
      if (active === undefined) return resolve({ timedOut: false })
      if (active.info.status !== "running") return resolve({ info: snapshot(active), timedOut: false })
      if (input.timeout === undefined) {
        active.waiters.add(resolve)
        return
      }
      if (input.timeout <= 0) return resolve({ info: snapshot(active), timedOut: true })

      let done = false
      let waiter!: (result: JobWaitResult) => void
      const cancelTimer = schedule(input.timeout, () => {
        done = true
        active.waiters.delete(waiter)
        resolve({ info: snapshot(active), timedOut: true })
      })
      waiter = (result) => {
        if (done) return
        done = true
        cancelTimer()
        resolve(result)
      }
      active.waiters.add(waiter)
    })

  const promote = (id: string): JobInfo | undefined => {
    const active = jobs.get(id)
    if (active === undefined) return undefined
    if (active.info.status !== "running") return snapshot(active)
    if (active.info.metadata?.background === true) return snapshot(active)
    active.info = { ...active.info, metadata: { ...active.info.metadata, background: true } }
    const info = snapshot(active)
    for (const waiter of active.promoteWaiters) waiter(info)
    active.promoteWaiters.clear()
    return info
  }

  const waitForPromotion = (id: string): Promise<JobInfo> =>
    new Promise<JobInfo>((resolve) => {
      const active = jobs.get(id)
      // Not running: never resolves (mirrors the core's `Effect.never`).
      if (active === undefined || active.info.status !== "running") return
      if (active.info.metadata?.background === true) {
        resolve(snapshot(active))
        return
      }
      active.promoteWaiters.add(resolve)
    })

  const linkChild = (parentID: string, childID: string): void => {
    parentByChild.set(childID, parentID)
    const children = childrenByParent.get(parentID) ?? new Set<string>()
    children.add(childID)
    childrenByParent.set(parentID, children)
  }

  return {
    get size() {
      return jobs.size
    },
    start,
    extend,
    complete,
    fail,
    wait,
    waitForPromotion,
    promote,
    get: (id) => {
      const active = jobs.get(id)
      return active === undefined ? undefined : snapshot(active)
    },
    list: () =>
      [...jobs.values()]
        .map(snapshot)
        .toSorted((a, b) => a.started_at - b.started_at),
    cancel,
    linkChild,
    childrenOf: (parentID) => [...(childrenByParent.get(parentID) ?? [])],
    parentOf: (childID) => parentByChild.get(childID),
  }
}

export * as WorkerJobRegistry from "./job-registry"
