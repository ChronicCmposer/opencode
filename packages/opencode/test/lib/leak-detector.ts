// Leak detector (plan task 3.9).
//
// After teardown, the three resources a session worker can strand are:
//   1. Bun workers that were never terminated,
//   2. `MessagePort`s that were never closed,
//   3. child process groups (PGIDs) still running.
//
// The detector tracks each resource and, on `detect()`, returns an immutable
// report of the survivors. Workers are read live from a registry — pass
// `() => harness.workers` from `worker-harness` to reuse it. Ports are tracked
// through a small handle whose `close()` is observed; PGIDs are probed with a
// POSIX `kill(-pgid, 0)` by default, or an injected probe for deterministic
// unit tests.
//
// `detect()` is pure: same registered resources + same probe results means the
// same report, with no hidden mutation.

import { posixSupport } from "./process-tree"

export type LeakKind = "worker" | "port" | "process-group"

export interface Leak {
  readonly kind: LeakKind
  readonly id: string | number
  readonly detail: string
}

export interface LeakReport {
  readonly leaks: ReadonlyArray<Leak>
  /** Ids of workers still alive. */
  readonly workers: ReadonlyArray<number>
  /** Ids of ports still open. */
  readonly ports: ReadonlyArray<string>
  /** PGIDs still alive. */
  readonly processGroups: ReadonlyArray<number>
  readonly clean: boolean
}

export interface WorkerLike {
  readonly id: number
  readonly alive: boolean
}

export interface PortLike {
  close(): void
}

export interface TrackedPort {
  readonly id: string
  readonly port: PortLike
  close(): void
  readonly closed: boolean
}

export interface LeakDetectorOptions {
  /** Live worker registry, e.g. `() => harness.workers`. */
  readonly workers?: () => ReadonlyArray<WorkerLike>
  /**
   * Override the process-group liveness probe. Supplying one lifts the POSIX
   * requirement, which is what the unit tests use.
   */
  readonly isProcessGroupAlive?: (pgid: number) => boolean
  /** Override POSIX detection (defaults to `process.platform !== "win32"`). */
  readonly posix?: boolean
}

export interface LeakDetector {
  trackWorker(worker: WorkerLike): WorkerLike
  trackPort(port: PortLike, label?: string): TrackedPort
  trackProcessGroup(pgid: number): number
  readonly workers: ReadonlyArray<WorkerLike>
  readonly ports: ReadonlyArray<TrackedPort>
  readonly processGroups: ReadonlyArray<number>
  detect(): LeakReport
}

function defaultGroupProbe(pgid: number): boolean {
  try {
    process.kill(-pgid, 0)
    return true
  } catch (error) {
    return (error as NodeJS.ErrnoException).code !== "ESRCH"
  }
}

export function createLeakDetector(options: LeakDetectorOptions = {}): LeakDetector {
  const posix = options.posix ?? posixSupport().supported
  const isGroupAlive = options.isProcessGroupAlive ?? (posix ? defaultGroupProbe : undefined)

  const registeredWorkers: WorkerLike[] = []
  const registeredPorts: TrackedPort[] = []
  const registeredGroups: number[] = []
  let portSequence = 0

  const trackWorker = (worker: WorkerLike): WorkerLike => {
    registeredWorkers.push(worker)
    return worker
  }

  const trackPort = (port: PortLike, label?: string): TrackedPort => {
    const id = label ?? `port-${++portSequence}`
    let closed = false
    const tracked: TrackedPort = {
      id,
      port,
      get closed() {
        return closed
      },
      close: () => {
        if (closed) return
        closed = true
        port.close()
      },
    }
    registeredPorts.push(tracked)
    return tracked
  }

  const trackProcessGroup = (pgid: number): number => {
    if (!Number.isInteger(pgid) || pgid <= 0) {
      throw new Error(`invalid process group: ${pgid} (expected a positive integer)`)
    }
    if (!isGroupAlive) {
      throw new Error(`process group tracking requires POSIX; ${posixSupport().reason}`)
    }
    if (!registeredGroups.includes(pgid)) registeredGroups.push(pgid)
    return pgid
  }

  const detect = (): LeakReport => {
    const liveWorkers = new Set<number>()
    for (const worker of [...(options.workers?.() ?? []), ...registeredWorkers]) {
      if (worker.alive) liveWorkers.add(worker.id)
    }

    const openPorts = registeredPorts.filter((port) => !port.closed).map((port) => port.id)
    const liveGroups = registeredGroups.filter((pgid) => isGroupAlive!(pgid))

    const leaks: Leak[] = [
      ...[...liveWorkers].map((id): Leak => ({ kind: "worker", id, detail: `worker ${id} is still alive` })),
      ...openPorts.map((id): Leak => ({ kind: "port", id, detail: `port ${id} was never closed` })),
      ...liveGroups.map((pgid): Leak => ({ kind: "process-group", id: pgid, detail: `process group ${pgid} is still alive` })),
    ]

    return Object.freeze({
      leaks: Object.freeze(leaks.map((leak) => Object.freeze(leak))),
      workers: Object.freeze([...liveWorkers]),
      ports: Object.freeze(openPorts),
      processGroups: Object.freeze(liveGroups),
      clean: leaks.length === 0,
    })
  }

  return {
    trackWorker,
    trackPort,
    trackProcessGroup,
    get workers() {
      return [...(options.workers?.() ?? []), ...registeredWorkers]
    },
    get ports() {
      return registeredPorts.slice()
    },
    get processGroups() {
      return registeredGroups.slice()
    },
    detect,
  }
}
