// Hub-side mirrored PID/PGID registry.
//
// 3.10 proved `terminate()` reaps no children: whatever a worker spawned stays
// alive as an orphan when the isolate is torn down. The hub therefore mirrors
// every child PGID a worker owns and group-kills the mirrors on terminate
// ("hybrid PGID reaping"). A worker reports each detached child over its control
// port and the hub mirrors the entry; reaping is a whole-group kill, the only
// teardown that does not leak grandchildren (the same semantics
// `test/lib/process-tree.ts` asserts).
//
// PGID semantics are POSIX-only; Windows support is tracked separately
// (`ProcessReaper` TODO), so `reapSupport()` reports the reason the platform
// cannot be verified. The registry itself is platform-neutral bookkeeping.

import { Context } from "effect"
import { parsePositiveInt } from "@/util/parse"
import { assertWorkerID } from "./supervision"

/** Kills a whole process group. Injecting one makes reaping deterministic in tests. */
export interface ProcessReaper {
  killGroup(pgid: number, signal?: NodeJS.Signals): void
}

export interface ReapSupport {
  readonly supported: boolean
  /** Human-readable reason the platform cannot be verified; empty when supported. */
  readonly reason: string
}

const WINDOWS_REASON =
  "process-group/PGID reaping is POSIX-only; Windows is tracked separately (ProcessReaper TODO)"

/** POSIX platforms whose `kill(-pgid)` semantics are verified. */
const SUPPORTED_PLATFORMS: ReadonlyArray<NodeJS.Platform> = [
  "linux",
  "darwin",
  "freebsd",
  "openbsd",
  "netbsd",
  "aix",
  "sunos",
]

/** Explicit, platform-specific reasons for every platform outside the allow-list. */
const PLATFORM_REASONS: Readonly<Partial<Record<NodeJS.Platform, string>>> = {
  win32: WINDOWS_REASON,
  cygwin: "Cygwin process-group semantics differ from POSIX; reaping is disabled",
  android: "Android process-group reaping is unverified; reaping is disabled",
  haiku: "Haiku process-group reaping is unverified; reaping is disabled",
}

/**
 * Report whether PGID reaping is verified on `platform`. Only the explicit
 * POSIX allow-list is supported; anything else is disabled with a specific
 * reason, so an unverified platform fails safe instead of group-killing.
 */
export function reapSupport(platform: NodeJS.Platform = process.platform): ReapSupport {
  if (SUPPORTED_PLATFORMS.includes(platform)) return { supported: true, reason: "" }
  return {
    supported: false,
    reason: PLATFORM_REASONS[platform] ?? `process-group/PGID reaping is unverified on ${platform}`,
  }
}

/** The POSIX reaper: `kill(-pgid, signal)`, treating a vanished group as success. */
export function posixReaper(): ProcessReaper {
  return {
    killGroup(pgid, signal = "SIGTERM") {
      parsePositiveInt(pgid, "pgid")
      try {
        process.kill(-pgid, signal)
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error
      }
    },
  }
}

export interface RegisteredChild {
  readonly pid: number
  readonly pgid: number
  readonly workerID: number
  readonly label: string
  readonly generation: number
}

export interface RegisterChildInput {
  readonly pid: number
  /** Defaults to `pid`: the spawner spawns detached, so the child leads its own group. */
  readonly pgid?: number
  readonly workerID: number
  readonly label: string
  readonly generation?: number
}

// ---------------------------------------------------------------------------
// Worker→hub PID reporting.
//
// A session worker owns the detached shell children it spawns, but the hub owns
// reaping. The worker therefore reports each child's process group over its
// control port; the hub mirrors the entries into the `PidRegistry` above. The
// report shape is frozen because a sibling consumes it in `hub.ts`.
// ---------------------------------------------------------------------------

/** One reported child process group. `workerID` is the owner's hub id. */
export interface PidReportEntry {
  readonly pid: number
  readonly pgid: number
  readonly label: string
  readonly workerID: number
}

/** The worker→hub `pid-report` frame. */
export interface PidReport {
  readonly type: "pid-report"
  readonly workerID: number
  readonly entries: ReadonlyArray<PidReportEntry>
}

/**
 * The worker-side report seam. The shell tool calls it after a detached spawn;
 * the production implementation posts a {@link PidReport} over the worker's
 * control port. The worker does not know its own hub id, so the reporter injects
 * it (and the hub may override it with the authoritative port association).
 */
export interface PidReporterInterface {
  report(entry: Omit<PidReportEntry, "workerID">): void
}

/** The worker `PidReporter` service; absent on degraded/hub-only paths. */
export class PidReporter extends Context.Service<PidReporter, PidReporterInterface>()(
  "@opencode/session-worker/PidReporter",
) {}

export interface PidRegistry {
  readonly size: number
  /**
   * Mirror a child's PGID. Re-registering the same PGID for the same owner only
   * advances its generation; re-registering it for a different owner throws.
   */
  register(child: RegisterChildInput): RegisteredChild
  unregister(pgid: number): boolean
  has(pgid: number): boolean
  get(pgid: number): RegisteredChild | undefined
  forWorker(workerID: number): ReadonlyArray<RegisteredChild>
  entries(): ReadonlyArray<RegisteredChild>
  /**
   * Group-kill every PGID owned by `workerID`, then unregister them. Every target
   * is attempted and forgotten even if a kill throws; failures are rethrown as an
   * `AggregateError` afterwards.
   */
  reap(workerID: number, signal?: NodeJS.Signals): ReadonlyArray<number>
  /** Group-kill every mirrored PGID, then clear. Failures aggregate like `reap`. */
  reapAll(signal?: NodeJS.Signals): ReadonlyArray<number>
  clear(): void
}

export interface PidRegistryOptions {
  readonly reaper?: ProcessReaper
}

export function createPidRegistry(options: PidRegistryOptions = {}): PidRegistry {
  const reaper = options.reaper ?? posixReaper()
  const children = new Map<number, RegisteredChild>()

  const killAll = (targets: ReadonlyArray<RegisteredChild>, signal: NodeJS.Signals | undefined): ReadonlyArray<number> => {
    const reaped: number[] = []
    const failures: unknown[] = []
    for (const child of targets) {
      // Forget the target before killing: an attempted PGID is always removed,
      // so a later terminate cannot double-reap a stale mirror even if the kill
      // throws. Every remaining target is still attempted.
      children.delete(child.pgid)
      try {
        reaper.killGroup(child.pgid, signal)
        reaped.push(child.pgid)
      } catch (error) {
        failures.push(error)
      }
    }
    if (failures.length > 0) {
      throw new AggregateError(
        failures,
        `failed to reap ${failures.length} of ${targets.length} process group(s)`,
      )
    }
    return reaped
  }

  return {
    get size() {
      return children.size
    },
    register(input) {
      parsePositiveInt(input.pid, "pid")
      const pgid = input.pgid ?? input.pid
      parsePositiveInt(pgid, "pgid")
      assertWorkerID(input.workerID)
      assertLabel(input.label)
      const existing = children.get(pgid)
      if (existing !== undefined) {
        // A live PGID must never be silently reassigned to a different owner;
        // that would let a terminate kill the wrong worker's children.
        assertSameOwner(existing, input)
        // Same owner: a re-registration (e.g. a respawn) only advances generation.
        const updated: RegisteredChild = { ...existing, generation: input.generation ?? 0 }
        children.set(pgid, updated)
        return updated
      }
      const child: RegisteredChild = {
        pid: input.pid,
        pgid,
        workerID: input.workerID,
        label: input.label,
        generation: input.generation ?? 0,
      }
      children.set(pgid, child)
      return child
    },
    unregister: (pgid) => children.delete(pgid),
    has: (pgid) => children.has(pgid),
    get: (pgid) => children.get(pgid),
    forWorker: (workerID) => [...children.values()].filter((child) => child.workerID === workerID),
    entries: () => [...children.values()],
    reap: (workerID, signal) => killAll([...children.values()].filter((child) => child.workerID === workerID), signal),
    reapAll: (signal) => killAll([...children.values()], signal),
    clear: () => {
      children.clear()
    },
  }
}

/**
 * Mirror a worker's `pid-report` frame into the hub registry. Every entry is
 * keyed by its PGID via `register`.
 *
 * `workerID` is the authoritative owner when supplied: the hub knows which pool
 * slot a control port belongs to, while the worker only knows its own thread id.
 * It therefore overrides the frame's self-reported id.
 */
export function registerReported(
  registry: PidRegistry,
  report: PidReport,
  workerID: number = report.workerID,
): ReadonlyArray<RegisteredChild> {
  return report.entries.map((entry) => registry.register({ ...entry, workerID }))
}

function assertLabel(label: string): void {
  if (label.trim() === "") throw new Error("child label must not be empty")
}

function assertSameOwner(existing: RegisteredChild, input: RegisterChildInput): void {
  if (existing.workerID === input.workerID && existing.label === input.label) return
  throw new Error(
    `pgid ${existing.pgid} is owned by worker ${existing.workerID} (${existing.label}); ` +
      `refusing to reassign it to worker ${input.workerID} (${input.label})`,
  )
}
