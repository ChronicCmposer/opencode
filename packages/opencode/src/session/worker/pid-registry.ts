// Phase 11.5 — hub-side mirrored PID/PGID registry (plan decision line 52).
//
// 3.10 proved `terminate()` reaps no children: whatever a worker spawned stays
// alive as an orphan when the isolate is torn down. The hub therefore mirrors
// every child PGID a worker owns and group-kills the mirrors on terminate
// ("hybrid PGID reaping"). Two properties are load-bearing:
//
//   • registration happens BEFORE the spawn result is returned, so a terminate
//     racing a spawn can never miss a freshly-created PGID; and
//   • reaping is a whole-group kill, which is the only teardown that does not
//     leak grandchildren (the same semantics `test/lib/process-tree.ts` asserts).
//
// PGID semantics are POSIX-only; Windows support is tracked separately
// (`ProcessReaper` TODO), so `reapSupport()` reports the reason the platform
// cannot be verified. The registry itself is platform-neutral bookkeeping.

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

export function reapSupport(platform: NodeJS.Platform = process.platform): ReapSupport {
  if (platform === "win32") return { supported: false, reason: WINDOWS_REASON }
  return { supported: true, reason: "" }
}

/** The POSIX reaper: `kill(-pgid, signal)`, treating a vanished group as success. */
export function posixReaper(): ProcessReaper {
  return {
    killGroup(pgid, signal = "SIGTERM") {
      assertPositiveInteger(pgid, "pgid")
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

export interface PidRegistry {
  readonly size: number
  /** Mirror a child's PGID. Idempotent per PGID (re-registration updates the generation). */
  register(child: RegisterChildInput): RegisteredChild
  unregister(pgid: number): boolean
  has(pgid: number): boolean
  get(pgid: number): RegisteredChild | undefined
  forWorker(workerID: number): ReadonlyArray<RegisteredChild>
  entries(): ReadonlyArray<RegisteredChild>
  /** Group-kill every PGID owned by `workerID`, then unregister them. */
  reap(workerID: number, signal?: NodeJS.Signals): ReadonlyArray<number>
  /** Group-kill every mirrored PGID, then clear. */
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
    for (const child of targets) {
      reaper.killGroup(child.pgid, signal)
      children.delete(child.pgid)
      reaped.push(child.pgid)
    }
    return reaped
  }

  return {
    get size() {
      return children.size
    },
    register(input) {
      assertPositiveInteger(input.pid, "pid")
      const pgid = input.pgid ?? input.pid
      assertPositiveInteger(pgid, "pgid")
      if (!Number.isInteger(input.workerID) || input.workerID < 0) {
        throw new Error(`invalid workerID: ${input.workerID} (expected a non-negative integer)`)
      }
      if (input.label.trim() === "") throw new Error("child label must not be empty")
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

export interface SpawnLike {
  readonly pid?: number
  readonly pgid?: number
}

export interface SpawnRegisteredMeta {
  readonly workerID: number
  readonly label: string
  readonly generation?: number
}

/**
 * Run `spawn` synchronously and mirror the resulting PGID before returning, so
 * the PGID is present in the registry the instant the caller can observe it.
 * `spawn` is the synchronous `Bun.spawn(...)` (it exposes `pid` immediately);
 * the returned child is the registered mirror.
 */
export function spawnRegistered(
  spawn: () => SpawnLike,
  registry: PidRegistry,
  meta: SpawnRegisteredMeta,
): RegisteredChild {
  const spawned = spawn()
  if (spawned.pid === undefined) throw new Error(`spawn returned no pid (label ${meta.label})`)
  return registry.register({
    pid: spawned.pid,
    pgid: spawned.pgid ?? spawned.pid,
    workerID: meta.workerID,
    label: meta.label,
    generation: meta.generation,
  })
}

function assertPositiveInteger(value: number, label: string): void {
  if (!Number.isInteger(value) || value < 1) {
    throw new Error(`invalid ${label}: ${value} (expected a positive integer)`)
  }
}
