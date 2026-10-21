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
      assertPositiveInteger(input.pid, "pid")
      const pgid = input.pgid ?? input.pid
      assertPositiveInteger(pgid, "pgid")
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
 *
 * `meta` is validated before `spawn` runs, so a bad registration never leaks a
 * process. If mirroring still fails (e.g. a PGID already owned by another
 * worker) the freshly spawned group is killed rather than left unregistered.
 */
export function spawnRegistered(
  spawn: () => SpawnLike,
  registry: PidRegistry,
  meta: SpawnRegisteredMeta,
  reaper: ProcessReaper = posixReaper(),
): RegisteredChild {
  assertWorkerID(meta.workerID)
  assertLabel(meta.label)
  const spawned = spawn()
  if (spawned.pid === undefined) throw new Error(`spawn returned no pid (label ${meta.label})`)
  const pgid = spawned.pgid ?? spawned.pid
  try {
    return registry.register({
      pid: spawned.pid,
      pgid,
      workerID: meta.workerID,
      label: meta.label,
      generation: meta.generation,
    })
  } catch (error) {
    try {
      reaper.killGroup(pgid, "SIGKILL")
    } catch {
      // Best effort: the registration failure is the actionable error.
    }
    throw error
  }
}

function assertWorkerID(workerID: number): void {
  if (!Number.isInteger(workerID) || workerID < 0) {
    throw new Error(`invalid workerID: ${workerID} (expected a non-negative integer)`)
  }
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

function assertPositiveInteger(value: number, label: string): void {
  if (!Number.isInteger(value) || value < 1) {
    throw new Error(`invalid ${label}: ${value} (expected a positive integer)`)
  }
}
