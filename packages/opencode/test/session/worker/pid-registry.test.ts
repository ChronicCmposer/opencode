// Hub-side mirrored PID/PGID registry.
//
// The hub mirrors the child PGIDs a worker owns and group-kills them on
// terminate (terminate() reaps no children — 3.10). A worker reports each
// detached child over its control port; the hub mirrors the entry.
// POSIX-only process cases are skipped with the documented reason from
// `process-tree.posixSupport()`.

import { describe, expect, test } from "bun:test"
import {
  createPidRegistry,
  posixReaper,
  reapSupport,
  registerReported,
  type ProcessReaper,
} from "@/session/worker/pid-registry"
import { isAlive, posixSupport, spawnDetached, waitForDeath } from "../../lib/process-tree"

const support = posixSupport()

function recordingReaper(): { reaper: ProcessReaper; calls: Array<{ pgid: number; signal: string | undefined }> } {
  const calls: Array<{ pgid: number; signal: string | undefined }> = []
  return { reaper: { killGroup: (pgid, signal) => calls.push({ pgid, signal }) }, calls }
}

describe("11.5 mirrored PGID registry", () => {
  test("registers a child and is idempotent per PGID", () => {
    const registry = createPidRegistry()
    const child = registry.register({ pid: 100, pgid: 100, workerID: 1, label: "lsp" })
    expect(child).toMatchObject({ pid: 100, pgid: 100, workerID: 1, label: "lsp", generation: 0 })
    expect(registry.size).toBe(1)
    expect(registry.has(100)).toBe(true)

    registry.register({ pid: 100, pgid: 100, workerID: 1, label: "lsp", generation: 2 })
    expect(registry.size).toBe(1)
    expect(registry.get(100)?.generation).toBe(2)
  })

  test("refuses to reassign a live PGID to a different owner (fail loud)", () => {
    const registry = createPidRegistry()
    registry.register({ pid: 100, pgid: 100, workerID: 1, label: "lsp" })

    expect(() => registry.register({ pid: 100, pgid: 100, workerID: 2, label: "lsp" })).toThrow(/owned by worker 1/)
    expect(() => registry.register({ pid: 100, pgid: 100, workerID: 1, label: "mcp" })).toThrow(/owned by worker 1/)
    expect(registry.size).toBe(1)
    expect(registry.get(100)).toMatchObject({ workerID: 1, label: "lsp" })
  })

  test("rejects an invalid PGID or negative worker id (fail loud)", () => {
    const registry = createPidRegistry()
    expect(() => registry.register({ pid: 0, pgid: 0, workerID: 1, label: "x" })).toThrow()
    expect(() => registry.register({ pid: 5, pgid: 5, workerID: -1, label: "x" })).toThrow()
  })

  test("reap(workerID) group-kills only that worker's PGIDs and unregisters them", () => {
    const { reaper, calls } = recordingReaper()
    const registry = createPidRegistry({ reaper })
    registry.register({ pid: 10, pgid: 10, workerID: 1, label: "lsp" })
    registry.register({ pid: 20, pgid: 20, workerID: 1, label: "mcp" })
    registry.register({ pid: 30, pgid: 30, workerID: 2, label: "lsp" })

    const reaped = [...registry.reap(1, "SIGKILL")].sort((a, b) => a - b)
    expect(reaped).toEqual([10, 20])
    expect(calls).toEqual([
      { pgid: 10, signal: "SIGKILL" },
      { pgid: 20, signal: "SIGKILL" },
    ])
    expect(registry.has(10)).toBe(false)
    expect(registry.has(20)).toBe(false)
    expect(registry.has(30)).toBe(true)
    expect(registry.forWorker(1)).toHaveLength(0)
    expect(registry.forWorker(2)).toHaveLength(1)
  })

  test("reapAll group-kills every mirrored PGID and clears", () => {
    const { reaper, calls } = recordingReaper()
    const registry = createPidRegistry({ reaper })
    registry.register({ pid: 1, pgid: 1, workerID: 1, label: "a" })
    registry.register({ pid: 2, pgid: 2, workerID: 2, label: "b" })

    expect([...registry.reapAll()].sort((a, b) => a - b)).toEqual([1, 2])
    expect(calls).toHaveLength(2)
    expect(registry.size).toBe(0)
  })

  test("continues past a throwing kill, forgets every attempted PGID, and aggregates", () => {
    const calls: number[] = []
    const reaper: ProcessReaper = {
      killGroup: (pgid) => {
        calls.push(pgid)
        if (pgid === 20) throw new Error("kill failed")
      },
    }
    const registry = createPidRegistry({ reaper })
    registry.register({ pid: 10, pgid: 10, workerID: 1, label: "lsp" })
    registry.register({ pid: 20, pgid: 20, workerID: 1, label: "mcp" })
    registry.register({ pid: 30, pgid: 30, workerID: 1, label: "dap" })

    expect(() => registry.reap(1, "SIGKILL")).toThrow(AggregateError)
    // Every target was attempted and forgotten, including the one that failed.
    expect(calls).toEqual([10, 20, 30])
    expect(registry.size).toBe(0)
    expect(registry.has(20)).toBe(false)
  })

  test("reports POSIX-only support with a documented reason on win32", () => {
    expect(reapSupport("win32").supported).toBe(false)
    expect(reapSupport("win32").reason).not.toBe("")
    expect(reapSupport("linux").supported).toBe(true)
  })

  test("supports only the explicit POSIX allow-list and explains every other platform", () => {
    for (const platform of ["linux", "darwin", "freebsd", "openbsd", "netbsd", "aix", "sunos"] as const) {
      expect(reapSupport(platform)).toEqual({ supported: true, reason: "" })
    }
    for (const platform of ["win32", "cygwin", "android", "haiku"] as const) {
      const support = reapSupport(platform)
      expect(support.supported).toBe(false)
      expect(support.reason).not.toBe("")
    }
  })
})

// ---------------------------------------------------------------------------
// Worker→hub PID reporting. The worker spawns a detached
// shell child and reports `{ pid, pgid, label }` over its control port; the hub
// mirrors each entry into this registry. `registerReported` is the hub-side
// boundary that turns a report frame into registry entries.
// ---------------------------------------------------------------------------

describe("11.5 worker→hub reported children", () => {
  test("registers every reported entry, keyed by its PGID", () => {
    const registry = createPidRegistry()
    const registered = registerReported(registry, {
      type: "pid-report",
      workerID: 3,
      entries: [
        { pid: 101, pgid: 101, label: "shell:echo hi", workerID: 3 },
        { pid: 202, pgid: 202, label: "shell:sleep 5", workerID: 3 },
      ],
    })

    expect(registered.map((child) => child.pgid)).toEqual([101, 202])
    expect(registry.forWorker(3)).toHaveLength(2)
    expect(registry.get(101)).toMatchObject({ pid: 101, pgid: 101, label: "shell:echo hi", workerID: 3 })
    expect(registry.get(202)).toMatchObject({ pid: 202, pgid: 202, label: "shell:sleep 5", workerID: 3 })
  })

  test("an authoritative workerID overrides the frame's self-reported id", () => {
    const registry = createPidRegistry()
    registerReported(
      registry,
      { type: "pid-report", workerID: 0, entries: [{ pid: 5, pgid: 5, label: "shell", workerID: 0 }] },
      9,
    )

    expect(registry.get(5)?.workerID).toBe(9)
    expect(registry.forWorker(9)).toHaveLength(1)
    expect(registry.forWorker(0)).toHaveLength(0)
  })

  test("reported children are reaped by reap(workerID) and forgotten", () => {
    const { reaper, calls } = recordingReaper()
    const registry = createPidRegistry({ reaper })
    registerReported(registry, {
      type: "pid-report",
      workerID: 2,
      entries: [
        { pid: 11, pgid: 11, label: "shell:a", workerID: 2 },
        { pid: 12, pgid: 12, label: "shell:b", workerID: 2 },
      ],
    })

    expect(registry.reap(2, "SIGKILL")).toEqual([11, 12])
    expect(calls).toEqual([
      { pgid: 11, signal: "SIGKILL" },
      { pgid: 12, signal: "SIGKILL" },
    ])
    expect(registry.size).toBe(0)
  })
})

describe("11.5 real detached process-group reaping", () => {
  test.skipIf(!support.supported)(
    "a detached child registered in the registry is reaped by a group kill",
    async () => {
      const registry = createPidRegistry({ reaper: posixReaper() })
      const spawned = spawnDetached(["sleep", "30"])
      const child = registry.register({ pid: spawned.pid, pgid: spawned.pgid, workerID: 1, label: "lsp" })

      try {
        expect(registry.has(child.pgid)).toBe(true)
        expect(isAlive(child.pid)).toBe(true)

        expect(registry.reap(1, "SIGTERM")).toEqual([child.pgid])
        // A group SIGTERM reaches `sleep` immediately; poll tightly and fail in
        // seconds, never minutes, if the signal is lost.
        expect(await waitForDeath([child.pid], { timeoutMs: 3_000, intervalMs: 10 })).toBe(true)

        // ESRCH-safe: reaping a vanished group must not throw.
        expect(() => registry.reapAll("SIGTERM")).not.toThrow()
      } finally {
        try {
          process.kill(-child.pgid, "SIGKILL")
        } catch {
          // already gone
        }
      }
    },
    10_000,
  )
})
