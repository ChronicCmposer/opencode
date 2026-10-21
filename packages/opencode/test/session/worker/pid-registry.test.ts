// Phase 11.5 — hub-side mirrored PID/PGID registry (plan decision line 52).
//
// The hub mirrors the child PGIDs a worker owns and group-kills them on
// terminate (terminate() reaps no children — 3.10). Registration must happen
// BEFORE the spawn returns so a terminate racing the spawn cannot miss a PGID.
// POSIX-only process cases are skipped with the documented reason from
// `process-tree.posixSupport()`.

import { describe, expect, test } from "bun:test"
import {
  createPidRegistry,
  posixReaper,
  reapSupport,
  spawnRegistered,
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

  test("rejects an invalid PGID or negative worker id (fail loud)", () => {
    const registry = createPidRegistry()
    expect(() => registry.register({ pid: 0, pgid: 0, workerID: 1, label: "x" })).toThrow()
    expect(() => registry.register({ pid: 5, pgid: 5, workerID: -1, label: "x" })).toThrow()
  })

  test("registers the PGID synchronously before the spawn result is returned", () => {
    const registry = createPidRegistry()
    let spawnCalls = 0

    const child = spawnRegistered(
      () => {
        spawnCalls++
        return { pid: 4242 }
      },
      registry,
      { workerID: 9, label: "mcp" },
    )

    // No `await` has happened: the registry is already populated.
    expect(spawnCalls).toBe(1)
    expect(child).toMatchObject({ pid: 4242, pgid: 4242, workerID: 9, label: "mcp" })
    expect(registry.has(4242)).toBe(true)
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

  test("reports POSIX-only support with a documented reason on win32", () => {
    expect(reapSupport("win32").supported).toBe(false)
    expect(reapSupport("win32").reason).not.toBe("")
    expect(reapSupport("linux").supported).toBe(true)
  })
})

describe("11.5 real detached process-group reaping", () => {
  test.skipIf(!support.supported)(
    "a detached child registered via spawnRegistered is reaped by a group kill",
    async () => {
      const registry = createPidRegistry({ reaper: posixReaper() })
      const child = spawnRegistered(() => spawnDetached(["sleep", "30"]), registry, { workerID: 1, label: "lsp" })

      try {
        expect(registry.has(child.pgid)).toBe(true)
        expect(isAlive(child.pid)).toBe(true)

        expect(registry.reap(1, "SIGTERM")).toEqual([child.pgid])
        expect(await waitForDeath([child.pid], { timeoutMs: 10_000 })).toBe(true)

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
    20_000,
  )
})
