// Production boot cancellation (D11): the real spawn/transport
// path owns the spawn permit through a boot lifecycle. A cancel before the
// ready handshake returns the typed boot outcome, terminates the partially
// materialized isolate, releases the one permit, and never runs work. The
// permit accounting is idempotent and leaves the pool ready for the next spawn.

import { describe, expect, test } from "bun:test"
import { Database } from "bun:sqlite"
import { existsSync, mkdtempSync, rmSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { makeSpawnGate } from "@/session/worker/worker-pools"
import {
  createBunExecutorWorkerPool,
  createBunExecutorTransport,
  createSessionHub,
  createWorkerCancellation,
} from "@/session/worker/hub"
import { createTestBarrier } from "../../lib/run-barrier-test"
import type { ExecutorWorker } from "@/session/worker/execute"
import { BOOT_CANCELLED, type BootTelemetry } from "@/session/worker/cancel"
import { flush, settleOf, sid } from "../../lib/async"

/** The gated real worker: announces boot, delays, then installs the executor. */
const GATED_FIXTURE = new URL("../../fixture/boot-gated-worker.ts", import.meta.url)
/** The dependency-light executor fixture (boots immediately). */
const EXECUTOR_FIXTURE = new URL("../../fixture/session-execute-worker.ts", import.meta.url)

/** Boot cancellation is wired on the executor worker surface. */
type BootCancellable = { cancelBoot?: () => Promise<typeof BOOT_CANCELLED | undefined> }

function cancelBootOf(worker: ExecutorWorker): Promise<typeof BOOT_CANCELLED | undefined> {
  return (worker as ExecutorWorker & BootCancellable).cancelBoot?.() ?? Promise.resolve(undefined)
}

async function waitFor(predicate: () => boolean, label: string, timeoutMs = 5_000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${label}`)
    await flush()
    await new Promise((resolve) => setTimeout(resolve, 10))
  }
}

const runFrame = (session: string) => ({ sessionID: sid(session), depth: 0 })

describe("production boot cancellation", () => {
  test("cancel during materialization returns the boot outcome, releases one permit, and runs nothing", async () => {
    const dir = mkdtempSync(join(tmpdir(), "boot-cancel-"))
    const marker = join(dir, "executed")
    const gate = makeSpawnGate({ timeoutMs: 1_000 })
    const telemetry: BootTelemetry[] = []
    const pool = createBunExecutorWorkerPool({
      entry: GATED_FIXTURE,
      workerData: { bootDelayMs: 60_000, markerPath: marker },
      spawnGate: gate,
      onBootTelemetry: (event) => telemetry.push(event),
    })
    try {
      const worker = pool.spawn(1) as ExecutorWorker
      const run = settleOf(worker.execute(runFrame("ses_boot_cancel")))
      // The permit is granted (and the isolate constructed) before `ready`.
      await waitFor(() => gate.held === true, "the spawn permit")

      const cancelled = await cancelBootOf(worker)
      expect(cancelled).toEqual(BOOT_CANCELLED)

      const settled = await run
      expect(settled.ok).toBe(false)

      // The one permit is free exactly once, so the next spawn is immediate.
      expect(gate.held).toBe(false)
      expect(gate.waiting).toBe(0)
      expect(existsSync(marker)).toBe(false)
      expect(telemetry).toEqual([{ type: "boot-cancelled", phase: "boot" }])

      const next = createBunExecutorWorkerPool({ entry: EXECUTOR_FIXTURE, workKind: "instant", spawnGate: gate })
      const nextWorker = next.spawn(2) as ExecutorWorker
      const nextRun = settleOf(nextWorker.execute(runFrame("ses_after_cancel")))
      await waitFor(() => gate.held === false, "the next spawn's ready handshake")
      expect((await nextRun).ok).toBe(true)
      await next.terminateAll()
    } finally {
      await pool.terminateAll()
      rmSync(dir, { recursive: true, force: true })
    }
  })

  test("a boot cancel closes through the shutdown frame before terminate", async () => {
    const calls: string[] = []
    const original = (globalThis as { Worker?: unknown }).Worker
    class ClosingWorker {
      onmessage: ((event: MessageEvent) => void) | null = null
      onerror: ((event: ErrorEvent) => void) | null = null
      private readonly listeners = new Set<(event: { data: unknown }) => void>()

      postMessage(message: unknown): void {
        const type = (message as { type?: unknown } | undefined)?.type
        calls.push(`post:${String(type)}`)
        if (type !== "shutdown") return
        for (const listener of this.listeners) listener({ data: { type: "shutdown-ack", id: 0 } })
      }

      addEventListener(type: string, listener: (event: { data: unknown }) => void): void {
        if (type === "message") this.listeners.add(listener)
      }

      removeEventListener(type: string, listener: (event: { data: unknown }) => void): void {
        if (type === "message") this.listeners.delete(listener)
      }

      async terminate(): Promise<number> {
        calls.push("terminate")
        return 0
      }
    }
    ;(globalThis as { Worker?: unknown }).Worker = ClosingWorker
    const gate = makeSpawnGate({ timeoutMs: 1_000 })
    try {
      const transport = createBunExecutorTransport({ entry: "fake://boot-close", spawnGate: gate })
      transport.post({ type: "run", id: 1, sessionID: sid("ses_boot_close"), kind: "session" })
      await waitFor(() => gate.held === true, "the spawn permit")
      // The permit is granted before the isolate is constructed; let the boot
      // continuation reach `new Worker` so this cancels the materializing boot.
      await flush()

      expect(await transport.cancelBoot?.()).toEqual(BOOT_CANCELLED)
      // The typed boot outcome is an orderly close: the isolate sees `shutdown`
      // (so it can release its WAL handle/lease) before the thread is destroyed.
      expect(calls).toEqual(["post:run", "post:shutdown", "terminate"])
      // The single permit is released exactly once and the close is idempotent.
      expect(gate.held).toBe(false)
      expect(await transport.cancelBoot?.()).toEqual(BOOT_CANCELLED)
      expect(calls.filter((call) => call === "post:shutdown")).toHaveLength(1)
      expect(calls.filter((call) => call === "terminate")).toHaveLength(1)
      expect(gate.held).toBe(false)
    } finally {
      ;(globalThis as { Worker?: unknown }).Worker = original
    }
  })

  test("a boot cancel after the WAL open releases the write lock (no SQLITE_BUSY)", async () => {
    const dir = mkdtempSync(join(tmpdir(), "boot-cancel-wal-"))
    const dbPath = join(dir, "boot.sqlite")
    const dbReadyPath = join(dir, "db-ready")
    const gate = makeSpawnGate({ timeoutMs: 1_000 })
    const pool = createBunExecutorWorkerPool({
      entry: GATED_FIXTURE,
      // The fixture opens the WAL handle and holds `BEGIN IMMEDIATE` before the
      // ready handshake, exactly like a production worker's pre-ready DB open.
      workerData: { bootDelayMs: 60_000, dbPath, dbReadyPath },
      spawnGate: gate,
      timeoutMs: 10_000,
    })
    try {
      const worker = pool.spawn(1) as ExecutorWorker
      const run = settleOf(worker.execute(runFrame("ses_boot_wal")))
      await waitFor(() => gate.held === true, "the spawn permit")
      await waitFor(() => existsSync(dbReadyPath), "the worker's WAL write lock")

      // The worker holds the only write lock, so a second immediate writer must
      // fail before the cancel.
      const locked = new Database(dbPath)
      expect(() => locked.exec("BEGIN IMMEDIATE")).toThrow()
      locked.close()

      expect(await cancelBootOf(worker)).toEqual(BOOT_CANCELLED)
      expect((await run).ok).toBe(false)
      expect(gate.held).toBe(false)

      // The orderly close released the handle/transaction, so the next writer
      // can take the lock immediately.
      const next = new Database(dbPath)
      try {
        next.exec("BEGIN IMMEDIATE")
        next.exec("COMMIT")
      } finally {
        next.close()
      }
    } finally {
      await pool.terminateAll()
      rmSync(dir, { recursive: true, force: true })
    }
  })

  test("cancel is idempotent and never double-releases the permit", async () => {
    const dir = mkdtempSync(join(tmpdir(), "boot-cancel-idem-"))
    const marker = join(dir, "executed")
    const gate = makeSpawnGate({ timeoutMs: 1_000 })
    const pool = createBunExecutorWorkerPool({
      entry: GATED_FIXTURE,
      workerData: { bootDelayMs: 60_000, markerPath: marker },
      spawnGate: gate,
    })
    try {
      const worker = pool.spawn(1) as ExecutorWorker
      const run = settleOf(worker.execute(runFrame("ses_boot_idem")))
      await waitFor(() => gate.held === true, "the spawn permit")

      const [first, second] = await Promise.all([cancelBootOf(worker), cancelBootOf(worker)])
      expect(first).toBe(second)
      expect(first).toEqual(BOOT_CANCELLED)
      expect((await run).ok).toBe(false)
      expect(gate.held).toBe(false)

      // A third cancel after the boot settled stays the same typed outcome.
      expect(await cancelBootOf(worker)).toEqual(BOOT_CANCELLED)
      expect(gate.held).toBe(false)
      expect(existsSync(marker)).toBe(false)
    } finally {
      await pool.terminateAll()
      rmSync(dir, { recursive: true, force: true })
    }
  })

  test("a partial isolate that dies during abort is still a cancel win plus telemetry", async () => {
    // Fault-injected isolate: a terminate that throws models the isolate dying
    // while the boot abort tears it down. A real worker cannot be made to fail
    // termination deterministically, so the transport surface is exercised
    // directly with a dying terminate.
    const original = (globalThis as { Worker?: unknown }).Worker
    class DyingWorker {
      onmessage: ((event: MessageEvent) => void) | null = null
      onerror: ((event: ErrorEvent) => void) | null = null
      postMessage(): void {}
      async terminate(): Promise<number> {
        throw new Error("isolate died during abort")
      }
    }
    ;(globalThis as { Worker?: unknown }).Worker = DyingWorker
    const gate = makeSpawnGate({ timeoutMs: 1_000 })
    const telemetry: BootTelemetry[] = []
    try {
      const transport = createBunExecutorTransport({
        entry: "fake://dying",
        spawnGate: gate,
        onBootTelemetry: (event) => telemetry.push(event),
      })
      transport.post({ type: "run", id: 1, sessionID: sid("ses_dying"), kind: "session" })
      await waitFor(() => gate.held === true, "the spawn permit")
      await flush()

      expect(await transport.cancelBoot?.()).toEqual(BOOT_CANCELLED)
      expect(gate.held).toBe(false)
      expect(telemetry).toEqual([
        {
          type: "abort-death",
          phase: "boot",
          reason: "materialization or termination failed during abort",
        },
      ])
    } finally {
      ;(globalThis as { Worker?: unknown }).Worker = original
    }
  })
})

describe("dispatch boot cancellation", () => {
  test("cancel during the first boot reports a typed boot outcome and releases the permit", async () => {
    const dir = mkdtempSync(join(tmpdir(), "boot-cancel-dispatch-"))
    const marker = join(dir, "executed")
    const gate = makeSpawnGate({ timeoutMs: 1_000 })
    const sessionID = sid("ses_boot_dispatch")
    const pool = createBunExecutorWorkerPool({
      entry: GATED_FIXTURE,
      workerData: { bootDelayMs: 60_000, markerPath: marker },
      spawnGate: gate,
    })
    const hub = createSessionHub({ ...{ barrier: createTestBarrier() }, pool, cancellation: (p) => createWorkerCancellation(p, { graceMs: 5_000 }) })
    try {
      const run = settleOf(hub.ensureRunning(sessionID))
      // The permit is held once the isolate has been constructed, before the
      // worker can announce ready: this is the first-boot window.
      await waitFor(() => gate.held === true, "the spawn permit")

      const report = await hub.cancelWithReport(sessionID)
      expect(report.outcome?.mode).toBe("boot")
      expect(report.outcome?.winner).toBe("cancel")
      // A boot cancel never started a run, so no worker published the terminal
      // idle. `acknowledged: false` makes run-state write the fallback idle.
      expect(report.outcome?.acknowledged).toBe(false)
      // The typed boot path releases the one permit; it is not a terminate-only
      // settlement that would leave the gate wedged.
      expect(gate.held).toBe(false)
      expect((await run).ok).toBe(false)
      expect(existsSync(marker)).toBe(false)
    } finally {
      await hub.terminateAll()
      rmSync(dir, { recursive: true, force: true })
    }
  })
})
