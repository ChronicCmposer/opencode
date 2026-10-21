// Phase 3.6 — cross-worker fake clock (plan task 3.6).
//
// Effect's `TestClock` is per-runtime and cannot cross isolates. Instead the
// worker owns a frame-driven clock state; the hub drives it over a dedicated
// control `MessagePort` (`clock.set` / `clock.advance` / `clock.read`) and the
// worker's Effect runtime is given a `Clock` layer backed by that state, so
// `Effect.sleep` resolves exactly when the hub advances time.

import { describe, expect, test } from "bun:test"
import { createWorkerHarness, type HarnessWorker, type WorkerHarness } from "./worker-harness"
import {
  createClockController,
  type ClockController,
  type ClockSleptEvent,
  type ClockWorkerInbound,
  type ClockWorkerOutbound,
} from "./worker-clock"

const CLOCK_WORKER = new URL("../fixture/clock-worker.ts", import.meta.url)

type WorkerOut = ClockWorkerOutbound | MessagePort

interface ClockWorkerContext {
  readonly harness: WorkerHarness
  readonly worker: HarnessWorker<WorkerOut, ClockWorkerInbound>
  readonly controller: ClockController
}

async function pollUntil(predicate: () => Promise<boolean>, timeoutMs = 2000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (await predicate()) return
    await new Promise((resolve) => setTimeout(resolve, 5))
  }
  throw new Error(`pollUntil timed out after ${timeoutMs}ms`)
}

async function spawnClockWorker(): Promise<ClockWorkerContext> {
  const harness = createWorkerHarness()
  const worker = harness.spawn<WorkerOut, ClockWorkerInbound>(CLOCK_WORKER)
  const port = await worker.waitFor((message) => message instanceof MessagePort)
  const controller = createClockController({ port: port as MessagePort })
  return { harness, worker, controller }
}

async function withClockWorker(body: (context: ClockWorkerContext) => Promise<void>) {
  const context = await spawnClockWorker()
  try {
    await body(context)
  } finally {
    context.controller.dispose()
    await context.harness.dispose()
  }
}

describe("worker fake clock", () => {
  test("reports the worker clock's initial time", async () => {
    await withClockWorker(async ({ controller }) => {
      const state = await controller.read()
      expect(state.timestamp).toBe(0)
      expect(state.pending).toBe(0)
    })
  })

  test("advance resolves an Effect.sleep scheduled for the advanced time", async () => {
    await withClockWorker(async ({ worker, controller }) => {
      worker.post({ type: "sleep", id: "s1", ms: 1000 })
      await pollUntil(async () => (await controller.read()).pending === 1)

      const advanced = await controller.advance(1000)
      expect(advanced.timestamp).toBe(1000)
      expect(advanced.pending).toBe(0)

      const slept = await worker.waitFor((message) => (message as ClockSleptEvent).type === "slept")
      expect((slept as ClockSleptEvent).id).toBe("s1")
      expect((slept as ClockSleptEvent).at).toBe(1000)
    })
  })

  test("advancing past a sleep's target resolves it at the target time", async () => {
    await withClockWorker(async ({ worker, controller }) => {
      worker.post({ type: "sleep", id: "s2", ms: 500 })
      await pollUntil(async () => (await controller.read()).pending === 1)

      await controller.advance(2000)

      const slept = await worker.waitFor((message) => (message as ClockSleptEvent).type === "slept")
      expect((slept as ClockSleptEvent).at).toBe(500)
    })
  })

  test("an insufficient advance leaves the sleep pending", async () => {
    await withClockWorker(async ({ worker, controller }) => {
      worker.post({ type: "sleep", id: "s3", ms: 1000 })
      await pollUntil(async () => (await controller.read()).pending === 1)

      const advanced = await controller.advance(400)
      expect(advanced.timestamp).toBe(400)
      expect(advanced.pending).toBe(1)

      const slept = worker.messages.filter((message) => (message as ClockSleptEvent).type === "slept")
      expect(slept).toHaveLength(0)
    })
  })

  test("set jumps to an absolute timestamp and fires due sleeps", async () => {
    await withClockWorker(async ({ worker, controller }) => {
      worker.post({ type: "sleep", id: "s4", ms: 1000 })
      await pollUntil(async () => (await controller.read()).pending === 1)

      const set = await controller.set(1000)
      expect(set.timestamp).toBe(1000)
      expect(set.pending).toBe(0)

      const slept = await worker.waitFor((message) => (message as ClockSleptEvent).type === "slept")
      expect((slept as ClockSleptEvent).at).toBe(1000)
    })
  })

  test("pending sleeps fire in timestamp order", async () => {
    await withClockWorker(async ({ worker, controller }) => {
      worker.post({ type: "sleep", id: "late", ms: 300 })
      worker.post({ type: "sleep", id: "early", ms: 100 })
      await pollUntil(async () => (await controller.read()).pending === 2)

      await controller.advance(300)

      const slept = await worker.waitFor(
        (message) =>
          (message as ClockSleptEvent).type === "slept" && (message as ClockSleptEvent).id === "early",
      )
      expect((slept as ClockSleptEvent).at).toBe(100)
      const ids = worker.messages
        .filter((message) => (message as ClockSleptEvent).type === "slept")
        .map((message) => (message as ClockSleptEvent).id)
      expect(ids).toEqual(["early", "late"])
    })
  })
})
