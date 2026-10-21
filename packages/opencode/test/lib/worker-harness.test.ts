import { describe, expect, test } from "bun:test"
import path from "path"
import { coreCount, createWorkerHarness, type WorkerMessage } from "./worker-harness"

const workerEntry = path.join(import.meta.dir, "../fixture/harness-worker.ts")

describe("core count injection", () => {
  test("uses the injected core count for deterministic pool sizing", () => {
    const cores = coreCount(7)

    expect(cores.cores).toBe(7)
    expect(cores.availableParallelism()).toBe(7)
  })

  test("falls back to real available parallelism when not injected", () => {
    expect(coreCount().cores).toBeGreaterThanOrEqual(1)
  })

  test("fails loudly on an invalid injected count", () => {
    expect(() => coreCount(0)).toThrow(/core count/)
    expect(() => coreCount(2.5)).toThrow(/core count/)
  })
})

describe("worker harness", () => {
  test("spawns a worker, observes its messages, then terminates it", async () => {
    await using harness = createWorkerHarness({ cores: 3 })
    expect(harness.cores).toBe(3)

    const worker = harness.spawn<WorkerMessage>(workerEntry)
    const ready = await worker.received((message) => message.type === "ready")
    expect(ready.pid).toBeGreaterThan(0)
    expect(worker.alive).toBe(true)

    worker.post({ type: "echo", data: { value: 42 } })
    const echo = await worker.waitFor("echo")
    expect(echo.data).toEqual({ value: 42 })

    const code = await worker.terminate()
    expect(typeof code).toBe("number")
    expect(worker.alive).toBe(false)
    expect(harness.workers).toHaveLength(1)
  })

  test("waitFor reports a descriptive timeout error", async () => {
    await using harness = createWorkerHarness({ cores: 1, timeoutMs: 100 })
    const worker = harness.spawn<WorkerMessage>(workerEntry)
    await worker.received((message) => message.type === "ready")

    await expect(worker.waitFor("never")).rejects.toThrow(/timed out waiting for worker message/)
  })

  test("terminateAll stops every spawned worker", async () => {
    await using harness = createWorkerHarness({ cores: 2 })
    const first = harness.spawn<WorkerMessage>(workerEntry)
    const second = harness.spawn<WorkerMessage>(workerEntry)

    await Promise.all([
      first.received((message) => message.type === "ready"),
      second.received((message) => message.type === "ready"),
    ])
    await harness.terminateAll()

    expect(first.alive).toBe(false)
    expect(second.alive).toBe(false)
  })

  test("surfaces a worker-side failure as an observable message", async () => {
    await using harness = createWorkerHarness({ cores: 1 })
    const worker = harness.spawn<WorkerMessage>(workerEntry)
    await worker.received((message) => message.type === "ready")

    worker.post({ type: "error", message: "boom" })
    const error = await worker.waitFor("error")
    expect(error.message).toBe("boom")
  })
})
