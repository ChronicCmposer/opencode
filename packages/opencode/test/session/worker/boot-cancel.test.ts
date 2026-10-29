import { describe, expect, test } from "bun:test"
import { flush } from "../../lib/async"
import { makeSpawnGate } from "@/session/worker/worker-pools"
import { BOOT_CANCELLED, createBootLifecycle, type BootTelemetry } from "@/session/worker/cancel"

function countingPermit() {
  const releases: Array<"ready" | "failed" | undefined> = []
  return {
    permit: { release: (outcome?: "ready" | "failed") => releases.push(outcome) },
    releases,
  }
}

function deferred<T>() {
  let resolve!: (value: T) => void
  let reject!: (error: unknown) => void
  const promise = new Promise<T>((res, rej) => {
    resolve = res
    reject = rej
  })
  return { promise, resolve, reject }
}

describe("boot cancellation (D11)", () => {
  test("cancel while materializing aborts construction, releases one permit, and starts no run", async () => {
    const { permit, releases } = countingPermit()
    const materialize = deferred<{ id: string }>()
    const terminated: Array<{ id: string } | undefined> = []
    let runs = 0
    let aborted = false

    const boot = createBootLifecycle({
      permit,
      materialize: (signal) => {
        signal.addEventListener("abort", () => {
          aborted = true
          materialize.reject(new Error("aborted"))
        })
        return materialize.promise
      },
      terminate: (partial) => {
        terminated.push(partial)
      },
      onRun: () => {
        runs++
      },
    })

    const cancelled = await boot.cancel()
    expect(cancelled).toEqual(BOOT_CANCELLED)
    expect(aborted).toBe(true)
    expect(releases).toEqual(["failed"])
    expect(terminated).toEqual([undefined])
    expect(runs).toBe(0)
    expect(await boot.ready).toEqual(BOOT_CANCELLED)
  })

  test("a graph that is ready but not started takes the same path", async () => {
    const { permit, releases } = countingPermit()
    const terminated: string[] = []
    let runs = 0
    const boot = createBootLifecycle({
      permit,
      materialize: async () => "worker",
      terminate: (partial) => {
        if (partial !== undefined) terminated.push(partial)
      },
      onRun: () => {
        runs++
      },
    })

    expect(await boot.ready).toBe("worker")
    expect(boot.state).toBe("ready")
    expect(await boot.cancel()).toEqual(BOOT_CANCELLED)
    expect(terminated).toEqual(["worker"])
    expect(releases).toEqual(["failed"])
    expect(runs).toBe(0)
    expect(boot.start("worker")).toBe(false)
  })

  test("death during abort is a cancel win plus telemetry", async () => {
    const { permit, releases } = countingPermit()
    const telemetry: BootTelemetry[] = []
    const boot = createBootLifecycle({
      permit,
      materialize: (signal) =>
        new Promise((_resolve, reject) => {
          signal.addEventListener("abort", () => reject(new Error("isolate died during abort")))
        }),
      terminate: () => undefined,
      onTelemetry: (event) => telemetry.push(event),
    })

    expect(await boot.cancel()).toEqual(BOOT_CANCELLED)
    expect(releases).toHaveLength(1)
    expect(telemetry).toEqual([
      { type: "abort-death", phase: "boot", reason: "materialization or termination failed during abort" },
    ])
  })

  test("cancel is idempotent and never double-releases the permit", async () => {
    const { permit, releases } = countingPermit()
    const boot = createBootLifecycle({
      permit,
      materialize: (signal) =>
        new Promise((_resolve, reject) => {
          signal.addEventListener("abort", () => reject(new Error("aborted")))
        }),
      terminate: () => undefined,
    })

    const [first, second] = await Promise.all([boot.cancel(), boot.cancel()])
    expect(first).toBe(second)
    expect(releases).toEqual(["failed"])
  })

  test("after the run starts, boot cancel yields to normal cancellation", async () => {
    const { permit, releases } = countingPermit()
    let runs = 0
    const boot = createBootLifecycle({
      permit,
      materialize: async () => "worker",
      terminate: () => undefined,
      onRun: () => {
        runs++
      },
    })
    const worker = await boot.ready
    if (typeof worker !== "string") throw new Error("unexpected boot cancel")
    expect(boot.start(worker)).toBe(true)
    expect(runs).toBe(1)
    expect(await boot.cancel()).toBeUndefined()
    expect(releases).toEqual([])
  })

  test("the spawn permit count is unchanged across boot cancel", async () => {
    const gate = makeSpawnGate({ timeoutMs: 1_000 })
    const permit = await gate.acquire()
    expect(gate.held).toBe(true)

    const boot = createBootLifecycle({
      permit,
      materialize: (signal) =>
        new Promise((_resolve, reject) => {
          signal.addEventListener("abort", () => reject(new Error("aborted")))
        }),
      terminate: () => undefined,
    })
    expect(await boot.cancel()).toEqual(BOOT_CANCELLED)
    expect(gate.held).toBe(false)

    const next = await gate.acquire()
    expect(gate.held).toBe(true)
    next.release("ready")
    expect(gate.held).toBe(false)
    expect(gate.waiting).toBe(0)
    await flush()
  })
})
