// Phase 4.1 — production session-worker bootstrap (plan task 4.1).
//
// Proves the PRODUCTION worker entry point `src/session/worker/bootstrap.ts`
// (distinct from the test fixture `test/fixture/effect-worker.ts`) boots an
// Effect runtime inside a Bun Worker isolate and returns a value across the
// boundary. Uses the existing `test/lib/worker-harness.ts` primitives.

import { describe, expect, test } from "bun:test"
import { threadId as mainThreadId } from "node:worker_threads"
import { createWorkerHarness } from "../../lib/worker-harness"
import type { BootstrapInbound, BootstrapOutbound } from "../../../src/session/worker/bootstrap"

const WORKER_ENTRY = new URL("../../../src/session/worker/bootstrap.ts", import.meta.url)

describe("production worker bootstrap", () => {
  test("runs a trivial Effect in an isolate and returns its value", async () => {
    await using harness = createWorkerHarness({ cores: 1 })
    const worker = harness.spawn<BootstrapOutbound, BootstrapInbound>(WORKER_ENTRY)

    const ready = await worker.waitFor("ready")
    if (ready.type !== "ready") throw new Error("bootstrap worker did not report ready")
    expect(ready.threadId).toBeGreaterThan(0)
    expect(ready.threadId).not.toBe(mainThreadId)

    worker.post({ type: "run", id: 1, value: 21 })
    const result = await worker.waitFor((message) => message.type === "value")
    if (result.type !== "value") throw new Error("bootstrap worker did not return a value")
    expect(result.id).toBe(1)
    expect(result.value).toBe(42)
  })

  test("surfaces an Effect failure as a structured error message", async () => {
    await using harness = createWorkerHarness({ cores: 1 })
    const worker = harness.spawn<BootstrapOutbound, BootstrapInbound>(WORKER_ENTRY)
    await worker.waitFor("ready")

    worker.post({ type: "fail", id: 7, message: "boom" })
    const failed = await worker.waitFor((message) => message.type === "error")
    if (failed.type !== "error") throw new Error("bootstrap worker did not return an error")
    expect(failed.id).toBe(7)
    expect(failed.message).toBe("boom")
  })
})
