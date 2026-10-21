// Phase 3.13 — Effect-in-a-worker bootstrap (plan task 3.13).
//
// Proves that a Bun `Worker` isolate can (a) import `effect` and run a trivial
// Effect program that returns a value, and (b) resolve the repo `@/` alias to
// real `src` modules. The hub imports the same `@/` modules so an equality
// assertion proves both sides resolved to identical modules, not look-alikes.

import { describe, expect, test } from "bun:test"
import { threadId as mainThreadId } from "node:worker_threads"
import { decodeDataUrl } from "@/util/data-url"
import { iife } from "@/util/iife"
import { createWorkerHarness } from "./worker-harness"
import type { EffectWorkerInbound, EffectWorkerOutbound } from "../fixture/effect-worker"

const WORKER_ENTRY = new URL("../fixture/effect-worker.ts", import.meta.url)

const ALIAS_SENTINEL = "alias-resolved"
const ALIAS_DATA_URL = `data:text/plain;base64,${Buffer.from(ALIAS_SENTINEL).toString("base64")}`

describe("effect in a worker", () => {
  test("imports effect and the @/ alias, runs an Effect, and returns its value", async () => {
    await using harness = createWorkerHarness({ cores: 1 })
    const worker = harness.spawn<EffectWorkerOutbound, EffectWorkerInbound>(WORKER_ENTRY)

    const ready = await worker.waitFor("ready")
    expect(ready.threadId).toBeGreaterThan(0)
    expect(ready.threadId).not.toBe(mainThreadId)

    worker.post({ type: "compute", value: 21 })
    const result = await worker.waitFor("computed")

    // Effect ran inside the isolate and its success channel crossed the boundary.
    expect(result.value).toBe(42)
    // `@/util/data-url` resolved in-worker to the same module the hub imports.
    expect(result.alias).toBe(decodeDataUrl(ALIAS_DATA_URL))
    expect(result.alias).toBe(ALIAS_SENTINEL)
    // `@/util/iife` resolved in-worker to the same module the hub imports.
    expect(result.marker).toBe(iife(() => "iife-ok"))
  })

  test("surfaces an Effect failure across the worker boundary", async () => {
    await using harness = createWorkerHarness({ cores: 1 })
    const worker = harness.spawn<EffectWorkerOutbound, EffectWorkerInbound>(WORKER_ENTRY)
    await worker.waitFor("ready")

    worker.post({ type: "fail", message: "boom" })
    const failed = await worker.waitFor("failed")
    expect(failed.message).toBe("boom")
  })
})
