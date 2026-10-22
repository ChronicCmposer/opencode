// Step 1 — `SessionWorkerInit` builder + structured-clone round-trip helper.
//
// The production entry parses `SessionWorkerInit` (`{snapshot, env, model}`) out
// of `workerData` and fails loud on a malformed payload. Tests need one
// dependency-free way to build that payload and to prove it survives the
// isolate boundary, so the same trusted shape is used everywhere.

import { describe, expect, test } from "bun:test"
import { TEST_WORKER_MODEL, assertStructuredCloneable, sessionWorkerInit } from "./session-worker-init"
import { workerSnapshot } from "./snapshot"

describe("sessionWorkerInit builder", () => {
  test("defaults to a cloneable snapshot, empty env and the test model", () => {
    const init = sessionWorkerInit()

    expect(init.snapshot).toEqual(workerSnapshot())
    expect(init.env).toEqual({ values: {} })
    expect(init.model).toEqual(TEST_WORKER_MODEL)
  })

  test("overrides are layered over the default descriptor", () => {
    const snapshot = workerSnapshot({ epoch: 7, directory: "/tmp/worker-override" })
    const init = sessionWorkerInit({
      snapshot,
      env: { values: { OPENCODE_TEST_WORKER_KEY: "worker-local" } },
      model: { modelID: "other-model", limit: { context: 4_096, output: 512 } },
    })

    expect(init.snapshot).toBe(snapshot)
    expect(init.env).toEqual({ values: { OPENCODE_TEST_WORKER_KEY: "worker-local" } })
    expect(init.model).toEqual({
      ...TEST_WORKER_MODEL,
      modelID: "other-model",
      limit: { context: 4_096, output: 512 },
    })
  })

  test("a legacy `model` override is normalized to `modelID`", () => {
    const init = sessionWorkerInit({ model: { model: "other-model" } })

    expect(init.model).toEqual({ ...TEST_WORKER_MODEL, modelID: "other-model" })
    expect(assertStructuredCloneable(init)).toEqual(init)
  })

  test("a built init survives a structuredClone round-trip unchanged", () => {
    const init = sessionWorkerInit({ env: { values: { A: "1" } } })

    expect(assertStructuredCloneable(init)).toEqual(init)
  })
})

describe("assertStructuredCloneable", () => {
  test("returns the clone and preserves nested values", () => {
    const clone = assertStructuredCloneable({ a: [1, 2, { b: "c" }] })

    expect(clone).toEqual({ a: [1, 2, { b: "c" }] })
  })

  test("fails loud when a value cannot be cloned", () => {
    expect(() => assertStructuredCloneable(() => 1)).toThrow(/structuredClone/)
  })
})
