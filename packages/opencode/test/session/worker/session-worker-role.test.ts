// fix2-ws3 — `parseSessionWorkerInit` must fail loud on a missing/invalid role.
//
// The role is a required field of the structured-cloned `workerData`: the worker
// bootstrap adopts it (`setProcessRole(init.role)`) and the graph gate reads it.
// A payload without a valid role must be rejected at the isolate boundary rather
// than silently defaulting to something the worker is not.

import { describe, expect, test } from "bun:test"
import { parseSessionWorkerInit } from "@/session/worker/session-worker"
import { workerSnapshot } from "../../lib/snapshot"

/** The complete model descriptor every required-field test needs. */
function fullModel(): Record<string, unknown> {
  return {
    providerID: "test",
    modelID: "test-model",
    apiId: "test-api-id",
    baseUrl: "http://127.0.0.1:0/v1",
    apiKey: "sk-test",
    capabilities: { temperature: false, reasoning: true },
    limit: { context: 1_000, output: 1_000 },
  }
}

/** The minimal payload `parseSessionWorkerInit` accepts. */
function payload(extra: Record<string, unknown> = {}): unknown {
  return {
    snapshot: workerSnapshot(),
    env: { values: {} },
    model: fullModel(),
    ...extra,
  }
}

describe("parseSessionWorkerInit — role", () => {
  test("accepts the session role", () => {
    expect(parseSessionWorkerInit(payload({ role: "session" })).role).toBe("session")
  })

  test("accepts every declared process role", () => {
    for (const role of ["session", "hub"] as const) {
      expect(parseSessionWorkerInit(payload({ role })).role).toBe(role)
    }
  })

  test("throws loud when the role is missing", () => {
    expect(() => parseSessionWorkerInit(payload())).toThrow(/role/)
  })

  test("throws loud when the role is not a known role", () => {
    expect(() => parseSessionWorkerInit(payload({ role: "overseer" }))).toThrow(/role/)
  })

  test("throws loud when the role has the wrong type", () => {
    expect(() => parseSessionWorkerInit(payload({ role: 3 }))).toThrow(/role/)
  })
})

describe("parseSessionWorkerInit — required fields", () => {
  test("throws loud when env.values is missing", () => {
    expect(() => parseSessionWorkerInit(payload({ env: {} }))).toThrow(/env\.values/)
  })

  test("throws loud when the model is missing", () => {
    const { model, ...withoutModel } = payload() as Record<string, unknown>
    expect(() => parseSessionWorkerInit(withoutModel)).toThrow(/model is required/)
  })

  test.each(["providerID", "modelID", "apiId", "baseUrl", "apiKey"])(
    "throws loud when model.%s is missing",
    (field) => {
      const model = { ...fullModel() }
      delete model[field]
      expect(() => parseSessionWorkerInit(payload({ model }))).toThrow(new RegExp(`model\\.${field}`))
    },
  )

  test("throws loud when model.capabilities is missing", () => {
    const model = { ...fullModel(), capabilities: undefined }
    expect(() => parseSessionWorkerInit(payload({ model }))).toThrow(/capabilities/)
  })

  test("accepts a payload with every required field", () => {
    expect(parseSessionWorkerInit(payload({ role: "session" })).model.providerID).toBe("test")
  })
})
