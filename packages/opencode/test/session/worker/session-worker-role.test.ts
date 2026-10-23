// fix2-ws3 — `parseSessionWorkerInit` must fail loud on a missing/invalid role.
//
// The role is a required field of the structured-cloned `workerData`: the worker
// bootstrap adopts it (`setProcessRole(init.role)`) and the graph gate reads it.
// A payload without a valid role must be rejected at the isolate boundary rather
// than silently defaulting to something the worker is not.

import { describe, expect, test } from "bun:test"
import { parseSessionWorkerInit } from "@/session/worker/session-worker"
import { workerSnapshot } from "../../lib/snapshot"

/** The minimal payload `parseSessionWorkerInit` inspects (only `model.baseUrl`). */
function payload(extra: Record<string, unknown> = {}): unknown {
  return {
    snapshot: workerSnapshot(),
    env: { values: {} },
    model: { baseUrl: "http://127.0.0.1:0/v1" },
    ...extra,
  }
}

describe("parseSessionWorkerInit — role", () => {
  test("accepts the session role", () => {
    expect(parseSessionWorkerInit(payload({ role: "session" })).role).toBe("session")
  })

  test("accepts every declared process role", () => {
    for (const role of ["session", "writer", "broker", "hub"] as const) {
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
