// The worker-side lifecycle reporting seam is a hub-proxy call. Every RPC is
// bounded so a wedged control port cannot hang a run, and an unacknowledged
// `toolStarted` must reject: the tool path then fails closed before running a
// side effect the hub never recorded.

import { describe, expect, test } from "bun:test"
import {
  LIFECYCLE_RPC_TIMEOUT_MS,
  LifecycleRpcTimeoutError,
  createProxyReporting,
} from "@/session/ledger/lifecycle"

describe("lifecycle proxy reporting", () => {
  test("toolStarted rejects with a typed timeout when the hub never acks", async () => {
    const reporting = createProxyReporting(() => new Promise(() => {}), { timeoutMs: 10 })
    const failure = reporting.toolStarted("ses_1", "call_1")
    await expect(failure).rejects.toBeInstanceOf(LifecycleRpcTimeoutError)
    // The error name carries the bounded RPC, not a generic rejection.
    await expect(reporting.toolStarted("ses_1", "call_1")).rejects.toMatchObject({
      method: "lifecycle.toolStarted",
      timeoutMs: 10,
    })
  })

  test("toolStarted rejects when the port tears down", async () => {
    const reporting = createProxyReporting(() => Promise.reject(new Error("port closed")), { timeoutMs: 10 })
    await expect(reporting.toolStarted("ses_1", "call_1")).rejects.toThrow("port closed")
  })

  test("best-effort reports still swallow a torn-down port and time out bounded", async () => {
    const reporting = createProxyReporting(() => Promise.reject(new Error("port closed")), { timeoutMs: 10 })
    await expect(reporting.commit("ses_1", "msg_1")).resolves.toBeUndefined()
    await expect(reporting.phase("ses_1", "tool")).resolves.toBeUndefined()
    await expect(reporting.toolFinished("ses_1", "call_1")).resolves.toBeUndefined()
  })

  test("the production bound is five seconds", () => {
    expect(LIFECYCLE_RPC_TIMEOUT_MS).toBe(5_000)
  })
})
