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

  test("reports still swallow a torn-down port and the bounded ones time out", async () => {
    const reporting = createProxyReporting(() => Promise.reject(new Error("port closed")), { timeoutMs: 10 })
    await expect(reporting.commit("ses_1", "msg_1")).resolves.toBeUndefined()
    await expect(reporting.phase("ses_1", "tool")).resolves.toBeUndefined()
    await expect(reporting.toolFinished("ses_1", "call_1")).resolves.toBeUndefined()
  })

  test("commit does not resolve until the hub has observed the message", async () => {
    // The hub tracker a crash reads is updated by `lifecycle.commit`. If commit
    // resolved before that RPC landed, a crash immediately after `commit()`
    // would resume from a stale message (or refuse to resume). Gate the ack so
    // the ordering is deterministic rather than a race.
    let observed: string | undefined
    let releaseHub!: () => void
    const hubGate = new Promise<void>((resolve) => {
      releaseHub = resolve
    })
    const reporting = createProxyReporting(async (method, args) => {
      if (method !== "lifecycle.commit") return
      await hubGate
      observed = String(args[1])
    })

    let settled = false
    const commit = reporting.commit("ses_1", "msg_1").then(() => {
      settled = true
    })
    await Promise.resolve()
    expect(observed).toBeUndefined()
    expect(settled).toBe(false)

    releaseHub()
    await commit
    expect(observed).toBe("msg_1")
    expect(settled).toBe(true)
  })

  test("the production bound is five seconds", () => {
    expect(LIFECYCLE_RPC_TIMEOUT_MS).toBe(5_000)
  })
})
