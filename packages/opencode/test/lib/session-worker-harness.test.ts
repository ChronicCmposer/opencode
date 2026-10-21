// Step 2 — boot the real `session-worker.ts` entry and observe its control frames.
//
// Contract: the production entry installs the executor on the default worker
// channel and transfers a `job-port` frame (its dedicated job control link)
// before announcing `ready`. The harness surfaces both plus an `ExecutorTransport`
// so tests can drive the isolate the way the hub does.

import { describe, expect, test } from "bun:test"
import { bootSessionWorker } from "./session-worker-harness"
import { sessionWorkerInit } from "./session-worker-init"

describe("session worker harness", () => {
  test("boots the production entry and observes ready + job-port frames", async () => {
    const handle = await bootSessionWorker({ timeoutMs: 20_000 })
    try {
      expect(handle.ready.type).toBe("ready")
      // `threadId` is 0 on the hub; a positive id proves the entry ran in an isolate.
      expect(handle.ready.threadId).toBeGreaterThan(0)
      expect(handle.jobPort).toBeInstanceOf(MessagePort)
      expect(handle.port.alive).toBe(true)
    } finally {
      await handle.terminate()
    }

    expect(handle.port.alive).toBe(false)
  })

  test("round-trips an executor ping/pong frame over the transport", async () => {
    const handle = await bootSessionWorker({ timeoutMs: 20_000 })
    try {
      handle.port.post({ type: "ping", id: 42 })
      const message = await handle.port.waitFor((m) => m.type === "pong")
      expect(message).toEqual({ type: "pong", id: 42 })
    } finally {
      await handle.terminate()
    }
  })

  test("accepts a caller-supplied init payload", async () => {
    const handle = await bootSessionWorker({
      timeoutMs: 20_000,
      init: sessionWorkerInit({ env: { values: { OPENCODE_TEST_WORKER_KEY: "harness" } } }),
    })
    try {
      expect(handle.ready.threadId).toBeGreaterThan(0)
    } finally {
      await handle.terminate()
    }
  })

  test("supports `await using` disposal", async () => {
    let port: Awaited<ReturnType<typeof bootSessionWorker>>["port"] | undefined
    {
      await using handle = await bootSessionWorker({ timeoutMs: 20_000 })
      port = handle.port
      expect(handle.port.alive).toBe(true)
    }
    expect(port?.alive).toBe(false)
  })
})
