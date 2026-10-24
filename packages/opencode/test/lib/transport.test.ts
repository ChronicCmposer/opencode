import { describe, expect, test } from "bun:test"
import type { SessionExecutorInbound, SessionExecutorOutbound } from "@/session/worker/execute"
import {
  EXECUTOR_WORKER_ENTRY,
  adaptTransport,
  noopTransport,
  recordingTransport,
  spawnTransport,
} from "./transport"
import { createWorkerHarness } from "./worker-harness"

const ping = (id: number): SessionExecutorInbound => ({ type: "ping", id })
const pong = (id: number): SessionExecutorOutbound => ({ type: "pong", id })

describe("noopTransport", () => {
  test("answers a pong and terminates its alive flag", async () => {
    const transport = noopTransport()
    expect(transport.alive).toBe(true)
    await expect(transport.waitFor(() => true)).resolves.toEqual({ type: "pong", id: 0 })
    await transport.terminate()
    expect(transport.alive).toBe(false)
  })
})

describe("recordingTransport", () => {
  test("records posted frames and replies from the most recent request", async () => {
    const { transport, posted } = recordingTransport((request) => pong((request as { id: number }).id))

    transport.post(ping(7))
    await expect(transport.waitFor((message) => message.type === "pong")).resolves.toEqual({ type: "pong", id: 7 })
    expect(posted).toEqual([{ type: "ping", id: 7 }])
  })

  test("fails loudly when a waiter does not match the scripted reply", async () => {
    const { transport } = recordingTransport((request) => pong((request as { id: number }).id))
    transport.post(ping(1))
    await expect(transport.waitFor((message) => message.type === "ready")).rejects.toThrow(
      /waiter did not match/,
    )
  })
})

describe("spawnTransport / adaptTransport", () => {
  test("drives the executor fixture over a real harness handle", async () => {
    const harness = createWorkerHarness({ cores: 2, timeoutMs: 10_000 })
    try {
      const transport = spawnTransport(harness)
      expect(transport.alive).toBe(true)

      transport.post(ping(3))
      await expect(
        transport.waitFor((message) => message.type === "pong", { timeoutMs: 10_000 }),
      ).resolves.toEqual(pong(3))

      await transport.terminate()
      expect(transport.alive).toBe(false)
    } finally {
      await harness.terminateAll().catch(() => undefined)
    }
  })

  test("adapts an already-spawned handle the same way", async () => {
    const harness = createWorkerHarness({ cores: 2, timeoutMs: 10_000 })
    try {
      const handle = harness.spawn<SessionExecutorOutbound, SessionExecutorInbound>(EXECUTOR_WORKER_ENTRY)
      const transport = adaptTransport(handle)

      transport.post(ping(9))
      await expect(transport.waitFor((message) => message.type === "pong", { timeoutMs: 10_000 })).resolves.toEqual(
        pong(9),
      )
    } finally {
      await harness.terminateAll().catch(() => undefined)
    }
  })
})
