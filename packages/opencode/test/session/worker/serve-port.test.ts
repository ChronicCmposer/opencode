// Shared request/reply server loop — `servePort` contract.
//
// `servePort` is the one implementation behind the hub broker port server, the
// broker worker's RPC loop and the hub writer port server. These tests pin its
// three paths: a parsed request executes and is answered, a parse failure is
// delivered with `request: undefined` (so the caller correlates from the raw
// frame), and a rejected execute is answered instead of escaping.

import { describe, expect, test } from "bun:test"
import { rawRequestId, servePort, type FrameChannel, type ServeOutcome } from "@/session/worker/port"
import { flush } from "../../lib/async"

/** An in-memory channel that records posts and lets the test deliver frames. */
function memoryChannel(): FrameChannel<unknown> & { readonly sent: unknown[]; deliver(frame: unknown): void } {
  const handlers = new Set<(frame: unknown) => void>()
  const sent: unknown[] = []
  return {
    sent,
    post: (frame) => void sent.push(frame),
    onMessage: (handler) => {
      handlers.add(handler)
      return () => handlers.delete(handler)
    },
    deliver: (frame) => {
      for (const handler of [...handlers]) handler(frame)
    },
  }
}

type Request = { readonly requestId: string; readonly value: number }
type Outcome = ServeOutcome<Request, number>

function harness(execute: (request: Request) => number | Promise<number>) {
  const channel = memoryChannel()
  const outcomes: Array<{ raw: unknown; outcome: Outcome }> = []
  servePort(
    channel,
    (raw) => {
      if (typeof raw !== "object" || raw === null || typeof (raw as { value?: unknown }).value !== "number") {
        throw new Error("bad request")
      }
      return { requestId: rawRequestId(raw), value: (raw as { value: number }).value }
    },
    execute,
    (raw, outcome) => void outcomes.push({ raw, outcome }),
  )
  return { channel, outcomes }
}

describe("servePort", () => {
  test("parses, executes and answers a valid request", async () => {
    const { channel, outcomes } = harness((request) => request.value * 2)
    channel.deliver({ requestId: "r1", value: 21 })
    await flush()

    expect(outcomes).toHaveLength(1)
    expect(outcomes[0]?.outcome).toEqual({ ok: true, request: { requestId: "r1", value: 21 }, value: 42 })
  })

  test("delivers a parse failure with no request", () => {
    const { channel, outcomes } = harness(() => 0)
    channel.deliver({ requestId: "r9", value: "nope" })

    expect(outcomes).toHaveLength(1)
    expect(outcomes[0]?.outcome).toMatchObject({ ok: false, request: undefined })
    expect((outcomes[0]?.outcome as { error: Error }).error).toBeInstanceOf(Error)
  })

  test("answers a rejected execute with the parsed request", async () => {
    const { channel, outcomes } = harness(() => Promise.reject(new Error("boom")))
    channel.deliver({ requestId: "r2", value: 1 })
    await flush()

    expect(outcomes).toHaveLength(1)
    const outcome = outcomes[0]?.outcome
    expect(outcome?.ok).toBe(false)
    if (outcome !== undefined && !outcome.ok) {
      expect(outcome.request?.requestId).toBe("r2")
      expect(outcome.error).toBeInstanceOf(Error)
    }
  })

  test("answers a synchronous execute throw", async () => {
    const { channel, outcomes } = harness(() => {
      throw new Error("sync boom")
    })
    channel.deliver({ requestId: "r3", value: 1 })
    await flush()

    expect(outcomes).toHaveLength(1)
    expect(outcomes[0]?.outcome.ok).toBe(false)
  })
})

describe("rawRequestId", () => {
  test("reads a string requestId and defaults otherwise", () => {
    expect(rawRequestId({ requestId: "r1" })).toBe("r1")
    expect(rawRequestId({ requestId: 7 })).toBe("unknown")
    expect(rawRequestId(null)).toBe("unknown")
    expect(rawRequestId("nope")).toBe("unknown")
  })
})
