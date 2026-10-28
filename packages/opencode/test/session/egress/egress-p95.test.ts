// Egress p95 budgets: the two latency-sensitive crossings must stay well under
// any human-visible stall — event-frame encoding on the worker hot path, and a
// hub-proxy request/reply round trip over a real `MessagePort`.
//
// These are budgets, not microbenchmarks: each test warms up, then asserts the
// 95th percentile of real wall-clock samples stays within a generous bound on a
// loaded CI host. A regression that reintroduces a per-frame replacer walk, a
// double encode/size pass, or a synchronous stall blows the budget.

import { describe, expect, test } from "bun:test"
import { encodeFrame } from "@/session/egress/frame"
import { eventFrame } from "@/session/worker/frame"
import { createHubProxyClient, serveHubProxy } from "@/session/worker/hub-proxy-wire"

/** 95th percentile, nearest-rank. */
function p95(samples: readonly number[]): number {
  const sorted = [...samples].sort((a, b) => a - b)
  const rank = Math.ceil(sorted.length * 0.95)
  return sorted[Math.min(rank, sorted.length) - 1] ?? 0
}

function sample(runs: number, run: () => void | Promise<void>): Promise<number[]> {
  return (async () => {
    const samples: number[] = []
    for (let index = 0; index < runs; index += 1) {
      const started = performance.now()
      await run()
      samples.push(performance.now() - started)
    }
    return samples
  })()
}

describe("egress p95 budgets", () => {
  test("event frame encode p95 stays under budget", async () => {
    const frame = eventFrame({
      event: {
        id: "evt_p95",
        type: "message.part.delta",
        properties: { sessionID: "ses_p95", partID: "prt_1", delta: "x".repeat(64 * 1024) },
      },
    })
    // Warm the JIT and the string allocator before measuring.
    for (let index = 0; index < 200; index += 1) encodeFrame(frame)

    const samples = await sample(2000, () => {
      encodeFrame(frame)
    })

    expect(p95(samples)).toBeLessThan(5)
  })

  test("hub-proxy request/reply p95 stays under budget", async () => {
    const channel = new MessageChannel()
    const dispose = serveHubProxy({ "sessionStatus.get": () => ({ type: "idle" }) }, channel.port2)
    const client = createHubProxyClient(channel.port1)
    try {
      for (let index = 0; index < 20; index += 1) await client.call("sessionStatus.get", ["ses_p95"])

      const samples = await sample(500, async () => {
        const value = await client.call("sessionStatus.get", ["ses_p95"])
        if ((value as { type?: string } | undefined)?.type !== "idle") throw new Error("unexpected hub-proxy reply")
      })

      expect(p95(samples)).toBeLessThan(25)
    } finally {
      client.dispose()
      dispose()
      channel.port1.close()
      channel.port2.close()
    }
  })
})
