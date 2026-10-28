// The run barrier's four phases are drained by real observations, not by the hub
// assuming they happened. This drives the production hub wiring:
//
//   * `commits`/`projectors` settle from the bound `lifecycle.commit` report,
//   * `promotion` settles from the background-promotion decision surface,
//   * `tools` settles for a zero-tool run and stays closed while a call is in
//     flight (bounded by the typed timeout; cancel wins),
//   * the production `egressBarrier` waits on the worker's real durable ack.

import { describe, expect, test } from "bun:test"
import { createLifecycleTracker } from "@/session/ledger/lifecycle"
import {
  buildProductionPortBackends,
  createEgressDeliveryBarrier,
  createSessionHub,
} from "@/session/worker/hub"
import { ackFrame, eventFrame } from "@/session/worker/frame"
import { encodeFrame } from "@/session/egress/frame"
import { createHubProxyClient } from "@/session/worker/hub-proxy-wire"
import {
  RUN_BARRIER_TIMEOUT_MS,
  RunBarrierTimeout,
  createRunBarrier,
  type RunBarrierScheduler,
} from "@/session/run-barrier"
import { createFakeExecutorPool } from "../lib/fake-worker-pool"
import { flush, sid } from "../lib/async"

function fakeScheduler() {
  let now = 0
  let nextID = 1
  const timers = new Map<number, { readonly at: number; readonly run: () => void }>()
  return {
    scheduler: {
      schedule: (run: () => void, delayMs: number) => {
        const id = nextID++
        timers.set(id, { at: now + delayMs, run })
        return id
      },
      cancel: (handle: unknown) => void timers.delete(handle as number),
    } satisfies RunBarrierScheduler,
    advance(ms: number) {
      now += ms
      for (const [id, timer] of [...timers.entries()]) {
        if (timer.at <= now) {
          timers.delete(id)
          timer.run()
        }
      }
    },
  }
}

/** Whether `promise` is still pending after a short settle window. */
async function stillPending(promise: Promise<unknown>): Promise<boolean> {
  const outcome = await Promise.race([
    promise.then(
      () => "settled" as const,
      () => "settled" as const,
    ),
    new Promise<"pending">((resolve) => setTimeout(() => resolve("pending"), 25)),
  ])
  return outcome === "pending"
}

/** Whether `promise` settles inside the bound (a missing reporter fails loud). */
async function settlesWithin(promise: Promise<unknown>, ms = 1_000): Promise<boolean> {
  return Promise.race([
    promise.then(
      () => true,
      () => true,
    ),
    new Promise<boolean>((resolve) => setTimeout(() => resolve(false), ms)),
  ])
}

function reportsCommit(sessionID: string, workerID: number, hub: ReturnType<typeof createSessionHub>) {
  const channel = new MessageChannel()
  const dispose = hub.attachHubProxyPort(workerID, channel.port1)
  const client = createHubProxyClient(channel.port2)
  return {
    commit: (messageID: string) => client.call("lifecycle.commit", [sessionID, messageID]),
    toolStarted: (callID: string) => client.call("lifecycle.toolStarted", [sessionID, callID, true]),
    toolFinished: (callID: string) => client.call("lifecycle.toolFinished", [sessionID, callID]),
    dispose: () => {
      client.dispose()
      dispose()
      channel.port1.close()
      channel.port2.close()
    },
  }
}

describe("run-barrier phase reporters", () => {
  test("a committed, zero-tool run drains from the real reports", async () => {
    const lifecycle = createLifecycleTracker()
    const pool = createFakeExecutorPool(1, { mode: "blocking" })
    const outcomes: string[] = []
    const hub = createSessionHub({
      pool,
      lifecycle,
      ports: { hubProxy: {} },
      promotionOutcome: (sessionID) => {
        outcomes.push(sessionID)
        return "no-job"
      },
    })
    const sessionID = sid("ses_barrier_reported_commit")
    const run = hub.ensureRunning(sessionID)
    await flush()
    const port = reportsCommit(sessionID, 1, hub)

    try {
      expect(await stillPending(run)).toBe(true)
      await port.commit("msg_committed")
      await (pool.spawned[0] ?? pool.workers[0])!.cancel(sessionID)
      expect(await settlesWithin(run)).toBe(true)
      expect(hub.barrier.state(sessionID)).toBe("drained")
      expect(lifecycle.crashState(sessionID).lastCommittedMessageID).toBe("msg_committed")
      // The promotion phase was reported from the decision the hub observed.
      expect(outcomes).toEqual([sessionID])
    } finally {
      port.dispose()
      await hub.terminateAll()
    }
  })

  test("a run that commits nothing drains explicitly and admits the next prompt", async () => {
    const lifecycle = createLifecycleTracker()
    const clock = fakeScheduler()
    const barrier = createRunBarrier({ scheduler: clock.scheduler })
    // `auto` settles the run without any lifecycle report: a no-commit run.
    const pool = createFakeExecutorPool(1)
    const hub = createSessionHub({ pool, lifecycle, barrier })
    const sessionID = sid("ses_barrier_no_commit")

    try {
      const run = hub.ensureRunning(sessionID)
      await flush()
      // The run exited without a commit: `commits`/`projectors` settle as an
      // explicit `none` instead of waiting for the 5 s bound.
      expect(hub.barrier.state(sessionID)).toBe("drained")
      await run

      await hub.ensureRunning(sessionID)
      expect(hub.barrier.state(sessionID)).toBe("drained")
      expect(hub.barrier.state(sessionID)).not.toBe("timed-out")
    } finally {
      await hub.terminateAll()
    }
  })

  test("an in-flight tool holds the barrier until its report", async () => {
    const lifecycle = createLifecycleTracker()
    const pool = createFakeExecutorPool(1, { mode: "blocking" })
    const hub = createSessionHub({ pool, lifecycle, ports: { hubProxy: {} } })
    const sessionID = sid("ses_barrier_in_flight_tool")

    const run = hub.ensureRunning(sessionID)
    await flush()
    const port = reportsCommit(sessionID, 1, hub)
    try {
      await port.toolStarted("call_slow")
      await port.commit("msg_committed")
      await (pool.spawned[0] ?? pool.workers[0])!.cancel(sessionID)
      await flush()
      // The run ended, but its tool never settled: no fabricated `tools`.
      expect(hub.barrier.state(sessionID)).toBe("draining")
      expect(await stillPending(run)).toBe(true)

      await port.toolFinished("call_slow")
      await run
      expect(hub.barrier.state(sessionID)).toBe("drained")
    } finally {
      port.dispose()
      await hub.terminateAll()
    }
  })

  test("a tool that never settles times out typed; cancel wins", async () => {
    const lifecycle = createLifecycleTracker()
    const clock = fakeScheduler()
    const barrier = createRunBarrier({ scheduler: clock.scheduler })
    const pool = createFakeExecutorPool(1, { mode: "blocking" })
    const hub = createSessionHub({ pool, lifecycle, barrier, ports: { hubProxy: {} } })
    const sessionID = sid("ses_barrier_tool_timeout")

    const run = hub.ensureRunning(sessionID)
    await flush()
    const port = reportsCommit(sessionID, 1, hub)
    try {
      await port.toolStarted("call_stuck")
      await port.commit("msg_committed")
      await (pool.spawned[0] ?? pool.workers[0])!.cancel(sessionID)
      await flush()
      expect(hub.barrier.state(sessionID)).toBe("draining")

      clock.advance(RUN_BARRIER_TIMEOUT_MS)
      await run
      await flush()
      expect(hub.barrier.state(sessionID)).toBe("timed-out")
      await expect(hub.ensureRunning(sessionID)).rejects.toBeInstanceOf(RunBarrierTimeout)
    } finally {
      port.dispose()
      await hub.terminateAll()
    }

    const cancelledID = sid("ses_barrier_tool_cancel")
    const cancelledPool = createFakeExecutorPool(1, { mode: "blocking" })
    const cancelledHub = createSessionHub({
      pool: cancelledPool,
      lifecycle: createLifecycleTracker(),
      ports: { hubProxy: {} },
    })
    const cancelledRun = cancelledHub.ensureRunning(cancelledID).catch(() => undefined)
    await flush()
    const cancelledPort = reportsCommit(cancelledID, 1, cancelledHub)
    try {
      await cancelledPort.toolStarted("call_cancelled")
      await cancelledHub.cancel(cancelledID)
      await cancelledRun
      await flush()
      expect(cancelledHub.barrier.state(cancelledID)).toBe("cancelled")
    } finally {
      cancelledPort.dispose()
      await cancelledHub.terminateAll()
    }
  })
})

describe("production durable-egress barrier", () => {
  test("waits for the ack of every durable frame it observed", async () => {
    const sessionID = sid("ses_egress_ack")
    const barrier = createEgressDeliveryBarrier({ timeoutMs: 1_000 })
    barrier.observeDurable(sessionID)
    let resolved = false
    const wait = barrier.wait(sessionID).then(() => {
      resolved = true
    })

    await flush()
    expect(resolved).toBe(false)

    barrier.observeAck(sessionID, 1)
    expect(await settlesWithin(wait)).toBe(true)
    expect(resolved).toBe(true)
  })

  test("the production events backend feeds the barrier from the real wire ack", async () => {
    const sessionID = sid("ses_egress_wire")
    const barrier = createEgressDeliveryBarrier({ timeoutMs: 5_000 })
    const backends = buildProductionPortBackends({
      events: { publish: () => {} } as never,
      egressDelivery: barrier,
      sessionOfWorker: () => sessionID,
    })
    const channel = new MessageChannel()
    const dispose = backends.egress!(1, channel.port1)
    try {
      // A durable frame is observed as it reaches the production events port.
      channel.port2.postMessage(
        encodeFrame(
          eventFrame({
            event: { id: "evt_durable", type: "session.message", properties: { ordinal: 1 } },
            aggregateID: sessionID,
            seq: 1,
          }),
        ),
      )
      await flush()

      let resolved = false
      const wait = barrier.wait(sessionID).then(() => {
        resolved = true
      })
      await flush()
      expect(resolved).toBe(false)

      // The worker's sender posts the ack immediately after its durable batch.
      channel.port2.postMessage(encodeFrame(ackFrame({ count: 1 })))
      expect(await settlesWithin(wait)).toBe(true)
      expect(resolved).toBe(true)
    } finally {
      dispose()
      channel.port1.close()
      channel.port2.close()
    }
  })
})
