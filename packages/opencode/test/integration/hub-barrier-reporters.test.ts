// The run barrier's four phases are drained by real observations, not by the hub
// assuming they happened. This drives the production hub wiring:
//
//   * `commits`/`projectors` settle from the bound `lifecycle.commit` report,
//   * `promotion` settles from the background-promotion decision surface,
//   * `tools` settles for a zero-tool run and stays closed while a call is in
//     flight (bounded by the typed timeout; cancel wins),
//   * the production `egressBarrier` waits on the worker's real durable ack.

import { describe, expect, test } from "bun:test"
import { Effect, Fiber } from "effect"
import { createLifecycleTracker } from "@/session/ledger/lifecycle"
import {
  buildProductionPortBackends,
  createEgressDeliveryBarrier,
  createSessionHub,
} from "@/session/worker/hub"
import { ackFrame, eventFrame } from "@/session/worker/frame"
import { encodeFrame } from "@/session/egress/frame"
import { createHubProxyClient } from "@/session/worker/hub-proxy-wire"
import { RunBarrier, RunBarrierTimeout } from "@/session/run-barrier"
import { createFakeExecutorPool } from "../lib/fake-worker-pool"
import { flush, pollUntil, sid, withDeadline } from "../lib/async"
import { testEffect } from "../lib/effect"
import { advanceBarrierTimeout, clockedRunBarrierLayer, createTestBarrier } from "../lib/run-barrier-test"

const it = testEffect(clockedRunBarrierLayer)

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
      ...{ barrier: createTestBarrier() },
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
      // The run is still in flight: the hub's published bind says so, not a sleep.
      await pollUntil(() => hub.sessionOfWorker(1) === sessionID, {
        message: "run never bound to its worker",
      })
      expect(hub.dispatcher.busySlotCount).toBe(1)
      await port.commit("msg_committed")
      await (pool.spawned[0] ?? pool.workers[0]).cancel(sessionID)
      await withDeadline(run, { timeoutMs: 5_000, message: "reported run never drained" })
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
    // `auto` settles the run without any lifecycle report: a no-commit run.
    const pool = createFakeExecutorPool(1)
    const hub = createSessionHub({ ...{ barrier: createTestBarrier() }, pool, lifecycle })
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
    const hub = createSessionHub({ ...{ barrier: createTestBarrier() }, pool, lifecycle, ports: { hubProxy: {} } })
    const sessionID = sid("ses_barrier_in_flight_tool")

    const run = hub.ensureRunning(sessionID)
    await flush()
    const port = reportsCommit(sessionID, 1, hub)
    try {
      await port.toolStarted("call_slow")
      await port.commit("msg_committed")
      await (pool.spawned[0] ?? pool.workers[0]).cancel(sessionID)
      await flush()
      // The run ended, but its tool never settled: no fabricated `tools`.
      await pollUntil(() => hub.barrier.state(sessionID) === "draining", {
        message: "barrier never stayed draining with a tool in flight",
      })
      expect(hub.dispatcher.busySlotCount).toBe(1)

      await port.toolFinished("call_slow")
      await withDeadline(run, { timeoutMs: 5_000, message: "tool-settled run never drained" })
      expect(hub.barrier.state(sessionID)).toBe("drained")
    } finally {
      port.dispose()
      await hub.terminateAll()
    }
  })

  it.effect("a tool that never settles times out typed; cancel wins", () =>
    Effect.gen(function* () {
      const barrier = yield* RunBarrier.Service
      const lifecycle = createLifecycleTracker()
      const pool = createFakeExecutorPool(1, { mode: "blocking" })
      const hub = createSessionHub({ pool, lifecycle, barrier, ports: { hubProxy: {} } })
      const sessionID = sid("ses_barrier_tool_timeout")

      const run = yield* Effect.promise(() => hub.ensureRunning(sessionID).catch(() => undefined)).pipe(
        Effect.forkChild,
      )
      yield* Effect.promise(flush)
      const port = reportsCommit(sessionID, 1, hub)
      try {
        yield* Effect.promise(() => port.toolStarted("call_stuck"))
        yield* Effect.promise(() => port.commit("msg_committed"))
        yield* Effect.promise(() => (pool.spawned[0] ?? pool.workers[0]).cancel(sessionID))
        yield* Effect.promise(flush)
        expect(hub.barrier.state(sessionID)).toBe("draining")

        yield* advanceBarrierTimeout()
        yield* Fiber.join(run)
        yield* Effect.promise(flush)
        expect(hub.barrier.state(sessionID)).toBe("timed-out")
        const refused = yield* Effect.tryPromise({
          try: () => hub.ensureRunning(sessionID),
          catch: (error) => error,
        }).pipe(Effect.flip)
        expect(refused).toBeInstanceOf(RunBarrierTimeout)
      } finally {
        port.dispose()
        yield* Effect.promise(() => hub.terminateAll())
      }

      const cancelledID = sid("ses_barrier_tool_cancel")
      const cancelledPool = createFakeExecutorPool(1, { mode: "blocking" })
      const cancelledHub = createSessionHub({
        ...{ barrier: createTestBarrier() },
        pool: cancelledPool,
        lifecycle: createLifecycleTracker(),
        ports: { hubProxy: {} },
      })
      const cancelledRun = yield* Effect.promise(() => cancelledHub.ensureRunning(cancelledID).catch(() => undefined)).pipe(
        Effect.forkChild,
      )
      yield* Effect.promise(flush)
      const cancelledPort = reportsCommit(cancelledID, 1, cancelledHub)
      try {
        yield* Effect.promise(() => cancelledPort.toolStarted("call_cancelled"))
        yield* Effect.promise(() => cancelledHub.cancel(cancelledID))
        yield* Fiber.join(cancelledRun)
        yield* Effect.promise(flush)
        expect(cancelledHub.barrier.state(cancelledID)).toBe("cancelled")
      } finally {
        cancelledPort.dispose()
        yield* Effect.promise(() => cancelledHub.terminateAll())
      }
    }),
  )
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
    await withDeadline(wait, { timeoutMs: 5_000, message: "egress barrier never observed its ack" })
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
      await withDeadline(wait, { timeoutMs: 5_000, message: "egress barrier never observed its ack" })
      expect(resolved).toBe(true)
    } finally {
      dispose()
      channel.port1.close()
      channel.port2.close()
    }
  })
})
