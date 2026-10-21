// Phase 3.6 worker entry point (plan task 3.6).
//
// Creates a control `MessageChannel`, transfers the hub end to the parent, and
// attaches the worker end to a frame-driven clock. Each `sleep` request runs a
// real Effect `Effect.sleep` on a runtime whose `Clock` is backed by that
// frame-driven state, so advancing time from the hub resolves the sleep.

import { Effect } from "effect"
import { attachClock, type ClockWorkerInbound } from "../lib/worker-clock"

interface ClockWorkerScope {
  postMessage(message: unknown, transfer?: readonly Transferable[]): void
  onmessage: ((event: { data: unknown }) => void) | null
}

const scope = globalThis as unknown as ClockWorkerScope
const channel = new MessageChannel()
const clock = attachClock(channel.port1)

scope.onmessage = (event) => {
  const message = event.data as ClockWorkerInbound
  if (!message || message.type !== "sleep") return
  Effect.gen(function* () {
    yield* Effect.sleep(message.ms)
    scope.postMessage({ type: "slept", id: message.id, at: clock.currentTimeMillis() })
  }).pipe(Effect.provide(clock.layer), Effect.runFork)
}

scope.postMessage(channel.port2, [channel.port2])
