// Phase 3.6 worker entry point (plan task 3.6).
//
// Creates a control `MessageChannel`, transfers the hub end to the parent, and
// attaches the worker end to a frame-driven clock. Each `sleep` request runs a
// real Effect `Effect.sleep` on a runtime whose `Clock` is backed by that
// frame-driven state, so advancing time from the hub resolves the sleep.

import { Effect } from "effect"
import { attachClock, type ClockWorkerInbound } from "../lib/worker-clock"
import { onMessage, postToParent } from "./worker-scope"

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

const channel = new MessageChannel()
const clock = attachClock(channel.port1)

onMessage<ClockWorkerInbound>((message) => {
  if (!message || message.type !== "sleep") return
  Effect.gen(function* () {
    yield* Effect.sleep(message.ms)
    postToParent({ type: "slept", id: message.id, at: clock.currentTimeMillis() })
  })
    .pipe(Effect.provide(clock.layer), Effect.runPromise)
    .catch((error) => postToParent({ type: "error", id: message.id, message: messageOf(error) }))
})

postToParent(channel.port2, [channel.port2])
