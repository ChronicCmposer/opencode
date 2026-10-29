// Generation asks are tagged with the epoch current when their proxy wrapper is
// INVOKED, not when the effect starts running. That is what lets retirement
// cancel an old isolate's ask whose effect begins after the epoch advance, while
// a successor's ask — begun after the advance — always survives.

import { describe, expect, test } from "bun:test"
import { Effect, Exit, Fiber } from "effect"
import {
  askInGeneration,
  createGenerationAskTracker,
  createPerSessionResolvers,
} from "@/session/worker/production-hub"
import { sid } from "./lib/async"

/** The factory only calls session-sensitive deps on dispatch/release paths. */
function resolversForTest() {
  return createPerSessionResolvers({
    services: {} as never,
    bridge: {} as never,
    authorities: {} as never,
    count: 1,
    epochSequence: {} as never,
    initOptions: undefined,
    plugins: undefined,
    requestHooksViaHub: false,
    subagentDepth: 0,
    sessionOfWorker: () => undefined,
  } as never)
}

describe("generation ask epochs", () => {
  test("an old ask whose effect starts after the advance is still cancelled", async () => {
    const tracker = createGenerationAskTracker()
    const sessionID = sid("ses_generation_late_ask")
    // The wrapper captures epoch 0 at this call site.
    const pendingOldAsk = askInGeneration(tracker, sessionID, Effect.never, () => "retired")

    // Retirement advances synchronously, BEFORE the old effect ever starts.
    tracker.retire(sessionID)
    expect(tracker.current(sessionID)).toBe(1)

    const exit = await Effect.runPromise(Effect.exit(pendingOldAsk))
    expect(Exit.isFailure(exit)).toBe(true)
    expect(tracker.pending(sessionID)).toBe(0)
  })

  test("a retirement cancels a running old ask but leaves the successor's ask pending", async () => {
    const tracker = createGenerationAskTracker()
    const sessionID = sid("ses_generation_ask_scope")
    const oldAsk = Effect.runFork(askInGeneration(tracker, sessionID, Effect.never, () => "retired"))
    expect(tracker.pending(sessionID)).toBe(1)

    tracker.retire(sessionID)

    const oldExit = await Effect.runPromise(Fiber.await(oldAsk))
    expect(Exit.isFailure(oldExit)).toBe(true)
    expect(tracker.pending(sessionID)).toBe(0)

    // Registered after the advance: same epoch, so the retirement cannot catch it.
    const successor = Effect.runFork(
      askInGeneration(tracker, sessionID, Effect.never, () => "retired"),
    )
    expect(tracker.pending(sessionID)).toBe(1)
    await Effect.runPromise(Fiber.interrupt(successor))
    expect(tracker.pending(sessionID)).toBe(0)
  })

  test("session release forgets the epoch so the map cannot grow without bound", async () => {
    const resolvers = resolversForTest()
    const sessionID = sid("ses_generation_forget")
    await resolvers.releaseGeneration(sessionID, 7)
    expect(resolvers.generationEpochOf(sessionID)).toBe(1)
    expect(resolvers.generationSessionCount()).toBe(1)

    resolvers.releaseSession(sessionID)
    expect(resolvers.generationEpochOf(sessionID)).toBe(0)
    expect(resolvers.generationSessionCount()).toBe(0)

    // Repeating retire+release for many sessions must never accumulate entries.
    for (let index = 0; index < 50; index += 1) {
      const released = sid(`ses_generation_released_${index}`)
      await resolvers.releaseGeneration(released, 7)
      resolvers.releaseSession(released)
    }
    expect(resolvers.generationSessionCount()).toBe(0)
  })
})
