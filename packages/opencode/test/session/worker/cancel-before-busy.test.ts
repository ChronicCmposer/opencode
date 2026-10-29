// A cancel that reaches a worker after its run was admitted but before the run
// table marks it busy (the prompt is still being persisted) must not be lost.

import { describe, expect, test } from "bun:test"
import { Deferred, Effect } from "effect"
import { cancelOnceBusy, type WorkerRunControl } from "@/session/worker/session-worker"
import { sid } from "../../lib/async"

const control = (busy: Deferred.Deferred<void>, cancelled: string[]): WorkerRunControl => ({
  busy: async () => Deferred.isDoneUnsafe(busy),
  cancel: async (sessionID) => {
    cancelled.push(sessionID)
  },
  awaitBusy: () => Deferred.await(busy),
})

describe("cancel before the run is busy", () => {
  test("is delivered once the run becomes busy", async () => {
    const id = sid("ses_1")
    const busy = Deferred.makeUnsafe<void>()
    const ended = Deferred.makeUnsafe<void>()
    const cancelled: string[] = []

    const pending = Effect.runPromise(cancelOnceBusy(control(busy, cancelled), id, ended))
    await new Promise((resolve) => setImmediate(resolve))
    expect(cancelled).toEqual([])

    Deferred.doneUnsafe(busy, Effect.void)
    await pending
    expect(cancelled).toEqual([id])
  })

  test("resolves without cancelling when the run ends first", async () => {
    const id = sid("ses_1")
    const busy = Deferred.makeUnsafe<void>()
    const ended = Deferred.makeUnsafe<void>()
    const cancelled: string[] = []

    const pending = Effect.runPromise(cancelOnceBusy(control(busy, cancelled), id, ended))
    Deferred.doneUnsafe(ended, Effect.void)
    await pending
    expect(cancelled).toEqual([])
  })
})
