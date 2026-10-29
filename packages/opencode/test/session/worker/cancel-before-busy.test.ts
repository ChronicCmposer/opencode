// A cancel that reaches a worker after its run was admitted but before the run
// table marks it busy (the prompt is still being persisted) must not be lost.

import { describe, expect, test } from "bun:test"
import { Effect } from "effect"
import type { SessionRunControl } from "@/session/worker/execute"
import { cancelOnceBusy } from "@/session/worker/session-worker"
import { sid } from "../../lib/async"

describe("cancel before the run is busy", () => {
  test("is delivered once the run becomes busy", async () => {
    const id = sid("ses_1")
    const active = new Set([id])
    let busy = false
    const cancelled: string[] = []
    const control: SessionRunControl = {
      busy: async () => busy,
      cancel: async (sessionID) => {
        cancelled.push(sessionID)
      },
    }

    const pending = Effect.runPromise(cancelOnceBusy(control, id, active))
    await new Promise((resolve) => setImmediate(resolve))
    expect(cancelled).toEqual([])

    busy = true
    await pending
    expect(cancelled).toEqual([id])
  })

  test("resolves without cancelling when the run ends first", async () => {
    const id = sid("ses_1")
    const active = new Set([id])
    const cancelled: string[] = []
    const control: SessionRunControl = {
      busy: async () => false,
      cancel: async (sessionID) => {
        cancelled.push(sessionID)
      },
    }

    const pending = Effect.runPromise(cancelOnceBusy(control, id, active))
    active.delete(id)
    await pending
    expect(cancelled).toEqual([])
  })
})
