// A cancel that arrives before the first graph build has registered a run
// control must not be lost. The bootstrap latches it and the run consumes the
// latch at start (or immediately after the build), so a cancelled first turn
// never executes its prompt.

import { describe, expect, test } from "bun:test"
import { Context, Effect, Exit } from "effect"
import { createSessionWork } from "@/session/worker/session-worker"
import type { WriterClientHandle } from "@/session/worker/layer"
import { sid } from "../../lib/async"

describe("pre-run cancel latch", () => {
  test("a latched cancel interrupts before the message read/pipeline runs", async () => {
    let read = false
    const work = createSessionWork({
      context: Context.empty() as Context.Context<unknown>,
      writer: {} as WriterClientHandle,
      threadId: 1,
      prompt: false,
      consumePreRunCancel: () => true,
      readMessages: () =>
        Effect.sync(() => {
          read = true
          return []
        }),
    })

    const exit = await Effect.runPromise(Effect.exit(work(sid("ses_pre_run_cancel"))))
    expect(Exit.hasInterrupts(exit)).toBe(true)
    expect(read).toBe(false)
  })

  test("without a latch the same run reaches the pipeline", async () => {
    let read = false
    const work = createSessionWork({
      context: Context.empty() as Context.Context<unknown>,
      writer: {} as WriterClientHandle,
      threadId: 1,
      prompt: false,
      consumePreRunCancel: () => false,
      readMessages: () =>
        Effect.sync(() => {
          read = true
          return []
        }),
    })

    // The pipeline service is absent, so the run fails past the read; the point
    // is that the latch did not short-circuit it before the message read.
    await Effect.runPromise(Effect.exit(work(sid("ses_no_pre_run_cancel"))))
    expect(read).toBe(true)
  })
})
