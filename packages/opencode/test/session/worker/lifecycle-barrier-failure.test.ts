// A rejected barrier phase is a real failure, not a timeout. The caller must see
// the phase error and the state must stay distinct from `timed-out`.

import { describe, expect, test } from "bun:test"
import { RunBarrierTimeout, createRunBarrier } from "@/session/run-barrier"

const settled = Promise.resolve()

describe("distinct barrier failure state (contained)", () => {
  test("a rejected phase surfaces the original error and a `failed` state", async () => {
    const barrier = createRunBarrier()
    const failure = new Error("projector write broke")
    const pending = barrier.wait("s1", {
      commits: settled,
      projectors: Promise.reject(failure),
      tools: settled,
      promotion: settled,
    })

    const error = await pending.catch((reason: unknown) => reason)
    expect(error).toBe(failure)
    expect(error).not.toBeInstanceOf(RunBarrierTimeout)
    expect(barrier.state("s1")).toBe("failed")
  })

  test("a timed-out barrier keeps the timeout state", async () => {
    let fire!: () => void
    const barrier = createRunBarrier({
      timeoutMs: 5,
      scheduler: {
        schedule: (run) => {
          fire = run
          return 1
        },
        cancel: () => undefined,
      },
    })
    const pending = barrier.wait("s2", {
      commits: new Promise(() => {}),
      projectors: settled,
      tools: settled,
      promotion: settled,
    })
    fire()
    await expect(pending).rejects.toBeInstanceOf(RunBarrierTimeout)
    expect(barrier.state("s2")).toBe("timed-out")
  })
})
