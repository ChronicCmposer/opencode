// Production hub wiring: `onSessionReleased` is the single per-retirement hook.
// The pool's own `releaseSession` is internal cleanup (its init cache), so a
// retirement must call the caller's callback exactly once, never twice.
//
// The release is also ordered AFTER the isolate confirms its exit: a still-live
// old generation could otherwise write after its lease was zeroed. The test
// therefore gates `terminate()` and awaits the dispatcher's explicit release
// barrier instead of assuming the settled run implies the callback ran.

import { describe, expect, test } from "bun:test"
import { createProductionSessionHub } from "@/session/worker/hub"
import type { SessionID } from "@/session/schema"
import { sessionWorkerInit } from "../../lib/session-worker-init"
import { pollUntil } from "../../lib/poll"
import { flush, sid } from "../../lib/async"

/** A `Worker` stand-in: never boots, so the test controls retirement directly. */
class FakeWorker {
  static instances: FakeWorker[] = []
  onmessage: ((event: { readonly data: unknown }) => void) | null = null
  onerror: ((event: { readonly message: string; readonly error?: unknown }) => void) | null = null
  readonly posted: unknown[] = []
  terminated = false
  private confirm!: () => void
  private readonly termination = new Promise<number>((resolve) => {
    this.confirm = () => {
      this.terminated = true
      resolve(0)
    }
  })

  constructor() {
    FakeWorker.instances.push(this)
  }

  postMessage(message: unknown): void {
    this.posted.push(message)
  }

  terminate(): Promise<number> {
    return this.termination
  }

  /** Confirm the isolate's exit (production: the thread's `close`). */
  confirmTerminated(): void {
    this.confirm()
  }
}

async function withFakeWorker(run: () => Promise<void>): Promise<void> {
  const original = (globalThis as { Worker?: unknown }).Worker
  FakeWorker.instances = []
  ;(globalThis as { Worker?: unknown }).Worker = FakeWorker
  try {
    await run()
  } finally {
    // A failed assertion must not leave `terminate()` pending: teardown joins it.
    for (const worker of FakeWorker.instances) worker.confirmTerminated()
    ;(globalThis as { Worker?: unknown }).Worker = original
  }
}

describe("production hub session release", () => {
  test("onSessionReleased fires exactly once per retirement, after the exit is confirmed", async () => {
    await withFakeWorker(async () => {
      const released: SessionID[] = []
      const releasedAfterExit: boolean[] = []
      const hub = createProductionSessionHub({
        init: () => sessionWorkerInit(),
        count: 1,
        entry: "fake://release-once",
        workKind: "instant",
        onSessionReleased: (sessionID) => {
          released.push(sessionID)
          releasedAfterExit.push(FakeWorker.instances[0]?.terminated === true)
        },
      })
      try {
        const sessionID = sid("ses_release_once")
        const running = hub.ensureRunning(sessionID).catch(() => undefined)
        // Retire only once the lazy worker has actually materialized.
        await pollUntil(() => FakeWorker.instances.length === 1, {
          message: "production worker was never materialized",
        })

        // The cancel's boot path waits for the isolate to exit; start it without
        // awaiting so the ordering below is observable.
        const cancelling = hub.dispatcher.cancel(sessionID)
        await flush()
        const worker = FakeWorker.instances[0]!

        // The retirement has not released anything before the isolate exits.
        expect(released).toEqual([])
        expect(worker.terminated).toBe(false)

        worker.confirmTerminated()
        await cancelling
        await running
        // Explicit release barrier: the callback (and its lease release) runs
        // before this resolves; awaiting only the run settlement would observe [].
        await hub.dispatcher.awaitRelease(sessionID)

        expect(released).toEqual([sessionID])
        expect(releasedAfterExit).toEqual([true])
      } finally {
        for (const worker of FakeWorker.instances) worker.confirmTerminated()
        await hub.terminateAll()
      }
    })
  }, 30_000)
})
