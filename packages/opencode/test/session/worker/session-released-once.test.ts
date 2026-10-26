// Production hub wiring: `onSessionReleased` is the single per-retirement hook.
// The pool's own `releaseSession` is internal cleanup (its init cache), so a
// retirement must call the caller's callback exactly once, never twice.

import { describe, expect, test } from "bun:test"
import { createProductionSessionHub } from "@/session/worker/hub"
import type { SessionID } from "@/session/schema"
import { sessionWorkerInit } from "../../lib/session-worker-init"
import { pollUntil } from "../../lib/poll"
import { sid } from "../../lib/async"

/** A `Worker` stand-in: never boots, so the test controls retirement directly. */
class FakeWorker {
  static instances: FakeWorker[] = []
  onmessage: ((event: { readonly data: unknown }) => void) | null = null
  onerror: ((event: { readonly message: string; readonly error?: unknown }) => void) | null = null
  readonly posted: unknown[] = []

  constructor() {
    FakeWorker.instances.push(this)
  }

  postMessage(message: unknown): void {
    this.posted.push(message)
  }

  terminate(): Promise<number> {
    return Promise.resolve(0)
  }
}

async function withFakeWorker(run: () => Promise<void>): Promise<void> {
  const original = (globalThis as { Worker?: unknown }).Worker
  FakeWorker.instances = []
  ;(globalThis as { Worker?: unknown }).Worker = FakeWorker
  try {
    await run()
  } finally {
    ;(globalThis as { Worker?: unknown }).Worker = original
  }
}

describe("production hub session release", () => {
  test("onSessionReleased fires exactly once per retirement", async () => {
    await withFakeWorker(async () => {
      const released: SessionID[] = []
      const hub = createProductionSessionHub({
        init: () => sessionWorkerInit(),
        count: 1,
        entry: "fake://release-once",
        workKind: "instant",
        onSessionReleased: (sessionID) => released.push(sessionID),
      })
      try {
        const sessionID = sid("ses_release_once")
        const running = hub.ensureRunning(sessionID).catch(() => undefined)
        // Retire only once the lazy worker has actually materialized.
        await pollUntil(() => FakeWorker.instances.length === 1, {
          message: "production worker was never materialized",
        })

        await hub.dispatcher.cancel(sessionID)
        await running

        expect(released).toEqual([sessionID])
      } finally {
        await hub.terminateAll()
      }
    })
  }, 30_000)
})
