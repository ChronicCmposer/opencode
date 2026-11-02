// Terminal release of a session's in-flight plugin tools.
//
// `releaseSession` aborts every registered call and drops the session map. One
// failing abort must never strand the remaining calls: the failure is reported
// through `onAbortError`, and a second release of the same session is a no-op.

import { describe, expect, test } from "bun:test"
import { createPluginToolAbortRegistry } from "@/session/worker/hub"

describe("createPluginToolAbortRegistry", () => {
  test("releaseSession aborts both calls, clears the map, and reports the failing abort", () => {
    const original = globalThis.AbortController
    // Bun reports an `abort` listener throw asynchronously, so a controller
    // whose `abort()` throws is the synchronous failure boundary the registry
    // guards: the first controller fails, the second must still abort.
    class FailingAbortController extends original {
      static failing: FailingAbortController | undefined
      static readonly created: FailingAbortController[] = []
      constructor() {
        super()
        FailingAbortController.created.push(this)
      }
      override abort(reason?: unknown): void {
        super.abort(reason)
        if (FailingAbortController.failing === this) throw new Error("abort listener failed")
      }
    }

    ;(globalThis as { AbortController?: typeof AbortController }).AbortController = FailingAbortController
    try {
      const failures: Array<{ readonly error: unknown; readonly sessionID: string; readonly callID: string }> = []
      const registry = createPluginToolAbortRegistry({
        onAbortError: (error, sessionID, callID) => failures.push({ error, sessionID, callID }),
      })
      const first = registry.registerAbort("ses_1", "call_1")
      const second = registry.registerAbort("ses_1", "call_2")
      FailingAbortController.failing = FailingAbortController.created[0]

      registry.releaseSession("ses_1")

      expect(first.signal.aborted).toBe(true)
      expect(second.signal.aborted).toBe(true)
      expect(failures).toHaveLength(1)
      expect(failures[0]).toMatchObject({ sessionID: "ses_1", callID: "call_1" })
      expect((failures[0]!.error as Error).message).toBe("abort listener failed")

      // The session map is dropped: a second release has nothing left to report.
      registry.releaseSession("ses_1")
      expect(failures).toHaveLength(1)
    } finally {
      ;(globalThis as { AbortController?: typeof AbortController }).AbortController = original
    }
  })

  test("abort and abortSession still fire the matching calls", () => {
    const registry = createPluginToolAbortRegistry()
    const first = registry.registerAbort("ses_1", "call_1")
    const second = registry.registerAbort("ses_1", "call_2")
    const other = registry.registerAbort("ses_2", "call_3")

    registry.abort("ses_1", "call_1")
    expect(first.signal.aborted).toBe(true)
    expect(second.signal.aborted).toBe(false)

    registry.abortSession("ses_1")
    expect(second.signal.aborted).toBe(true)
    expect(other.signal.aborted).toBe(false)

    registry.abortSession("ses_2")
    expect(other.signal.aborted).toBe(true)
  })

  test("releasing an unknown session is a no-op", () => {
    const registry = createPluginToolAbortRegistry()
    const handle = registry.registerAbort("ses_known", "call_1")

    registry.releaseSession("ses_unknown")

    expect(handle.signal.aborted).toBe(false)
    registry.abort("ses_known", "call_1")
    expect(handle.signal.aborted).toBe(true)
  })
})
