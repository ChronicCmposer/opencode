// Cancel modes are transient reconstruction state: they exist only until the
// cancel report consumes them. They are dropped on report so a long-lived hub
// does not accumulate one entry per cancelled session.

import { describe, expect, test } from "bun:test"
import { SessionID } from "@/session/schema"
import type { Dispatcher } from "@/session/worker/dispatch"
import { createCancelReporter } from "@/session/worker/cancel-report"

const dispatcher = { cancelWithGrace: async () => undefined } as unknown as Dispatcher
const noChildren = () => []
const log = () => {}

function reporter(options: { readonly maxSessions?: number; readonly children?: (id: SessionID) => SessionID[] } = {}) {
  return createCancelReporter({
    dispatcher: () => dispatcher,
    childrenOf: options.children ?? noChildren,
    log,
    ...(options.maxSessions === undefined ? {} : { maxSessions: options.maxSessions }),
  })
}

describe("cancel reporter mode retention", () => {
  test("many-session churn is bounded by the LRU cap", () => {
    const state = reporter({ maxSessions: 8 })
    for (let index = 0; index < 100; index += 1) {
      state.recordCancelled(`ses_cancel_${index}`, "cooperative")
    }
    expect(state.size).toBe(8)
    // The most recent sessions survive; the oldest were evicted.
    state.recordCancelled("ses_newest", "terminated")
    expect(state.size).toBe(8)
  })

  test("a report drops the session and every descendant it consumed", async () => {
    const child = SessionID.make("ses_cancel_child")
    const grandchild = SessionID.make("ses_cancel_grandchild")
    const parent = SessionID.make("ses_cancel_parent")
    // The dispatcher records each descendant's mode while it cascades, exactly
    // like the hub's cancel path.
    const state = createCancelReporter({
      dispatcher: () =>
        ({
          cancelWithGrace: async () => {
            state.recordCancelled(child, "completed")
            state.recordCancelled(grandchild, "terminated")
            return undefined
          },
        }) as unknown as Dispatcher,
      childrenOf: (id) => (id === parent ? [child] : id === child ? [grandchild] : []),
      log,
    })

    state.recordCancelled(parent, "cooperative")
    const report = await state.cancelWithReport(parent)

    expect(report.descendants).toEqual([
      { sessionID: grandchild, needsFallbackIdle: true },
      { sessionID: child, needsFallbackIdle: false },
    ])
    expect(state.size).toBe(0)
  })

  test("a report does not retain modes for unrelated sessions", async () => {
    const state = reporter()
    state.recordCancelled("ses_other", "cooperative")
    state.recordCancelled("ses_cancel_target", "cooperative")

    await state.cancelWithReport(SessionID.make("ses_cancel_target"))

    expect(state.size).toBe(1)
  })
})
