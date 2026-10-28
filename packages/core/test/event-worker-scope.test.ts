import { describe, expect, test } from "bun:test"
import { EventV2 } from "@opencode-ai/core/event"

describe("EventV2 worker scope isolation", () => {
  test("bindings are per database, not process globals", () => {
    const first = {}
    const second = {}
    const firstScope = EventV2.createWorkerScope("session-a")
    const secondScope = EventV2.createWorkerScope("session-b")

    EventV2.bindWorkerScope(first, firstScope)
    EventV2.bindWorkerScope(second, secondScope)

    expect(EventV2.workerScopeOf(first)).toBe(firstScope)
    expect(EventV2.workerScopeOf(second)).toBe(secondScope)
    expect(EventV2.workerScopeOf({})).toBeUndefined()
  })

  test("lease and deletion capabilities do not leak between workers", () => {
    const first = EventV2.createWorkerScope("session-a")
    const second = EventV2.createWorkerScope("session-b")
    const operations = { deleteAggregate: () => 1 }

    first.leaseOwner = "owner-a"
    first.durable = operations

    expect(first.sessionID).toBe("session-a")
    expect(second.sessionID).toBe("session-b")
    expect(second.leaseOwner).toBeUndefined()
    expect(second.durable).toBeUndefined()
  })
})
