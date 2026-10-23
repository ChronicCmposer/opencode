// W2 — delta coalescing window over the events envelope (spec Q7).
//
// Non-durable deltas buffer for a 16 ms window and flush as ONE batch of `event`
// frames on idle, on threshold, or when a durable boundary / end arrives. Durable
// boundary events are flushed immediately, never coalesced, and never reordered.
// The clock and timer are injected so the window is deterministic.

import { describe, expect, test } from "bun:test"
import { CoalescerEndedError, createCoalescer, type CoalescerScheduler } from "@/session/egress/coalesce"
import { type DurableMeta } from "@/session/egress/frame"
import { type EventsFrame, type EventV2Event, endFrame, eventFrame } from "@/session/worker/frame"

const deltaEvent = (id: string, text?: string): EventV2Event => ({
  id,
  type: "message.part.delta",
  properties: text === undefined ? { id } : { id, text },
})

const durableMeta: DurableMeta = { generation: 1, ordinal: 1 }

/** Read the streamed `text` out of an event frame, failing loud on a wrong shape. */
function textOf(frame: EventsFrame): string {
  if (frame.kind !== "event") throw new Error(`expected an event frame, got ${frame.kind}`)
  const properties = frame.payload.event.properties
  if (typeof properties !== "object" || properties === null || !("text" in properties)) {
    throw new Error("expected event properties to carry a text field")
  }
  const text = properties.text
  if (typeof text !== "string") throw new Error("expected text to be a string")
  return text
}

interface FakeScheduler {
  readonly scheduler: CoalescerScheduler
  advance(ms: number): void
  readonly pending: number
}

function createFakeScheduler(): FakeScheduler {
  let now = 0
  let nextId = 1
  const timers = new Map<unknown, { readonly at: number; readonly run: () => void }>()
  return {
    scheduler: {
      schedule: (run, delayMs) => {
        const id = nextId++
        timers.set(id, { at: now + delayMs, run })
        return id
      },
      cancel: (handle) => {
        timers.delete(handle)
      },
    },
    advance(ms) {
      now += ms
      const due = [...timers.entries()].filter(([, timer]) => timer.at <= now).sort((a, b) => a[1].at - b[1].at)
      for (const [id, timer] of due) {
        timers.delete(id)
        timer.run()
      }
    },
    get pending() {
      return timers.size
    },
  }
}

function setup(options: { windowMs?: number; maxBatch?: number } = {}) {
  const batches: EventsFrame[][] = []
  const fake = createFakeScheduler()
  const coalescer = createCoalescer({
    ...options,
    scheduler: fake.scheduler,
    onBatch: (frames) => batches.push([...frames]),
  })
  return { batches, fake, coalescer }
}

describe("delta coalescer over event frames", () => {
  test("buffers a lone delta until the window elapses (idle flush)", () => {
    const { batches, fake, coalescer } = setup({ windowMs: 16 })
    coalescer.pushDelta(deltaEvent("a"))
    expect(batches).toEqual([])
    expect(fake.pending).toBe(1)

    fake.advance(15)
    expect(batches).toEqual([])
    fake.advance(1)
    expect(batches).toEqual([[eventFrame({ event: deltaEvent("a") })]])
    expect(fake.pending).toBe(0)
  })

  test("coalesces several deltas inside the window into one batch, in order", () => {
    const { batches, fake, coalescer } = setup()
    coalescer.pushDelta(deltaEvent("a"))
    coalescer.pushDelta(deltaEvent("b"))
    coalescer.pushDelta(deltaEvent("c"))
    fake.advance(16)
    expect(batches).toEqual([
      [
        eventFrame({ event: deltaEvent("a") }),
        eventFrame({ event: deltaEvent("b") }),
        eventFrame({ event: deltaEvent("c") }),
      ],
    ])
  })

  test("a coalesced batch reconstructs the exact streamed text", () => {
    const { batches, fake, coalescer } = setup()
    for (const fragment of ["Hel", "lo ", "wor", "ld"]) coalescer.pushDelta(deltaEvent("p", fragment))
    fake.advance(16)
    const text = batches.flat().map(textOf).join("")
    expect(text).toBe("Hello world")
  })

  test("flushes immediately at the batch threshold", () => {
    const { batches, fake, coalescer } = setup({ maxBatch: 3 })
    coalescer.pushDelta(deltaEvent("a"))
    coalescer.pushDelta(deltaEvent("b"))
    expect(batches).toEqual([])
    coalescer.pushDelta(deltaEvent("c"))
    expect(batches).toHaveLength(1)
    expect(batches[0]).toHaveLength(3)
    expect(fake.pending).toBe(0)
  })

  test("a durable boundary flushes buffered deltas first, then emits its own batch", () => {
    const { batches, coalescer } = setup()
    coalescer.pushDelta(deltaEvent("a"))
    const durable = { id: "d", type: "message.updated", properties: { role: "assistant" } }
    coalescer.pushDurable(durable, durableMeta)
    expect(batches).toEqual([
      [eventFrame({ event: deltaEvent("a") })],
      [eventFrame({ event: durable, generation: 1, ordinal: 1 })],
    ])
  })

  test("durable frames are never coalesced or reordered", () => {
    const { batches, coalescer } = setup()
    const first = { id: "d1", type: "message.updated", properties: {} }
    const second = { id: "d2", type: "message.updated", properties: {} }
    coalescer.pushDurable(first, { generation: 1, ordinal: 1 })
    coalescer.pushDurable(second, { generation: 1, ordinal: 2 })
    expect(batches).toEqual([
      [eventFrame({ event: first, generation: 1, ordinal: 1 })],
      [eventFrame({ event: second, generation: 1, ordinal: 2 })],
    ])
  })

  test("end flushes buffered deltas then emits the end frame", () => {
    const { batches, coalescer } = setup()
    coalescer.pushDelta(deltaEvent("a"))
    coalescer.end()
    expect(batches).toEqual([[eventFrame({ event: deltaEvent("a") })], [endFrame()]])
    expect(coalescer.ended).toBe(true)
  })

  test("end is idempotent and flushes exactly once", () => {
    const { batches, coalescer } = setup()
    coalescer.pushDelta(deltaEvent("a"))
    coalescer.end()
    coalescer.end()
    expect(batches).toEqual([[eventFrame({ event: deltaEvent("a") })], [endFrame()]])
  })

  test("pushing after end fails loud", () => {
    const { coalescer } = setup()
    coalescer.end()
    expect(() => coalescer.pushDelta(deltaEvent("a"))).toThrow(/ended/)
    expect(() => coalescer.pushDurable(deltaEvent("d"), durableMeta)).toThrow(/ended/)
  })

  test("an explicit flush cancels the idle timer and a later push reschedules", () => {
    const { batches, fake, coalescer } = setup()
    coalescer.pushDelta(deltaEvent("a"))
    expect(fake.pending).toBe(1)
    coalescer.flush()
    expect(fake.pending).toBe(0)
    expect(batches).toHaveLength(1)

    coalescer.pushDelta(deltaEvent("b"))
    expect(fake.pending).toBe(1)
    fake.advance(16)
    expect(batches).toHaveLength(2)
  })

  test("end cancels the pending idle timer", () => {
    const { fake, coalescer } = setup()
    coalescer.pushDelta(deltaEvent("a"))
    expect(fake.pending).toBe(1)
    coalescer.end()
    expect(fake.pending).toBe(0)
  })

  test("flush after end is a no-op", () => {
    const { batches, coalescer } = setup()
    coalescer.pushDelta(deltaEvent("a"))
    coalescer.end()
    const emitted = batches.length
    coalescer.flush()
    expect(batches.length).toBe(emitted)
  })

  test("pushing after end throws CoalescerEndedError", () => {
    const { coalescer } = setup()
    coalescer.end()
    expect(() => coalescer.pushDelta(deltaEvent("a"))).toThrow(CoalescerEndedError)
    expect(() => coalescer.pushDurable(deltaEvent("d"), durableMeta)).toThrow(CoalescerEndedError)
  })

  test("rejects a negative, fractional, or non-numeric window", () => {
    for (const windowMs of [-1, 1.5, Number.NaN, Number.POSITIVE_INFINITY]) {
      expect(() => createCoalescer({ windowMs, onBatch: () => {} })).toThrow(/window/)
    }
  })

  test("rejects a non-positive or fractional batch threshold", () => {
    for (const maxBatch of [0, -1, 2.5, Number.NaN]) {
      expect(() => createCoalescer({ maxBatch, onBatch: () => {} })).toThrow(/batch/)
    }
  })

  test("accepts a zero window (immediate flush) and a positive threshold", () => {
    expect(() => createCoalescer({ windowMs: 0, maxBatch: 1, onBatch: () => {} })).not.toThrow()
  })
})
