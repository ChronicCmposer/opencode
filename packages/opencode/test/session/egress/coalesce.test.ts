// Phase 8.3 + 8.4 — delta coalescing window (plan tasks 8.3, 8.4).
//
// Non-durable deltas buffer for a 16 ms window and flush as ONE `deltas` frame on
// idle, on threshold, or when a durable boundary / end arrives. Durable boundary
// events are flushed immediately, never coalesced, and never reordered. The clock
// and timer are injected so the window is deterministic.

import { describe, expect, test } from "bun:test"
import { createCoalescer, type CoalescerScheduler } from "@/session/egress/coalesce"
import type { EgressEvent, EgressFrame } from "@/session/egress/frame"

interface FakeScheduler {
  readonly scheduler: CoalescerScheduler
  advance(ms: number): void
  readonly pending: number
}

function createFakeScheduler(): FakeScheduler {
  let now = 0
  let nextId = 1
  const timers = new Map<number, { readonly at: number; readonly run: () => void }>()
  return {
    scheduler: {
      now: () => now,
      schedule: (run, delayMs) => {
        const id = nextId++
        timers.set(id, { at: now + delayMs, run })
        return id
      },
      cancel: (handle) => {
        timers.delete(handle as number)
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

const delta = (id: string): EgressEvent => ({ id, type: "message.part.updated", data: { id } })

function setup(options: { windowMs?: number; maxBatch?: number } = {}) {
  const frames: EgressFrame[] = []
  const fake = createFakeScheduler()
  const coalescer = createCoalescer({
    ...options,
    scheduler: fake.scheduler,
    onFrame: (frame) => frames.push(frame),
  })
  return { frames, fake, coalescer }
}

describe("delta coalescer", () => {
  test("buffers a lone delta until the window elapses (idle flush)", () => {
    const { frames, fake, coalescer } = setup({ windowMs: 16 })
    coalescer.pushDelta(delta("a"))
    expect(frames).toEqual([])
    expect(fake.pending).toBe(1)

    fake.advance(15)
    expect(frames).toEqual([])
    fake.advance(1)
    expect(frames).toEqual([{ kind: "deltas", v: 1, events: [delta("a")] }])
    expect(fake.pending).toBe(0)
  })

  test("coalesces several deltas inside the window into one frame", () => {
    const { frames, fake, coalescer } = setup()
    coalescer.pushDelta(delta("a"))
    coalescer.pushDelta(delta("b"))
    coalescer.pushDelta(delta("c"))
    fake.advance(16)
    expect(frames).toEqual([{ kind: "deltas", v: 1, events: [delta("a"), delta("b"), delta("c")] }])
  })

  test("flushes immediately at the batch threshold", () => {
    const { frames, fake, coalescer } = setup({ maxBatch: 3 })
    coalescer.pushDelta(delta("a"))
    coalescer.pushDelta(delta("b"))
    expect(frames).toEqual([])
    coalescer.pushDelta(delta("c"))
    expect(frames).toEqual([{ kind: "deltas", v: 1, events: [delta("a"), delta("b"), delta("c")] }])
    expect(fake.pending).toBe(0)
  })

  test("a durable boundary flushes buffered deltas first, then emits the durable frame", () => {
    const { frames, coalescer } = setup()
    coalescer.pushDelta(delta("a"))
    coalescer.pushDurable(
      { id: "d", type: "message.updated", data: {} },
      { generation: 1, ordinal: 1 },
    )
    expect(frames).toEqual([
      { kind: "deltas", v: 1, events: [delta("a")] },
      { kind: "durable", v: 1, meta: { generation: 1, ordinal: 1 }, event: { id: "d", type: "message.updated", data: {} } },
    ])
  })

  test("durable frames are never coalesced or reordered", () => {
    const { frames, coalescer } = setup()
    coalescer.pushDurable({ id: "d1", type: "t", data: {} }, { generation: 1, ordinal: 1 })
    coalescer.pushDurable({ id: "d2", type: "t", data: {} }, { generation: 1, ordinal: 2 })
    expect(frames.map((frame) => frame.kind)).toEqual(["durable", "durable"])
    expect(frames).toEqual([
      { kind: "durable", v: 1, meta: { generation: 1, ordinal: 1 }, event: { id: "d1", type: "t", data: {} } },
      { kind: "durable", v: 1, meta: { generation: 1, ordinal: 2 }, event: { id: "d2", type: "t", data: {} } },
    ])
  })

  test("end flushes buffered deltas then emits the end frame", () => {
    const { frames, coalescer } = setup()
    coalescer.pushDelta(delta("a"))
    coalescer.end()
    expect(frames).toEqual([
      { kind: "deltas", v: 1, events: [delta("a")] },
      { kind: "end", v: 1 },
    ])
    expect(coalescer.ended).toBe(true)
  })

  test("end is idempotent and flushes exactly once", () => {
    const { frames, coalescer } = setup()
    coalescer.pushDelta(delta("a"))
    coalescer.end()
    coalescer.end()
    expect(frames).toEqual([
      { kind: "deltas", v: 1, events: [delta("a")] },
      { kind: "end", v: 1 },
    ])
  })

  test("pushing after end fails loud", () => {
    const { coalescer } = setup()
    coalescer.end()
    expect(() => coalescer.pushDelta(delta("a"))).toThrow(/ended/)
    expect(() =>
      coalescer.pushDurable({ id: "d", type: "t", data: {} }, { generation: 1, ordinal: 1 }),
    ).toThrow(/ended/)
  })

  test("an explicit flush cancels the idle timer and a later push reschedules", () => {
    const { frames, fake, coalescer } = setup()
    coalescer.pushDelta(delta("a"))
    expect(fake.pending).toBe(1)
    coalescer.flush()
    expect(fake.pending).toBe(0)
    expect(frames).toHaveLength(1)

    coalescer.pushDelta(delta("b"))
    expect(fake.pending).toBe(1)
    fake.advance(16)
    expect(frames).toHaveLength(2)
  })
})
