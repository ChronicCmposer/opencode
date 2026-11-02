// The synchronous session → subagent-depth mirror the dispatch path reads
// before a worker materializes. Roots carry no entry (absence resolves to 0),
// so only nested sessions consume headroom state.

import { describe, expect, test } from "bun:test"
import { createSessionDepthIndex } from "@/session/worker/production-hub"
import { sid } from "../../lib/async"

const parent = sid("ses_depth_parent")
const child = sid("ses_depth_child")
const grandchild = sid("ses_depth_grandchild")

describe("createSessionDepthIndex", () => {
  test("get defaults to undefined for an unrecorded session", () => {
    const index = createSessionDepthIndex()
    expect(index.get(parent)).toBeUndefined()
    expect(index.size).toBe(0)
  })

  test("record keeps positive depths and zero deletes the entry", () => {
    const index = createSessionDepthIndex()
    index.record(parent, 1)
    index.record(child, 2)
    expect(index.get(parent)).toBe(1)
    expect(index.get(child)).toBe(2)
    expect(index.size).toBe(2)

    // A root needs no entry: a later zero removes it.
    index.record(parent, 0)
    expect(index.get(parent)).toBeUndefined()
    expect(index.size).toBe(1)
  })

  test("release drops one session without touching the others", () => {
    const index = createSessionDepthIndex()
    index.record(parent, 1)
    index.record(child, 2)

    index.release(parent)

    expect(index.get(parent)).toBeUndefined()
    expect(index.get(child)).toBe(2)
    expect(index.size).toBe(1)

    index.release(sid("ses_depth_unknown"))
    expect(index.size).toBe(1)
  })

  test("clear drops every session", () => {
    const index = createSessionDepthIndex()
    index.record(parent, 1)
    index.record(child, 2)
    index.record(grandchild, 3)

    index.clear()

    expect(index.size).toBe(0)
    expect(index.get(parent)).toBeUndefined()
    expect(index.get(child)).toBeUndefined()
    expect(index.get(grandchild)).toBeUndefined()
  })
})
