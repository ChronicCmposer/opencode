// Phase 4.5 — bounded admission queue (plan task 4.5).
//
// A generic FIFO with a hard capacity: enqueue past the bound fails loud with a
// typed `SessionQueueFullError` rather than growing without limit.

import { describe, expect, test } from "bun:test"
import {
  ADMISSION_QUEUE_CAPACITY,
  CHILD_QUEUE_CAPACITY,
  createBoundedQueue,
  SessionQueueFullError,
} from "@/session/worker/queue"

describe("bounded queue", () => {
  test("dequeues in FIFO order", () => {
    const queue = createBoundedQueue<string>("admission", 4)
    queue.enqueue("a")
    queue.enqueue("b")
    queue.enqueue("c")

    expect(queue.size).toBe(3)
    expect(queue.dequeue()).toBe("a")
    expect(queue.dequeue()).toBe("b")
    expect(queue.dequeue()).toBe("c")
    expect(queue.dequeue()).toBeUndefined()
  })

  test("reports fullness and rejects past capacity with SessionQueueFullError", () => {
    const queue = createBoundedQueue<string>("admission", 2)
    queue.enqueue("a")
    queue.enqueue("b")

    expect(queue.full).toBe(true)
    let thrown: unknown
    try {
      queue.enqueue("c")
    } catch (error) {
      thrown = error
    }
    expect(thrown).toBeInstanceOf(SessionQueueFullError)
    if (!(thrown instanceof SessionQueueFullError)) throw new Error("expected SessionQueueFullError")
    expect(thrown.queue).toBe("admission")
    expect(thrown.capacity).toBe(2)
    expect(queue.size).toBe(2)
  })

  test("remove pulls one matching entry and preserves the rest", () => {
    const queue = createBoundedQueue<number>("admission", 4)
    for (const value of [1, 2, 3, 4]) queue.enqueue(value)

    expect(queue.remove((value) => value === 3)).toBe(3)
    expect(queue.items).toEqual([1, 2, 4])
    expect(queue.remove((value) => value === 99)).toBeUndefined()
  })

  test("clear empties the queue", () => {
    const queue = createBoundedQueue<number>("child", 4)
    queue.enqueue(1)
    queue.enqueue(2)
    queue.clear()
    expect(queue.size).toBe(0)
    expect(queue.items).toEqual([])
  })

  test("the approved capacities are 1024 admission / 64 child", () => {
    expect(ADMISSION_QUEUE_CAPACITY).toBe(1024)
    expect(CHILD_QUEUE_CAPACITY).toBe(64)
  })

  test("fails loud on an invalid capacity", () => {
    expect(() => createBoundedQueue<string>("admission", 0)).toThrow(/capacity/)
    expect(() => createBoundedQueue<string>("admission", 1.5)).toThrow(/capacity/)
  })
})
