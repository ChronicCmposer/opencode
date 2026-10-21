// Phase 4.5 — bounded admission queue (plan task 4.5, plan line 61).
//
// The hub admits a bounded number of waiting sessions. Past the bound the
// dispatcher must reject the caller rather than grow without limit, so the
// queue throws a typed `SessionQueueFullError` on overflow. The queue itself is
// a pure FIFO data structure; scheduling lives in `dispatch.ts`.

import { Schema } from "effect"

/** Approved admission-queue bound (plan decision line 61). */
export const ADMISSION_QUEUE_CAPACITY = 1024

/** Approved foreground child-queue bound (plan decision line 71). */
export const CHILD_QUEUE_CAPACITY = 64

/** Raised when a bounded queue is at capacity and cannot admit another item. */
export class SessionQueueFullError extends Schema.TaggedErrorClass<SessionQueueFullError>()("SessionQueueFullError", {
  message: Schema.String,
  queue: Schema.String,
  capacity: Schema.Number,
}) {}

export interface BoundedQueue<T> {
  readonly name: string
  readonly capacity: number
  readonly size: number
  readonly full: boolean
  readonly items: ReadonlyArray<T>
  /** Append `item`; throws `SessionQueueFullError` when already at capacity. */
  enqueue(item: T): void
  /** Remove and return the oldest item, or `undefined` when empty. */
  dequeue(): T | undefined
  /** Remove and return the first item matching `predicate`, or `undefined`. */
  remove(predicate: (item: T) => boolean): T | undefined
  clear(): void
}

export function createBoundedQueue<T>(name: string, capacity: number): BoundedQueue<T> {
  if (!Number.isInteger(capacity) || capacity < 1) {
    throw new Error(`invalid queue capacity: ${capacity} (expected a positive integer)`)
  }

  const entries: T[] = []

  return {
    name,
    capacity,
    get size() {
      return entries.length
    },
    get full() {
      return entries.length >= capacity
    },
    get items() {
      return entries.slice()
    },
    enqueue(item) {
      if (entries.length >= capacity) {
        throw new SessionQueueFullError({
          message: `queue "${name}" is full (capacity ${capacity})`,
          queue: name,
          capacity,
        })
      }
      entries.push(item)
    },
    dequeue: () => entries.shift(),
    remove(predicate) {
      const index = entries.findIndex(predicate)
      if (index < 0) return undefined
      return entries.splice(index, 1)[0]
    },
    clear() {
      entries.length = 0
    },
  }
}

export * as SessionQueue from "./queue"
