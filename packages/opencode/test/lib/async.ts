// Async test primitives shared across worker/pipeline tests (extracted from the
// `flush` / `capture` / `sid` helpers that were copy-pasted into many test files).
//
// `flush` lets a test yield once so queued microtasks and zero-delay timers run.
// `capture` observes a promise's settlement as a plain, frozen value so a test
// can assert on a rejection without racing `expect(...).rejects`. `sid` parses a
// plain string into a trusted `SessionID` at the test boundary.

import type { SessionID } from "@/session/schema"

/** Resolve after one macrotask, flushing pending microtasks and timer callbacks. */
export function flush(): Promise<void> {
  return new Promise<void>((resolve) => setTimeout(resolve, 0))
}

/** The outcome of a captured promise: either a value or the rejection reason. */
export type Settled<T> = { readonly ok: true; readonly value: T } | { readonly ok: false; readonly error: unknown }

/**
 * Await `promise` and capture its settlement as a plain value. Never rejects, so
 * a test can assert on both the success and failure paths uniformly (Law of
 * Atomic Predictability: same input, a predictable data result — no throw).
 */
export async function capture<T>(promise: Promise<T>): Promise<Settled<T>> {
  try {
    return { ok: true, value: await promise }
  } catch (error) {
    return { ok: false, error }
  }
}

/** Parse a plain string into a trusted `SessionID` at the test boundary. */
export function sid(value: string): SessionID {
  return value as SessionID
}
