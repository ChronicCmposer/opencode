// Metrics collector (plan task 3.14).
//
// A tiny counters/gauges registry for the hub-load budget (Phase 13.3) and the
// "main loop idle" / "hub loop I/O-only" observability requirements. Counters are
// monotonic (default 0); gauges hold the last value. A snapshot is a frozen,
// isolated copy, so a recorded sample can never be mutated after the fact.
//
// Design: inputs are validated at the boundary and fail loudly with a descriptive
// error; once a value is accepted it is trusted (no defensive re-checks downstream).

export interface MetricsSnapshot {
  readonly counters: Readonly<Record<string, number>>
  readonly gauges: Readonly<Record<string, number>>
}

export interface Metrics {
  /** Add `by` (default 1) to counter `name`; returns the new value. */
  increment(name: string, by?: number): number
  /** Set gauge `name` to `value` (last write wins). */
  gauge(name: string, value: number): void
  /** Current counter value, or 0 if never incremented. */
  counter(name: string): number
  /** Current gauge value, or `undefined` if never set. */
  gaugeValue(name: string): number | undefined
  /** Frozen, isolated copy of all counters and gauges. */
  snapshot(): MetricsSnapshot
  /** Clear all counters and gauges. */
  reset(): void
}

function assertName(name: string): void {
  if (typeof name !== "string" || name.length === 0) {
    throw new Error(`metric name must be a non-empty string, got: ${String(name)}`)
  }
}

function assertFinite(value: number, what: string): void {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new Error(`${what} must be a finite number, got: ${String(value)}`)
  }
}

export function createMetrics(): Metrics {
  const counters = new Map<string, number>()
  const gauges = new Map<string, number>()

  return {
    increment(name, by = 1) {
      assertName(name)
      assertFinite(by, `increment for "${name}"`)
      if (by <= 0) throw new Error(`increment for "${name}" must be positive, got: ${by}`)
      const next = (counters.get(name) ?? 0) + by
      counters.set(name, next)
      return next
    },
    gauge(name, value) {
      assertName(name)
      assertFinite(value, `gauge "${name}"`)
      gauges.set(name, value)
    },
    counter(name) {
      assertName(name)
      return counters.get(name) ?? 0
    },
    gaugeValue(name) {
      assertName(name)
      return gauges.get(name)
    },
    snapshot() {
      return Object.freeze({
        counters: Object.freeze(Object.fromEntries(counters)),
        gauges: Object.freeze(Object.fromEntries(gauges)),
      })
    },
    reset() {
      counters.clear()
      gauges.clear()
    },
  }
}
