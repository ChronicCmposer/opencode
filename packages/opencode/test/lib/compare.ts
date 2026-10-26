// Shared comparison primitives for the parity and golden harnesses.
//
// Both harnesses answer the same question: "are these two captures equivalent?"
// They differ only in what an item is (sink event, `{table,row}` durable row,
// golden row). Keying, multiset subtraction and the canonical JSON form are
// identity-agnostic, so they live here instead of drifting in each harness.

/** Deterministic JSON with sorted object keys, so key order never spuriously diffs. */
export function stableStringify(value: unknown): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value) ?? "undefined"
  if (Array.isArray(value)) return `[${value.map(stableStringify).join(",")}]`
  const record = value as Record<string, unknown>
  const entries = Object.keys(record)
    .sort()
    .map((key) => `${JSON.stringify(key)}:${stableStringify(record[key])}`)
  return `{${entries.join(",")}}`
}

/** Key an item by its normalized projection; the default compares the item as-is. */
export function normalizeKey<T>(normalize: (item: T) => unknown = (item) => item): (item: T) => string {
  return (item) => stableStringify(normalize(item))
}

/** The key at `index`, or `undefined` when the sequence is shorter. */
export function keyAt<T>(items: ReadonlyArray<T>, index: number, keyOf: (item: T) => string): string | undefined {
  const item = items[index]
  return item === undefined ? undefined : keyOf(item)
}

/** Items of `from` (multiset) whose key is not consumed by `other`. */
export function multisetDifference<T>(
  from: ReadonlyArray<T>,
  other: ReadonlyArray<T>,
  keyOf: (item: T) => string,
): T[] {
  const remaining = new Map<string, number>()
  for (const item of other) {
    const key = keyOf(item)
    remaining.set(key, (remaining.get(key) ?? 0) + 1)
  }
  const missing: T[] = []
  for (const item of from) {
    const key = keyOf(item)
    const count = remaining.get(key) ?? 0
    if (count > 0) remaining.set(key, count - 1)
    else missing.push(item)
  }
  return missing
}

export interface CompareMultisetsOptions<T> {
  /** Project an item to the comparable shape; defaults to the item itself. */
  readonly normalize?: (item: T) => unknown
  /** Override keying entirely (takes precedence over `normalize`). */
  readonly key?: (item: T) => string
}

export interface MultisetComparison<T> {
  readonly equal: boolean
  readonly leftCount: number
  readonly rightCount: number
  /** Items present in `left` (multiset) with no counterpart in `right`. */
  readonly missingInRight: ReadonlyArray<T>
  /** Items present in `right` (multiset) with no counterpart in `left`. */
  readonly missingInLeft: ReadonlyArray<T>
}

/**
 * Compare two captures as multisets, keyed by the normalized item. This is the
 * committed-state contract: every row written on one side must appear exactly
 * once on the other, order-independent.
 */
export function compareMultisets<T>(
  left: ReadonlyArray<T>,
  right: ReadonlyArray<T>,
  options: CompareMultisetsOptions<T> = {},
): MultisetComparison<T> {
  const keyOf = options.key ?? normalizeKey(options.normalize)
  const missingInRight = multisetDifference(left, right, keyOf)
  const missingInLeft = multisetDifference(right, left, keyOf)

  return {
    equal: left.length === right.length && missingInRight.length === 0 && missingInLeft.length === 0,
    leftCount: left.length,
    rightCount: right.length,
    missingInRight,
    missingInLeft,
  }
}
