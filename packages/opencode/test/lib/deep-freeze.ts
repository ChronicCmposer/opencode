// Shared deep-freeze helper (plan consolidation).
//
// Both the 3.3 capture harness and the 3.12 golden helper must hand back deeply
// immutable records/snapshots, so the recursive freeze lives in exactly one
// place. `deepFreeze` mutates nothing into existence: it freezes the given value
// and every nested object/array in place and returns the same reference, which
// keeps call sites flat (`return deepFreeze(...)`).

/**
 * Recursively `Object.freeze` `value` and every nested object or array.
 * Primitives and `null`/`undefined` are returned unchanged.
 *
 * Only own enumerable string-keyed properties are traversed (the same set
 * `Object.values` sees). `Map`, `Set`, typed arrays and other exotic objects are
 * frozen as objects but their *contents* are not reached, because `Object.values`
 * does not enumerate their entries. Callers that need entry-level immutability
 * must freeze those containers explicitly.
 */
export function deepFreeze<T>(value: T): T {
  if (value === null || typeof value !== "object") return value
  Object.freeze(value)
  for (const nested of Object.values(value)) deepFreeze(nested)
  return value
}
