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
 */
export function deepFreeze<T>(value: T): T {
  if (value === null || typeof value !== "object") return value
  Object.freeze(value)
  for (const nested of Object.values(value as Record<string, unknown>)) deepFreeze(nested)
  return value
}
