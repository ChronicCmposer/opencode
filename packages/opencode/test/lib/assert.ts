// Small assertion predicates for test boundaries.
//
// These are TypeScript assertion functions: after the call the compiler knows the
// narrowed type, so the guard disappears from the rest of the code and there is
// no repeated validation deep in a test body (Law of "Parse, Don't Validate" and
// Law of "Fail Fast, Fail Loud" — an invalid input halts immediately with a
// message that names the offending field and value).

/**
 * A compact, total string form of an arbitrary value for error messages.
 *
 * `JSON.stringify` throws on cycles and returns `undefined` for `undefined`; both
 * are handled so a diagnostic never throws while reporting a failure.
 */
export function stringify(value: unknown): string {
  if (value === undefined) return "undefined"
  try {
    return JSON.stringify(value) ?? String(value)
  } catch {
    return String(value)
  }
}

const describe = stringify

/** Assert `value` is a non-empty string; narrows the type on return. */
export function assertNonEmptyString(value: unknown, name = "value"): asserts value is string {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`${name} must be a non-empty string, got ${describe(value)}`)
  }
}

/** Assert `value` is a positive integer (> 0); narrows the type on return. */
export function assertPositiveInteger(value: unknown, name = "value"): asserts value is number {
  if (typeof value !== "number" || !Number.isInteger(value) || value <= 0) {
    throw new Error(`${name} must be a positive integer, got ${describe(value)}`)
  }
}

/** Assert `value` is a non-negative integer (>= 0); narrows the type on return. */
export function assertNonNegativeInteger(value: unknown, name = "value"): asserts value is number {
  if (typeof value !== "number" || !Number.isInteger(value) || value < 0) {
    throw new Error(`${name} must be a non-negative integer, got ${describe(value)}`)
  }
}
