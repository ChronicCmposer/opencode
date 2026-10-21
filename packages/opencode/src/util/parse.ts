// Shared numeric parsing for option/env boundaries.
//
// Every caller that resolves a count or bound from an untrusted source (an env
// var, a constructor option, a CLI flag) wants the same rule: a positive
// integer, or a loud failure naming the offender. This is that single rule, so
// the boundary logic is not re-derived per module.

/** True when `value` is an integer >= 1. */
export function isPositiveInt(value: number): boolean {
  return Number.isInteger(value) && value >= 1
}

/**
 * Parse `value` as a positive integer, or throw a descriptive error. Accepts a
 * raw string so the message can echo exactly what the caller supplied instead of
 * a lossy `NaN`.
 */
export function parsePositiveInt(value: string | number, label: string): number {
  const parsed = typeof value === "number" ? value : Number(value)
  if (!isPositiveInt(parsed)) {
    throw new Error(`invalid ${label}: ${value} (expected a positive integer)`)
  }
  return parsed
}

export * as Parse from "./parse"
