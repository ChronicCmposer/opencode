// Shared object shaping for optional spreads.
//
// The worker and writer ports carry many optional fields. The idiomatic
// construction `{ ...(x === undefined ? {} : { x }) }` is noisy and easy to get
// wrong; `...defined({ x })` keeps the call site flat and strips `undefined`
// values in one place.

/**
 * Return a shallow copy of `obj` with every `undefined`-valued key removed.
 * Pure: the input is never mutated.
 *
 * Use it to spread optional fields: `{ type, ...defined({ cache }) }`.
 */
export function defined<T extends object>(obj: T): Partial<T> {
  const result: Partial<T> = {}
  for (const [key, value] of Object.entries(obj)) {
    if (value !== undefined) (result as Record<string, unknown>)[key] = value
  }
  return result
}

export * as ObjectUtil from "./object"
