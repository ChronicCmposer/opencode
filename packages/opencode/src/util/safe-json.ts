// Boundary JSON decoding that cannot pollute `Object.prototype`.
//
// `JSON.parse` records `__proto__` as an own property, so a later spread or
// `Object.assign` turns it into the prototype setter. The same is true of
// `constructor` / `prototype` chains. Rejecting those names once at the decode
// boundary is cheaper and safer than auditing every downstream merge.

/** Object keys that can reach a prototype through a spread or assignment. */
export const DANGEROUS_KEYS = ["__proto__", "constructor", "prototype"] as const

const dangerous: ReadonlySet<string> = new Set(DANGEROUS_KEYS)

/** Raised when an untrusted JSON payload carries a prototype-reaching key. */
export class PrototypePollutionError extends Error {
  readonly tag = "PROTOTYPE_POLLUTION"

  constructor(
    readonly path: string,
    readonly key: string,
    options?: { readonly cause?: unknown },
  ) {
    super(`refusing JSON key "${key}" at ${path}: it can reach Object.prototype`, options)
    this.name = "PrototypePollutionError"
  }
}

function walk(value: unknown, path: string): void {
  if (Array.isArray(value)) {
    value.forEach((item, index) => walk(item, `${path}[${index}]`))
    return
  }
  if (typeof value !== "object" || value === null) return

  for (const key of Object.keys(value)) {
    if (dangerous.has(key)) throw new PrototypePollutionError(path, key)
    walk((value as Record<string, unknown>)[key], `${path}.${key}`)
  }
}

/**
 * Parse one JSON string and reject any prototype-reaching key, at any depth.
 * Invalid JSON is translated to the same typed error so callers handle one
 * failure class at the boundary.
 */
export function parseJson(text: string): unknown {
  let parsed: unknown
  try {
    parsed = JSON.parse(text)
  } catch (error) {
    throw new PrototypePollutionError("$", "invalid JSON", { cause: error })
  }
  walk(parsed, "$")
  return parsed
}

export * as SafeJson from "./safe-json"
