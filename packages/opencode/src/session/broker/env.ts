// Phase 10 — shared environment overlay for the broker.
//
// Both isolates merge a caller-supplied overlay onto `process.env` before
// spawning a broker worker (`broker.ts`) or a child server (`broker-worker.ts`).
// This is the single, pure implementation they share: no hidden mutation, the
// same inputs always produce the same overlay.
//
// Policy: an overlay value of `undefined` *deletes* the inherited key; any
// other value overrides it. This lets a caller scrub a variable it does not want
// to leak into a child, which the broker's `env` option relies on.

/** Merge `overlay` onto `process.env`; `undefined` overlay values delete keys. */
export function mergeEnv(
  overlay?: Readonly<Record<string, string | undefined>>,
  base: Readonly<Record<string, string | undefined>> = process.env,
): Record<string, string> {
  const env: Record<string, string> = {}
  for (const [key, value] of Object.entries(base)) {
    if (value !== undefined) env[key] = value
  }
  for (const [key, value] of Object.entries(overlay ?? {})) {
    if (value === undefined) delete env[key]
    else env[key] = value
  }
  return env
}

export * as BrokerEnv from "./env"
