// Shared serialized-header parsing for the session worker.
//
// Both `init.ts` (serializing a provider's options onto the worker descriptor)
// and `layer.ts` (rebuilding the reduced provider's SDK from that descriptor)
// need to keep only the string-valued entries of a serialized header bag. This
// is the single implementation, so the two sides cannot drift.

/** Keep only string-valued header entries from a serialized provider option. */
export function stringHeaders(value: unknown): Record<string, string> {
  if (value === null || typeof value !== "object") return {}
  const headers: Record<string, string> = {}
  for (const [key, header] of Object.entries(value as Record<string, unknown>)) {
    if (typeof header === "string") headers[key] = header
  }
  return headers
}

export * as WorkerHeaders from "./headers"
