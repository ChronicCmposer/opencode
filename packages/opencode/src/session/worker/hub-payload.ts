import { MAX_WORKER_FRAME_BYTES } from "./frame"

/** Leaves room for the frame envelope and for the reply echoing the output. */
const PAYLOAD_BUDGET_BYTES = Math.floor(MAX_WORKER_FRAME_BYTES * 0.75)
const DATA_URL_MIN_CHARS = 16 * 1024
const LARGE_STRING_MIN_CHARS = 64 * 1024

type Substitution = (value: string) => boolean

const isPlain = (value: unknown): value is Record<string, unknown> => {
  if (typeof value !== "object" || value === null) return false
  const proto = Object.getPrototypeOf(value)
  return proto === Object.prototype || proto === null
}

function bytesOf(value: unknown): number {
  const text = JSON.stringify(value)
  return text === undefined ? 0 : Buffer.byteLength(text)
}

function mapStrings(value: unknown, replace: (text: string) => string): unknown {
  if (typeof value === "string") return replace(value)
  if (Array.isArray(value)) return value.map((item) => mapStrings(item, replace))
  if (!isPlain(value)) return value
  return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, mapStrings(item, replace)]))
}

/**
 * Runs `call` with `args` after swapping oversized strings for placeholders, then
 * restores the originals in the result, so a payload the frame cap would reject
 * still reaches the hub. Payloads within budget cross untouched.
 */
export async function withLargeStringsHeld<A extends readonly unknown[], R>(
  args: A,
  call: (args: A) => Promise<R>,
): Promise<R> {
  if (bytesOf(args) <= PAYLOAD_BUDGET_BYTES) return call(args)
  const held = new Map<string, string>()
  const nonce = crypto.randomUUID()
  const hold = (matches: Substitution) => (current: A) =>
    mapStrings(current, (text) => {
      if (!matches(text)) return text
      const key = `opencode-worker-held:${nonce}:${held.size}`
      held.set(key, text)
      return key
    }) as A
  let sent = hold((text) => text.length >= DATA_URL_MIN_CHARS && text.startsWith("data:"))(args)
  if (bytesOf(sent) > PAYLOAD_BUDGET_BYTES) sent = hold((text) => text.length >= LARGE_STRING_MIN_CHARS)(sent)
  const result = await call(sent)
  return mapStrings(result, (text) => held.get(text) ?? text) as R
}
