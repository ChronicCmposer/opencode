// Seeded JSON and events-frame generators plus byte mutators for the egress decoder fuzz tests.

import { encodeEventsFrameBytes } from "@/session/egress/frame-bytes"
import { WORKER_FRAME_VERSION, type EventsFrame } from "@/session/worker/frame"

export interface Rng {
  readonly next: () => number
  readonly int: (maxExclusive: number) => number
  readonly range: (min: number, maxInclusive: number) => number
  readonly pick: <T>(items: readonly T[]) => T
  readonly chance: (probability: number) => boolean
}

/** mulberry32: small, fast, and reproducible from a 32-bit seed. */
export function makeRng(seed: number): Rng {
  let state = seed >>> 0
  const next = () => {
    state = (state + 0x6d2b79f5) >>> 0
    let t = state
    t = Math.imul(t ^ (t >>> 15), t | 1)
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61)
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
  const int = (maxExclusive: number) => Math.floor(next() * maxExclusive)
  return {
    next,
    int,
    range: (min, max) => min + int(max - min + 1),
    pick: (items) => items[int(items.length)]!,
    chance: (probability) => next() < probability,
  }
}

export const DANGEROUS = ["__proto__", "constructor", "prototype"] as const
export const MAX_DEPTH = 512

const ALPHABET = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-. "
const SPECIAL = ['"', "\\", "/", "\n", "\r", "\t", "\b", "\f", "\u0000", "\u001f", "\u007f", " ", " "]
const UNICODE = ["é", "ß", "漢", "字", "😀", "𝄞", "\u{10ffff}", "\u{1f1fa}\u{1f1f8}", "\ud800", "\udfff", "﻿", "￿"]

export function randomString(rng: Rng, maxLength = 24): string {
  let out = ""
  const length = rng.chance(0.02) ? rng.range(2000, 20000) : rng.int(maxLength + 1)
  for (let i = 0; i < length; i++) {
    const roll = rng.next()
    out += roll < 0.75 ? ALPHABET[rng.int(ALPHABET.length)]! : roll < 0.88 ? rng.pick(SPECIAL) : rng.pick(UNICODE)
  }
  return out
}

function safeKey(rng: Rng): string {
  const key = randomString(rng, 10)
  return (DANGEROUS as readonly string[]).includes(key) ? `${key}_` : key
}

export function randomNumber(rng: Rng): number {
  switch (rng.int(7)) {
    case 0:
      return rng.range(-1000, 1000)
    case 1:
      return (rng.next() - 0.5) * 1e6
    case 2:
      return rng.next() * 10 ** rng.range(-30, 30) * (rng.chance(0.5) ? -1 : 1)
    case 3:
      return rng.pick([0, -0, 1e21, 1e-7, 5e-324, Number.MAX_VALUE, Number.MAX_SAFE_INTEGER, -Number.MAX_SAFE_INTEGER])
    case 4:
      return rng.range(0, 2 ** 31)
    default:
      return rng.int(100)
  }
}

export function randomJson(rng: Rng, depth = 0, budget = { nodes: 60 }): unknown {
  budget.nodes--
  const container = depth < 6 && budget.nodes > 0
  const roll = rng.next()
  if (container && roll < 0.2) {
    const out: Record<string, unknown> = {}
    for (let i = rng.int(6); i > 0; i--) out[safeKey(rng)] = randomJson(rng, depth + 1, budget)
    return out
  }
  if (container && roll < 0.35) return Array.from({ length: rng.int(6) }, () => randomJson(rng, depth + 1, budget))
  if (roll < 0.55) return randomString(rng)
  if (roll < 0.75) return randomNumber(rng)
  if (roll < 0.85) return rng.chance(0.5)
  return null
}

/** `levels` nested containers (alternating arrays and objects) around a leaf; the container count is the depth. */
export function nested(levels: number, leaf: unknown = 0, rng?: Rng): unknown {
  let value = leaf
  for (let i = 0; i < levels; i++) value = (rng ? rng.chance(0.5) : i % 2 === 0) ? [value] : { k: value }
  return value
}

export function randomDepth(rng: Rng): number {
  return rng.pick([MAX_DEPTH - 2, MAX_DEPTH - 1, MAX_DEPTH, MAX_DEPTH + 1, MAX_DEPTH + 2, rng.range(1, MAX_DEPTH)])
}

function randomProperties(rng: Rng): unknown {
  const roll = rng.next()
  const base: Record<string, unknown> = {}
  for (let i = rng.int(4); i > 0; i--) base[safeKey(rng)] = randomJson(rng, 1)
  if (roll < 0.3) return { sessionID: `ses_${randomString(rng, 8)}`, ...base }
  if (roll < 0.4) return { aggregateID: randomString(rng, 8), sessionID: rng.chance(0.5) ? 7 : "s", ...base }
  if (roll < 0.5) return { __eventBlobPosition: { aggregateID: "a", seq: rng.int(1000) }, frameBytes: rng.int(10 ** 7), ...base }
  if (roll < 0.55) return { __eventBlobPosition: 1 }
  if (roll < 0.6) return randomJson(rng)
  if (roll < 0.65) return { deep: nested(rng.range(1, 40), 1, rng) }
  if (roll < 0.68) return { deep: nested(randomDepth(rng) - 1, 1, rng) }
  return base
}

export function randomFrame(rng: Rng): EventsFrame {
  const id = rng.chance(0.5) ? rng.int(10_000) : undefined
  const withId = id === undefined ? {} : { id }
  const roll = rng.next()
  if (roll < 0.7) {
    return {
      v: WORKER_FRAME_VERSION,
      port: "events",
      kind: "event",
      payload: {
        event: { id: `evt_${rng.int(1e9)}`, type: randomString(rng, 20) || "t", properties: randomProperties(rng) },
        ...(rng.chance(0.5) ? { generation: rng.int(100), ordinal: rng.int(1e6) } : {}),
        ...(rng.chance(0.5) ? { aggregateID: randomString(rng, 12), seq: rng.int(1e6) } : {}),
      },
      ...withId,
    }
  }
  if (roll < 0.8) return { v: WORKER_FRAME_VERSION, port: "events", kind: "end", ...withId }
  if (roll < 0.9)
    return {
      v: WORKER_FRAME_VERSION,
      port: "events",
      kind: "resync",
      ...(rng.chance(0.6)
        ? { payload: { ...(rng.chance(0.5) ? { pattern: randomString(rng, 12) } : {}) } }
        : {}),
      ...withId,
    }
  return {
    v: WORKER_FRAME_VERSION,
    port: "events",
    kind: "ack",
    ...(rng.chance(0.7) ? { payload: { count: rng.int(500) } } : {}),
    ...withId,
  }
}

export function randomFrameBytes(rng: Rng): Uint8Array {
  return new Uint8Array(encodeEventsFrameBytes(randomFrame(rng)))
}

const encoder = new TextEncoder()

/** Rebuild a frame from hand-written segments so the mutators can break either one. */
export function assembleFrame(envelope: Uint8Array | string, properties?: Uint8Array | string): Uint8Array {
  const env = typeof envelope === "string" ? encoder.encode(envelope) : envelope
  const props = properties === undefined ? new Uint8Array() : typeof properties === "string" ? encoder.encode(properties) : properties
  const out = new Uint8Array(4 + env.byteLength + props.byteLength)
  new DataView(out.buffer).setUint32(0, env.byteLength, true)
  out.set(env, 4)
  out.set(props, 4 + env.byteLength)
  return out
}

function splice(bytes: Uint8Array, at: number, remove: number, insert: Uint8Array): Uint8Array {
  const out = new Uint8Array(bytes.byteLength - remove + insert.byteLength)
  out.set(bytes.subarray(0, at), 0)
  out.set(insert, at)
  out.set(bytes.subarray(at + remove), at + insert.byteLength)
  return out
}

function segments(bytes: Uint8Array): { envelopeEnd: number } {
  const length = bytes.byteLength >= 4 ? new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getUint32(0, true) : 0
  return { envelopeEnd: Math.min(bytes.byteLength, 4 + length) }
}

export interface Mutator {
  readonly name: string
  readonly apply: (rng: Rng, bytes: Uint8Array) => Uint8Array
}

const insertAt = (rng: Rng, bytes: Uint8Array, piece: Uint8Array) => splice(bytes, rng.int(bytes.byteLength + 1), 0, piece)

export const mutators: readonly Mutator[] = [
  {
    name: "byte-flip",
    apply: (rng, bytes) => {
      const out = bytes.slice()
      for (let i = rng.range(1, 3); i > 0 && out.byteLength > 0; i--) out[rng.int(out.byteLength)] ^= 1 << rng.int(8)
      return out
    },
  },
  { name: "truncate", apply: (rng, bytes) => bytes.slice(0, rng.int(bytes.byteLength + 1)) },
  {
    name: "control-char",
    apply: (rng, bytes) => insertAt(rng, bytes, Uint8Array.of(rng.pick([0, 1, 8, 9, 10, 13, 31, 127]))),
  },
  {
    name: "invalid-utf8",
    apply: (rng, bytes) =>
      insertAt(
        rng,
        bytes,
        rng.pick([[0xff], [0xc0, 0xaf], [0x80], [0xed, 0xa0, 0x80], [0xf4, 0x90, 0x80, 0x80], [0xe2, 0x82], [0xef, 0xbb, 0xbf]].map((b) => Uint8Array.from(b))),
      ),
  },
  {
    name: "dangerous-key",
    apply: (rng, bytes) => {
      const { envelopeEnd } = segments(bytes)
      const key = rng.pick(DANGEROUS)
      const spelled = rng.chance(0.4) ? key.split("").map((c) => `\\u00${c.charCodeAt(0).toString(16)}`).join("") : key
      const value = rng.pick(["1", "null", "{}", '{"a":[]}'])
      // Inject right after an opening brace of the properties segment (or envelope) when one exists.
      const start = rng.chance(0.7) && envelopeEnd < bytes.byteLength ? envelopeEnd : 4
      for (let i = start; i < bytes.byteLength; i++) {
        if (bytes[i] !== 0x7b) continue
        const piece = encoder.encode(`"${spelled}":${value}${bytes[i + 1] === 0x7d ? "" : ","}`)
        const out = splice(bytes, i + 1, 0, piece)
        return start === 4 ? fixHeader(out, piece.byteLength) : out
      }
      return bytes
    },
  },
  {
    name: "duplicate-key",
    apply: (rng, bytes) => {
      const key = rng.pick(["a", "id", "sessionID", "type", "kind", "payload"])
      for (let i = 4; i < bytes.byteLength; i++) {
        if (bytes[i] === 0x7b && rng.chance(0.5)) {
          const piece = encoder.encode(`"${key}":1,"${key}":"x",`)
          const { envelopeEnd } = segments(bytes)
          const out = splice(bytes, i + 1, 0, bytes[i + 1] === 0x7d ? encoder.encode(`"${key}":1,"${key}":"x"`) : piece)
          return i < envelopeEnd ? fixHeader(out, out.byteLength - bytes.byteLength) : out
        }
      }
      return bytes
    },
  },
  {
    name: "trailing-bytes",
    apply: (rng, bytes) =>
      splice(bytes, bytes.byteLength, 0, encoder.encode(rng.pick([" ", "\n", "x", "{}", ",", "0", "\u0000", "   \t\r\n"]))),
  },
  {
    name: "header-length",
    apply: (rng, bytes) => {
      const out = bytes.slice()
      if (out.byteLength < 4) return out
      const view = new DataView(out.buffer)
      const current = view.getUint32(0, true)
      view.setUint32(
        0,
        rng.pick([0, 1, current + 1, Math.max(0, current - 1), out.byteLength, out.byteLength - 4, 0xffffffff, 0x7fffffff, rng.int(out.byteLength + 8)]) >>> 0,
        true,
      )
      return out
    },
  },
  {
    name: "long-key",
    apply: (rng, bytes) => {
      const { envelopeEnd } = segments(bytes)
      if (envelopeEnd >= bytes.byteLength) return bytes
      const length = rng.pick([65535, 65536, 65537])
      const member = encoder.encode(`"${"k".repeat(length)}":1`)
      for (let i = envelopeEnd; i < bytes.byteLength; i++) {
        if (bytes[i] === 0x7b) {
          const comma = bytes[i + 1] === 0x7d ? new Uint8Array() : encoder.encode(",")
          return splice(bytes, i + 1, 0, Uint8Array.from([...member, ...comma]))
        }
      }
      return bytes
    },
  },
  {
    name: "deep-nesting",
    apply: (rng, bytes) => {
      const { envelopeEnd } = segments(bytes)
      const levels = randomDepth(rng)
      const body = encoder.encode(JSON.stringify(nested(levels)))
      return envelopeEnd >= 4 ? splice(bytes, envelopeEnd, bytes.byteLength - envelopeEnd, body) : bytes
    },
  },
]

function fixHeader(bytes: Uint8Array, delta: number): Uint8Array {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
  view.setUint32(0, view.getUint32(0, true) + delta, true)
  return bytes
}

/** Apply 1-2 random mutators to a valid frame. */
export function mutate(rng: Rng, bytes: Uint8Array): { readonly bytes: Uint8Array; readonly applied: readonly string[] } {
  const applied: string[] = []
  let out = bytes
  for (let i = rng.range(1, 2); i > 0; i--) {
    const mutator = rng.pick(mutators)
    applied.push(mutator.name)
    out = mutator.apply(rng, out)
  }
  return { bytes: out, applied }
}
