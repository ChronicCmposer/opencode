import { describe, expect, test } from "bun:test"
import {
  FrameTooLargeError,
  MAX_WORKER_FRAME_BYTES,
  ProtocolError,
  WORKER_FRAME_VERSION,
  decodeWorkerFrame,
} from "@/session/worker/frame"
import { workerFrameChannel } from "@/session/worker/port"

interface FakePort {
  readonly posted: unknown[]
  readonly port: MessagePort
  deliver(data: unknown): void
}

/** A MessagePort double that records posts and exposes one listener for delivery. */
function fakePort(): FakePort {
  const posted: unknown[] = []
  let listener: ((event: MessageEvent) => void) | undefined
  const port = {
    postMessage: (message: unknown) => {
      posted.push(message)
    },
    addEventListener: (_type: string, handler: (event: MessageEvent) => void) => {
      listener = handler
    },
    removeEventListener: () => {
      listener = undefined
    },
    start: () => {},
  } as unknown as MessagePort
  return {
    posted,
    port,
    deliver: (data) => listener?.({ data } as MessageEvent),
  }
}

const envelopeFor = (payload: { kind: string; id?: number }, port: string) =>
  JSON.stringify({
    v: WORKER_FRAME_VERSION,
    port,
    kind: payload.kind,
    ...(payload.id === undefined ? {} : { id: payload.id }),
    payload,
  })

describe("workerFrameChannel transport", () => {
  test("outbound frames are JSON strings, never structured-clone objects", () => {
    const fake = fakePort()
    const channel = workerFrameChannel<{ kind: "ping"; id: number }>(fake.port, "hub-proxy")

    channel.post({ kind: "ping", id: 7 })

    expect(fake.posted).toHaveLength(1)
    const message = fake.posted[0]
    expect(typeof message).toBe("string")
    expect(JSON.parse(message as string)).toEqual({
      v: WORKER_FRAME_VERSION,
      port: "hub-proxy",
      kind: "ping",
      id: 7,
      payload: { kind: "ping", id: 7 },
    })
  })

  test("decodes the wire string exactly once and hands the payload through unchanged", () => {
    const fake = fakePort()
    const channel = workerFrameChannel<{ kind: "echo"; nested: string }>(fake.port, "hub-proxy")
    const seen: unknown[] = []
    channel.onMessage((payload) => seen.push(payload))

    // The nested value looks like another frame; a second decode would replace it.
    const nested = JSON.stringify({ v: WORKER_FRAME_VERSION, port: "hub-proxy", kind: "echo" })
    fake.deliver(
      JSON.stringify({
        v: WORKER_FRAME_VERSION,
        port: "hub-proxy",
        kind: "echo",
        payload: { kind: "echo", nested },
      }),
    )

    expect(seen).toEqual([{ kind: "echo", nested }])
  })

  test("rejects malformed JSON at the boundary as a ProtocolError", () => {
    const fake = fakePort()
    const channel = workerFrameChannel<{ kind: "ping" }>(fake.port, "hub-proxy")
    channel.onMessage(() => {
      throw new Error("handler must not run for a malformed frame")
    })

    expect(() => fake.deliver("{not json")).toThrow(ProtocolError)
  })

  test("rejects a structured-clone object on the inbound side (JSON strings only)", () => {
    const fake = fakePort()
    const channel = workerFrameChannel<{ kind: "ping" }>(fake.port, "hub-proxy")
    channel.onMessage(() => {
      throw new Error("handler must not run for an object frame")
    })

    expect(() => fake.deliver({ v: WORKER_FRAME_VERSION, port: "hub-proxy", kind: "ping" })).toThrow(ProtocolError)
  })

  test("rejects a frame from another port", () => {
    const fake = fakePort()
    const channel = workerFrameChannel<{ kind: "ping" }>(fake.port, "hub-proxy")
    channel.onMessage(() => {
      throw new Error("handler must not run for a misrouted frame")
    })

    expect(() =>
      fake.deliver(
        JSON.stringify({ v: WORKER_FRAME_VERSION, port: "events", kind: "ping", payload: { kind: "ping" } }),
      ),
    ).toThrow(ProtocolError)
  })

  test("an oversized outbound frame is rejected before it reaches postMessage", () => {
    const fake = fakePort()
    const channel = workerFrameChannel<{ kind: "big"; value: string }>(fake.port, "hub-proxy")

    expect(() => channel.post({ kind: "big", value: "x".repeat(MAX_WORKER_FRAME_BYTES) })).toThrow(FrameTooLargeError)
    expect(fake.posted).toEqual([])
  })

  test("an oversized inbound frame is rejected without parsing or truncating it", () => {
    const fake = fakePort()
    const channel = workerFrameChannel<{ kind: "big" }>(fake.port, "hub-proxy")
    channel.onMessage(() => {
      throw new Error("handler must not run for an oversized frame")
    })

    expect(() => fake.deliver("x".repeat(MAX_WORKER_FRAME_BYTES + 1))).toThrow(FrameTooLargeError)
  })

  test("decodeWorkerFrame is the single decode exported by the transport", () => {
    const envelope = decodeWorkerFrame(envelopeFor({ kind: "ping", id: 3 }, "hub-proxy"))
    expect(envelope).toEqual({
      v: WORKER_FRAME_VERSION,
      port: "hub-proxy",
      kind: "ping",
      id: 3,
      payload: { kind: "ping", id: 3 },
    })
  })
})
