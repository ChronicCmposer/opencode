import { describe, expect, test } from "bun:test"
import { createCancelPair, createListenerPair, type ListenerPort } from "./port-pair"
import type { CancelFrame } from "@/session/worker/cancel"

interface Frame {
  readonly type: string
  readonly n?: number
}

describe("createListenerPair", () => {
  test("delivers a frame posted on one side to the peer's subscribers", () => {
    const pair = createListenerPair<Frame>()
    const toWorker: Frame[] = []
    const toHub: Frame[] = []

    pair.worker.onMessage((frame) => toWorker.push(frame))
    pair.hub.onMessage((frame) => toHub.push(frame))

    pair.hub.post({ type: "cancel" })
    pair.worker.post({ type: "ack", n: 1 })

    expect(toWorker).toEqual([{ type: "cancel" }])
    expect(toHub).toEqual([{ type: "ack", n: 1 }])
    expect(pair.worker.received).toEqual([{ type: "cancel" }])
    expect(pair.hub.received).toEqual([{ type: "ack", n: 1 }])
    expect(pair.hub.side).toBe("hub")
    expect(pair.worker.side).toBe("worker")
  })

  test("fans out to every subscriber and supports unsubscribe", () => {
    const pair = createListenerPair<Frame>()
    const seen: string[] = []
    const unsubscribe = pair.worker.onMessage(() => seen.push("a"))
    pair.worker.onMessage(() => seen.push("b"))

    pair.hub.post({ type: "one" })
    unsubscribe()
    pair.hub.post({ type: "two" })

    expect(seen).toEqual(["a", "b", "b"])
  })

  test("a handler that unsubscribes mid-delivery does not skip a peer", () => {
    const pair = createListenerPair<Frame>()
    const seen: string[] = []
    const first = () => {
      seen.push("first")
      unsubscribe()
    }
    const unsubscribe = pair.worker.onMessage(first)
    pair.worker.onMessage(() => seen.push("second"))

    pair.hub.post({ type: "x" })

    expect(seen).toEqual(["first", "second"])
  })

  test("close detaches both sides", () => {
    const pair = createListenerPair<Frame>()
    const seen: string[] = []
    pair.worker.onMessage((frame) => seen.push(frame.type))

    pair.close()
    pair.hub.post({ type: "ignored" })

    expect(pair.hub.closed).toBe(true)
    expect(pair.worker.closed).toBe(true)
    expect(seen).toEqual([])
  })

  test("honours injected post/onMessage hooks", () => {
    const posted: string[] = []
    const pair = createListenerPair<Frame>(
      (_port, frame) => posted.push(frame.type),
      (port, handler) => port.onMessage(handler),
    )

    pair.hub.post({ type: "hooked" })

    expect(posted).toEqual(["hooked"])
    expect(pair.worker.received).toEqual([])
  })

  test("endpoints are immutable and hooks receive the real endpoint", () => {
    const seen: ListenerPort<Frame>[] = []
    const pair = createListenerPair<Frame>((port) => {
      seen.push(port)
    })

    expect(Object.isFrozen(pair.hub)).toBe(true)
    expect(Object.isFrozen(pair.worker)).toBe(true)

    pair.worker.post({ type: "from-worker" })

    expect(seen).toEqual([pair.worker])
  })
})

describe("createCancelPair", () => {
  test("hub.notify delivers a typed cancel frame to the worker side", () => {
    const pair = createCancelPair()
    const seen: CancelFrame[] = []
    pair.worker.onMessage((frame) => seen.push(frame))

    pair.hub.notify({ type: "cancel", sessionID: "ses_1", reason: "user" })

    expect(seen).toEqual([{ type: "cancel", sessionID: "ses_1", reason: "user" }])
    pair.close()
  })
})
