import { describe, expect, it } from "bun:test"
import { Cause, Effect, Exit } from "effect"
import { createRequestChannel, WorkerReplyTimeout, type FrameChannel } from "@/session/worker/port"

type RequestFrame = { readonly kind: "request"; readonly id: number }
type ReplyFrame = { readonly kind: "reply"; readonly id: number }
type Frame = RequestFrame | ReplyFrame

type Reply = ReplyFrame

interface Harness {
  readonly channel: FrameChannel<Frame>
  deliver(frame: Frame): void
}

function harness(): Harness {
  let listener: ((frame: Frame) => void) | undefined
  return {
    channel: {
      post: () => {},
      onMessage: (handler) => {
        listener = handler
        return () => {
          listener = undefined
        }
      },
    },
    deliver: (frame) => listener?.(frame),
  }
}

const channelOptions = {
  isReply: (frame: Frame): frame is Reply => frame.kind === "reply",
  idOf: (reply: Reply) => reply.id,
  nextId: (() => { let next = 0; return () => ++next })(),
  disposedError: () => new Error("disposed"),
  timeoutError: (id: number, timeoutMs: number) => new WorkerReplyTimeout({ id, timeoutMs }),
}

describe("worker request channel", () => {
  it("settles a pending request from the message handler's Deferred", async () => {
    const { channel, deliver } = harness()
    const requests = createRequestChannel<Frame, Reply, number>(channel, channelOptions)

    const pending = Effect.runPromise(requests.request((id) => ({ kind: "request", id })))
    await Effect.runPromise(Effect.yieldNow)
    deliver({ kind: "reply", id: 1 })

    expect(await pending).toEqual({ kind: "reply", id: 1 })
    requests.dispose()
  })

  it("fails every pending request with one typed timeout error", async () => {
    const { channel } = harness()
    const requests = createRequestChannel<Frame, Reply, number>(channel, channelOptions)

    const exit = await Effect.runPromiseExit(requests.request((id) => ({ kind: "request", id }), 10))

    expect(Exit.isFailure(exit)).toBe(true)
    if (Exit.isFailure(exit)) {
      const error = exit.cause.reasons.find(Cause.isFailReason)?.error
      expect(error).toBeInstanceOf(WorkerReplyTimeout)
    }
    requests.dispose()
  })

  it("fails every pending Deferred in one place when the channel closes", async () => {
    const { channel } = harness()
    const requests = createRequestChannel<Frame, Reply, number>(channel, channelOptions)

    const first = Effect.runPromiseExit(requests.request((id) => ({ kind: "request", id })))
    const second = Effect.runPromiseExit(requests.request((id) => ({ kind: "request", id })))
    await Effect.runPromise(Effect.yieldNow)
    requests.dispose()

    expect((await first)._tag).toBe("Failure")
    expect((await second)._tag).toBe("Failure")
  })
})
