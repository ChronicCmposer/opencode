// The hub-proxy reply clone guard.
//
// A hub-side handler may return a value the structured clone algorithm rejects
// (a function, symbol, class instance, `WeakMap`, ...) — e.g. a plugin hook that
// leaks a callback into the rewritten payload. `serveHubProxy` posts the reply
// over the multiplexed control port; a non-cloneable success value throws
// `DataCloneError` synchronously from `port.postMessage`.
//
// Historically that throw was swallowed ("the worker is gone"), so the worker
// never received a reply: it hung (no timeout) or surfaced only as a generic
// timeout, hiding the real cause. This suite pins the fail-loud contract:
//   * a non-cloneable success reply rejects the worker's `call` with a
//     `HubProxyRemoteError` whose message names the clone failure — NOT a
//     timeout — and the channel stays usable;
//   * the recovery post is exactly ONE clone-safe `ok:false` frame echoing the
//     same id;
//   * a genuinely dead peer (both posts throw) stays silent: `serveHubProxy`
//     does not throw, no unhandled rejection escapes, and the worker only sees
//     its timeout.

import { describe, expect, test } from "bun:test"
import { HubProxyRemoteError, createHubProxyClient, serveHubProxy } from "@/session/worker/hub-proxy-wire"

/** Serve `handlers` on a fresh channel and return the worker-end client. */
function clientFor(
  handlers: Parameters<typeof serveHubProxy>[0],
  timeoutMs?: number,
): ReturnType<typeof createHubProxyClient> {
  const channel = new MessageChannel()
  serveHubProxy(handlers, channel.port2)
  return createHubProxyClient(channel.port1, timeoutMs === undefined ? {} : { timeoutMs })
}

/**
 * Resolve when `signal` fires, or reject after `timeoutMs`. The timeout is a
 * backstop against a hung suite — never the synchronization primitive itself.
 */
function awaitSignal(signal: Promise<void>, timeoutMs: number, description: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      reject(new Error(`timed out after ${timeoutMs}ms waiting for ${description}`))
    }, timeoutMs)
    void signal.then(() => {
      clearTimeout(timer)
      resolve()
    })
  })
}

describe("hub-proxy reply clone guard", () => {
  test("a non-cloneable success reply fails loud (descriptive error, not a timeout) and the channel stays usable", async () => {
    let calls = 0
    const client = clientFor(
      {
        // First call returns a non-cloneable value; the second is clone-safe.
        messagesTransform: () => {
          calls += 1
          return calls === 1 ? { onEvent: () => {} } : { ok: true }
        },
      },
      // A generous backstop: a silent drop (the pre-fix behaviour) is caught by
      // the message assertion, not by racing a short timeout.
      2000,
    )

    try {
      let caught: unknown
      try {
        await client.call("messagesTransform", [{}, { messages: [] }])
      } catch (error) {
        caught = error
      }

      expect(caught).toBeInstanceOf(HubProxyRemoteError)
      const message = (caught as Error).message
      // The real cause is named, not masked as a timeout.
      expect(message).toContain("not structured-cloneable")
      expect(message).not.toContain("timed out")

      // Fail loud must not poison the multiplexed control port.
      const ok = await client.call("messagesTransform", [{}, { messages: [] }])
      expect(ok).toEqual({ ok: true })
      expect(calls).toBe(2)
    } finally {
      client.dispose()
    }
  })

  test("recovery is exactly one clone-safe ok:false frame echoing the request id", async () => {
    const channel = new MessageChannel()
    // The success value fails JSON encoding before the port is touched, so only
    // the recovery frame is ever posted. The fallback post IS the completion
    // signal: the stub resolves this deferred the moment it captures the frame.
    const posted: string[] = []
    let signalFallbackPosted: (() => void) | undefined
    const fallbackPosted = new Promise<void>((resolve) => {
      signalFallbackPosted = resolve
    })
    channel.port2.postMessage = ((frame: string) => {
      posted.push(frame)
      signalFallbackPosted?.()
    }) as never

    serveHubProxy({ messagesTransform: () => ({ onEvent: () => {} }) }, channel.port2)
    const client = createHubProxyClient(channel.port1, { timeoutMs: 2000 })

    try {
      void client.call("messagesTransform", [{}, { messages: [] }]).catch(() => {})
      await awaitSignal(fallbackPosted, 2000, "the JSON-safe fallback post")

      // Exactly one post: the recovery frame. The un-serialisable success value
      // never reached `postMessage`.
      expect(posted).toHaveLength(1)

      const envelope = JSON.parse(posted[0]!) as Record<string, unknown>
      expect(envelope.kind).toBe("reply")
      expect(envelope.id).toBe(1)
      const payload = envelope.payload as Record<string, unknown>
      expect(payload.ok).toBe(false)
      expect(payload.id).toBe(1)
      expect(String(payload.message)).toContain("not structured-cloneable")
      expect(String(payload.message)).toContain("id 1")
    } finally {
      client.dispose()
    }
  })

  test("a genuinely dead peer stays silent: no throw, no unhandled rejection, worker sees only its timeout", async () => {
    const channel = new MessageChannel()
    // Simulate a peer whose port rejects every post (the "worker is gone" case:
    // both the success reply and the clone-safe fallback fail).
    channel.port2.postMessage = (() => {
      throw new DOMException("The object can not be cloned.", "DataCloneError")
    }) as never

    const rejections: unknown[] = []
    const onRejection = (reason: unknown): void => {
      rejections.push(reason)
    }
    process.on("unhandledRejection", onRejection)

    let serveThrew: unknown
    try {
      try {
        serveHubProxy({ messagesTransform: () => ({ ok: true }) }, channel.port2)
      } catch (error) {
        serveThrew = error
      }
      // The only test that pins a genuine timeout: both posts fail, so no reply
      // can ever arrive and the worker sees a timeout. The backstop is generous
      // so a slow CI host cannot turn a real reply into a false timeout.
      const client = createHubProxyClient(channel.port1, { timeoutMs: 2000 })
      try {
        let caught: unknown
        try {
          await client.call("messagesTransform", [{}, { messages: [] }])
        } catch (error) {
          caught = error
        }
        // Only the timeout surfaces; the reply path failed twice and stayed quiet.
        expect(caught).toBeInstanceOf(HubProxyRemoteError)
        expect((caught as Error).message).toContain("timed out")
      } finally {
        client.dispose()
      }
    } finally {
      process.off("unhandledRejection", onRejection)
    }

    expect(serveThrew).toBeUndefined()
    expect(rejections).toEqual([])
  })
})
