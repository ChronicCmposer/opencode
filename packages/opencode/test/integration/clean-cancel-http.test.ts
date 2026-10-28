// A clean cancel is a typed HTTP success, never `BadRequest`.
//
// The abort route reports the discriminated `AbortOutcome`; an idle session is
// the explicit "completed" outcome (not an empty body), and a cooperative cancel
// carries the interrupted assistant message the run's `onInterrupt` produced.
// The flag-on `onInterrupt` branch itself is pinned by
// `test/session/run-state.test.ts`; this suite pins the HTTP status/payload.

import { afterEach, describe, expect, test } from "bun:test"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Effect, Layer, Schema } from "effect"
import { Session as SessionNs } from "@/session/session"
import { TestInstance, disposeAllInstances } from "../fixture/fixture"
import { testEffect } from "../lib/effect"
import { AbortOutcome, SessionPaths } from "../../src/server/routes/instance/httpapi/groups/session"
import { resolveAbortOutcome, toAbortOutcome } from "../../src/server/routes/instance/httpapi/handlers/session"
import { SessionID } from "../../src/session/schema"
import { httpApiLayer, requestInDirectory } from "../server/httpapi-layer"

delete process.env.OPENCODE_DISABLE_PROJECT_CONFIG
delete process.env.OPENCODE_CONFIG_CONTENT
delete process.env.OPENCODE_CONFIG_DIR

const it = testEffect(Layer.mergeAll(LayerNode.compile(SessionNs.node), httpApiLayer))

afterEach(async () => {
  await disposeAllInstances()
})

const sessionID = SessionID.make("ses_clean_cancel_http")

const interruptedMessage = {
  info: {
    id: "msg_clean_cancel",
    role: "assistant",
    sessionID,
    time: { created: 1, completed: 2 },
    error: { name: "MessageAbortedError", data: { message: "Aborted" } },
  },
  parts: [],
} as never

describe("clean cancel over HTTP", () => {
  it.instance(
    "an idle abort is a 200 typed completed outcome, not an empty body",
    () =>
      Effect.gen(function* () {
        const test = yield* TestInstance
        const session = yield* Effect.acquireRelease(SessionNs.use.create({}), (created) =>
          SessionNs.use.remove(created.id).pipe(Effect.ignore),
        )

        const res = yield* requestInDirectory(SessionPaths.abort.replace(":sessionID", session.id), test.directory, {
          method: "POST",
        })

        expect(res.status).toBe(200)
        expect(yield* res.json).toEqual({
          sessionID: session.id,
          mode: "completed",
          winner: "completion",
          acknowledged: false,
          durationMs: 0,
        })
      }),
  )

  test("a cooperative cancel projects the interrupted message", () => {
    const outcome = toAbortOutcome({
      sessionID,
      mode: "cooperative",
      winner: "cancel",
      acknowledged: true,
      durationMs: 12,
      message: interruptedMessage,
    })
    expect(outcome).toEqual({
      sessionID,
      mode: "cooperative",
      winner: "cancel",
      acknowledged: true,
      durationMs: 12,
      message: interruptedMessage,
    })
  })

  test("every cancel mode maps to a decodable typed body", () => {
    const bodies: Array<typeof AbortOutcome.Type> = [
      toAbortOutcome({ sessionID, mode: "cooperative", winner: "cancel", acknowledged: true, durationMs: 1 }),
      toAbortOutcome({ sessionID, mode: "terminated", winner: "cancel", acknowledged: false, durationMs: 4 }),
      toAbortOutcome({ sessionID, mode: "boot", winner: "cancel", acknowledged: false, durationMs: 0 }),
      resolveAbortOutcome(sessionID, undefined),
    ]
    for (const body of bodies) {
      expect(() => Schema.decodeUnknownSync(AbortOutcome)(body)).not.toThrow()
    }
    expect(bodies.map((body) => body.mode)).toEqual(["cooperative", "terminated", "boot", "completed"])
  })
})
