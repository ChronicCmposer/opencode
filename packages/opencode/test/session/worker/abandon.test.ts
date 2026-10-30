// Worker E2E: discarding reverted messages must abandon the tool-call ledger
// rows they own. A `started` row left behind would block automatic resume after
// its owning message disappears; the ledger update must be scoped to the
// discarded message ids and leave every other call untouched.

import { describe, expect, test } from "bun:test"
import { Effect } from "effect"
import { Session } from "@/session/session"
import { InstanceRef } from "@/effect/instance-ref"
import { MessageID, SessionID } from "@/session/schema"
import { createE2EContext } from "../../lib/parity-harness"
import { workerSnapshot } from "../../lib/snapshot"
import { tmpdir } from "../../fixture/fixture"

describe("Session.discardReverted tool-call abandonment", () => {
  test("discardReverted abandons only the discarded messages' started tool calls", async () => {
    await using tmp = await tmpdir()
    const ctx = createE2EContext({
      snapshot: workerSnapshot({ directory: tmp.path }),
      modelConfig: {
        providerID: "mock",
        modelID: "mock",
        apiId: "mock",
        baseUrl: "http://127.0.0.1:1",
        apiKey: "test",
        headers: {},
      },
    })
    try {
      const sessionID = SessionID.make("ses_abandon")
      const discarded = MessageID.ascending()
      const kept = MessageID.ascending()
      // The e2e context seeds the `e2e` project; insert the session row directly.
      ctx.connection.run(
        "INSERT INTO session (id, project_id, slug, directory, title, version, metadata, permission, model, agent, revert, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [sessionID, "e2e", "abandon", tmp.path, "abandon", "1", "{}", "[]", null, "build", null, 1, 1],
      )
      ctx.connection.run(
        "INSERT INTO tool_call (call_id, session_id, tool, side_effecting, state, outcome, time_started) VALUES (?, ?, ?, ?, ?, ?, ?)",
        [`${discarded}:call_1`, sessionID, "bash", 1, "started", null, 1],
      )
      ctx.connection.run(
        "INSERT INTO tool_call (call_id, session_id, tool, side_effecting, state, outcome, time_started) VALUES (?, ?, ?, ?, ?, ?, ?)",
        [`${kept}:call_2`, sessionID, "bash", 1, "started", null, 2],
      )

      await Effect.runPromise(
        Effect.gen(function* () {
          const sessions = yield* Session.Service
          yield* sessions.discardReverted({ sessionID, messageIDs: [discarded], parts: [] })
        }).pipe(Effect.provide(ctx.layer), Effect.provideService(InstanceRef, ctx.instance)),
      )

      const rows = ctx.connection.all<{
        call_id: string
        state: string
        outcome: string | null
        time_finished: number | null
      }>("SELECT call_id, state, outcome, time_finished FROM tool_call ORDER BY call_id", [])
      const abandoned = rows.find((row) => row.call_id === `${discarded}:call_1`)
      const untouched = rows.find((row) => row.call_id === `${kept}:call_2`)
      expect(abandoned).toMatchObject({ state: "finished", outcome: "abandoned" })
      expect(abandoned!.time_finished).toBeGreaterThan(0)
      expect(untouched).toMatchObject({ state: "started", outcome: null })
      // The hub's auto-resume decision reads exactly this query, so the abandoned
      // row no longer blocks it.
      const started = ctx.connection.all<{ call_id: string }>(
        "SELECT call_id FROM tool_call WHERE session_id = ? AND state = 'started' ORDER BY call_id",
        [sessionID],
      )
      expect(started).toEqual([{ call_id: `${kept}:call_2` }])
    } finally {
      await ctx.dispose()
    }
  }, 60_000)
})
