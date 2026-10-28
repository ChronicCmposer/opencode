// Full-row golden durable equivalence for the SAME mock-SSE session.
//
// One mock session (one streamed assistant reply) is executed twice:
//   * flag OFF (hub path) — the session runs in-process on the hub, using the same
//     portable `SessionRunMachine` the hub keeps when workers are disabled;
//   * flag ON (worker path) — the session runs in a real Bun worker isolate over
//     the landed executor pool.
//
// Each run's chat result is projected to the session's durable transcript and
// recorded with the 3.12 golden helper, which stores the FULL row
// `{aggregate_id, seq, type, data, owner_id}`. The test then asserts:
//   14.2a  the two paths commit an identical full-row sequence (flag off preserves
//          behavior), and
//   14.2b  the worker path matches the SPEC (a golden capture built directly from
//          the known mock reply).
//
// No real model APIs: the provider is the in-process `TestLLMServer` (mock SSE).

import { describe, expect } from "bun:test"
import { Effect } from "effect"
import type { SessionID } from "@/session/schema"
import { testEffect } from "../../lib/effect"
import { assertGoldenEqual, createGoldenCapture, diffGolden, formatGoldenDiff } from "../../lib/golden"
import { TestLLMServer, reply } from "../../lib/llm-server"
import {
  DEFAULT_OWNER,
  recordSessionDurable,
  runHubSession,
  runWorkerSession,
  specCapture,
} from "../../lib/rollout-equivalence"

const live = testEffect(TestLLMServer.layer)

const SESSION_ID = "ses_rollout_1" as SessionID
const BURN_ITERATIONS = 200_000
const CORES = 4

// The mock reply is two content deltas that concatenate to "hello world".
const REPLY_TEXT = "hello world"
const REPLY_CHUNKS = 2

describe("14.2 flag-off/flag-on full-row golden equivalence (mock SSE)", () => {
  live.live("the same session commits an identical durable row sequence on both paths", () =>
    Effect.gen(function* () {
      const llm = yield* TestLLMServer
      const item = reply().text("hello ").text("world").stop().item()
      // One reply per path: hub path consumes the first, worker path the second.
      yield* llm.push(item, item)

      const off = createGoldenCapture()
      const hubResult = yield* Effect.promise(() =>
        runHubSession({ baseUrl: llm.url, sessionID: SESSION_ID, burnIterations: BURN_ITERATIONS }),
      )
      recordSessionDurable(off, hubResult, DEFAULT_OWNER)

      const on = createGoldenCapture()
      const workerResult = yield* Effect.promise(() =>
        runWorkerSession({
          baseUrl: llm.url,
          sessionID: SESSION_ID,
          burnIterations: BURN_ITERATIONS,
          cores: CORES,
        }),
      )
      recordSessionDurable(on, workerResult, DEFAULT_OWNER)

      // 14.2a — equivalence: flag OFF (hub) == flag ON (worker), full row.
      const differences = diffGolden(off.snapshot(), on.snapshot())
      expect(differences, formatGoldenDiff(differences, "flag-off vs flag-on")).toEqual([])
      assertGoldenEqual(off.snapshot(), on.snapshot(), "flag-off vs flag-on")

      // 14.2b — flag ON matches the spec.
      const spec = specCapture(SESSION_ID, { text: REPLY_TEXT, chunks: REPLY_CHUNKS }, DEFAULT_OWNER)
      assertGoldenEqual(spec.snapshot(), on.snapshot(), "flag-on vs spec")

      // The durable sequence is the expected, non-trivial per-aggregate one.
      expect(on.sequence().map((row) => `${row.aggregate_id}:${row.seq}:${row.type}`)).toEqual([
        `${SESSION_ID}:0:session.created`,
        `${SESSION_ID}:1:message.assistant`,
        `${SESSION_ID}:2:session.idle`,
      ])
      expect(on.rows.map((row) => row.owner_id)).toEqual([DEFAULT_OWNER, DEFAULT_OWNER, DEFAULT_OWNER])

      // The golden helper records onto the 3.3 capture timeline (one `db` channel
      // record per durable row); the timeline is the immutable record under test.
      expect(on.capture.dbWrites().map((record) => record.kind)).toEqual(["durable", "durable", "durable"])
      expect(on.capture.records).toHaveLength(3)
    }),
  )

  live.live("the worker path's durable data matches the mock SSE content exactly", () =>
    Effect.gen(function* () {
      const llm = yield* TestLLMServer
      yield* llm.push(reply().text("alpha").text(" beta").stop().item())

      const on = createGoldenCapture()
      const result = yield* Effect.promise(() =>
        runWorkerSession({ baseUrl: llm.url, sessionID: "ses_rollout_2" as SessionID, burnIterations: BURN_ITERATIONS }),
      )
      recordSessionDurable(on, result, DEFAULT_OWNER)

      const spec = specCapture("ses_rollout_2" as SessionID, { text: "alpha beta", chunks: 2 }, DEFAULT_OWNER)
      assertGoldenEqual(spec.snapshot(), on.snapshot(), "flag-on vs spec (second reply)")
    }),
  )
})
