import { describe, expect, test } from "bun:test"
import type { SessionID } from "@/session/schema"
import type { SessionExecutionResult } from "@/session/worker/execute"
import { createGoldenCapture, diffGolden } from "./golden"
import { DEFAULT_OWNER, projectSessionDurable, recordSessionDurable, specCapture } from "./rollout-equivalence"

const sessionID = "ses_unit" as SessionID

describe("rollout durable transcript", () => {
  test("projectSessionDurable emits GoldenRow-shaped rows with contiguous seq", () => {
    const rows = projectSessionDurable(sessionID, { text: "hi", chunks: 2 })
    expect(rows.map((row) => row.seq)).toEqual([0, 1, 2])
    expect(rows.every((row) => row.aggregate_id === sessionID)).toBe(true)
    expect(rows.every((row) => row.owner_id === DEFAULT_OWNER)).toBe(true)
    expect(rows[1]?.type).toBe("message.assistant")
  })

  test("recordSessionDurable and specCapture agree", () => {
    const result = { sessionID, value: { text: "hi", chunks: 2 } } as SessionExecutionResult
    const recorded = createGoldenCapture()
    recordSessionDurable(recorded, result)
    const spec = specCapture(sessionID, { text: "hi", chunks: 2 })
    expect(diffGolden(spec.snapshot(), recorded.snapshot())).toEqual([])
  })
})
