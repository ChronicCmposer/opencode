import { describe, expect, test } from "bun:test"
import { Schema } from "effect"
import { LegacyEvent } from "../src/legacy-event"
import { PermissionV1 } from "../src/permission-v1"
import { QuestionV1 } from "../src/question-v1"
import { Project } from "../src/project"
import { SessionID } from "../src/session-id"
import { SessionV1 } from "../src/session-v1"

describe("legacy public event schemas", () => {
  test("owns all SessionV1 definitions", () => {
    expect(SessionV1.Event.Definitions.map((event) => event.type)).toEqual([
      "session.created",
      "session.updated",
      "session.deleted",
      "message.updated",
      "message.removed",
      "message.part.updated",
      "message.part.removed",
      "message.part.delta",
      "session.diff",
      "session.error",
      "session.cancelled",
    ])
    const durable = SessionV1.Event.Definitions.filter((event) => event.durable !== undefined)
    expect(durable).toHaveLength(7)
    expect(durable.every((event) => event.durable?.aggregate === "sessionID")).toBe(true)
    expect(durable.every((event) => event.durable?.version === 1)).toBe(true)
  })

  test("owns the transient session.cancelled outcome payload", () => {
    // Construct the ID through its schema instead of casting a bare string: the
    // brand is the decoded value the event payload accepts.
    const sessionID = SessionID.make("ses_1")
    const decoded = Schema.decodeUnknownSync(SessionV1.Event.Cancelled.data)({
      sessionID,
      mode: "cooperative",
      durationMs: 12,
      acknowledged: true,
    })
    expect(decoded).toEqual({
      sessionID,
      mode: "cooperative",
      durationMs: 12,
      acknowledged: true,
    })
    expect(SessionV1.Event.Cancelled.durable).toBeUndefined()
    const invalid = { sessionID: "ses_1", mode: "queued", durationMs: 0, acknowledged: false }
    expect(() => Schema.decodeUnknownSync(SessionV1.Event.Cancelled.data)(invalid as never)).toThrow()
  })

  test("owns the legacy transient public definitions", () => {
    expect([
      SessionV1.PartDelta.type,
      SessionV1.Diff.type,
      SessionV1.Error.type,
      PermissionV1.Event.Asked.type,
      PermissionV1.Event.Replied.type,
      QuestionV1.Event.Asked.type,
      QuestionV1.Event.Replied.type,
      QuestionV1.Event.Rejected.type,
      Project.Event.Updated.type,
      LegacyEvent.CommandExecuted.type,
    ]).toEqual([
      "message.part.delta",
      "session.diff",
      "session.error",
      "permission.asked",
      "permission.replied",
      "question.asked",
      "question.replied",
      "question.rejected",
      "project.updated",
      "command.executed",
    ])
  })
})
