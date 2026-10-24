import { describe, expect, test } from "bun:test"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import type { SessionID } from "@/session/schema"
import { withParts } from "./session-parts"

const sessionID = "ses_fixture" as SessionID

describe("withParts", () => {
  test("decodes a minimal valid session payload", () => {
    const value = withParts(sessionID)

    expect(value.info.role).toBe("user")
    expect(value.info.sessionID).toBe(sessionID)
    expect(value.info.id).toBe(SessionV1.MessageID.make("msg_test"))
    expect(value.parts).toEqual([])
  })

  test("is stable across calls", () => {
    expect(withParts(sessionID)).toEqual(withParts(sessionID))
  })
})
