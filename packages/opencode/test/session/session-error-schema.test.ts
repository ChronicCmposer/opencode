import { describe, expect, test } from "bun:test"
import { Schema } from "effect"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { SessionV1 as SessionSchemaV1 } from "@opencode-ai/schema/session-v1"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { MessageV2 } from "../../src/session/message-v2"

const incompleteStream = {
  name: "IncompleteStreamError",
  data: { message: "provider stream ended without a finish event" },
} as const
const maxStepsExceeded = {
  name: "MaxStepsExceededError",
  data: { message: "hard step cap reached", steps: 12 },
} as const

describe("session error schema", () => {
  test("IncompleteStreamError is a member of the session error union", () => {
    const error = new SessionV1.IncompleteStreamError({ message: incompleteStream.data.message })

    expect(error.name).toBe("IncompleteStreamError")
    expect(error.toObject()).toEqual(incompleteStream)
    expect(Schema.is(SessionV1.Assistant.fields.error)(error.toObject())).toBe(true)
  })

  test("MaxStepsExceededError is a member of the session error union", () => {
    const error = new SessionV1.MaxStepsExceededError({ message: maxStepsExceeded.data.message, steps: 12 })

    expect(error.name).toBe("MaxStepsExceededError")
    expect(error.toObject()).toEqual(maxStepsExceeded)
    expect(Schema.is(SessionV1.Assistant.fields.error)(error.toObject())).toBe(true)
  })

  test("both errors serialize and deserialize through the session error union", () => {
    const union = SessionV1.Assistant.fields.error
    const cases = [
      new SessionV1.IncompleteStreamError({ message: incompleteStream.data.message }),
      new SessionV1.MaxStepsExceededError({ message: maxStepsExceeded.data.message, steps: 12 }),
    ]

    for (const error of cases) {
      const encoded = Schema.encodeUnknownSync(union)(error.toObject())
      const decoded = Schema.decodeUnknownSync(union)(encoded)
      expect(decoded).toEqual(error.toObject())
    }
  })

  test("MessageV2.fromError preserves the typed IncompleteStreamError shape", () => {
    const error = new SessionV1.IncompleteStreamError({ message: "x" })
    const mapped = MessageV2.fromError(error, { providerID: ProviderV2.ID.make("test") })

    expect(SessionV1.IncompleteStreamError.isInstance(mapped)).toBe(true)
    if (!SessionV1.IncompleteStreamError.isInstance(mapped)) throw new Error("expected IncompleteStreamError")
    expect(mapped.name).toBe("IncompleteStreamError")
    expect(mapped.data).toEqual({ message: "x" })
    expect(Schema.is(SessionV1.Assistant.fields.error)(mapped.toObject())).toBe(true)
  })

  test("session.error event accepts both new errors", () => {
    expect(SessionV1.Event.Error === SessionSchemaV1.Error).toBe(true)

    for (const data of [incompleteStream, maxStepsExceeded]) {
      const event = { id: "evt_test", type: "session.error", data: { error: data } }
      expect(Schema.is(SessionSchemaV1.Error)(event)).toBe(true)
    }
  })
})
