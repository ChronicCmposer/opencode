// The V1 error contracts must have a single source: `@opencode-ai/schema` owns
// the field shapes and the core runtime facade derives its callable error
// classes from them. These tests pin both the derivation and wire compatibility.

import { describe, expect, test } from "bun:test"
import { Schema } from "effect"
import { SessionV1 as CoreSessionV1 } from "@opencode-ai/core/v1/session"
import { SessionV1 as SchemaSessionV1 } from "@opencode-ai/schema/v1/session"

const API_ERROR = {
  name: "APIError",
  data: { message: "boom", isRetryable: false },
} as const

describe("V1 error contracts", () => {
  test("core error classes derive from the canonical schema module", async () => {
    const source = await Bun.file(new URL("../src/v1/session.ts", import.meta.url)).text()

    expect(source).toContain('from "@opencode-ai/schema/session-v1"')
    // No restated field literal: the class is built from `SessionV1Schema.<Error>.data`.
    expect(source).not.toMatch(/NamedError\.create\(\s*"APIError",\s*\{/)
    expect(source).toContain("SessionV1Schema.APIError.data")
  })

  test("core and schema error schemas agree on the encoded wire shape", () => {
    const encoded = Schema.encodeUnknownSync(SchemaSessionV1.APIError.Schema)(API_ERROR)
    const decoded = Schema.decodeUnknownSync(CoreSessionV1.APIError.Schema)(encoded)

    expect(decoded).toEqual(API_ERROR)
  })

  test("core error class remains constructible and identifiable", () => {
    const error = new CoreSessionV1.APIError({ message: "boom", isRetryable: true })

    expect(error.name).toBe("APIError")
    expect(error.data).toEqual({ message: "boom", isRetryable: true })
    expect(CoreSessionV1.APIError.isInstance(error)).toBe(true)
    expect(CoreSessionV1.APIError.isInstance(new Error("nope"))).toBe(false)
  })

  test("every V1 error uses the schema-owned field contract", () => {
    const names = [
      "OutputLengthError",
      "AuthError",
      "AbortedError",
      "StructuredOutputError",
      "APIError",
      "ContextOverflowError",
      "ContentFilterError",
      "IncompleteStreamError",
      "MaxStepsExceededError",
    ] as const

    for (const name of names) {
      expect(CoreSessionV1[name].Schema).toBeDefined()
      expect(SchemaSessionV1[name].data).toBeDefined()
    }
  })
})
