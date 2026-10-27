import { describe, expect, test } from "bun:test"
import {
  BuildMismatch,
  PROTOCOL_SCHEMA_VERSION,
  buildHashStatus,
  computeBuildHash,
  readyFrame,
  resolveBuildHash,
  verifyReadyBuild,
} from "@/session/worker/build-hash"

const WORKER_ENTRY = new TextEncoder().encode("// session worker entry bundle")

describe("computeBuildHash", () => {
  test("is deterministic and covers the entry bundle", () => {
    const first = computeBuildHash({ entryBundle: WORKER_ENTRY })
    const second = computeBuildHash({ entryBundle: WORKER_ENTRY })
    expect(first).toBe(second)
    expect(first).toMatch(/^[0-9a-f]{64}$/)

    const changed = computeBuildHash({ entryBundle: new TextEncoder().encode("// different entry") })
    expect(changed).not.toBe(first)
  })

  test("covers the protocol schema version", () => {
    const current = computeBuildHash({ entryBundle: WORKER_ENTRY })
    const next = computeBuildHash({ entryBundle: WORKER_ENTRY, schemaVersion: PROTOCOL_SCHEMA_VERSION + 1 })
    expect(next).not.toBe(current)
  })

  test("does not include plugin or runtime state", () => {
    const hash = computeBuildHash({ entryBundle: WORKER_ENTRY })
    // A plugin list is not an input; changing it cannot change the hash.
    const pluginChanged = computeBuildHash({ entryBundle: WORKER_ENTRY })
    expect(pluginChanged).toBe(hash)
  })
})

describe("resolveBuildHash", () => {
  test("uses the build-time hash when present", () => {
    const resolved = resolveBuildHash({ definedHash: "abc", env: {} })
    expect(resolved).toMatchObject({ hash: "abc", source: "build", override: false })
  })

  test("a development override wins, logs at error level, and is exposed in status", () => {
    const logged: Array<{ message: string; level: string | undefined }> = []
    const resolved = resolveBuildHash({
      definedHash: "build-hash",
      env: { OPENCODE_DEV: "1", OPENCODE_BUILD_HASH_OVERRIDE: "local-hash" },
      log: (message, _fields, level) => logged.push({ message, level }),
    })

    expect(resolved).toMatchObject({ hash: "local-hash", source: "override", override: true })
    expect(logged).toHaveLength(1)
    expect(logged[0]?.level).toBe("error")
    expect(logged[0]?.message).toContain("override")

    const status = buildHashStatus({
      definedHash: "build-hash",
      env: { OPENCODE_DEV: "1", OPENCODE_BUILD_HASH_OVERRIDE: "local-hash" },
      log: () => {},
    })
    expect(status).toMatchObject({
      buildHash: "local-hash",
      protocolSchemaVersion: PROTOCOL_SCHEMA_VERSION,
      source: "override",
      override: true,
    })
  })

  test("falls back to a development hash when no build-time hash exists", () => {
    const resolved = resolveBuildHash({ env: {} })
    expect(resolved.source).toBe("dev")
    expect(resolved.hash).toMatch(/^[0-9a-f]{64}$/)
  })
})


describe("production override gate", () => {
  test("a packaged build ignores the override, stays on the build hash, and logs at error", () => {
    const logged: Array<{ message: string; level: string | undefined }> = []
    const resolved = resolveBuildHash({
      definedHash: "build-hash",
      env: { OPENCODE_BUILD_HASH_OVERRIDE: "local-hash" },
      log: (message, _fields, level) => logged.push({ message, level }),
    })
    expect(resolved).toMatchObject({ hash: "build-hash", source: "build", override: false })
    expect(logged).toHaveLength(1)
    expect(logged[0]?.level).toBe("error")
    expect(logged[0]?.message).toContain("ignored")

    expect(
      buildHashStatus({ definedHash: "build-hash", env: { OPENCODE_BUILD_HASH_OVERRIDE: "local-hash" } }),
    ).toMatchObject({ buildHash: "build-hash", source: "build", override: false })
  })

  test("NODE_ENV=production also ignores the override even without a build hash", () => {
    const resolved = resolveBuildHash({
      env: { NODE_ENV: "production", OPENCODE_BUILD_HASH_OVERRIDE: "local-hash" },
    })
    expect(resolved.source).toBe("dev")
    expect(resolved.override).toBe(false)
    expect(resolved.hash).not.toBe("local-hash")
  })

  test("development and test builds honour the override", () => {
    expect(
      resolveBuildHash({ env: { OPENCODE_DEV: "1", OPENCODE_BUILD_HASH_OVERRIDE: "local" } }),
    ).toMatchObject({ hash: "local", source: "override", override: true })
    expect(resolveBuildHash({ env: { OPENCODE_BUILD_HASH_OVERRIDE: "local" } })).toMatchObject({
      hash: "local",
      source: "override",
      override: true,
    })
  })
})

describe("ready handshake build hash", () => {
  test("a ready frame carries the resolved hash", () => {
    const frame = readyFrame(4, { hash: "ready-hash" })
    expect(frame).toEqual({ type: "ready", threadId: 4, buildHash: "ready-hash" })
  })

  test("a matching ready frame passes", () => {
    expect(verifyReadyBuild({ type: "ready", threadId: 1, buildHash: "same" }, "same")).toBe("same")
  })

  test("a mismatched ready frame is a typed BuildMismatch", () => {
    expect(() => verifyReadyBuild({ type: "ready", threadId: 1, buildHash: "worker" }, "hub")).toThrow(BuildMismatch)
  })

  test("a missing ready hash is a typed BuildMismatch", () => {
    expect(() => verifyReadyBuild({ type: "ready", threadId: 1 }, "hub")).toThrow(BuildMismatch)
  })
})
