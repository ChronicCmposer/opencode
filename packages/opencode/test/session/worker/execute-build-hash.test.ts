// The ready handshake carries the real worker build hash.
//
// These tests spawn a REAL Bun worker isolate that installs the production
// session executor (`execute.ts`), so the ready frame under test is the one the
// worker actually posts. The hub-side check is the production
// `verifyReadyBuild`: a matching hash passes, a different hash is a typed
// `BuildMismatch`.

import { describe, expect, test } from "bun:test"
import {
  BUILD_HASH_OVERRIDE_ENV,
  BuildMismatch,
  buildHashStatus,
  resolveBuildHash,
  verifyReadyBuild,
} from "@/session/worker/build-hash"
import { createWorkerHarness } from "../../lib/worker-harness"

const EXECUTOR_FIXTURE = new URL("../../fixture/session-execute-worker.ts", import.meta.url)
const READY = { type: "ready" } as const

describe("ready-frame build hash over a real worker", () => {
  test("a worker whose hash matches the hub boots normally", async () => {
    await using harness = createWorkerHarness({ timeoutMs: 10_000 })
    const worker = harness.spawn<{ type: string; threadId: number; buildHash?: string }>(EXECUTOR_FIXTURE)
    const ready = await worker.waitFor("ready")
    const hubHash = resolveBuildHash({ env: {} }).hash
    expect(verifyReadyBuild(ready, hubHash)).toBe(hubHash)
    await worker.terminate()
  })

  test("a worker built with a different hash fails with a typed BuildMismatch", async () => {
    await using harness = createWorkerHarness({ timeoutMs: 10_000 })
    const worker = harness.spawn<{ type: string; threadId: number; buildHash?: string }>(EXECUTOR_FIXTURE, {
      env: { OPENCODE_DEV: "1", [BUILD_HASH_OVERRIDE_ENV]: "wrong-hash" },
    })
    const ready = await worker.waitFor("ready")
    const hubHash = resolveBuildHash({ env: {} }).hash
    expect(() => verifyReadyBuild(ready, hubHash)).toThrow(BuildMismatch)
    try {
      verifyReadyBuild(ready, hubHash)
    } catch (error) {
      expect((error as BuildMismatch).actual).toBe("wrong-hash")
    }
    await worker.terminate()
  })

  test("the status projection exposes the resolved hash and schema version", () => {
    const resolved = resolveBuildHash({ env: {}, definedHash: "deadbeef" })
    expect(buildHashStatus({ env: {}, definedHash: "deadbeef" })).toEqual({
      buildHash: "deadbeef",
      protocolSchemaVersion: resolved.schemaVersion,
      source: "build",
      override: false,
    })
  })
})
