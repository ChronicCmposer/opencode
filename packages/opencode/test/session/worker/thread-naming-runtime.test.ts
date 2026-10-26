// Slice N — injectable session-worker thread-naming seam.
//
// `createThreadNamer` turns the pure grammar into the worker lifecycle:
//
//   onBind(sessionID)     -> idle    `ocs:<last6>`
//   onRunStart(sessionID) -> running `ocs:<last6>!`
//   onIdle()              -> idle form of the bound session (every terminal:
//                            settle, cooperative cancel, interrupt)
//   onDispose()           -> drops the binding; later calls never throw
//
// The seam owns no OS state: consumers inject `setName` (production:
// `setThreadName` from `@/util/thread-name`), so the lifecycle is testable
// without `/proc`.

import { describe, expect, test } from "bun:test"
import { createThreadNamer } from "@/session/worker/thread-naming"

const FIRST = "ses_0123456789ABCDEFabcdef3f9a2c"
const SECOND = "ses_second_worker_987654"

/** A namer whose writes are recorded instead of hitting `/proc`. */
function recordingNamer() {
  const names: string[] = []
  const namer = createThreadNamer({ setName: (name) => names.push(name) })
  return { names, namer }
}

describe("session.worker.thread-naming", () => {
  test("toggles idle -> running -> idle across the full run lifecycle", () => {
    const { names, namer } = recordingNamer()

    namer.onBind(FIRST)
    namer.onRunStart(FIRST)
    namer.onIdle() // settle
    namer.onRunStart(FIRST)
    namer.onIdle() // cooperative cancel
    namer.onRunStart(FIRST)
    namer.onIdle() // interrupt

    expect(names).toEqual([
      "ocs:3f9a2c",
      "ocs:3f9a2c!",
      "ocs:3f9a2c",
      "ocs:3f9a2c!",
      "ocs:3f9a2c",
      "ocs:3f9a2c!",
      "ocs:3f9a2c",
    ])
  })

  test("rebinding and running a different session never leaves a stale idle name", () => {
    const { names, namer } = recordingNamer()

    namer.onBind(FIRST)
    namer.onBind(SECOND)
    namer.onRunStart(SECOND)
    namer.onIdle()
    namer.onRunStart(FIRST)
    namer.onIdle()

    expect(names).toEqual([
      "ocs:3f9a2c",
      "ocs:987654",
      "ocs:987654!",
      "ocs:987654",
      "ocs:3f9a2c!",
      "ocs:3f9a2c",
    ])
  })

  test("idle before bind is a no-op", () => {
    const { names, namer } = recordingNamer()

    expect(() => namer.onIdle()).not.toThrow()
    expect(names).toEqual([])
  })

  test("dispose drops the binding without throwing or leaving a stale write", () => {
    const { names, namer } = recordingNamer()

    namer.onBind(FIRST)
    namer.onRunStart(FIRST)

    expect(() => namer.onDispose()).not.toThrow()
    expect(() => namer.onDispose()).not.toThrow()
    expect(() => namer.onIdle()).not.toThrow()

    expect(names).toEqual(["ocs:3f9a2c", "ocs:3f9a2c!"])
  })
})
