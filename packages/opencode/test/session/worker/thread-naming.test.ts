// Session-worker thread naming lifecycle (slice N).
//
// A worker advertises the session it serves through its OS thread name:
//
//   idle    `ocs:<last 6 of sessionID>`
//   running `ocs:<last 6 of sessionID>!`
//
// `createThreadNamer` owns the lifecycle over the pure grammar: boot/bind
// advertises idle, a run start advertises running, and every terminal (settle,
// cooperative cancel, interrupt) advertises idle again. The lifecycle tests
// inject a recording `setName`, so they are deterministic and never read `/proc`.
//
// The boot test spawns the real production entry with a bound `sessionID` and
// waits on its published `ready` frame plus the resulting thread name. Linux
// `/proc` is the only probe, so the test skips loudly elsewhere.

import { describe, expect, test } from "bun:test"
import { existsSync, readdirSync, readFileSync } from "node:fs"
import { sessionThreadName } from "@/session/worker/thread-name"
import { createThreadNamer } from "@/session/worker/thread-naming"
import { bootstrapSessionWorker } from "@/session/worker/session-worker"
import { pollUntil } from "../../lib/poll"
import { bootSessionWorker } from "../../lib/session-worker-harness"
import { sessionWorkerInit } from "../../lib/session-worker-init"

const TASK_DIR = "/proc/self/task"
const procAvailable = existsSync(TASK_DIR)

/** Unique last-6 suffix, so a lingering thread cannot satisfy another test. */
const SESSION_ID = "ses_boot_worker_a2c0de"
const IDLE_NAME = "ocs:a2c0de"

/** Every thread name currently visible for this process. */
function threadNames(): string[] {
  const names: string[] = []
  for (const tid of readdirSync(TASK_DIR)) {
    try {
      names.push(readFileSync(`${TASK_DIR}/${tid}/comm`, "utf8").trim())
    } catch {
      // A thread can exit between listing and reading; skip it.
    }
  }
  return names
}

/** Resolve once a thread with `name` exists, else fail loudly with the names seen. */
function waitForThreadName(name: string): Promise<void> {
  return pollUntil(() => threadNames().includes(name), {
    timeoutMs: 5000,
    message: `no thread named "${name}" appeared in ${TASK_DIR}; saw ${JSON.stringify(threadNames())}`,
  })
}

/** A namer whose writes are recorded instead of hitting the OS thread name. */
function recordingNamer() {
  const names: string[] = []
  const namer = createThreadNamer({ setName: (name) => names.push(name) })
  return { names, namer }
}

describe("session.worker.thread-naming lifecycle", () => {
  test("bind -> idle, run -> running, every terminal -> idle", () => {
    const { names, namer } = recordingNamer()

    namer.bind(SESSION_ID)
    namer.bind(SESSION_ID, true)
    namer.onIdle() // settle
    namer.bind(SESSION_ID, true)
    namer.onIdle() // cooperative cancel
    namer.bind(SESSION_ID, true)
    namer.onIdle() // interrupt

    expect(names).toEqual([
      "ocs:a2c0de",
      "ocs:a2c0de!",
      "ocs:a2c0de",
      "ocs:a2c0de!",
      "ocs:a2c0de",
      "ocs:a2c0de!",
      "ocs:a2c0de",
    ])
  })

  test("a run start binds the run's session even without an explicit boot bind", () => {
    const { names, namer } = recordingNamer()

    namer.bind(SESSION_ID, true)
    namer.onIdle()

    expect(names).toEqual(["ocs:a2c0de!", "ocs:a2c0de"])
  })

  test("rebinding follows the latest session, so idle never restores a stale one", () => {
    const { names, namer } = recordingNamer()
    const next = "ses_second_worker_987654"

    namer.bind(SESSION_ID)
    namer.bind(next, true)
    namer.onIdle()

    expect(names).toEqual(["ocs:a2c0de", "ocs:987654!", "ocs:987654"])
  })

  test("bind(id, running) is the single lifecycle entry point", () => {
    const { names, namer } = recordingNamer()

    namer.bind(SESSION_ID)
    namer.bind(SESSION_ID, true)
    namer.onIdle()

    expect(names).toEqual(["ocs:a2c0de", "ocs:a2c0de!", "ocs:a2c0de"])
  })

  test("idle before any bind is a no-op", () => {
    const { names, namer } = recordingNamer()

    expect(() => namer.onIdle()).not.toThrow()
    expect(names).toEqual([])
  })

  test("dispose drops the binding without throwing or writing a stale name", () => {
    const { names, namer } = recordingNamer()

    namer.bind(SESSION_ID)
    namer.bind(SESSION_ID, true)

    expect(() => namer.onDispose()).not.toThrow()
    expect(() => namer.onDispose()).not.toThrow()
    expect(() => namer.onIdle()).not.toThrow()

    expect(names).toEqual(["ocs:a2c0de", "ocs:a2c0de!"])
  })

  test.skipIf(!procAvailable)("boots the production worker with the bound session's idle name", async () => {
    const handle = await bootSessionWorker({
      init: { ...sessionWorkerInit(), sessionID: SESSION_ID },
    })
    try {
      await waitForThreadName(IDLE_NAME)
      expect(sessionThreadName(SESSION_ID)).toBe(IDLE_NAME)
    } finally {
      await handle.terminate()
    }
  })

  test("bootstrap writes the boot name exactly once through bind", () => {
    const { names, namer } = recordingNamer()
    const scope = { onmessage: null, postMessage: () => {} }

    const handle = bootstrapSessionWorker(
      { ...sessionWorkerInit(), sessionID: SESSION_ID },
      scope,
      { threadNamer: namer },
    )
    handle.dispose()

    expect(names).toEqual(["ocs:a2c0de"])
  })
})
