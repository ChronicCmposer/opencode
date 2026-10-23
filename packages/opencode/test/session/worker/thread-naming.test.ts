// WS5 — dynamic session-worker thread naming (slot lifecycle).
//
// The session worker advertises which session slot it serves through its OS
// thread name: idle/boot `ocs<slot>`, running `ocs<slot>:<last6 of sessionID>`,
// reset to `ocs<slot>` on every terminal (settle, cooperative cancel,
// interrupt). Naming is Linux-only and fail-soft; these tests read the kernel's
// per-thread `comm` file, so they skip loudly when `/proc` is unavailable.
//
// Two contracts:
//   * the PRODUCTION entry names its slot at boot (spawned via the shared
//     session-worker harness, with a `workerID`);
//   * the run lifecycle transitions the name and resets it on settle AND on a
//     cooperative cancel (driven through the dedicated thread-name fixture,
//     which mirrors the production executor wiring).

import { describe, expect, test } from "bun:test"
import { existsSync, readFileSync, readdirSync } from "node:fs"
import type { SessionID } from "@/session/schema"
import {
  createExecutorWorker,
  type ExecutorWorker,
  type SessionExecutorInbound,
  type SessionExecutorOutbound,
} from "@/session/worker/execute"
import { sessionThreadName } from "@/session/worker/thread-name"
import { pollUntil } from "../../lib/poll"
import { bootSessionWorker } from "../../lib/session-worker-harness"
import { sessionWorkerInit } from "../../lib/session-worker-init"
import { adaptTransport } from "../../lib/transport"
import { createWorkerHarness, type HarnessWorker, type WorkerHarness } from "../../lib/worker-harness"

const TASK_DIR = "/proc/self/task"
const procAvailable = existsSync("/proc/thread-self") || existsSync("/proc/self/task")

/** Distinct slots per test so a lingering thread cannot satisfy another test. */
const BOOT_SLOT = 9
const DYNAMIC_SLOT = 2
const CANCEL_SLOT = 3

const FIXTURE_ENTRY = new URL("../../fixture/thread-name-fixture.ts", import.meta.url)

/** The OS thread name of `tid`, or `undefined` if the thread is gone. */
function threadName(tid: number): string | undefined {
  try {
    return readFileSync(`${TASK_DIR}/${tid}/comm`, "utf8").trim()
  } catch {
    return undefined
  }
}

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

/** Resolve once a thread with `name` exists anywhere in the process. */
function waitForAnyThreadNamed(name: string): Promise<void> {
  return pollUntil(() => threadNames().includes(name), {
    timeoutMs: 3000,
    message: `no thread named "${name}" appeared in ${TASK_DIR}; saw ${JSON.stringify(threadNames())}`,
  })
}

/** Resolve once thread `tid`'s OS name is exactly `name`. */
function waitForThreadNamed(tid: number, name: string): Promise<void> {
  return pollUntil(() => threadName(tid) === name, {
    timeoutMs: 3000,
    message: `thread ${tid} never became "${name}"; saw "${threadName(tid)}"`,
  })
}

interface FixtureWorker {
  readonly worker: ExecutorWorker
  /** The fixture thread's real OS tid (from its `ready` frame). */
  readonly osTid: number
  terminate(): Promise<number>
}

/** Spawn the thread-name fixture on `harness` and read its real OS tid. */
async function bootFixture(harness: WorkerHarness, slot: number, id: number): Promise<FixtureWorker> {
  const handle: HarnessWorker<SessionExecutorOutbound, SessionExecutorInbound> = harness.spawn(FIXTURE_ENTRY, {
    workerData: { workerID: slot },
  })
  const ready = await handle.waitFor("ready", { timeoutMs: 15_000 })
  return {
    worker: createExecutorWorker({ id, transport: adaptTransport(handle) }),
    osTid: ready.threadId,
    terminate: () => handle.terminate(),
  }
}

describe("WS5 — dynamic session-worker thread naming", () => {
  test.skipIf(!procAvailable)("boots the production worker named ocs<slot>", async () => {
    const handle = await bootSessionWorker({ init: { ...sessionWorkerInit(), workerID: BOOT_SLOT } })
    try {
      await waitForAnyThreadNamed(sessionThreadName(BOOT_SLOT))
    } finally {
      await handle.terminate()
    }
  })

  test.skipIf(!procAvailable)(
    "transitions ocs<slot> -> ocs<slot>:<last6> during a run and resets on settle",
    async () => {
      const harness = createWorkerHarness({ timeoutMs: 15_000 })
      try {
        const fixture = await bootFixture(harness, DYNAMIC_SLOT, 1)
        try {
          expect(threadName(fixture.osTid)).toBe(sessionThreadName(DYNAMIC_SLOT))

          const sessionID = "ses_dynamic_settle_abcdef" as SessionID
          const running = sessionThreadName(DYNAMIC_SLOT, sessionID)
          const run = fixture.worker.execute({ sessionID, depth: 0 })

          await waitForThreadNamed(fixture.osTid, running)
          await run
          await waitForThreadNamed(fixture.osTid, sessionThreadName(DYNAMIC_SLOT))
        } finally {
          await fixture.terminate()
        }
      } finally {
        await harness.dispose()
      }
    },
  )

  test.skipIf(!procAvailable)("resets to ocs<slot> after a cooperative cancel", async () => {
    const harness = createWorkerHarness({ timeoutMs: 15_000 })
    try {
      const fixture = await bootFixture(harness, CANCEL_SLOT, 1)
      try {
        expect(threadName(fixture.osTid)).toBe(sessionThreadName(CANCEL_SLOT))

        const sessionID = "ses_dynamic_cancel_abcdef" as SessionID
        // The cancel settles the run with an interrupt, so `execute` rejects;
        // this test only cares that the thread name returns to idle.
        const run = fixture.worker.execute({ sessionID, depth: 0 }).catch(() => undefined)

        await waitForThreadNamed(fixture.osTid, sessionThreadName(CANCEL_SLOT, sessionID))
        await fixture.worker.cancel(sessionID)
        await run
        await waitForThreadNamed(fixture.osTid, sessionThreadName(CANCEL_SLOT))
      } finally {
        await fixture.terminate()
      }
    } finally {
      await harness.dispose()
    }
  })
})
