// WS6 — worker threads are born with their spawn name.
//
// Bun names a worker thread's OS `comm` from `WorkerOptions.name` at creation.
// Our workers self-name later (from inside the thread), which leaves a ~0.3s
// window where they still show as Bun's default `Worker` — and a worker spawned
// without a slot would stay `Worker` forever. This test proves the spawn `name`
// reaches the kernel WITHOUT any self-naming: the fixture never calls
// `setThreadName`, it only posts `ready` and idles.
//
// Linux-only; skips loudly when `/proc/self/task` is absent (same pattern as
// `thread-naming-static.test.ts`).

import { describe, expect, test } from "bun:test"
import { existsSync, readdirSync, readFileSync } from "node:fs"
import { createBunExecutorTransport } from "@/session/worker/hub"
import { pollUntil } from "../../lib/poll"

const TASK_DIR = "/proc/self/task"
const procAvailable = existsSync(TASK_DIR)

/** A worker that never self-names: it posts `ready` and idles. */
const FIXTURE_ENTRY = new URL("../../fixture/no-name-worker.ts", import.meta.url)

/** A name short enough to survive the kernel's 15-byte `comm` limit. */
const BIRTH_NAME = "ocx-birth"

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

describe("WS6 — worker threads are born with their spawn name", () => {
  test.skipIf(!procAvailable)(
    `createBunExecutorTransport sets WorkerOptions.name as the thread comm (${
      procAvailable ? "linux /proc" : "no /proc/self/task"
    })`,
    async () => {
      const transport = createBunExecutorTransport({ entry: FIXTURE_ENTRY, name: BIRTH_NAME })
      try {
        // The fixture reports `ready` without ever naming itself, so a thread
        // named `BIRTH_NAME` can only have come from the spawn option.
        await transport.waitFor((message) => message.type === "ready")
        await pollUntil(() => threadNames().includes(BIRTH_NAME), {
          timeoutMs: 3000,
          message: `no thread named "${BIRTH_NAME}" appeared in ${TASK_DIR}; saw ${JSON.stringify(threadNames())}`,
        })
        expect(threadNames()).toContain(BIRTH_NAME)
      } finally {
        await transport.terminate()
      }
    },
  )
})
