// WS4 — static thread names (writer `ocw`, broker `ocb`, hub `och`).
//
// The names are Linux-observable only: each isolate writes its own
// `/proc/thread-self/comm` from inside the thread. This test boots the real
// writer and broker isolates plus a production hub, then reads the process's
// per-thread `comm` files and asserts each static name appears.
//
// `/proc/self/task` is the only probe used: workers are threads of THIS process,
// so no OS-tid bookkeeping is needed. (The broker's `ready.threadId` is a logical
// `node:worker_threads` id, not an OS tid, and must never index `/proc`.)
//
// The reader needs Linux `/proc`; when it is absent the test skips loudly with
// the reason in its name (same pattern as `parallel-sessions.test.ts`).

import { describe, expect, test } from "bun:test"
import { existsSync, readdirSync, readFileSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { createBroker } from "@/session/broker/broker"
import { createWriterClient } from "@/session/writer/client"
import type { ExecutorSessionWorker, ExecutorWorkerKind, ExecutorWorkerPool } from "@/session/worker/execute"
import { createProductionSessionHub } from "@/session/worker/hub"
import { pollUntil } from "../../lib/poll"
import { sessionWorkerInit } from "../../lib/session-worker-init"
import { healthyEnv } from "../writer/fixtures"

const TASK_DIR = "/proc/self/task"
const procAvailable = existsSync(TASK_DIR)

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
    message: `no thread named "${name}" appeared in ${TASK_DIR}; saw ${JSON.stringify(threadNames())}`,
  })
}

/**
 * A pool that never spawns an isolate. The hub's `och` name is set synchronously
 * at construction on the calling thread, so the hub's worker pool is irrelevant
 * to this test — it must merely be constructible.
 */
function inertPool(count: number): ExecutorWorkerPool {
  const worker = (id: number): ExecutorSessionWorker => ({
    id,
    alive: true,
    execute: async () => {},
    terminate: async () => {},
  })
  return {
    workers: Array.from({ length: count }, (_, index) => worker(index + 1)),
    spawn: (_kind: ExecutorWorkerKind) => worker(count + 1),
    terminateAll: async () => {},
    byID: () => undefined,
    resultFor: () => undefined,
  }
}

describe("WS4 — static thread names", () => {
  test.skipIf(!procAvailable)(
    `names the writer (ocw), broker (ocb) and hub (och) threads (${
      procAvailable ? "linux /proc" : "no /proc/self/task"
    })`,
    async () => {
      const writer = createWriterClient({ env: healthyEnv })
      const broker = createBroker()
      try {
        await writer.open(join(tmpdir(), `oc-thread-naming-${process.pid}.db`))
        await broker.ready
        const hub = createProductionSessionHub({
          init: sessionWorkerInit(),
          count: 1,
          poolFactory: () => inertPool(1),
        })
        try {
          await waitForThreadName("ocw")
          await waitForThreadName("ocb")
          await waitForThreadName("och")
        } finally {
          await hub.terminateAll()
        }
      } finally {
        await broker.teardown().catch(() => undefined)
        await writer.close()
      }
    },
  )
})
