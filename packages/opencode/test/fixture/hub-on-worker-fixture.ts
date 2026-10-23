// Regression fixture — a production session hub built INSIDE a worker must not
// clobber that worker's own thread name.
//
// `createProductionSessionHub` names its constructing thread `och` for human
// observability, but a session-worker isolate also builds a nested hub on its own
// thread during bootstrap. Without a main-thread guard the nested hub overwrites
// the worker's `ocs<slot>` name with `och`, so `ps -T` collapses every worker
// into the same label as the main process.
//
// This fixture reproduces that exact sequence: name the thread `ocs9` (as the
// session worker does at boot), build the nested hub, then report the kernel's
// `/proc/thread-self/comm` back to the test. With the bug the report is `och`;
// with the guard it stays `ocs9`.

import { readFileSync } from "node:fs"
import type { ExecutorSessionWorker, ExecutorWorkerKind, ExecutorWorkerPool } from "@/session/worker/execute"
import { createProductionSessionHub } from "@/session/worker/hub"
import { setThreadName } from "@/util/thread-name"
import { sessionWorkerInit } from "../lib/session-worker-init"

interface WorkerScope {
  postMessage(message: unknown): void
}

const scope = globalThis as unknown as WorkerScope

/**
 * A pool that never spawns an isolate. The hub's thread name is set synchronously
 * at construction, so the pool only needs to be constructible — mirroring the
 * inert pool in `thread-naming-static.test.ts`.
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

// Boot name, mirroring `session-worker.ts`'s `ocs<slot>` self-naming.
setThreadName("ocs9")

createProductionSessionHub({
  init: sessionWorkerInit(),
  count: 1,
  poolFactory: () => inertPool(1),
})

// `/proc/thread-self` names the CALLING thread (this worker), unlike `/proc/self`.
const comm = readFileSync("/proc/thread-self/comm", "utf8").trim()
scope.postMessage({ type: "comm", comm })
