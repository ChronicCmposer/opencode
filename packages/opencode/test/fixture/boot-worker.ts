// Dependency-free Bun worker used by test/lib/boot-memory.test.ts (plan task 3.11).
//
// Reports a single readiness message so the hub can time the boot. Bun workers are
// threads inside one process, so `rssBytes` here is the process-wide RSS, not a
// per-thread figure; the harness records it as an observed sample.
//
// out: { type: "ready", workerId, rssBytes }

import { threadId } from "node:worker_threads"

interface WorkerScope {
  postMessage(message: unknown): void
}

const scope = globalThis as unknown as WorkerScope

scope.postMessage({
  type: "ready",
  workerId: threadId,
  rssBytes: process.memoryUsage().rss,
})
