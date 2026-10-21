// Event-loop-gap probe worker fixture (plan task 3.14).
//
// Runs the probe off the main thread so tests can prove the harness collects a
// loop-gap sample on a worker thread and that the POSIX `/proc/thread-self`
// reader resolves the worker's own OS thread id.
//
// out: { type: "sample", sample }

import { measureLoopGap } from "../lib/loop-gap"

interface WorkerScope {
  postMessage(message: unknown): void
}

const scope = globalThis as unknown as WorkerScope

measureLoopGap({ windowMs: 40, intervalMs: 10 }).then((sample) => {
  scope.postMessage({ type: "sample", sample })
})
