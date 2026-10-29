// A real Bun worker used by the production lifecycle integration test. It
// crashes before `ready` ("boot") or immediately after announcing `ready`
// ("materialize"), so the hub must classify the phase from its own lifecycle
// truth rather than defaulting every crash to "stream".

import { parentPort, threadId, workerData } from "node:worker_threads"

const data = (workerData ?? {}) as { readonly phase?: string }

if (data.phase === "boot") {
  throw new Error("intentional boot-phase crash")
}

parentPort?.postMessage({ type: "ready", threadId })

if (data.phase === "materialize") {
  setTimeout(() => {
    throw new Error("intentional materialize-phase crash")
  }, 20)
}
