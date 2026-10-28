// Event-loop-gap probe worker fixture.
//
// Runs the probe off the main thread so tests can prove the harness collects a
// loop-gap sample on a worker thread and that the POSIX `/proc/thread-self`
// reader resolves the worker's own OS thread id.
//
// out: { type: "sample", sample }

import { measureLoopGap } from "../lib/loop-gap"
import { postToParent } from "./worker-scope"

measureLoopGap({ windowMs: 40, intervalMs: 10 }).then(
  (sample) => postToParent({ type: "sample", sample }),
  (error) => postToParent({ type: "error", message: error instanceof Error ? error.message : String(error) }),
)
