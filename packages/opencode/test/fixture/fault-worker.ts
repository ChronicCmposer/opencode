// Dependency-free-ish worker fixture for the fault-injection seams (plan 3.7).
//
// Proves the `OPENCODE_TEST_FAULTS` gate crosses the isolate boundary and that
// `crashWorker` is point-scoped inside a real Bun worker thread.
//
// Protocol (JSON messages over the worker channel):
//   in : { type: "crash", point } -> out: { type: "crashing", point } then exit(70)
//                                    or   { type: "survived", point } when not armed
// On startup: { type: "ready" }

import { Faults } from "../../src/testing/faults"
import { onMessage, postToParent } from "./worker-scope"

interface Incoming {
  type?: string
  point?: string
}

onMessage<Incoming>((raw) => {
  const input: Incoming = raw ?? {}
  if (input.type !== "crash") return

  const point = input.point ?? ""
  Faults.crashWorker(point, {
    onCrash: (directive) => {
      postToParent({ type: "crashing", point: directive.point })
      process.exit(Faults.WORKER_CRASH_EXIT_CODE)
    },
  })
  postToParent({ type: "survived", point })
})

postToParent({ type: "ready" })
