// Phase 13.1 session-worker fixture — mock-SSE workload over the Phase 5 executor.
//
// Installs the Phase 5 executor with a `"sse"` work kind: each session fetches a
// streamed reply from the hub's mock provider and burns CPU synchronously (the
// simulated per-delta assembly cost). Because the burn is synchronous, a busy
// session pins exactly this worker thread.
//
// The executor's `ready` frame reports the worker's OS thread id (from
// `/proc/thread-self`), so the hub can read this thread's CPU from
// `/proc/self/task/<tid>/stat` — `node:worker_threads.threadId` is a logical id,
// not the OS tid.
//
// A dedicated egress `MessagePort` is transferred to the hub so the leak detector
// has a port to observe; the hub closes its end at teardown.

import { Effect } from "effect"
import {
  installSessionExecutor,
  type SessionExecutionResult,
  type SessionWork,
  type SessionWorkFactory,
} from "@/session/worker/execute"
import { currentThreadId } from "../lib/loop-gap"
import { MOCK_SSE_WORK_KIND, burnCpu, consumeMockSse, readMockSseConfig } from "../lib/mock-sse-worker"

interface WorkerScope {
  onmessage: ((event: { data: unknown }) => void) | null
  postMessage(message: unknown, transfer?: readonly Transferable[]): void
}

const scope = globalThis as unknown as WorkerScope

const egressChannel = new MessageChannel()

const work: SessionWorkFactory = (kind) => {
  if (kind !== MOCK_SSE_WORK_KIND) throw new Error(`unknown work kind: ${kind}`)
  const run: SessionWork = (sessionID) =>
    Effect.gen(function* () {
      const config = readMockSseConfig(process.env)
      const parsed = yield* Effect.promise(() =>
        consumeMockSse(config, [{ role: "user", content: `session ${sessionID}` }]),
      )
      const hash = burnCpu(config.burnIterations, parsed.text.length)
      return {
        sessionID,
        value: { text: parsed.text, chunks: parsed.chunks, hash },
      } satisfies SessionExecutionResult
    })
  return run
}

installSessionExecutor(scope, { work, threadId: currentThreadId() ?? 0 })

scope.postMessage({ type: "ports", egress: egressChannel.port2 }, [egressChannel.port2])
