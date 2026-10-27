// Transport fixture: a real Bun worker whose executor boot is deliberately
// delayed, so a hub can cancel it during materialization. It announces the
// start of its boot delay, waits, then installs the executor. Any work that
// actually runs writes a marker file, so a cancelled boot can prove no run
// executed.

import { writeFileSync } from "node:fs"
import { parentPort, workerData } from "node:worker_threads"
import { Effect } from "effect"
import { installSessionExecutor, type SessionWorkFactory } from "@/session/worker/execute"
import { threadId } from "node:worker_threads"
import { scope } from "./worker-scope"

interface BootGatedData {
  readonly bootDelayMs?: number
  readonly markerPath?: string
}

const data = (workerData ?? {}) as BootGatedData

parentPort?.postMessage({ type: "booting" })
await new Promise<void>((resolve) => setTimeout(resolve, data.bootDelayMs ?? 60_000))

const work: SessionWorkFactory = () => (sessionID) =>
  Effect.sync(() => {
    if (data.markerPath !== undefined) writeFileSync(data.markerPath, "executed")
    return { sessionID }
  })

installSessionExecutor(scope, { work, threadId })
