// Spawn-name probe launcher; the probe body lives in `probe-worker.ts`.
import { runProbe } from "./probe-worker"

await runProbe("no-name")
