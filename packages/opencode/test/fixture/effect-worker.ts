// Effect/alias probe launcher; the probe body lives in `probe-worker.ts`.
import { runProbe } from "./probe-worker"

export type {
  EffectWorkerComputed,
  EffectWorkerFailed,
  EffectWorkerInbound,
  EffectWorkerOutbound,
  EffectWorkerReady,
} from "./probe-worker"

await runProbe("effect")
