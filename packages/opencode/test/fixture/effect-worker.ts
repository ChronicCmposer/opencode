// Phase 3.13 Bun worker entry point (plan task 3.13).
//
// Bootstrap proof: a Worker isolate can import `effect`, run a trivial Effect
// program that returns a value, and resolve the repo `@/` alias to real `src`
// modules. Protocol (JSON messages over the worker channel):
//   startup                              -> out: { type: "ready", threadId }
//   in : { type: "compute", value }      -> out: { type: "computed", value, alias, marker }
//   in : { type: "fail", message }       -> out: { type: "failed", message }
//   anything else                        -> out: { type: "failed", message }

import { threadId } from "node:worker_threads"
import { Effect } from "effect"
import { decodeDataUrl } from "@/util/data-url"
import { iife } from "@/util/iife"
import { onMessage, postToParent } from "./worker-scope"

export interface EffectWorkerReady {
  readonly type: "ready"
  readonly threadId: number
}

export interface EffectWorkerComputed {
  readonly type: "computed"
  readonly value: number
  readonly alias: string
  readonly marker: string
}

export interface EffectWorkerFailed {
  readonly type: "failed"
  readonly message: string
}

export type EffectWorkerOutbound = EffectWorkerReady | EffectWorkerComputed | EffectWorkerFailed

export type EffectWorkerInbound =
  | { readonly type: "compute"; readonly value: number }
  | { readonly type: "fail"; readonly message: string }

interface ComputeResult {
  readonly value: number
  readonly alias: string
  readonly marker: string
}

const ALIAS_SENTINEL = "alias-resolved"
const ALIAS_DATA_URL = `data:text/plain;base64,${Buffer.from(ALIAS_SENTINEL).toString("base64")}`

// A trivial Effect program: doubles the input across an async boundary, then
// reads values from two modules resolved through the `@/` alias.
const compute = (value: number) =>
  Effect.gen(function* () {
    const doubled = yield* Effect.promise(async () => value * 2)
    const alias = yield* Effect.sync(() => decodeDataUrl(ALIAS_DATA_URL))
    const marker = yield* Effect.sync(() => iife(() => "iife-ok"))
    return { value: doubled, alias, marker } satisfies ComputeResult
  })

const runCompute = (value: number): Promise<EffectWorkerOutbound> =>
  compute(value).pipe(
    Effect.map((result) => ({ type: "computed", ...result }) as const),
    Effect.runPromise,
  )

const runFail = (message: string): Promise<EffectWorkerOutbound> =>
  Effect.fail(message).pipe(
    Effect.match({
      onFailure: (reason) => ({ type: "failed", message: reason }) as const,
      onSuccess: () => ({ type: "failed", message: "unexpected success" }) as const,
    }),
    Effect.runPromise,
  )

onMessage<Partial<EffectWorkerInbound>>((raw) => {
  const message: Partial<EffectWorkerInbound> = raw ?? {}

  if (message.type === "compute") {
    if (typeof message.value !== "number") {
      postToParent({ type: "failed", message: `compute requires a numeric value, received ${typeof message.value}` })
      return
    }
    void runCompute(message.value).then((out) => postToParent(out))
    return
  }

  if (message.type === "fail") {
    void runFail(message.message ?? "worker failure").then((out) => postToParent(out))
    return
  }

  postToParent({ type: "failed", message: `unknown message: ${String(message.type)}` })
})

postToParent({ type: "ready", threadId })
