// A3 — worker `Plugin.Service` dispatch for `experimental.chat.messages.transform`.
//
// The worker dispatch switch (`runWorkerPluginHook`) used to fall through to a
// pass-through `default`, silently dropping the messages transform (the DCP
// pruning plugin). These tests pin both halves of the fix: the injected
// hub-proxy reaches the host, and the switch actually routes the hook name to
// `host.messagesTransform` instead of swallowing it.

import { describe, expect, test } from "bun:test"
import { Effect, Layer } from "effect"
import { Plugin } from "@/plugin"
import { ModelClient, buildSessionWorkerLayer } from "@/session/worker/layer"
import { createWorkerPluginHost, type WorkerPluginHost } from "@/session/worker/plugin-host"
import { workerSnapshot } from "../../lib/snapshot"

/**
 * The hook name, pinned to the real `Plugin.TriggerName`. Tying the literal to
 * the `Hooks` key makes a rename break compilation here — a bare `as never` cast
 * would keep compiling while the dispatch switch silently no-oped.
 */
const MESSAGES_TRANSFORM = "experimental.chat.messages.transform" satisfies Plugin.TriggerName

const clients = {
  writer: { query: async () => [], publish: async () => ({ seq: 1, committed: true }) },
  egress: { emit: () => {} },
  backgroundJob: { wait: async () => null, cancel: async () => {} },
}

/** Build the worker graph with the given plugin host, then fire one trigger. */
async function triggerMessagesTransform(
  plugins: WorkerPluginHost,
  input: unknown,
  output: unknown,
): Promise<unknown> {
  const built = buildSessionWorkerLayer({
    snapshot: workerSnapshot(),
    env: { values: {} },
    clients,
    modelClient: Layer.succeed(ModelClient, { complete: () => Effect.succeed({ text: "stub" }) }),
    plugins,
  })

  return Effect.runPromise(
    Effect.gen(function* () {
      const plugin = yield* Plugin.Service
      return yield* plugin.trigger(MESSAGES_TRANSFORM, input, output)
    }).pipe(Effect.provide(built.layer)),
  )
}

describe("worker messages.transform dispatch", () => {
  test("the injected hub-proxy transform reaches the host through Plugin.Service", async () => {
    const calls: Array<{ input: unknown; output: unknown }> = []
    const host = createWorkerPluginHost([], {
      messagesTransform: async (input, output) => {
        calls.push({ input, output })
        return { messages: [...output.messages, { role: "user", content: "pruned" }] }
      },
    })

    const input = { sessionID: "s", model: { id: "m" } }
    const output = { messages: [{ role: "user", content: "base" }] }
    const result = (await triggerMessagesTransform(host, input, output)) as { messages: unknown[] }

    expect(result.messages).toEqual([
      { role: "user", content: "base" },
      { role: "user", content: "pruned" },
    ])
    expect(calls).toHaveLength(1)
    expect(calls[0]!.input).toEqual(input)
    expect(calls[0]!.output).toEqual(output)
  })

  test("the dispatch switch routes the hook name to host.messagesTransform (not default)", async () => {
    const calls: Array<{ input: unknown; output: unknown }> = []
    const host: WorkerPluginHost = {
      names: ["fake"],
      chatParams: async (_input, output) => output,
      chatHeaders: async (_input, output) => output,
      toolBefore: async (_input, output) => output,
      toolAfter: async (_input, output) => output,
      systemTransform: async (_input, output) => output,
      messagesTransform: async (input, output) => {
        calls.push({ input, output })
        return { messages: [...output.messages, { role: "assistant", content: "routed" }] }
      },
    }

    const input = { sessionID: "s" }
    const output = { messages: [{ role: "user", content: "base" }] }
    const result = (await triggerMessagesTransform(host, input, output)) as { messages: unknown[] }

    expect(calls).toHaveLength(1)
    expect(calls[0]!.input).toEqual(input)
    expect(calls[0]!.output).toBe(output)
    expect(result.messages).toEqual([
      { role: "user", content: "base" },
      { role: "assistant", content: "routed" },
    ])
  })
})
