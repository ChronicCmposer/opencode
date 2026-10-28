// `messagesTransformAuthority` — the context→plugin-resolution half of the hub's
// `messagesTransform` authority.
//
// `buildHubProxyHandlers` serves the wire; `messagesTransformAuthority` decides
// whether a plugin is reachable in the worker's session context at all. Both of
// its branches are load-bearing:
//   * no plugin (`Option.none`) — the worker's payload passes through BY IDENTITY
//     and the authority runner is never invoked;
//   * a plugin is present — its `experimental.chat.messages.transform` hook runs
//     under the runner and its resolved value is returned.
//
// Pinned here because the production `messagesTransformFor` was previously an
// unexported closure whose fallback and trigger call could be deleted silently.

import { describe, expect, test } from "bun:test"
import { Effect, Option } from "effect"
import { messagesTransformAuthority, type HubProxyRunner } from "@/session/worker/hub"
import type { Plugin } from "@/plugin"
import type { MessagesTransformInput, MessagesTransformOutput } from "@/session/worker/plugin-host"

const input: MessagesTransformInput = { sessionID: "ses_authority" }

interface TriggerCall {
  readonly name: string
  readonly input: MessagesTransformInput
  readonly output: MessagesTransformOutput
}

/** A `Plugin.Interface` double that records trigger calls and returns `returned`. */
function fakePlugin(returned: MessagesTransformOutput): { plugin: Plugin.Interface; calls: TriggerCall[] } {
  const calls: TriggerCall[] = []
  // `Plugin.Interface` is Effect-shaped; only `trigger` is exercised here.
  const plugin = {
    trigger: (name: string, i: MessagesTransformInput, output: MessagesTransformOutput) => {
      calls.push({ name, input: i, output })
      return Effect.succeed(returned)
    },
    list: () => Effect.succeed([]),
    init: () => Effect.void,
  } as unknown as Plugin.Interface
  return { plugin, calls }
}

/** A runner that counts invocations and runs the effect for real. */
function countingRunner(): { run: HubProxyRunner; calls: () => number } {
  let count = 0
  return {
    run: (effect) => {
      count++
      return Effect.runPromise(effect)
    },
    calls: () => count,
  }
}

describe("messagesTransformAuthority", () => {
  test("no plugin: passes the input output through by identity and never runs the authority", async () => {
    const { run, calls } = countingRunner()
    const authority = messagesTransformAuthority({ plugin: () => Option.none(), run })
    const output: MessagesTransformOutput = { messages: ["base"] }

    const returned = await authority(input, output)

    expect(returned).toBe(output)
    expect(calls()).toBe(0)
  })

  test("plugin present: triggers messages.transform under the runner and returns its value", async () => {
    const mutated: MessagesTransformOutput = { messages: ["base", "mutated"] }
    const { plugin, calls: triggerCalls } = fakePlugin(mutated)
    const { run, calls } = countingRunner()
    const authority = messagesTransformAuthority({ plugin: () => Option.some(plugin), run })
    const output: MessagesTransformOutput = { messages: ["base"] }

    const returned = await authority(input, output)

    expect(returned).toBe(mutated)
    expect(calls()).toBe(1)
    expect(triggerCalls).toEqual([{ name: "experimental.chat.messages.transform", input, output }])
  })
})
