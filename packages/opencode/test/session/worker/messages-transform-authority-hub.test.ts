// C2 (finding M2, integration half) — the per-worker `messagesTransform`
// authority resolves the plugin from a REAL session context and fires its hook
// across a REAL control port.
//
// B4's unit suite pins `messagesTransformAuthority`'s branches with a hand-fed
// `Option`. This suite closes the remaining gap: it builds the authority exactly
// as `hubProxyFor` does (read `Plugin.Service` from the captured session context,
// run the hook under that context), routes it through the production
// `buildHubProxyHandlers`, serves it on a real `MessageChannel` via
// `serveHubProxy`, and drives it with `createHubProxyClient`.
//
// Covered:
//   * context carries `Plugin.Service` → the hook fires, resolves the very plugin
//     stored in the context, and its mutation crosses the port;
//   * context carries no `Plugin.Service` → the worker payload passes through by
//     identity at the authority and is unchanged across the port (the reply is
//     structured-cloned, so content is compared, not identity).

import { describe, expect, test } from "bun:test"
import { Context, Effect, Option } from "effect"
import { Plugin } from "@/plugin"
import {
  buildHubProxyHandlers,
  messagesTransformAuthority,
  type HubProxyRunner,
} from "@/session/worker/hub"
import type { MessagesTransformInput, MessagesTransformOutput } from "@/session/worker/plugin-host"
import { recordingHubAuthorities, serveHubProxyFor } from "../../lib/hub-proxy"

const MARKER = "appended-by-plugin"
const input: MessagesTransformInput = { sessionID: "ses_authority_hub" }

interface TriggerCall {
  readonly name: string
  readonly input: MessagesTransformInput
}

/**
 * A real `Plugin.Interface` whose `experimental.chat.messages.transform` hook
 * appends a marker in place — the same contract the production hook uses. It is
 * only reachable through `Context.getOption(context, Plugin.Service)`, so the
 * hook firing at all proves the authority resolved the plugin from the context.
 */
function markerPlugin(): { plugin: Plugin.Interface; calls: TriggerCall[] } {
  const calls: TriggerCall[] = []
  // `Plugin.Interface.trigger` is generic over every hook; this double implements
  // only the messages transform, so type that hook concretely and cast once.
  const plugin = Plugin.Service.of({
    init: () => Effect.void,
    list: () => Effect.succeed([]),
    trigger: (name: string, hookInput: MessagesTransformInput, output: MessagesTransformOutput) => {
      calls.push({ name, input: hookInput })
      output.messages.push(MARKER)
      return Effect.succeed(output)
    },
  } as unknown as Plugin.Interface)
  return { plugin, calls }
}

/**
 * Mirror `hubProxyFor`'s authority construction: resolve `Plugin.Service` from
 * the worker's captured session context and run the trigger under it. Absent a
 * context there is no plugin, so the payload passes through and the runner is
 * never invoked.
 */
function perWorkerHandlers(context: Context.Context<never> | undefined) {
  let runs = 0
  const run: HubProxyRunner = (effect) => {
    runs++
    return Effect.runPromise(context === undefined ? effect : Effect.provide(effect, context))
  }
  const messagesTransform = messagesTransformAuthority({
    plugin: () => (context === undefined ? Option.none() : Context.getOption(context, Plugin.Service)),
    run,
  })
  const { authorities, calls } = recordingHubAuthorities({ messagesTransform })
  return {
    handlers: buildHubProxyHandlers(authorities, run),
    calls,
    runs: () => runs,
  }
}

describe("per-worker messagesTransform authority over the real hub-proxy port", () => {
  test("resolves Plugin.Service from the session context and returns its mutation across the port", async () => {
    const { plugin, calls: triggerCalls } = markerPlugin()
    const context = Context.make(Plugin.Service, plugin)
    const { handlers, calls: authorityCalls, runs } = perWorkerHandlers(context)
    const served = serveHubProxyFor(handlers)

    try {
      const output: MessagesTransformOutput = { messages: ["base"] }
      const returned = (await served.client.call("messagesTransform", [input, output])) as MessagesTransformOutput

      expect(returned).toEqual({ messages: ["base", MARKER] })
      // The worker-side payload was structured-cloned to the hub, never mutated
      // in place on the caller's side.
      expect(output).toEqual({ messages: ["base"] })
      // The trigger fired once, which is only possible because the authority
      // resolved `Plugin.Service` from the REAL context, and the runner ran the
      // hook effect.
      expect(triggerCalls).toHaveLength(1)
      expect(triggerCalls[0].name).toBe("experimental.chat.messages.transform")
      expect(triggerCalls[0].input).toEqual(input)
      expect(runs()).toBe(1)
      // The call reached the served authority, proving the port was crossed.
      expect(authorityCalls.map((call) => call.method)).toEqual(["messagesTransform"])
    } finally {
      served.dispose()
    }
  })

  test("no Plugin.Service in the context: identity at the authority and unchanged across the port", async () => {
    // A real context with no `Plugin.Service` — the flag-off / no-plugin case.
    const context = Context.empty()
    const { handlers, calls: authorityCalls, runs } = perWorkerHandlers(context)

    // Identity at the authority boundary: no plugin means no copy and no run.
    const output: MessagesTransformOutput = { messages: ["base"] }
    const returnedAtAuthority = await handlers.messagesTransform?.(input, output)
    expect(returnedAtAuthority).toBe(output)
    expect(runs()).toBe(0)

    const served = serveHubProxyFor(handlers)
    try {
      // The RPC reply is structured-cloned, so identity cannot survive the port;
      // the content must still be unchanged.
      const returned = (await served.client.call("messagesTransform", [input, output])) as MessagesTransformOutput
      expect(returned).toEqual({ messages: ["base"] })
      expect(authorityCalls.map((call) => call.method)).toEqual(["messagesTransform", "messagesTransform"])
    } finally {
      served.dispose()
    }
  })
})
