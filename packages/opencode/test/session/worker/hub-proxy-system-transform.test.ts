// The hub-proxy `systemTransform` RPC.
//
// A session worker's reduced `Plugin` cannot run `experimental.chat.system.transform`
// because plugins are loaded on the hub (decision 70). The worker reaches the hub
// over the existing multiplexed control port instead. This suite pins the wire
// contract:
//   * `buildHubProxyHandlers` always serves `systemTransform`;
//   * a caller-supplied authority runs on the hub and its mutated `{system}` is
//     returned to the worker;
//   * absent an authority (flag-off / no plugins) the handler returns `output`
//     unchanged — never a transport rejection.

import { describe, expect, test } from "bun:test"
import { buildHubProxyHandlers, type HubProxyAuthorities } from "@/session/worker/hub"
import { createHubProxyClient, serveHubProxy } from "@/session/worker/layer"
import type { SystemTransformInput, SystemTransformOutput } from "@/session/worker/plugin-host"

const instantRunner = (effect: unknown): Promise<never> => Promise.resolve(effect as never)

/** The authority map `buildHubProxyHandlers` accepts, with only systemTransform wired. */
function authorities(
  systemTransform?: (input: SystemTransformInput, output: SystemTransformOutput) => Promise<SystemTransformOutput>,
): HubProxyAuthorities {
  return {
    sessionStatus: {} as never,
    permission: {} as never,
    question: {} as never,
    ...(systemTransform === undefined ? {} : { systemTransform }),
  }
}

/** Serve `handlers` on a fresh channel and return the client end. */
function clientFor(handlers: Parameters<typeof serveHubProxy>[0]): ReturnType<typeof createHubProxyClient> {
  const channel = new MessageChannel()
  serveHubProxy(handlers, channel.port2)
  return createHubProxyClient(channel.port1)
}

const input: SystemTransformInput = { sessionID: "ses_system" }

describe("hub-proxy systemTransform RPC", () => {
  test("the hub authority runs and its mutated {system} returns to the worker", async () => {
    // A fake plugin: appends to the shared system array, exactly as the real
    // `Plugin.trigger` contract mutates its `output` in place.
    const plugin = async (_input: SystemTransformInput, output: SystemTransformOutput): Promise<SystemTransformOutput> => {
      output.system.push("appended-by-plugin")
      return output
    }
    const client = clientFor(buildHubProxyHandlers(authorities(plugin), instantRunner))

    const output: SystemTransformOutput = { system: ["base"] }
    const returned = (await client.call("systemTransform", [input, output])) as SystemTransformOutput

    expect(returned).toEqual({ system: ["base", "appended-by-plugin"] })
    client.dispose()
  })

  test("absent an authority the handler returns the input unchanged (degraded)", async () => {
    const client = clientFor(buildHubProxyHandlers(authorities(), instantRunner))

    const output: SystemTransformOutput = { system: ["base"] }
    const returned = (await client.call("systemTransform", [input, output])) as SystemTransformOutput

    expect(returned).toEqual({ system: ["base"] })
    client.dispose()
  })
})
