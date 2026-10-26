import { describe, expect, spyOn, test } from "bun:test"
import { Effect } from "effect"
import { buildHubProxyHandlers } from "@/session/worker/hub"
import type { MessagesTransformInput, MessagesTransformOutput } from "@/session/worker/plugin-host"
import type { SessionSpawnRequest } from "@/session/worker/subagent"
import { MessageID } from "@/session/schema"
import { sid } from "./async"
import { withParts } from "./session-parts"
import { instantRunner, recordingHubAuthorities, serveHubProxyFor } from "./hub-proxy"

describe("instantRunner", () => {
  test("runs a hub authority effect on the current runtime and resolves its value", async () => {
    await expect(instantRunner(Effect.succeed(42))).resolves.toBe(42)
  })
})

describe("serveHubProxyFor", () => {
  test("serves the handlers and returns the worker-side client", async () => {
    const served = serveHubProxyFor({
      "sessionStatus.get": () => ({ type: "idle" }),
    })
    try {
      await expect(served.client.call("sessionStatus.get", ["ses_1"])).resolves.toEqual({ type: "idle" })
    } finally {
      served.dispose()
    }
  })

  test("an unhandled method fails loudly across the port", async () => {
    const served = serveHubProxyFor({})
    try {
      await expect(served.client.call("permission.list", [])).rejects.toThrow(/unknown hub-proxy method/)
    } finally {
      served.dispose()
    }
  })

  test("dispose/close are idempotent and release the client exactly once", () => {
    const served = serveHubProxyFor({})
    const clientDispose = spyOn(served.client, "dispose")

    served.dispose()
    served.close()
    served.dispose()

    expect(clientDispose).toHaveBeenCalledTimes(1)
  })
})

describe("recordingHubAuthorities", () => {
  test("records each authority call with its arguments", async () => {
    const { authorities, calls } = recordingHubAuthorities()

    await Effect.runPromise(authorities.sessionStatus.set(sid("ses_1"), { type: "busy" }))

    expect(calls).toEqual([{ method: "sessionStatus.set", args: [sid("ses_1"), { type: "busy" }] }])
  })

  test("an override changes behaviour and is still recorded", async () => {
    const { authorities, calls } = recordingHubAuthorities({
      sessionStatus: { get: () => Effect.succeed({ type: "busy" }) },
    })

    await expect(Effect.runPromise(authorities.sessionStatus.get(sid("ses_1")))).resolves.toEqual({ type: "busy" })
    expect(calls).toEqual([{ method: "sessionStatus.get", args: [sid("ses_1")] }])
  })

  test("records a sessionSpawn override under the session.spawn wire method", async () => {
    const { authorities, calls } = recordingHubAuthorities({
      sessionSpawn: async (request) => ({
        childID: request.childID ?? request.parentID,
        result: withParts(request.parentID),
      }),
    })
    const request: SessionSpawnRequest = {
      parentID: sid("ses_parent"),
      childID: sid("ses_child"),
      type: "task",
      prompt: { messageID: MessageID.ascending(), sessionID: sid("ses_child"), parts: [{ type: "text", text: "hi" }] },
    }

    await expect(authorities.sessionSpawn!(request)).resolves.toMatchObject({ childID: sid("ses_child") })
    expect(calls).toEqual([{ method: "session.spawn", args: [request] }])
  })

  test("records a messagesTransform override and returns its value across the port", async () => {
    const { authorities, calls } = recordingHubAuthorities({
      messagesTransform: async (_input, output) => ({ messages: output.messages }),
    })
    const served = serveHubProxyFor(buildHubProxyHandlers(authorities, instantRunner))
    const input: MessagesTransformInput = { sessionID: "ses_1" }
    const output: MessagesTransformOutput = { messages: ["base"] }
    try {
      await expect(served.client.call("messagesTransform", [input, output])).resolves.toEqual({ messages: ["base"] })
      expect(calls).toEqual([{ method: "messagesTransform", args: [input, output] }])
    } finally {
      served.dispose()
    }
  })

  test("absent an override messagesTransform records and returns the output by identity", async () => {
    const { authorities, calls } = recordingHubAuthorities()
    const input: MessagesTransformInput = { sessionID: "ses_1" }
    const output: MessagesTransformOutput = { messages: ["base"] }

    const returned = await authorities.messagesTransform!(input, output)

    expect(returned).toBe(output)
    expect(calls).toEqual([{ method: "messagesTransform", args: [input, output] }])
  })
})
