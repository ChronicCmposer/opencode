// W0b — the plugin-tool wire contract v2: progress threading, the structural
// remote-definition guard, and the fail-loud non-serializable error.
//
// These pin the worker-side seam only (no hub). `remotePluginToolHooks` must
// build a `Hooks.tool` stub whose `execute(args, ctx, onProgress)` forwards the
// call frame AND the progress sink to the executor; `isRemotePluginToolDefinition`
// must distinguish that stub from a real plugin `ToolDefinition`; and
// `PluginToolNotSerializableError` must carry the `_tag` + `toolID` that other
// modules match on.

import { describe, expect, test } from "bun:test"
import type { ToolDefinition } from "@opencode-ai/plugin"
import {
  PluginToolNotSerializableError,
  isRemotePluginToolDefinition,
  remotePluginToolHooks,
  type PluginToolDescriptor,
  type PluginToolExecuteInput,
  type PluginToolExecuteResult,
  type PluginToolExecutor,
  type PluginToolProgress,
  type PluginToolProgressSink,
} from "@/session/worker/plugin-tool-wire"

/** A clone-safe descriptor, the only plugin-tool data that crosses the isolate. */
const descriptor = (id: string): PluginToolDescriptor => ({
  id,
  description: `${id} tool`,
  jsonSchema: { type: "object", properties: {}, additionalProperties: false },
})

/** The clone-safe result a hub execution returns. */
const executeResult: PluginToolExecuteResult = {
  title: "remote title",
  output: "hub output",
  metadata: { ranOnHub: true },
}

describe("remotePluginToolHooks (W0b)", () => {
  test("forwards the call frame and threads the progress sink to the executor", async () => {
    const calls: { input: PluginToolExecuteInput; onProgress: PluginToolProgressSink | undefined }[] = []
    const executor: PluginToolExecutor = {
      execute: async (input, onProgress) => {
        calls.push({ input, onProgress })
        return executeResult
      },
    }

    const hooks = remotePluginToolHooks([descriptor("compress")], executor)
    const def = hooks.tool.compress
    if (!isRemotePluginToolDefinition(def)) throw new Error("compress was not a remote plugin tool")

    const seen: PluginToolProgress[] = []
    const onProgress: PluginToolProgressSink = (progress) => seen.push(progress)

    const result = await def.execute(
      { reason: "prune" },
      { sessionID: "ses_1", messageID: "msg_1", callID: "call_1", agent: "build" },
      onProgress,
    )

    expect(result).toEqual(executeResult)
    expect(calls).toHaveLength(1)
    expect(calls[0].input).toEqual({
      toolID: "compress",
      sessionID: "ses_1",
      messageID: "msg_1",
      callID: "call_1",
      agent: "build",
      args: { reason: "prune" },
    })
    // The exact sink identity is threaded, not wrapped.
    expect(calls[0].onProgress).toBe(onProgress)
  })

  test("defaults callID to the empty string when the frame omits it", async () => {
    const calls: PluginToolExecuteInput[] = []
    const hooks = remotePluginToolHooks([descriptor("prune")], {
      execute: async (input) => {
        calls.push(input)
        return executeResult
      },
    })
    const def = hooks.tool.prune
    if (!isRemotePluginToolDefinition(def)) throw new Error("prune was not a remote plugin tool")

    await def.execute({}, { sessionID: "ses_1", messageID: "msg_1", agent: "build" })

    expect(calls[0].callID).toBe("")
  })
})

describe("remotePluginToolHooks hub callbacks (INT)", () => {
  const executor = (): PluginToolExecutor => ({ execute: async () => executeResult })

  test("attaches an abort bridge that adapts the remote context to (sessionID, callID)", async () => {
    const aborts: { sessionID: string; callID: string }[] = []
    const hooks = remotePluginToolHooks([descriptor("compress")], executor(), {
      abort: (input) => {
        aborts.push(input)
      },
    })
    const def = hooks.tool.compress
    if (!isRemotePluginToolDefinition(def)) throw new Error("compress was not a remote plugin tool")

    await def.abort?.({ sessionID: "ses_abort", messageID: "msg_abort", callID: "call_abort", agent: "build" })

    expect(aborts).toEqual([{ sessionID: "ses_abort", callID: "call_abort" }])
  })

  test("defaults the adapted callID to the empty string when the context omits it", async () => {
    const aborts: { sessionID: string; callID: string }[] = []
    const hooks = remotePluginToolHooks([descriptor("compress")], executor(), {
      abort: (input) => {
        aborts.push(input)
      },
    })

    await hooks.tool.compress?.abort?.({ sessionID: "ses_abort", messageID: "msg_abort", agent: "build" })

    expect(aborts).toEqual([{ sessionID: "ses_abort", callID: "" }])
  })

  test("attaches the refresh seam, and omits both seams when no callbacks are supplied", () => {
    let refreshed = 0
    const withCallbacks = remotePluginToolHooks([descriptor("compress")], executor(), {
      refresh: () => {
        refreshed++
      },
    })
    withCallbacks.tool.compress?.refresh?.()
    expect(refreshed).toBe(1)

    const without = remotePluginToolHooks([descriptor("compress")], executor())
    expect("abort" in without.tool.compress!).toBe(false)
    expect("refresh" in without.tool.compress!).toBe(false)
  })
})

describe("isRemotePluginToolDefinition (W0b)", () => {
  test("recognizes the remote stub remotePluginToolHooks builds", () => {
    const hooks = remotePluginToolHooks([descriptor("compress")], { execute: async () => executeResult })
    expect(isRemotePluginToolDefinition(hooks.tool.compress)).toBe(true)
  })

  test("rejects a plain plugin ToolDefinition", () => {
    const plain: ToolDefinition = {
      description: "a real plugin tool",
      args: {},
      execute: async () => "done",
    }
    expect(isRemotePluginToolDefinition(plain)).toBe(false)
  })

  test("rejects non-objects", () => {
    expect(isRemotePluginToolDefinition(undefined)).toBe(false)
    expect(isRemotePluginToolDefinition(null)).toBe(false)
    expect(isRemotePluginToolDefinition("compress")).toBe(false)
  })
})

describe("PluginToolNotSerializableError (W0b)", () => {
  test("carries the tag and toolID other modules match on", () => {
    const error = new PluginToolNotSerializableError("compress", "metadata")

    expect(error._tag).toBe("PluginToolNotSerializableError")
    expect(error.toolID).toBe("compress")
    expect(error.detail).toBe("metadata")
    expect(error).toBeInstanceOf(Error)
    expect(error.message).toContain("compress")
  })
})
