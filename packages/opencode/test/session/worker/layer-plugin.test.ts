// Worker plugin host tests (`chat.params` / `chat.headers` in-worker).

import { describe, expect, test } from "bun:test"
import { Cause, Effect, Exit, Layer } from "effect"
import { Plugin } from "@/plugin"
import { UnclassifiedPluginHookError, UnsupportedPluginHookError } from "@/plugin/routes"
import { ModelClient, buildSessionWorkerLayer } from "@/session/worker/layer"
import {
  DuplicatePluginError,
  createWorkerPluginHost,
  type SystemTransformInput,
  type WorkerPlugin,
} from "@/session/worker/plugin-host"
import { workerSnapshot } from "../../lib/snapshot"

describe("worker plugin host", () => {
  test("chat.params hooks run in registration order and compose mutations", async () => {
    const order: string[] = []
    const plugins: WorkerPlugin[] = [
      {
        name: "a",
        hooks: {
          "chat.params": (_input, output) => {
            order.push("a")
            output.temperature = 0.5
          },
        },
      },
      {
        name: "b",
        hooks: {
          "chat.params": (_input, output) => {
            order.push("b")
            output.options = { ...output.options, reasoning: true }
          },
        },
      },
    ]
    const host = createWorkerPluginHost(plugins)
    const result = await host.chatParams(
      { sessionID: "s", agent: "build" },
      { temperature: 0, topP: 1, topK: 1, maxOutputTokens: undefined, options: {} },
    )

    expect(order).toEqual(["a", "b"])
    expect(result.temperature).toBe(0.5)
    expect(result.options).toEqual({ reasoning: true })
    expect(host.names).toEqual(["a", "b"])
  })

  test("chat.headers merges plugin headers (later wins)", async () => {
    const host = createWorkerPluginHost([
      { name: "a", hooks: { "chat.headers": (_input, output) => void (output.headers["x-a"] = "1") } },
      {
        name: "b",
        hooks: {
          "chat.headers": (_input, output) => {
            output.headers["x-a"] = "2"
            output.headers["x-b"] = "3"
          },
        },
      },
    ])
    const result = await host.chatHeaders({}, { headers: {} })
    expect(result.headers).toEqual({ "x-a": "2", "x-b": "3" })
  })

  test("tool.execute.before/after hooks mutate args and result", async () => {
    const host = createWorkerPluginHost([
      {
        name: "rewrite",
        hooks: {
          "tool.execute.before": (_input, output) => {
            output.args = { ...(output.args as object), injected: true }
          },
          "tool.execute.after": (_input, output) => {
            output.output = `${output.output}!`
          },
        },
      },
    ])

    const before = await host.toolBefore({ tool: "bash" }, { args: { command: "ls" } })
    expect(before.args).toEqual({ command: "ls", injected: true })

    const after = await host.toolAfter({ tool: "bash" }, { title: "t", output: "done", metadata: {} })
    expect(after.output).toBe("done!")
  })

  test("a throwing hook aborts the chain (fail loud)", async () => {
    const host = createWorkerPluginHost([
      { name: "bad", hooks: { "chat.params": () => Promise.reject(new Error("hook exploded")) } },
    ])
    await expect(
      host.chatParams({}, { temperature: 0, topP: 1, topK: 1, maxOutputTokens: undefined, options: {} }),
    ).rejects.toThrow("hook exploded")
  })

  test("a plugin with no matching hook is a no-op", async () => {
    const host = createWorkerPluginHost([{ name: "empty", hooks: {} }])
    const result = await host.chatParams({}, { temperature: 1, topP: 1, topK: 1, maxOutputTokens: undefined, options: {} })
    expect(result.temperature).toBe(1)
  })

  test("duplicate plugin names are rejected (fail loud)", () => {
    expect(() =>
      createWorkerPluginHost([
        { name: "dup", hooks: {} },
        { name: "dup", hooks: {} },
      ]),
    ).toThrow(DuplicatePluginError)
  })

  test("names are snapshotted once at construction", () => {
    const plugins: WorkerPlugin[] = [{ name: "a", hooks: {} }]
    const host = createWorkerPluginHost(plugins)
    plugins.push({ name: "b", hooks: {} })
    expect(host.names).toEqual(["a"])
  })

  test("experimental.chat.system.transform applies a plugin's system rewrite", async () => {
    const host = createWorkerPluginHost([
      {
        name: "sys",
        hooks: {
          "experimental.chat.system.transform": (_input, output) => {
            output.system = [...output.system, "plugin-added"]
          },
        },
      },
    ])
    const input: SystemTransformInput = { sessionID: "s", model: {} }
    const result = await host.systemTransform(input, { system: ["base"] })
    expect(result.system).toEqual(["base", "plugin-added"])
  })

  test("the injected options.systemTransform runs after the plugin function hooks", async () => {
    const order: string[] = []
    const host = createWorkerPluginHost(
      [
        {
          name: "sys",
          hooks: {
            "experimental.chat.system.transform": (_input, output) => {
              order.push("plugin")
              output.system = [...output.system, "plugin"]
            },
          },
        },
      ],
      {
        systemTransform: async (_input, output) => {
          order.push("options")
          return { system: [...output.system, "options"] }
        },
      },
    )
    const result = await host.systemTransform({ sessionID: "s", model: {} }, { system: ["base"] })
    expect(order).toEqual(["plugin", "options"])
    expect(result.system).toEqual(["base", "plugin", "options"])
  })

  test("through the worker Plugin.Service, system.transform reaches the host", async () => {
    const host = createWorkerPluginHost([
      {
        name: "sys",
        hooks: {
          "experimental.chat.system.transform": (_input, output) => {
            output.system = [...output.system, "worker"]
          },
        },
      },
    ])
    const built = buildSessionWorkerLayer({
      snapshot: workerSnapshot(),
      env: { values: {} },
      clients: {
        writer: { query: async () => [], publish: async () => ({ seq: 1, committed: true }) },
        egress: { emit: () => {} },
        backgroundJob: { wait: async () => null, cancel: async () => {} },
      },
      modelClient: Layer.succeed(ModelClient, { complete: () => Effect.succeed({ text: "stub" }) }),
      plugins: host,
    })

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const plugin = (yield* Plugin.Service) as Plugin.Interface
        return yield* plugin.trigger(
          "experimental.chat.system.transform",
          { sessionID: "s", model: {} } as never,
          { system: ["base"] },
        )
      }).pipe(Effect.provide(built.layer)),
    )

    expect((result as { system: string[] }).system).toEqual(["base", "worker"])
  })

  test("a declared unsupported or unclassified trigger is a typed failure, never a pass-through", async () => {
    // A loaded plugin declares the unsupported hook: invocation fails typed. An
    // undeclared unsupported hook is a true no-op (see plugin-text-complete).
    const host = createWorkerPluginHost([{ name: "empty", hooks: {}, declared: ["command.execute.before"] }])
    const built = buildSessionWorkerLayer({
      snapshot: workerSnapshot(),
      env: { values: {} },
      clients: {
        writer: { query: async () => [], publish: async () => ({ seq: 1, committed: true }) },
        egress: { emit: () => {} },
        backgroundJob: { wait: async () => null, cancel: async () => {} },
      },
      modelClient: Layer.succeed(ModelClient, { complete: () => Effect.succeed({ text: "stub" }) }),
      plugins: host,
    })

    const trigger = (name: string) =>
      Effect.gen(function* () {
        const plugin = (yield* Plugin.Service) as Plugin.Interface
        return yield* plugin.trigger(name as never, {} as never, { env: {} })
      }).pipe(Effect.provide(built.layer), Effect.exit)

    const unsupported = await Effect.runPromise(trigger("command.execute.before"))
    expect(Exit.isFailure(unsupported)).toBe(true)
    if (Exit.isFailure(unsupported)) {
      expect(Cause.squash(unsupported.cause)).toBeInstanceOf(UnsupportedPluginHookError)
    }

    const unclassified = await Effect.runPromise(trigger("future.trigger"))
    expect(Exit.isFailure(unclassified)).toBe(true)
    if (Exit.isFailure(unclassified)) {
      expect(Cause.squash(unclassified.cause)).toBeInstanceOf(UnclassifiedPluginHookError)
    }
  })

  test("absent hook and options returns the input array unchanged", async () => {
    const output = { system: ["base"] }
    const host = createWorkerPluginHost([{ name: "empty", hooks: {} }])
    const result = await host.systemTransform({}, output)
    expect(result).toBe(output)
    expect(result.system).toBe(output.system)
  })
})
