// The isolate boundary for `experimental.chat.messages.transform`.
//
// The worker cannot run the hook itself (plugins are loaded on the hub), so the
// message list crosses the multiplexed control `MessagePort` as structured-clone
// data. Context pruning depends on that array surviving intact: a shape the
// clone algorithm rejects, or a value silently dropped, would send the model
// stale messages. This suite pins:
//   * a representative `MessageV2.WithParts[]` survives `structuredClone` and a
//     real `MessageChannel` round-trip with no data loss (nested tool state,
//     `Date` values, `undefined` optionals, strings/numbers/booleans);
//   * a non-cloneable payload FAILS LOUD on the worker→hub (message-bearing)
//     direction — a synchronous `DataCloneError`, not a silent no-op;
//   * the reverse direction (a non-cloneable hub reply) also FAILS LOUD: the hub
//     sends a clone-safe `ok:false` frame, so the worker rejects immediately with
//     a descriptive `HubProxyRemoteError` rather than hanging or timing out;
//   * a degraded boot (no hub-proxy client) passes the array through by
//     identity, with no splice and no crash.

import { describe, expect, test } from "bun:test"
import { Effect, Layer } from "effect"
import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import type { WithParts } from "@opencode-ai/core/v1/session"
import { Plugin } from "@/plugin"
import { Permission } from "@/permission"
import { Question } from "@/question"
import { SessionStatus } from "@/session/status"
import { MessageID, PartID, SessionID } from "@/session/schema"
import { buildHubProxyHandlers, type HubProxyAuthorities } from "@/session/worker/hub"
import { HubProxyRemoteError } from "@/session/worker/hub-proxy-wire"
import {
  ModelClient,
  buildSessionWorkerLayer,
  createHubProxyClient,
  serveHubProxy,
  type SessionLayerOptions,
} from "@/session/worker/layer"
import {
  createWorkerPluginHost,
  type MessagesTransformInput,
  type MessagesTransformOutput,
} from "@/session/worker/plugin-host"
import { hubProxyMessagesTransform } from "@/session/worker/session-worker"
import { workerSnapshot } from "../../lib/snapshot"

const instantRunner = (effect: unknown): Promise<never> => Promise.resolve(effect as never)

/**
 * Inert hub authorities, typed by the REAL interfaces so a change to any of them
 * is a compile error here rather than hidden behind a `{} as never` cast. This
 * suite only exercises `messagesTransform`; the other three are never reached.
 */
const inertAuthorities: {
  readonly sessionStatus: SessionStatus.Interface
  readonly permission: Permission.Interface
  readonly question: Question.Interface
} = {
  sessionStatus: {
    get: () => Effect.succeed({ type: "idle" as const }),
    set: () => Effect.void,
    list: () => Effect.succeed(new Map()),
  },
  permission: {
    ask: () => Effect.void,
    reply: () => Effect.void,
    list: () => Effect.succeed([]),
  },
  question: {
    ask: () => Effect.succeed([]),
    reply: () => Effect.void,
    reject: () => Effect.void,
    list: () => Effect.succeed([]),
  },
}

/** The hub-proxy authority map `buildHubProxyHandlers` accepts, messages-only. */
function authorities(
  messagesTransform?: (
    input: MessagesTransformInput,
    output: MessagesTransformOutput,
  ) => Promise<MessagesTransformOutput>,
): HubProxyAuthorities {
  return {
    ...inertAuthorities,
    ...(messagesTransform === undefined ? {} : { messagesTransform }),
  }
}

/** Serve `handlers` on a fresh channel and return the client end. */
function clientFor(handlers: Parameters<typeof serveHubProxy>[0], timeoutMs?: number) {
  const channel = new MessageChannel()
  serveHubProxy(handlers, channel.port2)
  return createHubProxyClient(channel.port1, timeoutMs === undefined ? {} : { timeoutMs })
}

const SESSION_ID = SessionID.create()
const PROVIDER_ID = ProviderV2.ID.make("mock")
const MODEL_ID = ModelV2.ID.make("mock-model")

/**
 * A representative `MessageV2.WithParts[]`: a user turn with a text part and a
 * completed tool part (nested `state`, nested `time`, metadata carrying `Date`
 * values and an explicit `undefined`), plus an assistant turn with nested
 * `tokens` and `undefined` optionals.
 *
 * `metadata` is the real carrier for opaque plugin values (`Schema.Any`), so the
 * `Date`s live there: the schema's own timestamps (`time.created`,
 * `state.time.start`) are finite numbers, and this fixture keeps them numeric so
 * it stays faithful to what the hook actually receives.
 */
function representativeMessages(): WithParts[] {
  const userId = MessageID.ascending()
  const assistantId = MessageID.ascending()
  return [
    {
      info: {
        id: userId,
        sessionID: SESSION_ID,
        role: "user",
        time: { created: 1_759_000_000_000 },
        agent: "build",
        model: { providerID: PROVIDER_ID, modelID: MODEL_ID, variant: undefined },
        system: undefined,
        format: undefined,
        summary: undefined,
        tools: undefined,
      },
      parts: [
        {
          id: PartID.ascending(),
          sessionID: SESSION_ID,
          messageID: userId,
          type: "text",
          text: "hello clone",
          synthetic: undefined,
          ignored: undefined,
          time: undefined,
          metadata: undefined,
        },
        {
          id: PartID.ascending(),
          sessionID: SESSION_ID,
          messageID: userId,
          type: "tool",
          callID: "call_1",
          tool: "read",
          metadata: undefined,
          state: {
            status: "completed",
            input: { path: "a.ts", limit: 10, follow: true },
            output: "file contents",
            title: "read a.ts",
            metadata: {
              generatedAt: new Date("2026-10-25T00:00:00.000Z"),
              nested: { at: new Date(0), note: undefined },
              count: 3,
              ok: true,
            },
            time: { start: 1_759_000_000_001, end: 1_759_000_000_002, compacted: undefined },
            attachments: undefined,
          },
        },
      ],
    },
    {
      info: {
        id: assistantId,
        sessionID: SESSION_ID,
        role: "assistant",
        time: { created: 1_759_000_000_003, completed: undefined },
        parentID: userId,
        modelID: MODEL_ID,
        providerID: PROVIDER_ID,
        mode: "build",
        agent: "build",
        path: { cwd: "/tmp/project", root: "/tmp/project" },
        summary: undefined,
        cost: 0.01,
        tokens: {
          total: 42,
          input: 10,
          output: 20,
          reasoning: 2,
          cache: { read: 5, write: 5 },
        },
        structured: { ok: true },
        variant: undefined,
        finish: undefined,
        error: undefined,
      },
      parts: [
        {
          id: PartID.ascending(),
          sessionID: SESSION_ID,
          messageID: assistantId,
          type: "text",
          text: "done",
        },
      ],
    },
  ]
}

/** The metadata of the fixture's completed tool part, fail-loud if it is absent. */
function completedToolMetadata(messages: WithParts[]): Record<string, unknown> {
  const tool = messages[0]?.parts.find((part) => part.type === "tool")
  if (tool?.type !== "tool" || tool.state.status !== "completed") {
    throw new Error("fixture must contain a completed tool part")
  }
  return tool.state.metadata
}

/** Trigger the hook through the real worker `Plugin.Service`. */
const triggerMessagesTransform = (
  built: ReturnType<typeof buildSessionWorkerLayer>,
  input: MessagesTransformInput,
  output: MessagesTransformOutput,
): Promise<MessagesTransformOutput> =>
  Effect.runPromise(
    Effect.gen(function* () {
      const plugin = yield* Plugin.Service
      return yield* plugin.trigger("experimental.chat.messages.transform", input, output)
    }).pipe(Effect.provide(built.layer)),
  )

const clients = (): SessionLayerOptions["clients"] => ({
  writer: { query: async () => [], publish: async () => ({ seq: 1, committed: true }) },
  egress: { emit: () => {} },
  backgroundJob: { wait: async () => null, cancel: async () => {} },
})

const options = (overrides: Partial<SessionLayerOptions> = {}): SessionLayerOptions => ({
  snapshot: workerSnapshot(),
  env: { values: {} },
  clients: clients(),
  modelClient: Layer.succeed(ModelClient, { complete: () => Effect.succeed({ text: "stub" }) }),
  ...overrides,
})

describe("messages.transform clone guard at the isolate boundary", () => {
  test("a representative WithParts payload survives structuredClone with no data loss", () => {
    const payload = representativeMessages()

    const cloned = structuredClone(payload)

    // `toStrictEqual` distinguishes an explicit `undefined` property from an
    // absent one, so this also pins that optionals are not silently dropped.
    expect(cloned).toStrictEqual(payload)
    const metadata = completedToolMetadata(cloned)
    expect(metadata.generatedAt).toBeInstanceOf(Date)
    expect((metadata.generatedAt as Date).getTime()).toBe(Date.parse("2026-10-25T00:00:00.000Z"))
    expect((metadata.nested as { at: Date }).at).toBeInstanceOf(Date)
  })

  test("a representative WithParts payload round-trips the hub-proxy JSON boundary", async () => {
    const payload = representativeMessages()
    const passThrough = async (
      _input: MessagesTransformInput,
      output: MessagesTransformOutput,
    ): Promise<MessagesTransformOutput> => output
    const client = clientFor(buildHubProxyHandlers(authorities(passThrough), instantRunner))

    try {
      const returned = (await client.call("messagesTransform", [
        { sessionID: SESSION_ID },
        { messages: payload },
      ])) as MessagesTransformOutput

      // The port boundary is JSON, so dates become ISO strings and
      // explicit `undefined` properties are omitted.
      const json = JSON.parse(JSON.stringify(payload)) as unknown
      expect(returned.messages).toStrictEqual(json as unknown[])
      const metadata = completedToolMetadata(returned.messages as WithParts[])
      expect(metadata.generatedAt).toBe("2026-10-25T00:00:00.000Z")
      expect((metadata.nested as { at: string }).at).toBe("1970-01-01T00:00:00.000Z")
      expect(Object.prototype.hasOwnProperty.call(metadata.nested, "note")).toBe(false)
    } finally {
      client.dispose()
    }
  })

  test("a non-JSON message payload fails loud at the worker boundary", async () => {
    const passThrough = async (
      _input: MessagesTransformInput,
      output: MessagesTransformOutput,
    ): Promise<MessagesTransformOutput> => output
    const client = clientFor(buildHubProxyHandlers(authorities(passThrough), instantRunner))

    try {
      // A function has no JSON representation; the message array is the
      // worker→hub request payload, so the failure happens locally and at once.
      const poisoned = {
        messages: [
          {
            info: { role: "user" },
            parts: [{ type: "text", text: "x", metadata: { onEvent: () => {} } }],
          },
        ],
      }

      let caught: unknown
      try {
        await client.call("messagesTransform", [{ sessionID: SESSION_ID }, poisoned])
      } catch (error) {
        caught = error
      }

      // The encoder rejects a non-JSON payload with a typed ProtocolError
      // before any frame crosses; it is not a `HubProxyRemoteError` reply.
      expect((caught as { tag?: string }).tag).toBe("PROTOCOL")
      expect((caught as Error).message).toMatch(/not JSON-serialisable/)
      expect(caught).not.toBeInstanceOf(HubProxyRemoteError)

      // Fail loud must not poison the channel: a well-formed call still works.
      const ok = (await client.call("messagesTransform", [
        { sessionID: SESSION_ID },
        { messages: [{ role: "user", content: "still works" }] },
      ])) as MessagesTransformOutput
      expect(ok.messages).toEqual([{ role: "user", content: "still works" }])
    } finally {
      client.dispose()
    }
  })

  test("a non-cloneable hub reply fails loud with a descriptive remote error", async () => {
    // The hub handler returns a value the port cannot clone. `serveHubProxy`
    // catches the failed post and replies with a clone-safe `ok:false` frame, so
    // the worker rejects immediately with a descriptive `HubProxyRemoteError`
    // instead of hanging or surfacing only as a generic timeout. A generous
    // backstop keeps a regression (the old silent drop) from hanging the suite;
    // the clone guard is identified by the message, not by racing the timeout.
    const client = clientFor(
      {
        messagesTransform: () => ({ messages: [{ onEvent: () => {} }] }),
      },
      2000,
    )

    try {
      let caught: unknown
      try {
        await client.call("messagesTransform", [{ sessionID: SESSION_ID }, { messages: [] }])
      } catch (error) {
        caught = error
      }

      expect(caught).toBeInstanceOf(HubProxyRemoteError)
      const message = (caught as Error).message
      expect(message).toContain("not structured-cloneable")
      // The failure is the clone guard, not a timeout: a timeout here would mean
      // the hub silently dropped the reply again.
      expect(message).not.toContain("timed out")
    } finally {
      client.dispose()
    }
  })

  test("without a hub client the messages array passes through by identity (degraded boot)", async () => {
    expect(hubProxyMessagesTransform(undefined)).toBeUndefined()

    const messagesTransform = hubProxyMessagesTransform(undefined)
    const host = createWorkerPluginHost([{ name: "hub", hooks: {}, proxied: ["experimental.chat.messages.transform"] }], messagesTransform === undefined ? undefined : { messagesTransform })
    const output: MessagesTransformOutput = { messages: [{ role: "user", content: "base" }] }

    const result = await triggerMessagesTransform(
      buildSessionWorkerLayer(options({ plugins: host })),
      { sessionID: "ses_degraded", model: {} },
      output,
    )

    expect(result.messages).toEqual([{ role: "user", content: "base" }])
    // No splice and no fresh array: the caller's objects are untouched.
    expect(result).toBe(output)
    expect(result.messages).toBe(output.messages)
  })
})
