// Shared fixture for the task-tool characterization tests.
//
// Mirrors the layer the canonical `test/tool/task.test.ts` suite builds, but
// exposes only the pieces the characterization directory needs: the graph, a
// seeded parent session, and deterministic `TaskPromptOps` doubles. Keeping the
// double local (rather than importing the tool suite) keeps this directory
// independent of tests owned by other slices.

import { SessionV1 } from "@opencode-ai/core/v1/session"
import { Database } from "@opencode-ai/core/database/database"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { Agent } from "@/agent/agent"
import { BackgroundJob } from "@/background/job"
import { EventV2Bridge } from "@/event-v2-bridge"
import { Config } from "@/config/config"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { Ripgrep } from "@opencode-ai/core/ripgrep"
import { Session } from "@/session/session"
import type { SessionPrompt } from "@/session/prompt"
import { MessageID, PartID } from "@/session/schema"
import { SessionRunState } from "@/session/run-state"
import { SessionStatus } from "@/session/status"
import { TaskTool, type TaskPromptOps } from "@/tool/task"
import { Truncate } from "@/tool/truncate"
import { ToolRegistry } from "@/tool/registry"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { ModelV2 } from "@opencode-ai/core/model"
import {
  SessionSpawnProxyService,
  type SessionSpawnRequest,
  type SessionSpawnResult,
} from "@/session/worker/subagent"
import type { SessionID } from "@/session/schema"
import { Effect, Layer } from "effect"

export { TaskTool, type TaskPromptOps }

const ref = {
  providerID: ProviderV2.ID.make("test"),
  modelID: ModelV2.ID.make("test-model"),
}

/** The production graph the task tool resolves its services from. */
export function taskLayer(flags: Partial<RuntimeFlags.Info> = {}) {
  return LayerNode.compile(
    LayerNode.group([
      Agent.node,
      BackgroundJob.node,
      EventV2Bridge.node,
      Config.node,
      CrossSpawnSpawner.node,
      Session.node,
      SessionProjector.node,
      SessionRunState.node,
      SessionStatus.node,
      Truncate.node,
      ToolRegistry.node,
      Database.node,
      RuntimeFlags.node,
      Ripgrep.node,
    ]),
    [[RuntimeFlags.node, RuntimeFlags.layer(flags)]],
  ).pipe(Layer.provideMerge(spawnProxyLayer))
}

/**
 * The spawn behaviour the current test controls. `TaskTool` now always calls
 * `Session.spawn`, which prefers a worker's `SessionSpawnProxyService`; the
 * characterization suite supplies one instead of running a provider.
 */
let spawnImpl: (request: SessionSpawnRequest) => Effect.Effect<SessionSpawnResult, Error> = (request) =>
  Effect.succeed(spawnResult(request, "done"))

const spawnProxyLayer = Layer.succeed(SessionSpawnProxyService, {
  spawn: (request) => spawnImpl(request),
})

/** Replace the spawn proxy behaviour for the current test. */
export function setSpawn(run: (request: SessionSpawnRequest) => Effect.Effect<SessionSpawnResult, Error>): void {
  spawnImpl = run
}

/** A resolve-after-use promise gate; no wall-clock sleeps. */
export function defer<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void
  const promise = new Promise<T>((done) => {
    resolve = done
  })
  return { promise, resolve }
}

/** The spawn result a hub proxy hands back for a task request. */
export function spawnResult(
  request: SessionSpawnRequest,
  text: string,
  error?: NonNullable<SessionV1.Assistant["error"]>,
): SessionSpawnResult {
  const childID = request.childID ?? request.prompt.sessionID
  return { childID, result: reply({ ...request.prompt, sessionID: childID }, text, error) }
}

/** Seed one parent session with a user + assistant message pair. */
export const seed = Effect.fn("CharacterizationTask.seed")(function* (title = "Pinned") {
  const session = yield* Session.Service
  const chat = yield* session.create({ title })
  const user = yield* session.updateMessage({
    id: MessageID.ascending(),
    role: "user",
    sessionID: chat.id,
    agent: "build",
    model: ref,
    time: { created: Date.now() },
  })
  const assistant: SessionV1.Assistant = {
    id: MessageID.ascending(),
    role: "assistant",
    parentID: user.id,
    sessionID: chat.id,
    mode: "build",
    agent: "build",
    cost: 0,
    path: { cwd: "/tmp", root: "/tmp" },
    tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    modelID: ref.modelID,
    providerID: ref.providerID,
    variant: "xhigh",
    time: { created: Date.now() },
  }
  yield* session.updateMessage(assistant)
  return { chat, assistant }
})

/** The `SessionV1.WithParts` a prompt op returns for `input`. */
export function reply(
  input: SessionPrompt.PromptInput,
  text: string,
  error?: NonNullable<SessionV1.Assistant["error"]>,
): SessionV1.WithParts {
  const id = MessageID.ascending()
  return {
    info: {
      id,
      role: "assistant",
      parentID: input.messageID ?? MessageID.ascending(),
      sessionID: input.sessionID,
      mode: input.agent ?? "general",
      agent: input.agent ?? "general",
      cost: 0,
      path: { cwd: "/tmp", root: "/tmp" },
      tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
      modelID: input.model?.modelID ?? ref.modelID,
      providerID: input.model?.providerID ?? ref.providerID,
      time: { created: Date.now() },
      finish: "stop",
      error,
    },
    parts: [
      {
        id: PartID.ascending(),
        messageID: id,
        sessionID: input.sessionID,
        type: "text",
        text,
      },
    ],
  }
}

/**
 * Prompt ops used by the task tool for part resolution/cancel and for injecting
 * a background result back into the parent. The child itself runs through
 * `Session.spawn` (see {@link setSpawn}), matching the current production path.
 */
export function stubOps(opts?: {
  onPrompt?: (input: SessionPrompt.PromptInput) => void
  text?: string
  error?: NonNullable<SessionV1.Assistant["error"]>
}): TaskPromptOps {
  return {
    cancel: () => Effect.void,
    resolvePromptParts: (template) => Effect.succeed([{ type: "text" as const, text: template }]),
    prompt: (input) =>
      Effect.sync(() => {
        opts?.onPrompt?.(input)
        return reply(input, opts?.text ?? "done", opts?.error)
      }),
  }
}

/** A minimal `Tool.Context` for executing the task tool. */
export function toolContext(input: {
  sessionID: SessionID
  messageID: MessageID
  promptOps: TaskPromptOps
  abort?: AbortSignal
  bypassAgentCheck?: boolean
}) {
  return {
    sessionID: input.sessionID,
    messageID: input.messageID,
    agent: "build",
    abort: input.abort ?? new AbortController().signal,
    extra: { promptOps: input.promptOps, ...(input.bypassAgentCheck ? { bypassAgentCheck: true } : {}) },
    messages: [],
    metadata: () => Effect.void,
    ask: () => Effect.void,
  }
}
