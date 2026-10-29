// The session prompt service key and the request vocabulary it accepts.
//
// This module is the single declaration of the `@opencode/SessionPrompt`
// service key. It deliberately contains no layer or implementation: the real
// layer lives in `prompt.ts`, while `session.ts`, the worker hub-proxy
// authorities, and the tests resolve the same key from here. A second
// `Context.Service(...)("@opencode/SessionPrompt")` call would create a
// distinct runtime key, so callers must never redeclare it.
//
// The only imports from implementation modules are type-only namespaces: they
// describe the interface's error channel and are erased at runtime, so this
// module never constructs a service or closes a cycle with `prompt.ts`.

import { Context, Effect, Schema } from "effect"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import type { CancelOutcome } from "./worker/cancel"
import { SessionID, MessageID, PartID } from "./schema"
import { DispatchOrigin } from "./origin"
import type { Image } from "@/image/image"
import type { Session } from "./session"
import type { SessionRunState } from "./run-state"

/**
 * A cancel outcome enriched with the message the interrupted run settled on.
 *
 * Cooperative cancellation must not lose the assistant message the worker just
 * finalized; the hub and HTTP layers can project the same outcome fields they
 * already use and read `message` when they need the conversation tail.
 */
export type CancelResult = CancelOutcome & { readonly message?: SessionV1.WithParts }

export interface Interface {
  readonly cancel: (sessionID: SessionID) => Effect.Effect<CancelResult | undefined>
  readonly prompt: (input: PromptInput) => Effect.Effect<SessionV1.WithParts, Image.Error | SessionRunState.RunError>
  readonly loop: (input: LoopInput) => Effect.Effect<SessionV1.WithParts, SessionRunState.RunError>
  readonly shell: (input: ShellInput) => Effect.Effect<SessionV1.WithParts, Session.BusyError | SessionRunState.RunError>
  readonly command: (input: CommandInput) => Effect.Effect<SessionV1.WithParts, Image.Error | SessionRunState.RunError>
  readonly resolvePromptParts: (template: string) => Effect.Effect<PromptInput["parts"]>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/SessionPrompt") {}

const ModelRef = Schema.Struct({
  providerID: ProviderV2.ID,
  modelID: ModelV2.ID,
})

export const PromptInput = Schema.Struct({
  sessionID: SessionID,
  messageID: Schema.optional(MessageID),
  model: Schema.optional(ModelRef),
  agent: Schema.optional(Schema.String),
  noReply: Schema.optional(Schema.Boolean),
  tools: Schema.optional(Schema.Record(Schema.String, Schema.Boolean)).annotate({
    description:
      "@deprecated tools and permissions have been merged, you can set permissions on the session itself now",
  }),
  format: Schema.optional(SessionV1.Format),
  system: Schema.optional(Schema.String),
  variant: Schema.optional(Schema.String),
  /**
   * Who asked for this prompt. The HTTP payload omits it (a direct prompt is a
   * user action); a spawned background child carries `"auto"` so a dead worker
   * fails fast instead of respawning forever.
   */
  origin: Schema.optional(DispatchOrigin),
  parts: Schema.Array(
    Schema.Union([
      SessionV1.TextPartInput,
      SessionV1.FilePartInput,
      SessionV1.AgentPartInput,
      SessionV1.SubtaskPartInput,
    ]).annotate({ discriminator: "type" }),
  ),
})
export type PromptInput = Schema.Schema.Type<typeof PromptInput>

export class LoopInput extends Schema.Class<LoopInput>("SessionPrompt.LoopInput")({
  sessionID: SessionID,
  origin: Schema.optional(DispatchOrigin),
}) {}

export const ShellInput = Schema.Struct({
  sessionID: SessionID,
  messageID: Schema.optional(MessageID),
  agent: Schema.String,
  model: Schema.optional(ModelRef),
  command: Schema.String,
})
export type ShellInput = Schema.Schema.Type<typeof ShellInput>

export const CommandInput = Schema.Struct({
  messageID: Schema.optional(MessageID),
  sessionID: SessionID,
  agent: Schema.optional(Schema.String),
  model: Schema.optional(Schema.String),
  arguments: Schema.String,
  command: Schema.String,
  variant: Schema.optional(Schema.String),
  // Inlined (no identifier annotation) to keep the original SDK output — the
  // PromptInput call site below references FilePartInput by ref via the
  // Schema export in message-v2.ts.
  parts: Schema.optional(
    Schema.Array(
      Schema.Union([
        Schema.Struct({
          id: Schema.optional(PartID),
          type: Schema.Literal("file"),
          mime: Schema.String,
          filename: Schema.optional(Schema.String),
          url: Schema.String,
          source: Schema.optional(SessionV1.FilePartSource),
        }),
      ]).annotate({ discriminator: "type" }),
    ),
  ),
})
export type CommandInput = Schema.Schema.Type<typeof CommandInput>

export * as SessionPromptService from "./prompt-service"
