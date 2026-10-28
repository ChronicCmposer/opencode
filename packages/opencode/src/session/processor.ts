import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { PermissionV1 } from "@opencode-ai/core/v1/permission"
import { Image } from "@/image/image"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { Cause, Deferred, Effect, Exit, Layer, Context, Scope, Schema } from "effect"
import * as Stream from "effect/Stream"
import { Agent } from "@/agent/agent"
import { Config } from "@/config/config"
import { Permission } from "@/permission"
import { Plugin } from "@/plugin"
import { Snapshot } from "@/snapshot"
import { Session } from "./session"
import { LLM } from "./llm"
import { MessageV2 } from "./message-v2"
import { isOverflow } from "./overflow"
import { PartID } from "./schema"
import type { SessionID } from "./schema"
import { SessionRetry } from "./retry"
import { SessionStatus } from "./status"
import { SessionSummary } from "./summary"
import type { Provider } from "@/provider/provider"
import { Question } from "@/question"
import { errorMessage } from "@/util/error"
import { isRecord } from "@/util/record"
import { EventV2Bridge } from "@/event-v2-bridge"
import { Database } from "@opencode-ai/core/database/database"
import { withBusyRetry } from "@opencode-ai/core/database/busy"
import { LLMError, UnknownProviderReason, Usage, type LLMEvent } from "@opencode-ai/llm"
import { Faults } from "@/testing/faults"

const DOOM_LOOP_THRESHOLD = 3

/**
 * One settlement deadline per run, shared by every call being drained. A result
 * that commits before it settles `ok`; a call that has not settled by then is
 * left `started` (indeterminate) rather than being guessed as `error`, because
 * its side effect may still have succeeded. The deadline only stops cleanup from
 * wedging on a tool that never reports at all.
 */
const RUN_SETTLEMENT_DEADLINE_MS = 5_000
export type Result = "compact" | "stop" | "continue"

/**
 * A tool call arrived while the assistant message was a summary. Summaries are
 * generated without tools, so a tool call here is a protocol violation rather
 * than a user-recoverable error. Typed so it flows through the process error
 * channel and the existing retry/halt pipeline instead of becoming a defect.
 */
export class ToolCallDuringSummaryError extends Schema.TaggedErrorClass<ToolCallDuringSummaryError>()(
  "SessionProcessor.ToolCallDuringSummaryError",
  { tool: Schema.String },
) {
  override get message() {
    return `Tool call not allowed while generating summary: ${this.tool}`
  }
}

/**
 * The durable settlement of one tool call, split by commit boundary.
 *
 * `commit` is the ledger write that must join the result transaction, so a
 * crash leaves either both the result and the finish or neither. `afterCommit`
 * is the hub-observed finish: it runs only once the result transaction has
 * committed, never while the writer is held. `settle` is the standalone path
 * for abort/cleanup, where no transaction exists.
 */
export type ToolSettlementPhase = "commit" | "afterCommit" | "settle" | "abandon"

export type ToolSettlement = (outcome: "ok" | "error", phase: ToolSettlementPhase) => Effect.Effect<void>

export interface Handle {
  readonly message: SessionV1.Assistant
  readonly updateToolCall: (
    toolCallID: string,
    update: (part: SessionV1.ToolPart) => SessionV1.ToolPart,
  ) => Effect.Effect<SessionV1.ToolPart | undefined>
  readonly completeToolCall: (
    toolCallID: string,
    output: {
      title: string
      metadata: Record<string, any>
      output: string
      attachments?: SessionV1.FilePart[]
    },
  ) => Effect.Effect<void>
  /**
   * Bridge the tool wrapper's settlement to the result commit. `commit` joins
   * the result transaction; `afterCommit` runs only after that transaction has
   * committed; `settle` is the standalone abort/cleanup path. Optional only for
   * reduced test handles; the real processor always provides it.
   */
  readonly registerToolSettlement?: (toolCallID: string, settle: ToolSettlement) => void
  readonly process: (streamInput: LLM.StreamInput) => Effect.Effect<Result>
}

type Input = {
  assistantMessage: SessionV1.Assistant
  sessionID: SessionID
  model: Provider.Model
  /** Test seam for the per-run settlement deadline; production uses 5s. */
  settlementDeadlineMs?: number
}

export interface Interface {
  readonly create: (input: Input) => Effect.Effect<Handle>
}

type ToolCall = {
  partID: SessionV1.ToolPart["id"]
  messageID: SessionV1.ToolPart["messageID"]
  sessionID: SessionV1.ToolPart["sessionID"]
  done: Deferred.Deferred<void>
}

interface ProcessorContext extends Input {
  toolcalls: Record<string, ToolCall>
  shouldBreak: boolean
  snapshot: string | undefined
  blocked: boolean
  needsCompaction: boolean
  currentText: SessionV1.TextPart | undefined
  reasoningMap: Record<string, SessionV1.ReasoningPart>
}

type StreamEvent = LLMEvent

export class Service extends Context.Service<Service, Interface>()("@opencode/SessionProcessor") {}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const session = yield* Session.Service
    const config = yield* Config.Service
    const snapshot = yield* Snapshot.Service
    const agents = yield* Agent.Service
    const llm = yield* LLM.Service
    const permission = yield* Permission.Service
    const plugin = yield* Plugin.Service
    const summary = yield* SessionSummary.Service
    const scope = yield* Scope.Scope
    const status = yield* SessionStatus.Service
    const image = yield* Image.Service
    const events = yield* EventV2Bridge.Service
    const database = yield* Database.Service

    const create = Effect.fn("SessionProcessor.create")(function* (input: Input) {
      // Pre-capture snapshot before the LLM stream starts. The AI SDK
      // may execute tools internally before emitting start-step events,
      // so capturing inside the event handler can be too late.
      const initialSnapshot = yield* snapshot.track()
      const ctx: ProcessorContext = {
        assistantMessage: input.assistantMessage,
        sessionID: input.sessionID,
        model: input.model,
        toolcalls: {},
        shouldBreak: false,
        snapshot: initialSnapshot,
        blocked: false,
        needsCompaction: false,
        currentText: undefined,
        reasoningMap: {},
      }
      let aborted = false

      const parse = (e: unknown) =>
        MessageV2.fromError(e, {
          providerID: input.model.providerID,
          aborted,
        })

      const settleToolCall = Effect.fn("SessionProcessor.settleToolCall")(function* (toolCallID: string) {
        const done = ctx.toolcalls[toolCallID]?.done
        delete ctx.toolcalls[toolCallID]
        if (done) yield* Deferred.succeed(done, undefined).pipe(Effect.ignore)
      })

      // A successful tool call is settled by `completeToolCall`, not by the tool
      // wrapper: the registration bridges the two so the ledger finish can share
      // the result transaction. Every path that commits a terminal part drains it.
      const settlements = new Map<string, ToolSettlement>()
      const registerToolSettlement = (toolCallID: string, settle: ToolSettlement): void => {
        settlements.set(toolCallID, settle)
      }
      const settleRegistered = Effect.fnUntraced(function* (
        toolCallID: string,
        outcome: "ok" | "error",
        phase: ToolSettlementPhase,
      ) {
        const settle = settlements.get(toolCallID)
        if (settle === undefined) return
        // `commit` must stay registered until its transaction succeeds, so a
        // rollback can still be settled as an error by cleanup.
        if (phase !== "commit" && phase !== "abandon") settlements.delete(toolCallID)
        // Ledger bookkeeping must never turn a committed result into a run
        // failure; the durable row is the only thing callers can rely on.
        yield* settle(outcome, phase).pipe(
          Effect.catchCause((cause) =>
            Effect.logError("tool settlement failed", { toolCallID, outcome, phase, cause }),
          ),
        )
      })

      const readToolCall = Effect.fn("SessionProcessor.readToolCall")(function* (toolCallID: string) {
        const call = ctx.toolcalls[toolCallID]
        if (!call) return undefined
        const part = yield* session.getPart({
          partID: call.partID,
          messageID: call.messageID,
          sessionID: call.sessionID,
        })
        if (!part || part.type !== "tool") {
          delete ctx.toolcalls[toolCallID]
          return undefined
        }
        return { call, part }
      })

      const updateToolCall = Effect.fn("SessionProcessor.updateToolCall")(function* (
        toolCallID: string,
        update: (part: SessionV1.ToolPart) => SessionV1.ToolPart,
      ) {
        const match = yield* readToolCall(toolCallID)
        if (!match) return undefined
        const part = yield* session.updatePart(update(match.part))
        ctx.toolcalls[toolCallID] = {
          ...match.call,
          partID: part.id,
          messageID: part.messageID,
          sessionID: part.sessionID,
        }
        return part
      })

      /**
       * A result writer (abort path, duplicate event, or failed call) owns the
       * call's `done` deferred: resolving it on every exit, including an
       * interrupt, lets cleanup's settlement drain end as soon as the writer's
       * decision is final instead of waiting out its bound.
       */
      const withResultDrain = <A, E, R>(toolCallID: string, effect: Effect.Effect<A, E, R>) => {
        const call = ctx.toolcalls[toolCallID]
        if (call === undefined) return effect
        return effect.pipe(
          Effect.ensuring(Deferred.succeed(call.done, undefined).pipe(Effect.ignore)),
        )
      }

      const completeToolCall = Effect.fn("SessionProcessor.completeToolCall")(function* (
        toolCallID: string,
        output: {
          title: string
          metadata: Record<string, any>
          output: string
          attachments?: SessionV1.FilePart[]
        },
      ) {
        return yield* withResultDrain(
          toolCallID,
          Effect.gen(function* () {
            const match = yield* readToolCall(toolCallID)
            if (!match) {
              yield* settleRegistered(toolCallID, "ok", "settle")
              return
            }
            const state = match.part.state
            // Cleanup may have written an interrupted placeholder after its
            // per-run deadline. A result that lands later still owns the call:
            // revive the placeholder into a completed result instead of losing
            // the success to the abort.
            const resumable =
              state.status === "running" ||
              (state.status === "error" && state.metadata?.interrupted === true)
            if (!resumable) {
              // The abort path may have written the result early; a later AI SDK
              // result event still has to drain the pending settlement.
              yield* settleRegistered(toolCallID, "ok", "settle")
              return
            }
            const completed: SessionV1.ToolPart = {
              ...match.part,
              state: {
                status: "completed",
                input: state.input,
                output: output.output,
                metadata: output.metadata,
                title: output.title,
                time: { start: state.time.start, end: Date.now() },
                attachments: output.attachments,
              },
            }
            // The result part and the success settlement commit together on the
            // worker's one connection. A crash at any point in this transaction rolls
            // both back, leaving the call `started` (indeterminate) instead of a
            // finished call whose result was lost and would be replayed.
            yield* database.db
              .transaction(
                () =>
                  Effect.gen(function* () {
                    yield* session.updatePart(completed)
                    yield* settleRegistered(toolCallID, "ok", "commit")
                    Faults.crashWorker("tool:settled")
                  }),
                { behavior: "immediate" },
              )
              // Busy retry owns the whole result/failure transaction; replaying a
              // single statement inside it would keep the stale snapshot.
              .pipe(withBusyRetry, Effect.orDie)
            yield* settleRegistered(toolCallID, "ok", "afterCommit")
            yield* settleToolCall(toolCallID)
          }),
        )
      })

      const failToolCall = Effect.fn("SessionProcessor.failToolCall")(function* (toolCallID: string, error: unknown) {
        return yield* withResultDrain(
          toolCallID,
          Effect.gen(function* () {
            const match = yield* readToolCall(toolCallID)
            if (!match || match.part.state.status !== "running") {
              yield* settleRegistered(toolCallID, "error", "settle")
              return false
            }
            const failed: SessionV1.ToolPart = {
              ...match.part,
              state: {
                status: "error",
                input: match.part.state.input,
                error: errorMessage(error),
                // Keep metadata streamed while running so failures retain progress detail (e.g. execute's child calls).
                metadata: match.part.state.metadata,
                time: { start: match.part.state.time.start, end: Date.now() },
              },
            }
            yield* database.db
              .transaction(
                () =>
                  Effect.gen(function* () {
                    yield* session.updatePart(failed)
                    yield* settleRegistered(toolCallID, "error", "commit")
                  }),
                { behavior: "immediate" },
              )
              // Busy retry owns the whole result/failure transaction; replaying a
              // single statement inside it would keep the stale snapshot.
              .pipe(withBusyRetry, Effect.orDie)
            yield* settleRegistered(toolCallID, "error", "afterCommit")
            if (error instanceof PermissionV1.RejectedError || error instanceof Question.RejectedError) {
              ctx.blocked = ctx.shouldBreak
            }
            yield* settleToolCall(toolCallID)
            return true
          }),
        )
      })

      const finishReasoning = Effect.fn("SessionProcessor.finishReasoning")(function* (reasoningID: string) {
        if (!(reasoningID in ctx.reasoningMap)) return
        // oxlint-disable-next-line no-self-assign -- reactivity trigger
        ctx.reasoningMap[reasoningID].text = ctx.reasoningMap[reasoningID].text
        ctx.reasoningMap[reasoningID].time = { ...ctx.reasoningMap[reasoningID].time, end: Date.now() }
        yield* session.updatePart(ctx.reasoningMap[reasoningID])
        delete ctx.reasoningMap[reasoningID]
      })

      const ensureToolCall = Effect.fn("SessionProcessor.ensureToolCall")(function* (input: {
        id: string
        name: string
        providerExecuted?: boolean
      }) {
        const existing = yield* readToolCall(input.id)
        if (existing) {
          if (!input.providerExecuted || existing.part.metadata?.providerExecuted) return existing
          const part = yield* session.updatePart({
            ...existing.part,
            metadata: { ...existing.part.metadata, providerExecuted: true },
          })
          ctx.toolcalls[input.id] = {
            ...existing.call,
            partID: part.id,
            messageID: part.messageID,
            sessionID: part.sessionID,
          }
          return { call: ctx.toolcalls[input.id], part }
        }
        const part = yield* session.updatePart({
          id: PartID.ascending(),
          messageID: ctx.assistantMessage.id,
          sessionID: ctx.assistantMessage.sessionID,
          type: "tool",
          tool: input.name,
          callID: input.id,
          state: { status: "pending", input: {}, raw: "" },
          metadata: input.providerExecuted ? { providerExecuted: true } : undefined,
        } satisfies SessionV1.ToolPart)
        ctx.toolcalls[input.id] = {
          done: yield* Deferred.make<void>(),
          partID: part.id,
          messageID: part.messageID,
          sessionID: part.sessionID,
        }
        return { call: ctx.toolcalls[input.id], part }
      })

      const isFilePart = (value: unknown): value is SessionV1.FilePart => Schema.is(SessionV1.FilePart)(value)

      const toolResultOutput = (
        value: Extract<StreamEvent, { type: "tool-result" }>,
      ): { title: string; metadata: Record<string, any>; output: string; attachments?: SessionV1.FilePart[] } => {
        if (isRecord(value.result.value) && typeof value.result.value.output === "string") {
          return {
            title: typeof value.result.value.title === "string" ? value.result.value.title : value.name,
            metadata: isRecord(value.result.value.metadata) ? value.result.value.metadata : {},
            output: value.result.value.output,
            attachments: Array.isArray(value.result.value.attachments)
              ? value.result.value.attachments.filter(isFilePart)
              : undefined,
          }
        }
        return {
          title: value.name,
          metadata: value.result.type === "json" && isRecord(value.result.value) ? value.result.value : {},
          output:
            typeof value.result.value === "string" ? value.result.value : (JSON.stringify(value.result.value) ?? ""),
        }
      }

      const handleEvent = Effect.fnUntraced(function* (value: StreamEvent) {
        switch (value.type) {
          case "reasoning-start":
            if (value.id in ctx.reasoningMap) return
            ctx.reasoningMap[value.id] = {
              id: PartID.ascending(),
              messageID: ctx.assistantMessage.id,
              sessionID: ctx.assistantMessage.sessionID,
              type: "reasoning",
              text: "",
              time: { start: Date.now() },
              metadata: value.providerMetadata,
            }
            yield* session.updatePart(ctx.reasoningMap[value.id])
            return

          case "reasoning-delta":
            // Match dev: silently drop orphan deltas (no preceding reasoning-start).
            if (!(value.id in ctx.reasoningMap)) return
            ctx.reasoningMap[value.id].text += value.text
            if (value.providerMetadata) ctx.reasoningMap[value.id].metadata = value.providerMetadata
            yield* session.updatePartDelta({
              sessionID: ctx.reasoningMap[value.id].sessionID,
              messageID: ctx.reasoningMap[value.id].messageID,
              partID: ctx.reasoningMap[value.id].id,
              field: "text",
              delta: value.text,
            })
            return

          case "reasoning-end":
            if (value.providerMetadata && value.id in ctx.reasoningMap) {
              ctx.reasoningMap[value.id].metadata = value.providerMetadata
            }
            yield* finishReasoning(value.id)
            return

          case "tool-input-start":
            if (ctx.assistantMessage.summary) {
              return yield* new ToolCallDuringSummaryError({ tool: value.name })
            }
            yield* ensureToolCall(value)
            return

          case "tool-input-delta":
            yield* ensureToolCall(value)
            return

          case "tool-input-end": {
            yield* ensureToolCall(value)
            return
          }

          case "tool-call": {
            if (ctx.assistantMessage.summary) {
              return yield* new ToolCallDuringSummaryError({ tool: value.name })
            }
            yield* ensureToolCall(value)
            const input = isRecord(value.input) ? value.input : { value: value.input }
            yield* updateToolCall(value.id, (match) => ({
              ...match,
              tool: value.name,
              state:
                match.state.status === "running"
                  ? { ...match.state, input }
                  : {
                      status: "running",
                      input,
                      time: { start: Date.now() },
                    },
              metadata: match.metadata?.providerExecuted
                ? { ...value.providerMetadata, providerExecuted: true }
                : value.providerMetadata,
            }))

            const parts = yield* MessageV2.parts(ctx.assistantMessage.id).pipe(
              Effect.provideService(Database.Service, database),
            )
            const recentParts = parts.slice(-DOOM_LOOP_THRESHOLD)

            if (
              recentParts.length !== DOOM_LOOP_THRESHOLD ||
              !recentParts.every(
                (part) =>
                  part.type === "tool" &&
                  part.tool === value.name &&
                  part.state.status !== "pending" &&
                  JSON.stringify(part.state.input) === JSON.stringify(input),
              )
            ) {
              return
            }

            const agent = yield* agents.get(ctx.assistantMessage.agent)
            yield* permission.ask({
              permission: "doom_loop",
              patterns: [value.name],
              sessionID: ctx.assistantMessage.sessionID,
              metadata: { tool: value.name, input },
              always: [value.name],
              ruleset: agent.permission,
            })
            return
          }

          case "tool-result": {
            const toolCall = yield* readToolCall(value.id)
            if (!toolCall && value.result.type === "error") return
            if (value.result.type === "error") {
              yield* failToolCall(value.id, value.result.value)
              return
            }
            const rawOutput = toolResultOutput(value)
            const normalized = yield* Effect.forEach(rawOutput.attachments ?? [], (attachment) =>
              attachment.mime.startsWith("image/")
                ? image.normalize(attachment).pipe(
                    Effect.catchIf(
                      (error) => error instanceof Image.ResizerUnavailableError,
                      () => Effect.succeed(attachment),
                    ),
                    Effect.exit,
                  )
                : Effect.succeed(Exit.succeed<SessionV1.FilePart>(attachment)),
            )
            const omitted = normalized.filter(Exit.isFailure).length
            const attachments = normalized.filter(Exit.isSuccess).map((item) => item.value)
            const output = {
              ...rawOutput,
              output:
                omitted === 0
                  ? rawOutput.output
                  : `${rawOutput.output}\n\n[${omitted} image${omitted === 1 ? "" : "s"} omitted: could not be resized below the image size limit.]`,
              attachments: attachments.length ? attachments : undefined,
            }
            yield* completeToolCall(value.id, output)
            return
          }

          case "tool-error": {
            yield* failToolCall(value.id, value.error ?? new Error(value.message))
            return
          }

          case "provider-error":
            return yield* new LLMError({
              module: "SessionProcessor",
              method: "handleEvent",
              reason: new UnknownProviderReason({ message: value.message }),
            })

          case "step-start":
            if (!ctx.snapshot) ctx.snapshot = yield* snapshot.track()
            yield* session.updatePart({
              id: PartID.ascending(),
              messageID: ctx.assistantMessage.id,
              sessionID: ctx.sessionID,
              snapshot: ctx.snapshot,
              type: "step-start",
            })
            return

          case "step-finish": {
            const completedSnapshot = yield* snapshot.track()
            yield* Effect.forEach(Object.keys(ctx.reasoningMap), finishReasoning)
            // Anthropic reports thinking blocks it removed before the model saw the
            // prompt. Prefix mismatches mean opencode changed history behind a signed
            // block; log them so the churn can be tracked down.
            const dropped = isRecord(value.providerMetadata?.anthropic)
              ? value.providerMetadata.anthropic.inputTransformations
              : undefined
            if (Array.isArray(dropped) && dropped.length > 0) {
              yield* Effect.logWarning("thinking blocks dropped by provider", {
                sessionID: ctx.sessionID,
                messageID: ctx.assistantMessage.id,
                model: ctx.model.id,
                transformations: JSON.stringify(dropped),
              })
            }
            const usage = Session.getUsage({
              model: ctx.model,
              usage: value.usage ?? new Usage({}),
              metadata: value.providerMetadata,
            })
            ctx.assistantMessage.finish = value.reason
            ctx.assistantMessage.cost += usage.cost
            ctx.assistantMessage.tokens = usage.tokens
            yield* session.updatePart({
              id: PartID.ascending(),
              reason: value.reason,
              snapshot: completedSnapshot,
              messageID: ctx.assistantMessage.id,
              sessionID: ctx.assistantMessage.sessionID,
              type: "step-finish",
              tokens: usage.tokens,
              cost: usage.cost,
            })
            yield* session.updateMessage(ctx.assistantMessage)
            if (ctx.snapshot) {
              const patch = yield* snapshot.patch(ctx.snapshot)
              if (patch.files.length) {
                yield* session.updatePart({
                  id: PartID.ascending(),
                  messageID: ctx.assistantMessage.id,
                  sessionID: ctx.sessionID,
                  type: "patch",
                  hash: patch.hash,
                  files: patch.files,
                })
              }
              ctx.snapshot = undefined
            }
            yield* summary
              .summarize({
                sessionID: ctx.sessionID,
                messageID: ctx.assistantMessage.parentID,
              })
              .pipe(Effect.ignore, Effect.forkIn(scope))
            if (
              !ctx.assistantMessage.summary &&
              isOverflow({ cfg: yield* config.get(), tokens: usage.tokens, model: ctx.model })
            ) {
              ctx.needsCompaction = true
            }
            return
          }

          case "text-start":
            ctx.currentText = {
              id: PartID.ascending(),
              messageID: ctx.assistantMessage.id,
              sessionID: ctx.assistantMessage.sessionID,
              type: "text",
              text: "",
              time: { start: Date.now() },
              metadata: value.providerMetadata,
            }
            yield* session.updatePart(ctx.currentText)
            return

          case "text-delta":
            if (!ctx.currentText) return
            ctx.currentText.text += value.text
            if (value.providerMetadata) ctx.currentText.metadata = value.providerMetadata
            yield* session.updatePartDelta({
              sessionID: ctx.currentText.sessionID,
              messageID: ctx.currentText.messageID,
              partID: ctx.currentText.id,
              field: "text",
              delta: value.text,
            })
            return

          case "text-end":
            if (!ctx.currentText) return
            // oxlint-disable-next-line no-self-assign -- reactivity trigger
            ctx.currentText.text = ctx.currentText.text
            ctx.currentText.text = (yield* plugin.trigger(
              "experimental.text.complete",
              {
                sessionID: ctx.sessionID,
                messageID: ctx.assistantMessage.id,
                partID: ctx.currentText.id,
              },
              { text: ctx.currentText.text },
            )).text
            {
              const end = Date.now()
              ctx.currentText.time = { start: ctx.currentText.time?.start ?? end, end }
            }
            if (value.providerMetadata) ctx.currentText.metadata = value.providerMetadata
            yield* session.updatePart(ctx.currentText)
            ctx.currentText = undefined
            return

          case "finish": {
            // Terminal stream event: some providers only emit a top-level finish
            // (no step-finish). Record the reason without clobbering one a
            // step-finish already set, and skip the durable write when the
            // recorded reason is unchanged.
            const finish = ctx.assistantMessage.finish ?? value.reason
            if (finish === ctx.assistantMessage.finish) return
            ctx.assistantMessage.finish = finish
            yield* session.updateMessage(ctx.assistantMessage)
            return
          }

          default: {
            const _exhaustive: never = value
            return void _exhaustive
          }
        }
      })

      const cleanup = Effect.fn("SessionProcessor.cleanup")(function* () {
        if (ctx.snapshot) {
          const patch = yield* snapshot.patch(ctx.snapshot)
          if (patch.files.length) {
            yield* session.updatePart({
              id: PartID.ascending(),
              messageID: ctx.assistantMessage.id,
              sessionID: ctx.sessionID,
              type: "patch",
              hash: patch.hash,
              files: patch.files,
            })
          }
          ctx.snapshot = undefined
        }

        if (ctx.currentText) {
          const end = Date.now()
          ctx.currentText.time = { start: ctx.currentText.time?.start ?? end, end }
          yield* session.updatePart(ctx.currentText)
          ctx.currentText = undefined
        }

        for (const part of Object.values(ctx.reasoningMap)) {
          const end = Date.now()
          yield* session.updatePart({
            ...part,
            time: { start: part.time.start ?? end, end },
          })
        }
        ctx.reasoningMap = {}

        const deadline = Date.now() + (input.settlementDeadlineMs ?? RUN_SETTLEMENT_DEADLINE_MS)
        yield* Effect.forEach(
          Object.values(ctx.toolcalls),
          (call) =>
            Deferred.await(call.done).pipe(
              Effect.timeout(Math.max(0, deadline - Date.now())),
              Effect.ignore,
            ),
          { concurrency: "unbounded" },
        )

        for (const toolCallID of Object.keys(ctx.toolcalls)) {
          const match = yield* readToolCall(toolCallID)
          if (!match) {
            // The part is gone but a settlement may still be parked. Leave the
            // durable row `started` (indeterminate): the effect may have run.
            yield* settleRegistered(toolCallID, "error", "abandon")
            continue
          }
          const part = match.part
          const state = part.state
          if (state.status === "completed") {
            // A part already returned keeps its successful settlement.
            yield* settleRegistered(toolCallID, "ok", "settle")
            continue
          }
          if (state.status === "pending") {
            // The effect never began, so no side effect can have happened.
            yield* settleRegistered(toolCallID, "error", "settle")
          } else if (state.status === "error" && state.metadata?.interrupted !== true) {
            // A genuine tool failure already owns the settlement.
            yield* settleRegistered(toolCallID, "error", "settle")
            continue
          } else {
            // The result is still owed when the deadline is reached. Abandon the
            // settlement (do not write an error) so a later success can still
            // settle `ok`; the ledger row stays indeterminate until then.
            yield* settleRegistered(toolCallID, "error", "abandon")
            if (state.status === "error") continue
          }
          const end = Date.now()
          const metadata = "metadata" in state && isRecord(state.metadata) ? state.metadata : {}
          yield* session.updatePart({
            ...part,
            state: {
              ...state,
              status: "error",
              error: "Tool execution aborted",
              metadata: { ...metadata, interrupted: true },
              time: { start: "time" in state ? state.time.start : end, end },
            },
          })
        }
        // Defensive: a registration whose part vanished from the run snapshot
        // must still be abandoned rather than leak a false `error`. Calls the
        // loop above already abandoned stay registered for a late success.
        const drained = new Set(Object.keys(ctx.toolcalls))
        for (const toolCallID of settlements.keys()) {
          if (drained.has(toolCallID)) continue
          yield* settleRegistered(toolCallID, "error", "abandon")
        }
        ctx.toolcalls = {}
        ctx.assistantMessage.time.completed = Date.now()
        yield* session.updateMessage(ctx.assistantMessage)
      })

      const halt = Effect.fn("SessionProcessor.halt")(function* (e: unknown) {
        yield* Effect.logError("process", {
          "session.id": input.sessionID,
          messageID: input.assistantMessage.id,
          error: errorMessage(e),
          stack: e instanceof Error ? e.stack : undefined,
        })
        const error = parse(e)
        if (SessionV1.ContextOverflowError.isInstance(error)) {
          if ((yield* config.get()).compaction?.auto === false && !ctx.assistantMessage.summary) {
            ctx.assistantMessage.error = error
            ctx.assistantMessage.finish = "error"
            yield* events.publish(Session.Event.Error, { sessionID: ctx.sessionID, error })
            yield* status.set(ctx.sessionID, { type: "idle" })
            return
          }
          ctx.needsCompaction = true
          yield* events.publish(Session.Event.Error, { sessionID: ctx.sessionID, error })
          return
        }
        ctx.assistantMessage.error = error
        yield* events.publish(Session.Event.Error, {
          sessionID: ctx.assistantMessage.sessionID,
          error: ctx.assistantMessage.error,
        })
        yield* status.set(ctx.sessionID, { type: "idle" })
      })

      const process = Effect.fn("SessionProcessor.process")(function* (streamInput: LLM.StreamInput) {
        yield* Effect.logInfo("process", {
          "session.id": input.sessionID,
          messageID: input.assistantMessage.id,
        })
        ctx.needsCompaction = false
        ctx.shouldBreak = (yield* config.get()).experimental?.continue_loop_on_deny !== true

        return yield* Effect.gen(function* () {
          yield* Effect.gen(function* () {
            ctx.currentText = undefined
            ctx.reasoningMap = {}
            yield* status.set(ctx.sessionID, { type: "busy" })
            const stream = llm.stream(streamInput)

            yield* stream.pipe(
              Stream.tap((event) => handleEvent(event)),
              Stream.takeUntil(() => ctx.needsCompaction),
              Stream.runDrain,
            )
          }).pipe(
            Effect.onInterrupt(() =>
              Effect.gen(function* () {
                aborted = true
                if (!ctx.assistantMessage.error) {
                  yield* halt(new DOMException("Aborted", "AbortError"))
                }
              }),
            ),
            Effect.catchCauseIf(
              (cause) => !Cause.hasInterruptsOnly(cause),
              (cause) => Effect.fail(Cause.squash(cause)),
            ),
            Effect.retry(
              SessionRetry.policy({
                provider: input.model.providerID,
                parse,
                set: (info) => {
                  return status.set(ctx.sessionID, {
                    type: "retry",
                    attempt: info.attempt,
                    message: info.message,
                    action: info.action,
                    next: info.next,
                  })
                },
              }),
            ),
            Effect.catch(halt),
            Effect.ensuring(cleanup()),
          )

          if (ctx.needsCompaction) return "compact"
          if (ctx.blocked || ctx.assistantMessage.error) return "stop"
          return "continue"
        })
      })

      return {
        get message() {
          return ctx.assistantMessage
        },
        updateToolCall,
        completeToolCall,
        registerToolSettlement,
        process,
      } satisfies Handle
    })

    return Service.of({ create })
  }),
)

export const node = LayerNode.make({
  service: Service,
  layer: layer,
  deps: [
    Session.node,
    Config.node,
    Snapshot.node,
    Agent.node,
    LLM.node,
    Permission.node,
    Plugin.node,
    SessionSummary.node,
    SessionStatus.node,
    Image.node,
    EventV2Bridge.node,
    Database.node,
  ],
})

export * as SessionProcessor from "./processor"
