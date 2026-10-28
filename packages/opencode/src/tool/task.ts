import * as Tool from "./tool"
import DESCRIPTION from "./task.txt"
import { ToolJsonSchema } from "./json-schema"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { BackgroundJob } from "@/background/job"
import { Session } from "@/session/session"
import { SessionDepth } from "@/session/depth"
import type { DispatchOrigin } from "@/session/origin"
import { SessionID, MessageID } from "../session/schema"
import { MessageV2 } from "../session/message-v2"
import { Agent } from "../agent/agent"
import { deriveSubagentSessionPermission } from "../agent/subagent-permissions"
import type { SessionPrompt } from "../session/prompt"
import { SessionScopeError } from "../session/worker/subagent"
import { Config } from "@/config/config"
import { Cause, Effect, Exit, Schema, Scope } from "effect"
import { EffectBridge } from "@/effect/bridge"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { Database } from "@opencode-ai/core/database/database"

export interface TaskPromptOps {
  cancel(sessionID: SessionID): Effect.Effect<void>
  resolvePromptParts(template: string): Effect.Effect<SessionPrompt.PromptInput["parts"]>
  prompt(input: SessionPrompt.PromptInput): Effect.Effect<SessionV1.WithParts>
}

const id = "task"
const BACKGROUND_DESCRIPTION = [
  "Background mode: background=true launches the subagent asynchronously and returns immediately.",
  "Foreground is the default; use it when you need the result before continuing.",
  "Use background only for independent work that can run while you continue elsewhere.",
  "You will be notified automatically when it finishes.",
].join(" ")
const BACKGROUND_STARTED = [
  "The task is working in the background. You will be notified automatically when it finishes.",
  "DO NOT sleep, poll for progress, ask the task for status, or duplicate this task's work — avoid working with the same files or topics it is using.",
  "Work on non-overlapping tasks, or briefly tell the user what you launched and end your response.",
].join("\n")
const BACKGROUND_UPDATED = [
  "Additional context sent to the running background task.",
  "The task is still working in the background. You will be notified automatically when it finishes.",
  "DO NOT sleep, poll for progress, ask the task for status, or duplicate this task's work — avoid working with the same files or topics it is using.",
  "Work on non-overlapping tasks, or briefly tell the user what you sent and end your response.",
].join("\n")

const BaseParameterFields = {
  description: Schema.String.annotate({ description: "A short (3-5 words) description of the task" }),
  prompt: Schema.String.annotate({ description: "The task for the agent to perform" }),
  subagent_type: Schema.String.annotate({ description: "The type of specialized agent to use for this task" }),
  task_id: Schema.optional(Schema.String).annotate({
    description:
      "This should only be set if you mean to resume a previous task (you can pass a prior task_id and the task will continue the same subagent session as before instead of creating a fresh one)",
  }),
  command: Schema.optional(Schema.String).annotate({ description: "The command that triggered this task" }),
}

const BaseParameters = Schema.Struct(BaseParameterFields)

export const Parameters = Schema.Struct({
  ...BaseParameterFields,
  background: Schema.optional(Schema.Boolean).annotate({
    description:
      "Run the agent in the background. You will be notified when it completes. DO NOT sleep, poll, or proactively check on its progress",
  }),
})

function renderOutput(input: {
  sessionID: SessionID
  state: "running" | "completed" | "error"
  summary?: string
  text: string
}) {
  const tag = input.state === "error" ? "task_error" : "task_result"
  return [
    `<task id="${input.sessionID}" state="${input.state}">`,
    ...(input.summary ? [`<summary>${input.summary}</summary>`] : []),
    `<${tag}>`,
    input.text,
    `</${tag}>`,
    "</task>",
  ].join("\n")
}

/** The child result's terminal text, or the failure the task tool surfaces. */
function requireTaskText(sessionID: SessionID, result: SessionV1.WithParts): Effect.Effect<string, Error> {
  if (result.info.role === "assistant" && result.info.error) {
    const message =
      "message" in result.info.error.data && typeof result.info.error.data.message === "string"
        ? result.info.error.data.message
        : result.info.error.name
    return Effect.fail(new Error(`Subagent failed (task_id: ${sessionID}): ${message}`))
  }
  const failed = result.parts.findLast((item) => item.type === "tool" && item.state.status === "error")
  if (failed?.type === "tool" && failed.state.status === "error") {
    return Effect.fail(new Error(`Subagent failed (task_id: ${sessionID}): ${failed.state.error}`))
  }
  return Effect.succeed(result.parts.findLast((item) => item.type === "text")?.text ?? "")
}

export const TaskTool = Tool.define(
  id,
  Effect.gen(function* () {
    const agent = yield* Agent.Service
    const background = yield* BackgroundJob.Service
    const config = yield* Config.Service
    const sessions = yield* Session.Service
    const scope = yield* Scope.Scope
    const flags = yield* RuntimeFlags.Service
    const database = yield* Database.Service

    const run = Effect.fn("TaskTool.execute")(function* (
      params: Schema.Schema.Type<typeof Parameters>,
      ctx: Tool.Context,
    ) {
      const cfg = yield* config.get()
      const runInBackground = params.background === true
      if (runInBackground && !flags.experimentalBackgroundSubagents) {
        return yield* Effect.fail(
          new Error("Background subagents require OPENCODE_EXPERIMENTAL_BACKGROUND_SUBAGENTS=true"),
        )
      }

      const parent = yield* sessions.get(ctx.sessionID)
      const depthLimit = cfg.subagent_depth ?? 1
      const childDepth = SessionDepth.nextDepth(parent.metadata)
      if (SessionDepth.exceedsSubagentDepth(childDepth, depthLimit)) {
        return yield* Effect.fail(new Error(SessionDepth.subagentDepthMessage(depthLimit)))
      }

      if (!ctx.extra?.bypassAgentCheck) {
        yield* ctx.ask({
          permission: id,
          patterns: [params.subagent_type],
          always: ["*"],
          metadata: {
            description: params.description,
            subagent_type: params.subagent_type,
          },
        })
      }

      const next = yield* agent.get(params.subagent_type)
      if (!next) {
        return yield* Effect.fail(new Error(`Unknown agent type: ${params.subagent_type} is not a valid agent type`))
      }

      const session = !params.task_id
        ? undefined
        : yield* sessions.get(SessionID.make(params.task_id)).pipe(
            Effect.catchCause((cause) => {
              // A bound worker's hub-scoped read refuses a foreign, sibling,
              // ancestor or grandchild row. That is a scope rejection, not a
              // missing session: never turn it into a fresh child.
              if (Cause.squash(cause) instanceof SessionScopeError) {
                return Effect.fail(
                  new Error(
                    `Task ${params.task_id} is not a direct child of this session; only direct children can be resumed`,
                  ),
                )
              }
              return Effect.succeed(undefined)
            }),
          )
      // A resume may only reach a DIRECT child of this session. A descendant
      // (grandchild) or a sibling is inside another run's authority; the hub
      // spawn would reject it, but fail here before any permission ask or row
      // work so the task never acts on an id it may not resume.
      //
      // This is a worker-boundary rule: in flag-off mode `sessions.get` returns
      // any persisted row and resuming a non-direct task was always allowed. The
      // hub-scoped read already rejects a foreign id with `SessionScopeError`
      // above, so only worker mode adds the explicit parent check.
      if (flags.experimentalSessionWorkers && session !== undefined && session.parentID !== ctx.sessionID) {
        return yield* Effect.fail(
          new Error(
            `Task ${params.task_id} is not a direct child of this session; only direct children can be resumed`,
          ),
        )
      }
      const childPermission = deriveSubagentSessionPermission({
        parentSessionPermission: parent.permission ?? [],
        subagent: next,
      })
      const childToolDenies = [
        ...(next.permission.some((rule) => rule.permission === "todowrite")
          ? []
          : [{ permission: "todowrite" as const, pattern: "*" as const, action: "deny" as const }]),
        ...(next.permission.some((rule) => rule.permission === id)
          ? []
          : [{ permission: id, pattern: "*" as const, action: "deny" as const }]),
        ...(cfg.experimental?.primary_tools?.map((permission) => ({
          permission,
          pattern: "*" as const,
          action: "deny" as const,
        })) ?? []),
      ]
      const nextSession =
        session ??
        (yield* sessions.create({
          parentID: ctx.sessionID,
          title: params.description + ` (@${next.name} subagent)`,
          agent: next.name,
          // Stamp the depth the hub spawn will persist, before the child exists:
          // a create that succeeds but whose spawn is refused still leaves a
          // correctly-classified child, and the hub enforces the same value.
          metadata: { depth: childDepth },
          permission: [
            ...childPermission,
            ...childToolDenies.filter(
              (deny) =>
                !childPermission.some(
                  (rule) =>
                    rule.permission === deny.permission && rule.pattern === deny.pattern && rule.action === deny.action,
                ),
            ),
          ],
        }))

      const msg = yield* MessageV2.get({ sessionID: ctx.sessionID, messageID: ctx.messageID }).pipe(
        Effect.provideService(Database.Service, database),
        Effect.orDie,
      )
      if (msg.info.role !== "assistant") return yield* Effect.fail(new Error("Not an assistant message"))
      const variant = msg.info.variant

      const model = next.model ?? {
        modelID: msg.info.modelID,
        providerID: msg.info.providerID,
      }
      const metadata = {
        parentSessionId: ctx.sessionID,
        sessionId: nextSession.id,
        model,
        ...(runInBackground ? { background: true } : {}),
      }

      yield* ctx.metadata({
        title: params.description,
        metadata,
      })

      const ops = ctx.extra?.promptOps as TaskPromptOps
      if (!ops) return yield* Effect.fail(new Error("TaskTool requires promptOps in ctx.extra"))

      const runSpawn = Effect.fn("TaskTool.runSpawn")(function* () {
        const parts = yield* ops.resolvePromptParts(params.prompt)
        // Background/auto work must fail fast on a dead worker; a foreground task
        // is an explicit user action and may reset crash supervision.
        const origin: DispatchOrigin = runInBackground ? "auto" : "user"
        const spawned = yield* sessions.spawn({
          parentID: ctx.sessionID,
          childID: nextSession.id,
          type: id,
          title: params.description,
          origin,
          metadata,
          depthLimit,
          prompt: {
            messageID: MessageID.ascending(),
            sessionID: nextSession.id,
            model: {
              modelID: model.modelID,
              providerID: model.providerID,
            },
            variant: next.model ? undefined : variant,
            agent: next.name,
            parts,
          },
        })
        return yield* requireTaskText(spawned.childID, spawned.result)
      })

      const inject = Effect.fn("TaskTool.injectBackgroundResult")(function* (
        state: "completed" | "error",
        text: string,
      ) {
        const currentParent = yield* sessions.get(ctx.sessionID)
        yield* ops
          .prompt({
            sessionID: ctx.sessionID,
            agent: currentParent.agent ?? ctx.agent,
            variant,
            parts: [
              {
                type: "text",
                synthetic: true,
                text: renderOutput({
                  sessionID: nextSession.id,
                  state,
                  summary:
                    state === "completed"
                      ? `Background task completed: ${params.description}`
                      : `Background task failed: ${params.description}`,
                  text,
                }),
              },
            ],
          })
          .pipe(Effect.ignore, Effect.forkIn(scope, { startImmediately: true }))
      })

      const notify = Effect.fn("TaskTool.notifyBackgroundResult")(function* (jobID: string) {
        yield* background.wait({ id: jobID }).pipe(
          Effect.flatMap((result) => {
            if (result.info?.status === "completed") return inject("completed", result.info.output ?? "")
            if (result.info?.status === "error") return inject("error", result.info.error ?? "")
            return Effect.void
          }),
          Effect.forkIn(scope, { startImmediately: true }),
        )
      })

      // The notification target is also the job id: a promoted job is only
      // allowed to outlive this call once it has an id the parent can be
      // notified through. No id means no background persistence.
      const notificationID = nextSession.id
      // The job always carries the id it would notify through; only promotion
      // makes it outlive the call, so an unpromoted foreground job is consumed
      // by its own wait while a background one has a durable notification key.
      const jobMetadata = { ...metadata, notificationID }

      const backgroundMetadata = {
        ...metadata,
        background: true,
        jobId: notificationID,
        notificationID,
      }

      const updatedResult = () => ({
        title: params.description,
        metadata: backgroundMetadata,
        output: renderOutput({
          sessionID: nextSession.id,
          state: "running",
          summary: "Background task updated",
          text: BACKGROUND_UPDATED,
        }),
      })

      const runningResult = () => ({
        title: params.description,
        metadata: backgroundMetadata,
        output: renderOutput({
          sessionID: nextSession.id,
          state: "running",
          summary: "Background task started",
          text: BACKGROUND_STARTED,
        }),
      })

      // Every task is registered as a local job, foreground included, so the
      // parent can promote an in-flight foreground subagent exactly as the
      // historical SubagentJob contract allowed. Promotion installs the
      // notification observer; an unpromoted foreground job settles and is
      // consumed by the caller's own `wait`.
      const startJob = (work: Effect.Effect<string, unknown>, options?: { rejectIfRunning?: boolean }) =>
        background
          .start({
            id: nextSession.id,
            type: id,
            title: params.description,
            metadata: jobMetadata,
            parentID: ctx.sessionID,
            ...(options?.rejectIfRunning === true ? { rejectIfRunning: true } : {}),
            onPromote: Effect.all([
              ctx.metadata({ title: params.description, metadata: backgroundMetadata }),
              notify(notificationID),
            ]),
            run: work.pipe(Effect.onInterrupt(() => ops.cancel(nextSession.id))),
          })
          .pipe(
            Effect.catchIf(
              (error): error is BackgroundJob.JobAlreadyRunningError =>
                error instanceof BackgroundJob.JobAlreadyRunningError,
              (error) =>
                Effect.fail(
                  new Error(`task session ${error.id} is already running; wait for it to finish before resuming`),
                ),
            ),
          )

      // A background child outlives this tool call, so the abort listener must be
      // installed before the early return — or a parent abort would leave the
      // child running. It is removed once the job settles.
      const awaitBackgroundAbort = Effect.fnUntraced(function* () {
        const runAbort = yield* EffectBridge.make()
        const abortChild = Effect.all([ops.cancel(nextSession.id), background.cancel(nextSession.id)], {
          discard: true,
        })
        function onBackgroundAbort() {
          runAbort.fork(abortChild)
        }
        ctx.abort.addEventListener("abort", onBackgroundAbort)
        if (ctx.abort.aborted) onBackgroundAbort()
        yield* background.wait({ id: nextSession.id }).pipe(
          Effect.ensuring(
            Effect.sync(() => {
              ctx.abort.removeEventListener("abort", onBackgroundAbort)
            }),
          ),
          Effect.forkIn(scope, { startImmediately: true }),
        )
      })

      // Background joins/registers a local job whose run awaits the child, so the
      // parent is notified through the existing background machinery. Foreground
      // awaits the spawn directly.
      if (runInBackground) {
        if (yield* background.extend({ id: nextSession.id, run: runSpawn() })) {
          // A foreground job has no background notification observer yet.
          // Promote it through the same single `onPromote` path Ctrl+B uses:
          // notifying here directly would let a later promotion deliver a
          // second completion injection, and promotion also stamps the job as
          // `background: true` for the hub mirror.
          const extended = yield* background.get(nextSession.id)
          if (extended && extended.metadata?.background !== true) {
            yield* background.promote(nextSession.id)
            yield* awaitBackgroundAbort()
          }
          return updatedResult()
        }
        const info = yield* startJob(runSpawn())
        yield* notify(notificationID)
        yield* awaitBackgroundAbort()
        return runningResult()
      }

      const runCancel = yield* EffectBridge.make()
      const cancel = ops.cancel(nextSession.id)

      function onAbort() {
        runCancel.fork(cancel)
      }

      return yield* Effect.acquireUseRelease(
        Effect.sync(() => {
          ctx.abort.addEventListener("abort", onAbort)
        }),
        () =>
          Effect.gen(function* () {
            const info = yield* startJob(runSpawn(), { rejectIfRunning: true })
            // Foreground races the child's own settlement against a promotion
            // signal. `waitForPromotion` only resolves for a running job that a
            // parent explicitly backgrounded; otherwise the wait wins.
            const outcome = yield* Effect.raceFirst(
              background
                .wait({ id: info.id })
                .pipe(Effect.map((waited) => ({ promoted: false as const, info: waited.info }))),
              background
                .waitForPromotion(info.id)
                .pipe(Effect.map((promoted) => ({ promoted: true as const, info: promoted }))),
            )
            if (outcome.promoted || outcome.info?.metadata?.background === true) {
              if (outcome.info?.id === undefined) {
                return yield* Effect.fail(
                  new Error("Background promotion has no notificationID; refusing to persist the subagent"),
                )
              }
              // The foreground abort listener is removed when this tool call
              // returns; a promoted child keeps running, so re-install the
              // background listener or a later parent abort would orphan it.
              yield* awaitBackgroundAbort()
              return runningResult()
            }
            if (outcome.info?.status === "error") {
              return yield* Effect.fail(new Error(outcome.info.error ?? "Task failed"))
            }
            if (outcome.info?.status === "cancelled") {
              return yield* Effect.fail(new Error("Task cancelled"))
            }
            return {
              title: params.description,
              metadata,
              output: renderOutput({
                sessionID: nextSession.id,
                state: "completed",
                text: outcome.info?.output ?? "",
              }),
            }
          }),
        (_, exit) =>
          Effect.gen(function* () {
            if (Exit.hasInterrupts(exit)) {
              yield* Effect.all([cancel, background.cancel(nextSession.id)], { discard: true })
            }
          }).pipe(
            Effect.ensuring(
              Effect.sync(() => {
                ctx.abort.removeEventListener("abort", onAbort)
              }),
            ),
          ),
      )
    })

    return {
      description: flags.experimentalBackgroundSubagents
        ? [DESCRIPTION, BACKGROUND_DESCRIPTION].join("\n\n")
        : DESCRIPTION,
      parameters: Parameters,
      jsonSchema: flags.experimentalBackgroundSubagents ? undefined : ToolJsonSchema.fromSchema(BaseParameters),
      execute: (params: Schema.Schema.Type<typeof Parameters>, ctx: Tool.Context) =>
        run(params, ctx).pipe(Effect.orDie),
    }
  }),
)
