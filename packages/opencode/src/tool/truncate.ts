import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Cause, Duration, Effect, Layer, Option, Schedule, Context } from "effect"
import path from "path"
import type { Agent } from "../agent/agent"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { evaluate } from "@/permission/evaluate"
import { Config } from "@/config/config"
import { ToolID } from "./schema"
import { TRUNCATION_DIR } from "./truncation-dir"

const RETENTION = Duration.days(7)

export const MAX_LINES = 2000
export const MAX_BYTES = 50 * 1024
/**
 * Maximum serialized size of a persisted tool result (`output` + `metadata`).
 * Results above this bound are externalized to the truncation directory and
 * referenced by path so `edit`/`write`/`read` cannot store multi-MB payloads in
 * `part`. Kept above the worst-case bounded read (~114 KB JSON) so ordinary
 * results pass through unchanged.
 */
export const MAX_TOOL_OUTPUT_BYTES = 128 * 1024

/**
 * Inline preview byte budget for an externalized result. Kept at `MAX_BYTES` (not
 * `MAX_TOOL_OUTPUT_BYTES`) so the preview reads like an ordinary bounded tool
 * result; the marker and its separator are carved out of this same budget below.
 */
const EXTERNALIZED_PREVIEW_BYTES = MAX_BYTES
const PREVIEW_SEPARATOR = "\n\n"
export const DIR = TRUNCATION_DIR
export const GLOB = path.join(TRUNCATION_DIR, "*")

export type Result = { content: string; truncated: false } | { content: string; truncated: true; outputPath: string }

export interface ResultInput {
  output: string
  metadata: Record<string, unknown>
}

export interface Options {
  maxLines?: number
  maxBytes?: number
  direction?: "head" | "tail"
}

function hasTaskTool(agent?: Agent.Info) {
  if (!agent?.permission) return false
  return evaluate("task", "*", agent.permission).action !== "deny"
}

const takePrefix = (input: string, maximumBytes: number) => {
  let bytes = 0
  let content = ""
  for (const char of input) {
    const size = Buffer.byteLength(char, "utf-8")
    if (bytes + size > maximumBytes) break
    content += char
    bytes += size
  }
  return content
}

export interface Interface {
  readonly cleanup: () => Effect.Effect<void>
  readonly write: (text: string) => Effect.Effect<string>
  /**
   * Returns output unchanged when it fits within the limits, otherwise writes the full text
   * to the truncation directory and returns a preview plus a hint to inspect the saved file.
   */
  readonly output: (text: string, options?: Options, agent?: Agent.Info) => Effect.Effect<Result>
  /**
   * Resolved truncation limits: values from `tool_output` in opencode config, or MAX_LINES / MAX_BYTES if unset.
   */
  readonly limits: () => Effect.Effect<{ maxLines: number; maxBytes: number }>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/Truncate") {}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const fs = yield* FSUtil.Service

    const cleanup = Effect.fn("Truncate.cleanup")(function* () {
      const cutoff = Date.now() - Duration.toMillis(RETENTION)
      const entries = yield* fs.readDirectory(TRUNCATION_DIR).pipe(
        Effect.map((all) => all.filter((name) => name.startsWith("tool_"))),
        Effect.catch(() => Effect.succeed([])),
      )
      for (const entry of entries) {
        const file = path.join(TRUNCATION_DIR, entry)
        const info = yield* fs.stat(file).pipe(Effect.catch(() => Effect.succeed(undefined)))
        const mtime = info && Option.getOrUndefined(info.mtime)
        if (!mtime || mtime.getTime() >= cutoff) continue
        yield* fs.remove(file).pipe(Effect.catch(() => Effect.void))
      }
    })

    const write = Effect.fn("Truncate.write")(function* (text: string) {
      const file = path.join(TRUNCATION_DIR, ToolID.ascending())
      yield* fs.ensureDir(TRUNCATION_DIR).pipe(Effect.orDie)
      yield* fs.writeFileString(file, text).pipe(Effect.orDie)
      return file
    })

    const limits = Effect.fn("Truncate.limits")(function* () {
      const configSvc = yield* Effect.serviceOption(Config.Service)
      if (Option.isNone(configSvc)) return { maxLines: MAX_LINES, maxBytes: MAX_BYTES }
      const cfg = yield* configSvc.value.get().pipe(Effect.catch(() => Effect.succeed(undefined)))
      return {
        maxLines: cfg?.tool_output?.max_lines ?? MAX_LINES,
        maxBytes: cfg?.tool_output?.max_bytes ?? MAX_BYTES,
      }
    })

    const output = Effect.fn("Truncate.output")(function* (text: string, options: Options = {}, agent?: Agent.Info) {
      const resolved = yield* limits()
      const maxLines = options.maxLines ?? resolved.maxLines
      const maxBytes = options.maxBytes ?? resolved.maxBytes
      const direction = options.direction ?? "head"
      const lines = text.split("\n")
      const totalBytes = Buffer.byteLength(text, "utf-8")

      if (lines.length <= maxLines && totalBytes <= maxBytes) {
        return { content: text, truncated: false } as const
      }

      const out: string[] = []
      let i = 0
      let bytes = 0
      let hitBytes = false

      if (direction === "head") {
        for (i = 0; i < lines.length && i < maxLines; i++) {
          const size = Buffer.byteLength(lines[i], "utf-8") + (i > 0 ? 1 : 0)
          if (bytes + size > maxBytes) {
            hitBytes = true
            break
          }
          out.push(lines[i])
          bytes += size
        }
      } else {
        for (i = lines.length - 1; i >= 0 && out.length < maxLines; i--) {
          const size = Buffer.byteLength(lines[i], "utf-8") + (out.length > 0 ? 1 : 0)
          if (bytes + size > maxBytes) {
            hitBytes = true
            break
          }
          out.unshift(lines[i])
          bytes += size
        }
      }

      const removed = hitBytes ? totalBytes - bytes : lines.length - out.length
      const unit = hitBytes ? "bytes" : "lines"
      const preview = out.join("\n")
      const file = yield* write(text)

      const hint = hasTaskTool(agent)
        ? `The tool call succeeded but the output was truncated. Full output saved to: ${file}\nUse the Task tool to have explore agent process this file with Grep and Read (with offset/limit). Do NOT read the full file yourself - delegate to save context.`
        : `The tool call succeeded but the output was truncated. Full output saved to: ${file}\nUse Grep to search the full content or Read with offset/limit to view specific sections.`

      return {
        content:
          direction === "head"
            ? `${preview}\n\n...${removed} ${unit} truncated...\n\n${hint}`
            : `...${removed} ${unit} truncated...\n\n${hint}\n\n${preview}`,
        truncated: true,
        outputPath: file,
      } as const
    })

    yield* cleanup().pipe(
      Effect.catchCause((cause) => Effect.logError("truncation cleanup failed", { cause: Cause.pretty(cause) })),
      Effect.repeat(Schedule.spaced(Duration.hours(1))),
      Effect.delay(Duration.minutes(1)),
      Effect.forkScoped,
    )

    return Service.of({ cleanup, write, output, limits })
  }),
)

/**
 * Bounds a complete tool result before persistence, reusing `service.output`
 * for text truncation and `service.write` for externalization. Results at or
 * below `MAX_TOOL_OUTPUT_BYTES` keep the existing text truncation behavior;
 * larger results are written to the truncation directory and replaced by a
 * bounded preview plus an `outputPath` reference. Keeping this a standalone
 * helper means every tool-result construction site shares one policy without
 * widening the `Truncate` service contract.
 */
export const boundResult = Effect.fn("Truncate.boundResult")(function* (
  service: Pick<Interface, "output" | "write">,
  input: ResultInput,
  agent?: Agent.Info,
) {
  const payload = JSON.stringify({ output: input.output, metadata: input.metadata })
  const bytes = Buffer.byteLength(payload, "utf-8")
  if (bytes <= MAX_TOOL_OUTPUT_BYTES) {
    const text = yield* service.output(input.output, {}, agent)
    const metadata: Record<string, unknown> = {
      ...input.metadata,
      truncated: text.truncated || input.metadata.truncated === true,
      ...(text.truncated ? { outputPath: text.outputPath } : {}),
    }
    return { output: text.content, metadata }
  }

  const file = yield* service.write(payload)
  const marker = `... tool result truncated; full result saved to ${file} ...`
  const prefix = takePrefix(
    input.output,
    Math.max(0, EXTERNALIZED_PREVIEW_BYTES - Buffer.byteLength(marker, "utf-8") - PREVIEW_SEPARATOR.length),
  )
  const metadata: Record<string, unknown> = {
    ...input.metadata,
    truncated: true,
    outputPath: file,
    originalBytes: bytes,
  }
  return { output: prefix ? `${prefix}${PREVIEW_SEPARATOR}${marker}` : marker, metadata }
})

export const node = LayerNode.make({ service: Service, layer: layer, deps: [FSUtil.node] })

export * as Truncate from "./truncate"
