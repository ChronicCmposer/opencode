// Phase 6.4 — worker-local tool registry + `tool.execute.before/after` hooks.
//
// The tool registry and its execute pipeline run in the worker isolate: the
// model-facing tool call is dispatched here, `tool.execute.before` hooks may
// rewrite the arguments, the tool runs, then `tool.execute.after` hooks may
// rewrite the result before it is published. The hub owns nothing on this path
// except the plugin *definitions* it shipped in the worker bootstrap.

import type { ToolAfterOutput, ToolBeforeOutput, WorkerPluginHost } from "./plugin-host"

export interface WorkerToolContext {
  readonly sessionID: string
  readonly callID: string
}

/**
 * The single hook input shape shared by `tool.execute.before` and
 * `tool.execute.after`. `args` is the live (possibly rewritten) argument bag.
 */
export interface WorkerToolHookInput extends WorkerToolContext {
  readonly tool: string
  args: unknown
}

export interface WorkerToolResult {
  readonly title: string
  readonly output: string
  readonly metadata: unknown
}

export interface WorkerTool {
  readonly name: string
  execute(args: unknown, context: WorkerToolContext): Promise<WorkerToolResult>
}

export interface WorkerToolHost {
  register(tool: WorkerTool): void
  list(): readonly string[]
  has(name: string): boolean
  execute(name: string, args: unknown, context: WorkerToolContext): Promise<WorkerToolResult>
}

export interface WorkerToolHostOptions {
  /** Plugin host supplying `tool.execute.before/after` hooks; optional. */
  readonly plugins?: WorkerPluginHost
}

/** Raised when a tool is executed but never registered (fail loud). */
export class UnknownToolError extends Error {
  constructor(name: string) {
    super(`unknown tool: ${name}`)
    this.name = "UnknownToolError"
  }
}

/** Raised when a tool name is registered twice (fail loud). */
export class DuplicateToolError extends Error {
  constructor(name: string) {
    super(`tool already registered: ${name}`)
    this.name = "DuplicateToolError"
  }
}

export function createWorkerToolHost(options: WorkerToolHostOptions = {}): WorkerToolHost {
  const tools = new Map<string, WorkerTool>()
  const plugins = options.plugins

  const register = (tool: WorkerTool): void => {
    if (tools.has(tool.name)) throw new DuplicateToolError(tool.name)
    tools.set(tool.name, tool)
  }

  const execute = async (name: string, args: unknown, context: WorkerToolContext): Promise<WorkerToolResult> => {
    const tool = tools.get(name)
    if (tool === undefined) throw new UnknownToolError(name)

    const input: WorkerToolHookInput = {
      tool: name,
      sessionID: context.sessionID,
      callID: context.callID,
      args,
    }
    const before: ToolBeforeOutput = { args }
    if (plugins !== undefined) await plugins.toolBefore(input, before)
    input.args = before.args

    const result = await tool.execute(before.args, context)

    const after: ToolAfterOutput = {
      title: result.title,
      output: result.output,
      metadata: result.metadata,
    }
    if (plugins !== undefined) await plugins.toolAfter(input, after)

    return { title: after.title, output: after.output, metadata: after.metadata }
  }

  return {
    register,
    list: () => [...tools.keys()],
    has: (name) => tools.has(name),
    execute,
  }
}

export * as WorkerToolHostModule from "./tool-host"
