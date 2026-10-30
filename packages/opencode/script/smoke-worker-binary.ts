#!/usr/bin/env bun
// Packaged-binary smoke gate for the experimental session-worker path.
//
// It boots the *built* artifact (`dist/<target>/bin/opencode`, not source) with
// `OPENCODE_EXPERIMENTAL_SESSION_WORKERS=true` against a local OpenAI-compatible
// mock provider, drives a real session over HTTP, and asserts the assistant's
// streamed events reach `GET /global/event`.
//
// This is the missing tier that catches the packaged-worker boot bug: when
// `src/session/worker/session-worker.ts` is not an entrypoint of the compiled
// binary, the isolate fails to load with
//
//   BuildMessage: ModuleNotFound resolving "/$bunfs/root/session-worker.ts"
//
// and the run dies as the generic `SessionCrashedError: worker 1 crashed`. No
// `message.part.*` / `session.status` frame ever reaches the client, so the user
// sees a prompt that never streams. In-process tests cannot catch this because
// they never execute the bundled artifact.
//
// Run:
//   bun run script/smoke-worker-binary.ts
//   OPENCODE_SMOKE_BINARY=/path/to/opencode bun run script/smoke-worker-binary.ts
//
// Exit code is 0 on PASS and 1 on FAIL; both print a human-readable report.

import { existsSync, mkdirSync, mkdtempSync, readFileSync, realpathSync, rmSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import path from "node:path"
import { pathToFileURL } from "node:url"
import { startMockProvider, type MockProviderScript } from "../test/lib/mock-provider"
import { readSsePayloads } from "../test/lib/sse"
import { testProviderConfig } from "../test/lib/test-provider"

// --- Contract ---------------------------------------------------------------

/** EventV2 types a healthy flag-ON worker run must surface on `/global/event`. */
export const REQUIRED_EVENT_TYPES = ["message.part.updated", "message.part.delta", "session.status"] as const

/** Log fragments that identify the packaged-worker boot failure this gate exists for. */
export const FAILURE_SIGNATURES: readonly RegExp[] = [
  /worker\s+\d+\s+crashed/i,
  /ModuleNotFound[^\n]*session-worker/i,
  /Cannot find module[^\n]*session-worker/i,
  /SessionCrashedError/i,
  /undefined is not an object[^\n]*project/i,
  // Writer isolate opened an un-migrated DB (defect C): a raw SQL failure on the
  // first worker query, before any streamed frame.
  /no such table: \w+/i,
  /Failed query: \w+/i,
  /ModuleNotFound/i,
  /UnsupportedPluginHookError|unsupported worker hook/i,
]

/**
 * A local plugin declaring every hook the worker triggers outside the chat path.
 * Its `text.complete` appends this marker, so the gate proves the hook ran on the
 * hub and its result reached the client.
 */
export const TEXT_COMPLETE_MARKER = "[smoke:text.complete]"

/**
 * The smoke plugin. Its `smoke_context` tool runs on the hub and records the
 * context fields plugins rely on in-process (`callID`, `messages`, `extra.model`).
 */
const smokePlugin = (contextFile: string) => `export default async () => ({
  "experimental.text.complete": async (_input, output) => {
    output.text = output.text + " ${TEXT_COMPLETE_MARKER}"
  },
  "command.execute.before": async () => {},
  "experimental.session.compacting": async () => {},
  "experimental.compaction.autocontinue": async () => {},
  tool: {
    smoke_context: {
      description: "Record the plugin tool context.",
      args: {},
      execute: async (_args, ctx) => {
        await Bun.write(${JSON.stringify(contextFile)}, JSON.stringify({
          callID: ctx.callID,
          messages: Array.isArray(ctx.messages) && ctx.messages.length > 0,
          model: typeof ctx.extra?.model?.id === "string",
        }))
        return "recorded"
      },
    },
  },
})
`

/** The tool call id the mock model gives the plugin tool; the hub must hand it to the plugin. */
const SMOKE_CONTEXT_CALL_ID = "call_smoke_context"

/** Prompt marker that makes the mock model call the write tool. */
const TOOL_TURN_PROMPT = "Write the smoke file."
const TOOL_TURN_FILE = "smoke-tool.txt"
const TOOL_TURN_CONTENT = "written by the smoke tool turn"

const SERVER_PASSWORD = "smoke-worker-gate"
const SERVER_USERNAME = "opencode"
const READY_TIMEOUT_MS = 30_000
const EVENT_TIMEOUT_MS = 20_000
const POLL_INTERVAL_MS = 25

export interface SmokeResult {
  readonly passed: boolean
  /** Absolute path of the artifact under test. */
  readonly binary: string
  /** Event types observed on `/global/event` (order of first appearance). */
  readonly observedTypes: readonly string[]
  /** Required types that never arrived. */
  readonly missing: readonly string[]
  /** Matched failure-signature strings, for the RED diagnostic. */
  readonly signatures: readonly string[]
  /** Server stderr/stdout tail, trimmed for reporting. */
  readonly logTail: string
  /** Server process exit code if it died during the run, else `null`. */
  readonly serverExitCode: number | null
  /** A human-readable one-line summary. */
  readonly summary: string
}

// --- Binary discovery -------------------------------------------------------

function targetName(): string {
  const os = process.platform === "win32" ? "windows" : process.platform
  return `opencode-${os}-${process.arch}`
}

/**
 * Absolute path of the packaged binary to smoke. Honors `OPENCODE_SMOKE_BINARY`
 * (useful in CI where the artifact is staged elsewhere) and otherwise resolves
 * the native target directory produced by `script/build.ts`.
 */
export function smokeBinaryPath(explicit = process.env["OPENCODE_SMOKE_BINARY"]): string {
  if (explicit) return path.resolve(explicit)
  const packageDir = path.resolve(import.meta.dir, "..")
  return path.join(packageDir, "dist", targetName(), "bin", "opencode")
}

// --- Environment ------------------------------------------------------------

/**
 * A hermetic child environment. Built from scratch (never spread from
 * `process.env`) so an inherited `OPENCODE_SERVER_PASSWORD`,
 * `OPENCODE_EXPERIMENTAL*`, or `OPENCODE_CONFIG_DIR` cannot leak into the gate.
 */
function isolatedEnv(home: string, configJSON: string): Record<string, string> {
  const dataDir = path.join(home, "data")
  return {
    PATH: process.env["PATH"] ?? "",
    TMPDIR: tmpdir(),
    HOME: home,
    XDG_CONFIG_HOME: path.join(home, ".config"),
    XDG_DATA_HOME: dataDir,
    XDG_STATE_HOME: path.join(home, ".local/state"),
    XDG_CACHE_HOME: path.join(home, ".cache"),
    OPENCODE_DB: path.join(dataDir, "opencode", "opencode.db"),
    OPENCODE_CONFIG_CONTENT: configJSON,
    OPENCODE_DISABLE_PROJECT_CONFIG: "1",
    OPENCODE_DISABLE_AUTOUPDATE: "1",
    OPENCODE_DISABLE_MODELS_FETCH: "1",
    OPENCODE_DISABLE_AUTOCOMPACT: "1",
    // Not OPENCODE_PURE: it drops configured plugins, and the gate's own smoke
    // plugin must load. HOME and the inline config already isolate the run.
    OPENCODE_EXPERIMENTAL_SESSION_WORKERS: "true",
    OPENCODE_SERVER_PASSWORD: SERVER_PASSWORD,
    OPENCODE_SERVER_USERNAME: SERVER_USERNAME,
  }
}

function authHeaders(): Record<string, string> {
  const token = Buffer.from(`${SERVER_USERNAME}:${SERVER_PASSWORD}`).toString("base64")
  return { authorization: `Basic ${token}` }
}

// --- Procedural helpers -----------------------------------------------------

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms))

/** Drain a byte stream into `sink` (text chunks); swallows the close error. */
function captureStream(stream: ReadableStream<Uint8Array>, sink: string[]): Promise<void> {
  const reader = stream.getReader()
  const decoder = new TextDecoder()
  return (async () => {
    try {
      for (;;) {
        const { done, value } = await reader.read()
        if (done) return
        sink.push(decoder.decode(value, { stream: true }))
      }
    } catch {
      // Stream closed with the process; nothing to report.
    }
  })()
}

function observedType(event: unknown): string | undefined {
  if (event === null || typeof event !== "object") return undefined
  const payload = (event as { payload?: unknown }).payload
  if (payload === null || typeof payload !== "object") return undefined
  const type = (payload as { type?: unknown }).type
  return typeof type === "string" ? type : undefined
}

/** Match the "listening on http://host:port" sentinel out of captured stdout. */
function parseListeningUrl(stdout: string): string | undefined {
  return /listening on (http:\/\/[^\s]+)/.exec(stdout)?.[1]
}

// --- The smoke run ----------------------------------------------------------

/**
 * Boot the packaged binary, run one prompt, and assert the required worker
 * events crossed to `/global/event`. Never throws for an expected RED gate —
 * it returns a structured result so callers (the CLI or the bun test) can report
 * and assert. Throws only for setup errors that are not the bug under test
 * (binary missing, mock provider failed to start).
 */
export async function runSmokeWorkerBinary(): Promise<SmokeResult> {
  const binary = smokeBinaryPath()
  if (!existsSync(binary)) {
    throw new Error(`packaged binary not found at ${binary}; build it first (bun run script/build.ts --single)`)
  }

  const home = realpathSync(mkdtempSync(path.join(tmpdir(), "oc-smoke-worker-")))
  const worktree = path.join(home, "worktree")
  const toolTurnPath = path.join(worktree, TOOL_TURN_FILE)
  await using provider = startMockProvider({
    chunks: ["Hel", "lo"],
    select: (body) => selectToolTurnScript(body, toolTurnPath),
  })
  const pluginFile = path.join(home, "smoke-plugin.js")
  const contextFile = path.join(home, "smoke-context.json")
  writeFileSync(pluginFile, smokePlugin(contextFile))
  const configJSON = JSON.stringify({
    ...testProviderConfig(provider.baseUrl),
    model: "test/test-model",
    plugin: [pathToFileURL(pluginFile).href],
  })
  mkdirSync(worktree, { recursive: true })
  const git = Bun.spawnSync(["git", "init", "--quiet"], { cwd: worktree })
  if (git.exitCode !== 0) throw new Error(`git init failed: ${git.stderr.toString()}`)

  const stdout: string[] = []
  const stderr: string[] = []
  const observed: string[] = []
  let serverExitCode: number | null = null

  const server = Bun.spawn(
    [binary, "serve", "--hostname", "127.0.0.1", "--port", "0", "--print-logs", "--log-level", "DEBUG"],
    {
      cwd: worktree,
      env: isolatedEnv(home, configJSON),
      stdout: "pipe",
      stderr: "pipe",
    },
  )
  const stdoutDrain = captureStream(server.stdout, stdout)
  const stderrDrain = captureStream(server.stderr, stderr)
  void server.exited.then((code) => {
    serverExitCode = code
  })

  const sseAbort = new AbortController()
  const eventTypeSeen = new Set<string>()
  let markerSeen = false
  let sseDrain: Promise<void> | undefined

  try {
    const url = await waitForListening(stdout, () => serverExitCode)
    if (url === undefined) {
      return result(false, {
        binary,
        observedTypes: observed,
        missing: [...REQUIRED_EVENT_TYPES],
        signatures: findSignatures(stdout, stderr),
        logTail: logTail(stdout, stderr),
        serverExitCode,
        summary: `server never printed its listening URL (exit=${serverExitCode ?? "running"})`,
      })
    }

    // Subscribe to global events BEFORE prompting so the first frame is captured.
    const eventResponse = await fetch(`${url}/global/event`, { headers: authHeaders(), signal: sseAbort.signal })
    if (!eventResponse.ok || eventResponse.body === null) {
      throw new Error(`GET /global/event failed: HTTP ${eventResponse.status}`)
    }
    sseDrain = readSsePayloads(eventResponse.body, (event) => {
      if (!markerSeen && JSON.stringify(event).includes(TEXT_COMPLETE_MARKER)) markerSeen = true
      const type = observedType(event)
      if (type === undefined || eventTypeSeen.has(type)) return
      eventTypeSeen.add(type)
      observed.push(type)
    }).catch(() => {
      // Aborted at teardown.
    })

    const session = await createSession(url, worktree)
    await promptAsync(url, worktree, session)
    await waitForEventTypes(eventTypeSeen, [...REQUIRED_EVENT_TYPES])
    const markerDeadline = Date.now() + EVENT_TIMEOUT_MS
    while (!markerSeen && Date.now() < markerDeadline) await sleep(POLL_INTERVAL_MS)

    const missing: string[] = REQUIRED_EVENT_TYPES.filter((type) => !eventTypeSeen.has(type))
    if (!markerSeen) missing.push(`text.complete marker ${TEXT_COMPLETE_MARKER}`)
    // A failed turn still streams parts and goes idle; the error only shows as this event.
    if (eventTypeSeen.has("session.error")) missing.push("no session.error")

    // Tool turn: a second session in the git-initialized worktree writes a file.
    const toolSession = await createSession(url, worktree)
    await promptAsync(url, worktree, toolSession, TOOL_TURN_PROMPT)
    const toolDeadline = Date.now() + EVENT_TIMEOUT_MS
    while (Date.now() < toolDeadline && !toolTurnWrote(toolTurnPath)) await sleep(POLL_INTERVAL_MS)
    if (!toolTurnWrote(toolTurnPath)) missing.push(`tool turn write of ${TOOL_TURN_FILE}`)
    const context = pluginToolContext(contextFile)
    if (context?.callID !== SMOKE_CONTEXT_CALL_ID) missing.push(`plugin tool ctx.callID ${SMOKE_CONTEXT_CALL_ID}`)
    if (context?.messages !== true) missing.push("plugin tool ctx.messages")
    if (context?.model !== true) missing.push("plugin tool ctx.extra.model")
    // Let the follow-up reply land so a late failure still shows up as session.error.
    await sleep(1_000)
    if (eventTypeSeen.has("session.error") && !missing.includes("no session.error")) missing.push("no session.error")

    const signatures = findSignatures(stdout, stderr)
    const passed = missing.length === 0 && signatures.length === 0
    return result(passed, {
      binary,
      observedTypes: observed,
      missing,
      signatures,
      logTail: logTail(stdout, stderr),
      serverExitCode,
      summary: passed
        ? `session worker streamed ${observed.length} event types to /global/event`
        : `missing ${missing.join(", ")}${signatures.length > 0 ? ` — ${signatures[0]}` : ""}`,
    })
  } finally {
    sseAbort.abort()
    await sseDrain
    await stopServer(server)
    await Promise.allSettled([stdoutDrain, stderrDrain])
    rmSync(home, { recursive: true, force: true })
  }
}

/** The slice of `Bun.Subprocess` teardown needs (avoids the generic spawn types). */
interface KillableServer {
  readonly exitCode: number | null
  readonly exited: Promise<number>
  readonly kill: (signal?: number | NodeJS.Signals) => void
}

/**
 * Script for the tool-turn prompt only: the first model request calls `write`,
 * the follow-up (last message is the tool result) replies with text. Title and
 * other tool-less requests, and the plain greeting prompt, use the default script.
 */
function selectToolTurnScript(body: Record<string, unknown>, filePath: string): MockProviderScript | undefined {
  const tools = body["tools"]
  if (!Array.isArray(tools) || tools.length === 0) return undefined
  const messages = Array.isArray(body["messages"])
    ? (body["messages"] as Array<{ role?: unknown; content?: unknown }>)
    : []
  if (!JSON.stringify(messages).includes(TOOL_TURN_PROMPT)) return undefined
  if (messages.at(-1)?.role === "tool") return { chunks: ["Wrote the file."] }
  return {
    toolCalls: [
      {
        id: "call_smoke_write",
        name: "write",
        arguments: JSON.stringify({ filePath, content: TOOL_TURN_CONTENT }),
      },
      { id: SMOKE_CONTEXT_CALL_ID, name: "smoke_context", arguments: "{}" },
    ],
  }
}

function pluginToolContext(
  contextFile: string,
): { readonly callID?: unknown; readonly messages?: unknown; readonly model?: unknown } | undefined {
  if (!existsSync(contextFile)) return undefined
  return JSON.parse(readFileSync(contextFile, "utf8"))
}

function toolTurnWrote(filePath: string): boolean {
  return existsSync(filePath) && readFileSync(filePath, "utf8") === TOOL_TURN_CONTENT
}

/** Terminate the child, escalating to SIGKILL so a wedged server cannot hang the gate. */
async function stopServer(server: KillableServer): Promise<void> {
  if (server.exitCode !== null) return
  server.kill()
  const exited = await Promise.race([server.exited, sleep(3_000).then(() => false as const)])
  if (exited === false && server.exitCode === null) {
    server.kill("SIGKILL")
    await server.exited
  }
}

// --- HTTP driving -----------------------------------------------------------

async function waitForListening(stdout: string[], isDead: () => number | null): Promise<string | undefined> {
  const deadline = Date.now() + READY_TIMEOUT_MS
  while (Date.now() < deadline) {
    const url = parseListeningUrl(stdout.join(""))
    if (url !== undefined) return url
    if (isDead() !== null) return undefined
    await sleep(POLL_INTERVAL_MS)
  }
  return undefined
}

async function createSession(url: string, directory: string): Promise<string> {
  const response = await fetch(`${url}/session`, {
    method: "POST",
    headers: { ...authHeaders(), "content-type": "application/json", "x-opencode-directory": directory },
    body: JSON.stringify({ title: "smoke-worker-binary" }),
  })
  if (!response.ok) throw new Error(`POST /session failed: HTTP ${response.status} ${await response.text()}`)
  const body = (await response.json()) as { id?: unknown }
  if (typeof body.id !== "string") throw new Error(`POST /session returned no session id: ${JSON.stringify(body)}`)
  return body.id
}

async function promptAsync(
  url: string,
  directory: string,
  sessionID: string,
  text = "Reply with a short greeting.",
): Promise<void> {
  const response = await fetch(`${url}/session/${sessionID}/prompt_async`, {
    method: "POST",
    headers: { ...authHeaders(), "content-type": "application/json", "x-opencode-directory": directory },
    body: JSON.stringify({
      model: { providerID: "test", modelID: "test-model" },
      parts: [{ type: "text", text }],
    }),
  })
  if (response.status !== 204) {
    throw new Error(`POST /session/:id/prompt_async failed: HTTP ${response.status} ${await response.text()}`)
  }
}

/** Poll the observed set until every required type arrives or the deadline passes. */
async function waitForEventTypes(seen: Set<string>, required: readonly string[]): Promise<void> {
  const deadline = Date.now() + EVENT_TIMEOUT_MS
  while (Date.now() < deadline) {
    if (required.every((type) => seen.has(type))) return
    await sleep(POLL_INTERVAL_MS)
  }
}

// --- Reporting --------------------------------------------------------------

function findSignatures(stdout: string[], stderr: string[]): string[] {
  const log = stdout.join("") + stderr.join("")
  const found: string[] = []
  for (const pattern of FAILURE_SIGNATURES) {
    const match = pattern.exec(log)
    if (match !== null) found.push(match[0].trim())
  }
  return found
}

function logTail(stdout: string[], stderr: string[], max = 4_000): string {
  const combined = stdout.join("") + stderr.join("")
  return combined.length > max ? combined.slice(-max) : combined
}

function result(passed: boolean, fields: Omit<SmokeResult, "passed">): SmokeResult {
  return { passed, ...fields }
}

function printReport(result: SmokeResult): void {
  const tag = result.passed ? "PASS" : "FAIL"
  console.log(`\n[smoke-worker-binary] ${tag}: ${result.summary}`)
  console.log(`  binary:   ${result.binary}`)
  console.log(`  observed: ${result.observedTypes.length > 0 ? result.observedTypes.join(", ") : "(none)"}`)
  if (result.missing.length > 0) console.log(`  missing:  ${result.missing.join(", ")}`)
  if (result.signatures.length > 0) console.log(`  signature: ${result.signatures.join(" | ")}`)
  if (result.serverExitCode !== null) console.log(`  server exit: ${result.serverExitCode}`)
  if (!result.passed && result.logTail.length > 0) {
    console.log("  --- server log tail ---")
    console.log(result.logTail)
    console.log("  --- end log tail ---")
  }
}

if (import.meta.main) {
  try {
    const result = await runSmokeWorkerBinary()
    printReport(result)
    process.exit(result.passed ? 0 : 1)
  } catch (error) {
    console.error(`[smoke-worker-binary] FAIL: ${error instanceof Error ? error.message : String(error)}`)
    process.exit(1)
  }
}
