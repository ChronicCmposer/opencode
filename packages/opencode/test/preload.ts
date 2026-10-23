// IMPORTANT: Set env vars BEFORE any imports from src/ directory
// xdg-basedir reads env vars at import time, so we must set these first
import os from "os"
import path from "path"
import fs from "fs/promises"
import { setTimeout as sleep } from "node:timers/promises"
import { afterAll } from "bun:test"

// Host config isolation: the ambient shell/harness exports OPENCODE_* config
// inputs (global config content, a config dir, project-config disabling, the
// host's real database, worker sizing/logging, ...). Left in place they leak into
// every suite and make the result depend on the machine running the tests. Clear
// them before any src/ import so the suite starts from a known-clean baseline.
// Tests that need a knob set it themselves at runtime, after this runs.
const HOST_CONFIG_VARS = [
  "OPENCODE",
  "OPENCODE_BIN",
  "OPENCODE_CONFIG",
  "OPENCODE_CONFIG_CONTENT",
  "OPENCODE_CONFIG_DIR",
  "OPENCODE_DB",
  "OPENCODE_DISABLE_AUTOUPDATE",
  "OPENCODE_DISABLE_PROJECT_CONFIG",
  "OPENCODE_EXPERIMENTAL_SESSION_WORKERS",
  "OPENCODE_LOG_LEVEL",
  "OPENCODE_PID",
  "OPENCODE_PRINT_LOGS",
  "OPENCODE_SESSION_WORKER_OVERSUBSCRIPTION",
  "OPENCODE_BASH_IONICE",
  "OPENCODE_BASH_NICE",
  "OPENCODE_BASH_SCHED",
] as const

// Verification escape hatch for the isolation above. A caller forcing the
// session-worker rollout flag on for a one-off verification run sets
// `W6D_DISABLE_HOST_ISOLATION=1`; that preserves ONLY
// `OPENCODE_EXPERIMENTAL_SESSION_WORKERS` so `...=on` survives into the suite,
// while every other host input — including the host's real `OPENCODE_DB` — is
// still cleared. The isolation therefore never weakens beyond forcing that flag.
const hostIsolationDisabled = process.env["W6D_DISABLE_HOST_ISOLATION"] === "1"
const isPreservedForVerification = (key: string): boolean =>
  hostIsolationDisabled && key === "OPENCODE_EXPERIMENTAL_SESSION_WORKERS"
for (const key of HOST_CONFIG_VARS) {
  if (isPreservedForVerification(key)) continue
  delete process.env[key]
}

// Set XDG env vars FIRST, before any src/ imports
const dir = path.join(os.tmpdir(), "opencode-test-data-" + process.pid)
await fs.mkdir(dir, { recursive: true })
afterAll(async () => {
  const { AppRuntime } = await import("../src/effect/app-runtime")
  await AppRuntime.dispose()

  const busy = (error: unknown) =>
    typeof error === "object" && error !== null && "code" in error && error.code === "EBUSY"
  const rm = async (left: number): Promise<void> => {
    Bun.gc(true)
    await sleep(100)
    return fs.rm(dir, { recursive: true, force: true }).catch((error) => {
      if (!busy(error)) throw error
      if (left <= 1 && process.platform !== "win32") throw error
      if (left <= 1) return
      return rm(left - 1)
    })
  }

  // Windows can keep SQLite WAL handles alive until GC finalizers run, so we
  // force GC and retry teardown to avoid flaky EBUSY in test cleanup.
  await rm(30)
})

process.env["XDG_DATA_HOME"] = path.join(dir, "share")
process.env["XDG_CACHE_HOME"] = path.join(dir, "cache")
process.env["XDG_CONFIG_HOME"] = path.join(dir, "config")
process.env["XDG_STATE_HOME"] = path.join(dir, "state")
process.env["OPENCODE_MODELS_PATH"] = path.join(import.meta.dir, "tool", "fixtures", "models-api.json")
process.env["OPENCODE_EXPERIMENTAL_EVENT_SYSTEM"] = "true"
process.env["OPENCODE_EXPERIMENTAL_WORKSPACES"] = "true"

// Set test home directory to isolate tests from user's actual home directory
// This prevents tests from picking up real user configs/skills from ~/.claude/skills
const testHome = path.join(dir, "home")
await fs.mkdir(testHome, { recursive: true })
process.env["OPENCODE_TEST_HOME"] = testHome

// Set test managed config directory to isolate tests from system managed settings
const testManagedConfigDir = path.join(dir, "managed")
process.env["OPENCODE_TEST_MANAGED_CONFIG_DIR"] = testManagedConfigDir

// Write the cache version file to prevent global/index.ts from clearing the cache
const cacheDir = path.join(dir, "cache", "opencode")
await fs.mkdir(cacheDir, { recursive: true })
await fs.writeFile(path.join(cacheDir, "version"), "14")

// Clear provider and server auth env vars to ensure clean test state
delete process.env["ANTHROPIC_API_KEY"]
delete process.env["OPENAI_API_KEY"]
delete process.env["GOOGLE_API_KEY"]
delete process.env["GOOGLE_GENERATIVE_AI_API_KEY"]
delete process.env["AZURE_OPENAI_API_KEY"]
delete process.env["AWS_ACCESS_KEY_ID"]
delete process.env["AWS_PROFILE"]
delete process.env["AWS_REGION"]
delete process.env["AWS_BEARER_TOKEN_BEDROCK"]
delete process.env["OPENROUTER_API_KEY"]
delete process.env["LLM_GATEWAY_API_KEY"]
delete process.env["GROQ_API_KEY"]
delete process.env["MISTRAL_API_KEY"]
delete process.env["PERPLEXITY_API_KEY"]
delete process.env["TOGETHER_API_KEY"]
delete process.env["XAI_API_KEY"]
delete process.env["DEEPSEEK_API_KEY"]
delete process.env["FIREWORKS_API_KEY"]
delete process.env["CEREBRAS_API_KEY"]
delete process.env["SAMBANOVA_API_KEY"]
delete process.env["OPENCODE_SERVER_PASSWORD"]
delete process.env["OPENCODE_SERVER_USERNAME"]
delete process.env["OPENCODE_EXPERIMENTAL"]
delete process.env["OPENCODE_ENABLE_EXPERIMENTAL_MODELS"]
delete process.env["OTEL_EXPORTER_OTLP_ENDPOINT"]
delete process.env["OTEL_EXPORTER_OTLP_HEADERS"]
delete process.env["OTEL_RESOURCE_ATTRIBUTES"]

// One shared database for the hub and the session worker's writer isolate.
//
// The session worker's `Database.Service` is writer-backed: every statement is
// forwarded over the writer port to the hub-owned writer isolate, which opens
// `Database.path()` as its OWN SQLite connection. SQLite `:memory:` is
// per-connection, so the hub's in-process connection and the writer isolate's
// connection would be two DIFFERENT databases — a worker `Session.get` then sees
// zero rows (NotFoundError -> HTTP 500). Two connections can only share state
// through a file, so when a session-worker path may run (the verification
// override above) we point `OPENCODE_DB` at an isolated per-run temp file,
// removed with `dir` below. The default suite never runs a worker path, so it
// keeps the cheap in-memory database. A harness that needs a specific database
// sets `OPENCODE_DB` after this preload (as `test/server/httpapi-exercise`
// does); preload only chooses the default.
process.env["OPENCODE_DB"] = hostIsolationDisabled ? path.join(dir, "opencode-test.db") : ":memory:"

// Now safe to import from src/
const { initProjectors } = await import("../src/server/projectors")

initProjectors()

// Fail-fast tripwire. Last in the file so every src module has already captured
// its flags: if the resolved database path is not isolated to the temp dir,
// abort the entire run before a single test body executes. This guards the
// 2026-10-23 capture-before-isolation failure, where a module pinned the host's
// real database path into the flag module and the env isolation above came too
// late — the suite then opened (and deleted) the real database. Do not swallow
// the error: an aborted run is the point.
const { Database } = await import("@opencode-ai/core/database/database")
const { assertIsolatedDatabasePath } = await import("./fixture/isolation")
await assertIsolatedDatabasePath(Database.path())
