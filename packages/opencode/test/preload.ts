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
const isBusy = (error: unknown): boolean =>
  typeof error === "object" && error !== null && "code" in error && error.code === "EBUSY"

/**
 * Remove the per-run temp directory. The caller awaits `AppRuntime.dispose()`
 * first — that is the disposal signal that releases the instances' SQLite
 * handles — so on POSIX the first `rm` succeeds. Windows can still hold WAL
 * handles until GC finalizers run, so there we retry a bounded number of times
 * instead of a fixed-sleep loop.
 */
async function removeDirectory(target: string, attempts = 10): Promise<void> {
  for (let attempt = 1; attempt <= attempts; attempt++) {
    try {
      await fs.rm(target, { recursive: true, force: true })
      return
    } catch (error) {
      if (!isBusy(error) || process.platform !== "win32" || attempt === attempts) throw error
      Bun.gc(true)
      await sleep(50 * attempt)
    }
  }
}

afterAll(async () => {
  const { AppRuntime } = await import("../src/effect/app-runtime")
  await AppRuntime.dispose()
  await removeDirectory(dir)
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

// Provider/server credentials and OTLP wiring from the ambient shell must not
// leak into the suite either; clear the whole set from one table.
const HOST_SECRET_VARS = [
  "ANTHROPIC_API_KEY",
  "OPENAI_API_KEY",
  "GOOGLE_API_KEY",
  "GOOGLE_GENERATIVE_AI_API_KEY",
  "AZURE_OPENAI_API_KEY",
  "AWS_ACCESS_KEY_ID",
  "AWS_PROFILE",
  "AWS_REGION",
  "AWS_BEARER_TOKEN_BEDROCK",
  "OPENROUTER_API_KEY",
  "LLM_GATEWAY_API_KEY",
  "GROQ_API_KEY",
  "MISTRAL_API_KEY",
  "PERPLEXITY_API_KEY",
  "TOGETHER_API_KEY",
  "XAI_API_KEY",
  "DEEPSEEK_API_KEY",
  "FIREWORKS_API_KEY",
  "CEREBRAS_API_KEY",
  "SAMBANOVA_API_KEY",
  "OPENCODE_SERVER_PASSWORD",
  "OPENCODE_SERVER_USERNAME",
  "OPENCODE_EXPERIMENTAL",
  "OPENCODE_ENABLE_EXPERIMENTAL_MODELS",
  "OTEL_EXPORTER_OTLP_ENDPOINT",
  "OTEL_EXPORTER_OTLP_HEADERS",
  "OTEL_RESOURCE_ATTRIBUTES",
] as const
for (const key of HOST_SECRET_VARS) delete process.env[key]

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

// Now safe to import from src/.
//
// Projector-wiring seam: `initProjectors` is currently a synchronous no-op stub
// (`src/server/projectors.ts`), so it is called, not awaited. Keep it here,
// above the fail-fast tripwire, so the tripwire stays the last statement.
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
