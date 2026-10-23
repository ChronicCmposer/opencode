/**
 * Guard tests for the httpapi-exercise database isolation.
 *
 * The harness runs via `bun run script/httpapi-exercise.ts`, so it does NOT load
 * `test/preload.ts`. Its `environment.ts` points `OPENCODE_DB` / `Flag.OPENCODE_DB`
 * at the exercise database and the runner calls `resetDatabase()` (runner.ts:265),
 * which deletes that file. If `OPENCODE_HTTPAPI_EXERCISE_DB` is pointed at a real
 * database, the harness would delete it. `environment.ts` must refuse to load a
 * non-temp database before anything can touch it.
 *
 * These tests spawn a subprocess that imports `environment.ts` exactly like the
 * harness does, then emulates the destructive `resetDatabase()` step so a missing
 * guard is observable as a deleted sentinel.
 */
import { describe, expect, test } from "bun:test"
import { existsSync, readFileSync, rmSync, writeFileSync } from "fs"
import os from "os"
import path from "path"

const here = import.meta.dir
const environmentPath = path.join(here, "environment.ts")
const packageRoot = path.resolve(here, "../../..")

/** Build a child env from the current one with explicit overrides/deletions. */
function childEnv(overrides: Record<string, string | undefined>): Record<string, string> {
  const env: Record<string, string> = {}
  for (const [key, value] of Object.entries(process.env)) if (value !== undefined) env[key] = value
  for (const [key, value] of Object.entries(overrides)) {
    if (value === undefined) delete env[key]
    else env[key] = value
  }
  return env
}

/**
 * Run a probe that imports `environment.ts` then deletes `OPENCODE_DB` the same
 * way `resetDatabase()` does. A working guard makes the import throw, so the
 * deletion never runs and the target survives.
 */
function runProbe(overrides: Record<string, string | undefined>) {
  const script = `
const envPath = ${JSON.stringify(environmentPath)}
try {
  await import(envPath)
  const { rm } = await import("fs/promises")
  const db = process.env.OPENCODE_DB
  await rm(db, { force: true })
  await rm(db + "-wal", { force: true })
  await rm(db + "-shm", { force: true })
  console.log("PROBE_OK " + db)
  process.exit(0)
} catch (error) {
  console.error(error && error.message ? error.message : String(error))
  process.exit(7)
}
`
  const proc = Bun.spawnSync(["bun", "-e", script], {
    cwd: packageRoot,
    env: childEnv(overrides),
    stdout: "pipe",
    stderr: "pipe",
  })
  return {
    exitCode: proc.exitCode,
    stdout: proc.stdout.toString(),
    stderr: proc.stderr.toString(),
  }
}

describe("httpapi-exercise environment isolation guard", () => {
  test("refuses a non-temp OPENCODE_HTTPAPI_EXERCISE_DB and preserves the target", () => {
    const sentinel = path.join(here, `.w4-non-temp-sentinel-${process.pid}.db`)
    try {
      rmSync(sentinel, { force: true })
      writeFileSync(sentinel, "stand-in-for-real-database")

      const result = runProbe({ OPENCODE_HTTPAPI_EXERCISE_DB: sentinel })

      expect(result.exitCode).toBe(7)
      expect(result.stderr).toContain("non-temp database")
      expect(result.stderr).toContain(sentinel)
      // The destructive resetDatabase() step must never have reached the target.
      expect(existsSync(sentinel)).toBe(true)
      expect(readFileSync(sentinel, "utf8")).toBe("stand-in-for-real-database")
    } finally {
      rmSync(sentinel, { force: true })
      rmSync(`${sentinel}-wal`, { force: true })
      rmSync(`${sentinel}-shm`, { force: true })
    }
  })

  test("accepts the default temp database path", () => {
    const result = runProbe({ OPENCODE_HTTPAPI_EXERCISE_DB: undefined, TMPDIR: os.tmpdir() })

    expect(result.stderr).toBe("")
    expect(result.exitCode).toBe(0)
    expect(result.stdout).toContain("PROBE_OK")
    const db = result.stdout.trim().slice("PROBE_OK ".length)
    expect(db).toContain("opencode-httpapi-exercise-")
    expect(path.relative(os.tmpdir(), db).startsWith("..")).toBe(false)
  })
})
