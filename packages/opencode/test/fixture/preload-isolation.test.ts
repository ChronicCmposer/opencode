/**
 * Guard tests for the database-isolation tripwire in `test/preload.ts`.
 *
 * The 2026-10-23 incident wiped the real database
 * (`~/.local/share/opencode/opencode.db`): a test run resolved the host's real
 * database path instead of an isolated one and deleted/recreated it. The
 * tripwire appended to `test/preload.ts` now aborts the WHOLE run — before any
 * test body executes — when the resolved database path is not isolated to the
 * temp dir.
 *
 * Preload cannot be trusted to catch this by simply inheriting `OPENCODE_DB`,
 * because preload deliberately clears that var and picks its own isolated
 * default. The dangerous failure is *capture before isolation*: a module that
 * reads the database flags before preload's env setup runs pins the real path
 * into the cached flag module, after which clearing the env has no effect. These
 * tests reproduce exactly that by running a child `bun test` whose preload chain
 * imports the core flag module FIRST (with `OPENCODE_DB` pointed at a sentinel),
 * then loads `test/preload.ts`.
 *
 * A missing tripwire lets the child run to completion (exit 0) without ever
 * touching the sentinel. A working tripwire aborts during preload, so the
 * sentinel survives untouched.
 */
import { describe, expect, test } from "bun:test"
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "fs"
import os from "os"
import path from "path"

const packageRoot = path.resolve(import.meta.dir, "..", "..")
const preloadPath = path.join(packageRoot, "test", "preload.ts")
// The exact source the package alias `@opencode-ai/core/flag/flag` resolves to.
// Importing it by absolute path from the probe shares the same cached module
// instance preload.ts later imports through the alias.
const flagPath = path.resolve(packageRoot, "..", "core", "src", "flag", "flag.ts")

const CHILD_BODY_MARKER = "CHILD_TEST_BODY_RAN"
const TRIVIAL_TEST = `import { test, expect } from "bun:test"
test("child test body", () => {
  console.log(${JSON.stringify(CHILD_BODY_MARKER)})
  expect(1).toBe(1)
})
`

/**
 * Build a child env: drop every `OPENCODE_*` input (especially any real
 * systemd `OPENCODE_DB`) and the isolation escape hatch, then apply overrides.
 * The child must never inherit the host database path.
 */
function isolatedChildEnv(overrides: Record<string, string>): Record<string, string> {
  const env: Record<string, string> = {}
  for (const [key, value] of Object.entries(process.env)) {
    if (value === undefined) continue
    if (key.startsWith("OPENCODE_")) continue
    if (key === "W6D_DISABLE_HOST_ISOLATION") continue
    env[key] = value
  }
  return { ...env, ...overrides }
}

type ChildResult = { exitCode: number; output: string }

/**
 * Run a child `bun test` that loads the real `test/preload.ts` under a probe
 * which pins the database flags first. Runs from a scratch cwd with no
 * bunfig.toml so the preload order is exactly [probe, preload.ts] — bunfig
 * preloads would otherwise run before any CLI `--preload`.
 */
function runChild(scratch: string, overrides: Record<string, string>): ChildResult {
  const probePath = path.join(scratch, "capture-flags-probe.ts")
  writeFileSync(
    probePath,
    `// Pin the database flags into the module cache before preload isolates them.\n` +
      `await import(${JSON.stringify(flagPath)})\n`,
  )
  writeFileSync(path.join(scratch, "trivial.test.ts"), TRIVIAL_TEST)

  const proc = Bun.spawnSync(
    [process.execPath, "test", "--preload", probePath, "--preload", preloadPath, scratch],
    {
      cwd: scratch,
      env: isolatedChildEnv(overrides),
      stdout: "pipe",
      stderr: "pipe",
    },
  )
  return {
    exitCode: proc.exitCode ?? -1,
    output: proc.stdout.toString() + proc.stderr.toString(),
  }
}

function makeScratch(): string {
  return mkdtempSync(path.join(os.tmpdir(), "w3-preload-isolation-"))
}

describe("preload database-isolation tripwire", () => {
  test("aborts a non-temp database before any test body, preserving the target", () => {
    const sentinel = path.join(os.homedir(), `.opencode-preload-sentinel-${process.pid}-${Date.now()}.db`)
    const scratch = makeScratch()
    try {
      writeFileSync(sentinel, "stand-in-for-real-database")

      const result = runChild(scratch, { OPENCODE_DB: sentinel, XDG_DATA_HOME: path.join(scratch, "xdg") })

      expect(result.exitCode).not.toBe(0)
      expect(result.output).toContain("non-temp database")
      expect(result.output).toContain(sentinel)
      // The tripwire must abort before a single test body runs.
      expect(result.output).not.toContain(CHILD_BODY_MARKER)
      // The destructive step never reached the target.
      expect(existsSync(sentinel)).toBe(true)
      expect(readFileSync(sentinel, "utf8")).toBe("stand-in-for-real-database")
    } finally {
      for (const file of [sentinel, `${sentinel}-wal`, `${sentinel}-shm`]) rmSync(file, { force: true })
      rmSync(scratch, { recursive: true, force: true })
    }
  })

  test("passes the default in-memory database", () => {
    const scratch = makeScratch()
    try {
      const result = runChild(scratch, { XDG_DATA_HOME: path.join(scratch, "xdg") })

      expect(result.exitCode).toBe(0)
      expect(result.output).toContain(CHILD_BODY_MARKER)
      expect(result.output).not.toContain("non-temp database")
    } finally {
      rmSync(scratch, { recursive: true, force: true })
    }
  })

  test("passes the temp-file database used by the host-isolation escape hatch", () => {
    const scratch = makeScratch()
    try {
      const result = runChild(scratch, {
        W6D_DISABLE_HOST_ISOLATION: "1",
        XDG_DATA_HOME: path.join(scratch, "xdg"),
      })

      expect(result.exitCode).toBe(0)
      expect(result.output).toContain(CHILD_BODY_MARKER)
      expect(result.output).not.toContain("non-temp database")
    } finally {
      rmSync(scratch, { recursive: true, force: true })
    }
  })
})
