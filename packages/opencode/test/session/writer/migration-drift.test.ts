// Migration codegen drift guard: fail on drift.
//
// `src/session/writer/migration.statements.gen.ts` is generated from the core
// schema + journal by `script/migration-statements.ts`. The writer isolate ships
// that snapshot to open a fresh database, so a core schema change that is not
// regenerated would leave the writer's schema stale. This test runs the
// generator's own `--check` mode in a subprocess: it compares the committed file
// against freshly rendered output and exits non-zero on drift.

import { describe, expect, test } from "bun:test"
import path from "node:path"

/** The package root (`packages/opencode`), two levels above this test's directory. */
const packageRoot = path.resolve(import.meta.dir, "../../..")
const generator = path.join(packageRoot, "script", "migration-statements.ts")

describe("writer migration statements are not stale (drift guard)", () => {
  test(
    "script/migration-statements.ts --check exits zero",
    async () => {
      const proc = Bun.spawn([process.execPath, generator, "--check"], {
        cwd: packageRoot,
        stdout: "pipe",
        stderr: "pipe",
      })
      const [stdout, stderr, exitCode] = await Promise.all([
        new Response(proc.stdout).text(),
        new Response(proc.stderr).text(),
        proc.exited,
      ])
      // Surface the generator's own diagnostics on drift so the fix is obvious.
      if (exitCode !== 0) throw new Error(`migration drift detected:\n${stdout}\n${stderr}`)
      expect(exitCode).toBe(0)
    },
    60_000,
  )
})
