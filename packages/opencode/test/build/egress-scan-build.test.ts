// Build gate for the native egress scanner: where it is embedded, what happens when the host has no C compiler,
// and (on a host that can) that the dev helper produces a loadable addon.

import { describe, expect, test } from "bun:test"
import { createRequire } from "node:module"
import { mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import { encodeEventsFrameBytes } from "@/session/egress/frame-bytes"
import { endFrame } from "@/session/worker/frame"
import {
  EgressScanCompilerMissingError,
  buildEgressScan,
  egressScanCompileCommand,
  embedsEgressScan,
} from "../../script/build-egress-scan"

const linuxArm64 = { platform: "linux", arch: "arm64" }

describe("embedsEgressScan", () => {
  test("only a linux arm64 host building the glibc linux arm64 target embeds the addon", () => {
    expect(embedsEgressScan(linuxArm64, { os: "linux", arch: "arm64" })).toBe(true)
    expect(embedsEgressScan(linuxArm64, { os: "linux", arch: "arm64", abi: "musl" })).toBe(false)
    expect(embedsEgressScan(linuxArm64, { os: "linux", arch: "x64" })).toBe(false)
    expect(embedsEgressScan(linuxArm64, { os: "darwin", arch: "arm64" })).toBe(false)
    expect(embedsEgressScan({ platform: "linux", arch: "x64" }, { os: "linux", arch: "arm64" })).toBe(false)
    expect(embedsEgressScan({ platform: "darwin", arch: "arm64" }, { os: "linux", arch: "arm64" })).toBe(false)
  })
})

describe("buildEgressScan", () => {
  test("fails with a clear message when no C compiler is on the host", async () => {
    const run = async () => {
      throw new Error("must not run")
    }
    const error = await buildEgressScan({ host: linuxArm64, which: () => null, run }).catch((caught) => caught)
    expect(error).toBeInstanceOf(EgressScanCompilerMissingError)
    expect(error.message).toContain("C compiler")
    expect(error.message).toContain("cc, clang, gcc")
  })

  test("uses the first compiler found and passes shared-object flags on linux", async () => {
    const commands: string[][] = []
    const output = await buildEgressScan({
      host: linuxArm64,
      output: "/out/egress-scan.node",
      which: (name) => (name === "clang" ? "/usr/bin/clang" : null),
      run: async (command) => void commands.push([...command]),
    })
    expect(output).toBe("/out/egress-scan.node")
    expect(commands[0]).toEqual(["mkdir", "-p", "/out"])
    const compile = commands[1]
    expect(compile[0]).toBe("/usr/bin/clang")
    for (const flag of ["-O3", "-fPIC", "-shared"]) expect(compile).toContain(flag)
    expect(compile).not.toContain("dynamic_lookup")
    expect(compile.at(-1)).toMatch(/addon\.c$/)
    expect(compile.slice(compile.indexOf("-o"), compile.indexOf("-o") + 2)).toEqual(["-o", "/out/egress-scan.node"])
  })

  test("macOS links with undefined symbols resolved at load time", () => {
    const command = egressScanCompileCommand({ platform: "darwin", arch: "arm64" }, "clang", "/out/x.node")
    expect(command).toContain("dynamic_lookup")
  })
})

const hostCanBuild = process.platform === "darwin" || process.platform === "linux"

describe.skipIf(!hostCanBuild)("dev helper", () => {
  test("builds an addon this process can load and use", async () => {
    const dir = await mkdtemp(path.join(tmpdir(), "egress-scan-build-"))
    try {
      const output = await buildEgressScan({ output: path.join(dir, "egress-scan.node") })
      const addon = createRequire(import.meta.url)(output) as { scan: (bytes: Uint8Array) => unknown }
      const bytes = new Uint8Array(encodeEventsFrameBytes(endFrame(1)))
      expect(addon.scan(bytes)).toEqual({ ok: true, kind: "end", id: 1 })
    } finally {
      await rm(dir, { recursive: true, force: true })
    }
  })
})
