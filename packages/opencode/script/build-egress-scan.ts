#!/usr/bin/env bun

// Builds the native events-frame scanner (native/egress-scan) with the system C compiler. `script/build.ts` calls
// this for the linux arm64 release binary; running it directly builds the addon for the current host for dev and tests.

import { $ } from "bun"
import path from "path"
import { fileURLToPath } from "url"

const packageDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..")
const sourceDir = path.join(packageDir, "native/egress-scan")

export const EGRESS_SCAN_SOURCES = ["scan.c", "addon.c"].map((file) => path.join(sourceDir, file))
export const EGRESS_SCAN_ADDON = path.join(sourceDir, "build/egress-scan.node")

export interface EgressScanHost {
  readonly platform: string
  readonly arch: string
}

export interface EgressScanTarget {
  readonly os: string
  readonly arch: string
  readonly abi?: string
}

export class EgressScanCompilerMissingError extends Error {
  constructor(readonly tried: readonly string[]) {
    super(
      `The native egress scanner needs a C compiler on the linux arm64 build host, but none of ${tried.join(", ")} was found on PATH. Install one (for example \`apt-get install build-essential\`) and rebuild.`,
    )
    this.name = "EgressScanCompilerMissingError"
  }
}

/** The addon ships in the linux arm64 glibc binary only, and only a linux arm64 host can build it natively. */
export function embedsEgressScan(host: EgressScanHost, target: EgressScanTarget): boolean {
  return (
    host.platform === "linux" &&
    host.arch === "arm64" &&
    target.os === "linux" &&
    target.arch === "arm64" &&
    target.abi === undefined
  )
}

export function egressScanCompileCommand(host: EgressScanHost, compiler: string, output: string): string[] {
  const common = [
    "-O3",
    "-std=c11",
    "-fPIC",
    "-fvisibility=hidden",
    "-Wall",
    "-Wextra",
    "-I" + path.join(sourceDir, "include"),
  ]
  const link = host.platform === "darwin" ? ["-shared", "-undefined", "dynamic_lookup"] : ["-shared"]
  return [compiler, ...common, ...link, "-o", output, ...EGRESS_SCAN_SOURCES]
}

export interface BuildEgressScanOptions {
  readonly host?: EgressScanHost
  readonly output?: string
  /** Resolves a compiler name to a path; defaults to `Bun.which`. */
  readonly which?: (name: string) => string | null
  readonly run?: (command: readonly string[]) => Promise<void>
}

/** Compile the addon for `host`; throws `EgressScanCompilerMissingError` when no C compiler is installed. */
export async function buildEgressScan(options: BuildEgressScanOptions = {}): Promise<string> {
  const host = options.host ?? { platform: process.platform, arch: process.arch }
  const output = options.output ?? EGRESS_SCAN_ADDON
  const which = options.which ?? Bun.which
  const candidates = host.platform === "darwin" ? ["clang", "cc"] : ["cc", "clang", "gcc"]
  const compiler = candidates.map((name) => which(name)).find((found) => found !== null && found !== undefined)
  if (compiler === undefined || compiler === null) throw new EgressScanCompilerMissingError(candidates)
  await (options.run ?? defaultRun)(["mkdir", "-p", path.dirname(output)])
  await (options.run ?? defaultRun)(egressScanCompileCommand(host, compiler, output))
  return output
}

async function defaultRun(command: readonly string[]): Promise<void> {
  await $`${command}`
}

if (import.meta.main) {
  console.log(`built ${await buildEgressScan()}`)
}
