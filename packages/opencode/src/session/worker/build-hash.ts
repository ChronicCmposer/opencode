// Build identity for the session-worker isolate.
//
// The hash covers exactly two inputs: the worker entry bundle bytes and the
// worker protocol schema version. Plugins, configuration, provider data, and
// runtime env are deliberately excluded, so a plugin change never forces a
// worker rebuild and a protocol change always does.
//
// The ready handshake carries the hash. A hub and worker that disagree fail with
// a typed `BuildMismatch`; there is no fallback to the in-process path. A
// development-only override (`OPENCODE_BUILD_HASH_OVERRIDE`) is logged at error
// level and surfaced through `buildHashStatus` so status consumers can see that
// the strict check was bypassed.

import { createHash } from "node:crypto"

import { Schema } from "effect"

import { WORKER_PROTOCOL_SCHEMA_VERSION } from "./frame"

export const PROTOCOL_SCHEMA_VERSION = WORKER_PROTOCOL_SCHEMA_VERSION

/** Runtime override honoured only when the caller explicitly asks for a dev hash. */
export const BUILD_HASH_OVERRIDE_ENV = "OPENCODE_BUILD_HASH_OVERRIDE"

const HASH_DOMAIN = "opencode.session-worker.build.v1"
const DEV_ENTRY = "opencode.session-worker.dev"

declare global {
  /** Compile-time worker build hash, injected by `script/build.ts`. */
  const OPENCODE_BUILD_HASH: string | undefined
}

/** Severity-compatible logger seam; mirrors the worker logger without importing it. */
export type BuildHashLogger = (
  message: string,
  fields: Record<string, unknown>,
  level?: "info" | "warn" | "error",
) => void

export interface BuildHashInput {
  readonly entryBundle: string | Uint8Array | ArrayBuffer
  readonly schemaVersion?: number
}

/**
 * Hash the worker entry bundle plus the protocol schema version. Deterministic
 * for the same bytes; plugins and runtime state are not inputs.
 */
export function computeBuildHash(input: BuildHashInput): string {
  const schemaVersion = input.schemaVersion ?? PROTOCOL_SCHEMA_VERSION
  const bundle =
    typeof input.entryBundle === "string"
      ? new TextEncoder().encode(input.entryBundle)
      : input.entryBundle instanceof Uint8Array
        ? input.entryBundle
        : new Uint8Array(input.entryBundle)

  return createHash("sha256")
    .update(HASH_DOMAIN)
    .update("\0")
    .update(String(schemaVersion))
    .update("\0")
    .update(bundle)
    .digest("hex")
}

export type BuildHashSource = "build" | "override" | "dev"

export interface ResolvedBuildHash {
  readonly hash: string
  readonly schemaVersion: number
  readonly source: BuildHashSource
  readonly override: boolean
}

export interface ResolveBuildHashOptions {
  readonly env?: Record<string, string | undefined>
  readonly definedHash?: string
  readonly schemaVersion?: number
  readonly log?: BuildHashLogger
}

const definedBuildHash = (): string | undefined =>
  typeof OPENCODE_BUILD_HASH !== "undefined" && OPENCODE_BUILD_HASH !== "" ? OPENCODE_BUILD_HASH : undefined

/** Resolve the build identity, logging an active development override at error. */
export function resolveBuildHash(options: ResolveBuildHashOptions = {}): ResolvedBuildHash {
  const env = options.env ?? process.env
  const schemaVersion = options.schemaVersion ?? PROTOCOL_SCHEMA_VERSION
  const defined = options.definedHash ?? definedBuildHash()
  const override = env[BUILD_HASH_OVERRIDE_ENV]

  if (override !== undefined && override !== "") {
    options.log?.(
      "session worker build hash override active",
      {
        level: "error",
        buildHash: override,
      },
      "error",
    )
    return { hash: override, schemaVersion, source: "override", override: true }
  }

  if (defined !== undefined && defined !== "") {
    return { hash: defined, schemaVersion, source: "build", override: false }
  }

  return {
    hash: computeBuildHash({ entryBundle: DEV_ENTRY, schemaVersion }),
    schemaVersion,
    source: "dev",
    override: false,
  }
}

/** The status-contract projection: build hash, schema version, and override visibility. */
export interface BuildHashStatus {
  readonly buildHash: string
  readonly protocolSchemaVersion: number
  readonly source: BuildHashSource
  readonly override: boolean
}

export function buildHashStatus(options: ResolveBuildHashOptions = {}): BuildHashStatus {
  const resolved = resolveBuildHash(options)
  return {
    buildHash: resolved.hash,
    protocolSchemaVersion: resolved.schemaVersion,
    source: resolved.source,
    override: resolved.override,
  }
}

/** Typed ready-handshake failure: the worker and hub were built differently. */
export class BuildMismatch extends Schema.TaggedErrorClass<BuildMismatch>()("BuildMismatch", {
  expected: Schema.String,
  actual: Schema.String,
}) {
  override get message() {
    return `session worker build mismatch: expected ${this.expected}, got ${this.actual}`
  }
}

export interface ReadyBuildFrame {
  readonly type: "ready"
  readonly threadId: number
  readonly buildHash: string
}

/** Stamp a ready frame with the resolved build hash. */
export function readyFrame(
  threadId: number,
  resolved: Pick<ResolvedBuildHash, "hash"> = resolveBuildHash(),
): ReadyBuildFrame {
  return { type: "ready", threadId, buildHash: resolved.hash }
}

/** True for a plain object; `null` and arrays are excluded. */
function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
}

/**
 * Check a ready frame's build hash. Missing, empty, or different hashes are a
 * typed `BuildMismatch`; a matching hash returns the trusted value.
 */
export function verifyReadyBuild(input: unknown, expected: string = resolveBuildHash().hash): string {
  if (!isRecord(input)) throw new BuildMismatch({ expected, actual: "<not-an-object>" })
  const actual = input["buildHash"]
  if (typeof actual !== "string" || actual.length === 0) {
    throw new BuildMismatch({ expected, actual: "<missing>" })
  }
  if (actual !== expected) throw new BuildMismatch({ expected, actual })
  return actual
}

export * as WorkerBuildHash from "./build-hash"
