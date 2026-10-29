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

import { Option, Schema } from "effect"

import { WORKER_PROTOCOL_SCHEMA_VERSION } from "./frame"

export const PROTOCOL_SCHEMA_VERSION = WORKER_PROTOCOL_SCHEMA_VERSION

/** Runtime override honoured only when the caller explicitly asks for a dev hash. */
export const BUILD_HASH_OVERRIDE_ENV = "OPENCODE_BUILD_HASH_OVERRIDE"

/** Explicit development marker; the override is ignored without it in prod. */
export const DEV_ENV = "OPENCODE_DEV"

const HASH_DOMAIN = "opencode.session-worker.build.v1"
const DEV_ENTRY = "opencode.session-worker.dev"

declare global {
  /** Compile-time worker build hash, injected by `script/build.ts`. */
  const OPENCODE_BUILD_HASH: string | undefined
  /**
   * Compile-time packaged identity, injected by `script/build.ts`. A source run
   * leaves it undefined; runtime mode never depends on `NODE_ENV`.
   */
  const OPENCODE_PACKAGED: boolean | undefined
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

const BUNDLE_HASH_DOMAIN = "opencode.session-worker.bundle.v1"

/** One emitted module/chunk of the worker bundle graph. */
export interface BundleArtifact {
  readonly path: string
  readonly bytes: string | Uint8Array | ArrayBuffer
}

function artifactBytes(value: BundleArtifact["bytes"]): Uint8Array {
  if (typeof value === "string") return new TextEncoder().encode(value)
  return value instanceof Uint8Array ? value : new Uint8Array(value)
}

/**
 * Hash the emitted worker module graph: every output path plus its bytes, in
 * deterministic path order, together with the protocol schema version. Only the
 * artifacts passed here are inputs, so plugin bundles (which are not part of the
 * worker entry graph) can never change the hash.
 */
export function computeBundleHash(
  artifacts: readonly BundleArtifact[],
  schemaVersion: number = PROTOCOL_SCHEMA_VERSION,
): string {
  const hash = createHash("sha256")
    .update(BUNDLE_HASH_DOMAIN)
    .update("\0")
    .update(String(schemaVersion))
  for (const artifact of [...artifacts].sort((a, b) => (a.path < b.path ? -1 : a.path > b.path ? 1 : 0))) {
    hash.update("\0").update(artifact.path).update("\0").update(artifactBytes(artifact.bytes))
  }
  return hash.digest("hex")
}

// NOTE: the runtime module deliberately has no bundler dependency. The build
// script owns `Bun.build`, hashes the emitted worker graph, and injects only the
// resulting digest as `OPENCODE_BUILD_HASH`.

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
  /**
   * Explicit dev/test mode. Defaults to true for an unpackaged source run or
   * when `OPENCODE_DEV` is set. Packaging is a compile-time identity, not
   * `NODE_ENV`: a source run with `NODE_ENV=production` still has no packaged
   * worker to verify.
   */
  readonly dev?: boolean
  /** Packaged identity override (defaults to the compile-time define). */
  readonly packaged?: boolean
}

const definedBuildHash = (): string | undefined =>
  typeof OPENCODE_BUILD_HASH !== "undefined" && OPENCODE_BUILD_HASH !== "" ? OPENCODE_BUILD_HASH : undefined

const isPackagedBuild = (): boolean => typeof OPENCODE_PACKAGED !== "undefined" && OPENCODE_PACKAGED === true

/** Normalize an explicit hash: an empty string is the same as "not defined". */
function definedHashOrUndefined(value: string | undefined): string | undefined {
  return value !== undefined && value !== "" ? value : undefined
}

/** Resolve the build identity, logging an active development override at error. */
export function resolveBuildHash(options: ResolveBuildHashOptions = {}): ResolvedBuildHash {
  const env = options.env ?? process.env
  const schemaVersion = options.schemaVersion ?? PROTOCOL_SCHEMA_VERSION
  const defined = definedHashOrUndefined(options.definedHash) ?? definedBuildHash()
  const packaged = options.packaged ?? (isPackagedBuild() || defined !== undefined)
  const override = env[BUILD_HASH_OVERRIDE_ENV]
  const dev = options.dev ?? (env[DEV_ENV] === "1" || env[DEV_ENV] === "true" || !packaged)

  if (override !== undefined && override !== "" && dev) {
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

  if (override !== undefined && override !== "" && !dev) {
    options.log?.(
      "session worker build hash override ignored outside development",
      {
        level: "error",
        buildHash: override,
      },
      "error",
    )
  }

  if (defined !== undefined && defined !== "") {
    return { hash: defined, schemaVersion, source: "build", override: false }
  }

  // A production/packaged run must carry the build-time hash. Falling back to a
  // dev hash here would silently disable the ready-handshake check, so fail loud
  // instead: a build that forgot the define is a build defect, not a runtime mode.
  if (!dev) throw new MissingBuildHashError()

  return {
    hash: computeBuildHash({ entryBundle: DEV_ENTRY, schemaVersion }),
    schemaVersion,
    source: "dev",
    override: false,
  }
}

/** Raised when a packaged/production run has no compiled-in build hash. */
export class MissingBuildHashError extends Schema.TaggedErrorClass<MissingBuildHashError>("MissingBuildHashError")(
  "WorkerBuildHash.Missing",
  {},
) {
  readonly tag = "BUILD_HASH_MISSING"
  constructor() {
    super({})
    this.name = "MissingBuildHashError"
  }
  override get message() {
    return "session worker build hash is missing: a packaged build must define OPENCODE_BUILD_HASH"
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

const ReadyBuildSchema = Schema.Struct({ buildHash: Schema.NonEmptyString })
const decodeReadyBuild = Schema.decodeUnknownOption(ReadyBuildSchema)

/**
 * Check a ready frame's build hash. Missing, empty, or different hashes are a
 * typed `BuildMismatch`; a matching hash returns the trusted value.
 */
export function verifyReadyBuild(input: unknown, expected: string = resolveBuildHash().hash): string {
  if (typeof input !== "object" || input === null) throw new BuildMismatch({ expected, actual: "<not-an-object>" })
  const decoded = decodeReadyBuild(input)
  if (Option.isNone(decoded)) throw new BuildMismatch({ expected, actual: "<missing>" })
  const actual = decoded.value.buildHash
  if (actual !== expected) throw new BuildMismatch({ expected, actual })
  return actual
}

export * as WorkerBuildHash from "./build-hash"
