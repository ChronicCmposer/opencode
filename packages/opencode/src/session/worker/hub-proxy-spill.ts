import { DurableLog } from "@opencode-ai/core/durable-log"
import { Option, Schema } from "effect"
import { randomUUID } from "node:crypto"
import fs from "node:fs/promises"
import path from "node:path"
import { MAX_HUB_PROXY_SPILL_BYTES } from "./frame"

/** A hub-proxy body that crossed through the spill store instead of inline (D15). */
export interface HubProxySpillReference {
  readonly __hubProxySpill: {
    /** SHA-256 hex digest of the UTF-8 body; the file name under `<dir>/<scope>/`. */
    readonly hash: string
    /** UTF-8 byte length of the body. */
    readonly bytes: number
    /** The writer's scope directory (a lease owner id); swept as a unit. */
    readonly scope: string
  }
}

const HubProxySpillReferenceSchema = Schema.Struct({
  __hubProxySpill: Schema.Struct({
    hash: Schema.String.check(Schema.isPattern(/^[0-9a-f]{64}$/)),
    bytes: Schema.Int.pipe(Schema.check(Schema.isGreaterThanOrEqualTo(0))),
    scope: Schema.String.check(Schema.isPattern(/^[A-Za-z0-9._-]{1,200}$/)),
  }),
})

const decodeReference = Schema.decodeUnknownOption(HubProxySpillReferenceSchema)

/** Only a well-formed reference (hex hash, safe scope) may ever name a file. */
export function isHubProxySpillReference(value: unknown): value is HubProxySpillReference {
  return Option.isSome(decodeReference(value))
}

export class HubProxySpillMissingError extends Schema.TaggedErrorClass<HubProxySpillMissingError>()(
  "HubProxySpillMissingError",
  { hash: Schema.String, scope: Schema.String },
) {
  override get message() {
    return `hub-proxy spill body ${this.scope}/${this.hash} is missing`
  }
}

export class HubProxySpillCorruptError extends Schema.TaggedErrorClass<HubProxySpillCorruptError>()(
  "HubProxySpillCorruptError",
  { hash: Schema.String, scope: Schema.String, reason: Schema.String },
) {
  override get message() {
    return `hub-proxy spill body ${this.scope}/${this.hash} is corrupt: ${this.reason}`
  }
}

export class HubProxySpillTooLargeError extends Schema.TaggedErrorClass<HubProxySpillTooLargeError>()(
  "HubProxySpillTooLargeError",
  { bytes: Schema.Number, limit: Schema.Number, label: Schema.String },
) {
  override get message() {
    return `${this.label} is ${this.bytes} bytes, above the hub-proxy spill limit of ${this.limit}`
  }
}

/**
 * The short-lived, content-addressed spill area for hub-proxy bodies. Promise-based because both ends of the
 * hub-proxy wire are Promise-based; hub-side IO must never block the main thread.
 */
export interface HubProxySpill {
  /** Write `body` under `<dir>/<scope>/<sha256>` atomically (temp file + rename); rejects above the spill limit. */
  write(body: string, scope: string, label: string): Promise<HubProxySpillReference>
  /** Read and verify (size + hash) a referenced body. */
  read(reference: HubProxySpillReference): Promise<string>
  /** Remove a referenced body; idempotent. */
  remove(reference: HubProxySpillReference): Promise<void>
  /** Remove a whole scope directory (a retired generation); idempotent. Returns files removed. */
  removeScope(scope: string): Promise<number>
  /** Remove every file older than `olderThanMs` (all files when 0). Returns files removed. */
  sweep(olderThanMs: number): Promise<number>
}

const SCOPE_PATTERN = /^[A-Za-z0-9._-]{1,200}$/

function isSafeScope(scope: string): boolean {
  return SCOPE_PATTERN.test(scope) && scope !== "." && scope !== ".."
}

function requireReference(value: unknown): HubProxySpillReference["__hubProxySpill"] {
  if (!isHubProxySpillReference(value) || !isSafeScope(value.__hubProxySpill.scope)) {
    throw new Error("invalid hub-proxy spill reference")
  }
  return value.__hubProxySpill
}

function requireScope(scope: string): string {
  if (!isSafeScope(scope)) throw new Error(`invalid hub-proxy spill scope: ${scope}`)
  return scope
}

const isNotFound = (error: unknown) => (error as NodeJS.ErrnoException | undefined)?.code === "ENOENT"

/** The production spill store rooted at `dir` (normally `dirname(db)/hub-proxy-spill`). */
export function createHubProxySpill(dir: string): HubProxySpill {
  const fileFor = (scope: string, hash: string) => path.join(dir, scope, hash)

  const write: HubProxySpill["write"] = async (body, scope, label) => {
    const bytes = Buffer.byteLength(body)
    if (bytes > MAX_HUB_PROXY_SPILL_BYTES) {
      throw new HubProxySpillTooLargeError({ bytes, limit: MAX_HUB_PROXY_SPILL_BYTES, label })
    }
    requireScope(scope)
    const hash = DurableLog.digest(body)
    const scopeDir = path.join(dir, scope)
    const file = path.join(scopeDir, hash)
    const result: HubProxySpillReference = { __hubProxySpill: { hash, bytes, scope } }
    await fs.mkdir(scopeDir, { recursive: true })
    const existing = await fs.stat(file).catch(() => undefined)
    if (existing?.isFile() && existing.size === bytes) return result
    const temp = path.join(scopeDir, `.${hash}.${randomUUID()}.tmp`)
    try {
      await fs.writeFile(temp, body)
      await fs.rename(temp, file)
    } catch (error) {
      await fs.rm(temp, { force: true }).catch(() => undefined)
      throw error
    }
    return result
  }

  const read: HubProxySpill["read"] = async (reference) => {
    const { hash, bytes, scope } = requireReference(reference)
    const text = await fs.readFile(fileFor(scope, hash), "utf8").catch((error) => {
      if (isNotFound(error)) throw new HubProxySpillMissingError({ hash, scope })
      throw error
    })
    const actual = Buffer.byteLength(text)
    if (actual !== bytes) {
      throw new HubProxySpillCorruptError({ hash, scope, reason: `expected ${bytes} bytes, found ${actual}` })
    }
    if (DurableLog.digest(text) !== hash) {
      throw new HubProxySpillCorruptError({ hash, scope, reason: "content hash does not match" })
    }
    return text
  }

  const remove: HubProxySpill["remove"] = async (reference) => {
    const { hash, scope } = requireReference(reference)
    await fs.rm(fileFor(scope, hash), { force: true })
  }

  const countFiles = async (target: string): Promise<number> => {
    const entries = await fs.readdir(target, { withFileTypes: true }).catch(() => [])
    let count = 0
    for (const entry of entries) {
      if (entry.isDirectory()) count += await countFiles(path.join(target, entry.name))
      else count += 1
    }
    return count
  }

  const removeScope: HubProxySpill["removeScope"] = async (scope) => {
    const target = path.join(dir, requireScope(scope))
    const count = await countFiles(target)
    await fs.rm(target, { recursive: true, force: true })
    return count
  }

  const sweep: HubProxySpill["sweep"] = async (olderThanMs) => {
    const cutoff = Date.now() - olderThanMs
    const scopes = await fs.readdir(dir, { withFileTypes: true }).catch(() => [])
    let removed = 0
    for (const scope of scopes) {
      if (!scope.isDirectory()) continue
      const scopeDir = path.join(dir, scope.name)
      const files = await fs.readdir(scopeDir).catch(() => [])
      for (const name of files) {
        const file = path.join(scopeDir, name)
        const stat = await fs.stat(file).catch(() => undefined)
        if (stat === undefined || (olderThanMs > 0 && stat.mtimeMs >= cutoff)) continue
        await fs.rm(file, { force: true })
        removed += 1
      }
      await fs.rmdir(scopeDir).catch(() => undefined)
    }
    return removed
  }

  return { write, read, remove, removeScope, sweep }
}

/** `dirname(db)/hub-proxy-spill`, or undefined for an in-memory database. */
export function hubProxySpillDirectoryFor(filename: string | undefined): string | undefined {
  if (filename === undefined || filename.length === 0) return undefined
  if (filename === ":memory:" || filename.startsWith("file::memory:")) return undefined
  return path.join(path.dirname(filename), "hub-proxy-spill")
}
