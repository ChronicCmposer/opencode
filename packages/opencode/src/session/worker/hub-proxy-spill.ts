import { Option, Schema } from "effect"

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

/** The production spill store rooted at `dir` (normally `dirname(db)/hub-proxy-spill`). */
export function createHubProxySpill(dir: string): HubProxySpill {
  const notImplemented = (): never => {
    throw new Error(`hub-proxy spill at ${dir} is not implemented yet`)
  }
  return {
    write: async () => notImplemented(),
    read: async () => notImplemented(),
    remove: async () => notImplemented(),
    removeScope: async () => notImplemented(),
    sweep: async () => notImplemented(),
  }
}

/** `dirname(db)/hub-proxy-spill`, or undefined for an in-memory database. */
export function hubProxySpillDirectoryFor(filename: string | undefined): string | undefined {
  void filename
  return undefined
}
