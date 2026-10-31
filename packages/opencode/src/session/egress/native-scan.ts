// The native events-frame scanner (a Node-API addon built for linux arm64). This module only loads it; the
// decoder contract and the JS fallback live in `scan.ts`.

import { existsSync } from "node:fs"
import { createRequire } from "node:module"
import { fileURLToPath } from "node:url"

/** The raw scan result the addon returns. Offsets are byte offsets into the scanned buffer. */
export type NativeScanResult =
  | {
      readonly ok: true
      readonly kind: "event" | "end" | "resync" | "ack"
      readonly id?: number
      readonly eventID?: string
      readonly eventType?: string
      readonly generation?: number
      readonly ordinal?: number
      readonly aggregateID?: string
      readonly seq?: number
      /** Resync payload fields. */
      readonly pattern?: string
      readonly inclusive?: boolean
      /** Ack payload field. */
      readonly count?: number
      /** Byte range of the properties segment (event frames only). */
      readonly propertiesStart?: number
      readonly propertiesEnd?: number
      readonly isBlobReference?: boolean
      readonly propertiesSessionID?: string
      readonly propertiesAggregateID?: string
    }
  | {
      readonly ok: false
      /** One of `NativeScanErrorCode`. */
      readonly code: NativeScanErrorCode
      readonly offset: number
      readonly message: string
      /** The offending key, for `dangerous-key`. */
      readonly key?: string
    }

export type NativeScanErrorCode =
  | "too-large"
  | "bad-header"
  | "invalid-json"
  | "invalid-utf8"
  | "too-deep"
  | "key-too-long"
  | "dangerous-key"
  | "bad-envelope"
  | "unsupported-version"

export interface NativeEgressScan {
  readonly scan: (bytes: Uint8Array) => NativeScanResult
}

declare global {
  /** Build-time define: true only in a binary that embeds `native/egress-scan` (linux arm64). */
  const OPENCODE_EGRESS_SCAN_EMBEDDED: boolean | undefined
}

let loaded: { readonly addon: NativeEgressScan | undefined } | undefined

/** The loaded addon, or undefined when it is not built for this platform or fails to load. */
export function loadNativeEgressScan(): NativeEgressScan | undefined {
  if (loaded === undefined) loaded = { addon: tryLoad() }
  return loaded.addon
}

function tryLoad(): NativeEgressScan | undefined {
  try {
    const addon: unknown =
      typeof OPENCODE_EGRESS_SCAN_EMBEDDED !== "undefined" && OPENCODE_EGRESS_SCAN_EMBEDDED
        ? require("../../../native/egress-scan/build/egress-scan.node")
        : requireBuilt()
    if (typeof (addon as Partial<NativeEgressScan> | undefined)?.scan !== "function") return undefined
    return addon as NativeEgressScan
  } catch {
    return undefined
  }
}

function requireBuilt(): unknown {
  const file = fileURLToPath(new URL("../../../native/egress-scan/build/egress-scan.node", import.meta.url))
  return existsSync(file) ? createRequire(import.meta.url)(file) : undefined
}
