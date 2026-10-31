// The native events-frame scanner (a Node-API addon built for linux arm64). This module only loads it; the
// decoder contract and the JS fallback live in `scan.ts`.

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

/** The loaded addon, or undefined when it is not built for this platform or fails to load. */
export function loadNativeEgressScan(): NativeEgressScan | undefined {
  return undefined
}
