// Egress decode benchmark: today's string path vs the JS byte decoder vs the native scanner.
//
//   bun run script/bench-egress-scan.ts
//
// Prints median ns/op of 5 runs per frame size and exits non-zero when the native bar is missed
// (native <= 0.5x jsDecoder at delta size, <= 1.0x at 4 KB and 480 KB). Without the addon the bar is skipped.

import { decodeFrame } from "../src/session/egress/frame"
import { encodeEventsFrameBytes } from "../src/session/egress/frame-bytes"
import { loadNativeEgressScan } from "../src/session/egress/native-scan"
import { jsDecoder } from "../src/session/egress/scan"
import { eventFrame, encodeWorkerFrame } from "../src/session/worker/frame"

const RUNS = 5
const sizes = [
  { label: "delta ~400 B", bytes: 400 },
  { label: "4 KB", bytes: 4 * 1024 },
  { label: "480 KB", bytes: 480 * 1024 },
] as const

function frameOf(propertiesBytes: number) {
  const text = "lorem \"ipsum\" dolor sit amet é漢 ".repeat(Math.ceil(propertiesBytes / 30)).slice(0, propertiesBytes - 120)
  const frame = eventFrame({
    event: {
      id: "evt_0123456789",
      type: "message.part.delta",
      properties: { sessionID: "ses_0123456789", messageID: "msg_0123456789", partID: "prt_0123456789", field: "text", delta: text },
    },
    generation: 1,
    ordinal: 42,
  })
  return { string: encodeWorkerFrame(frame), bytes: new Uint8Array(encodeEventsFrameBytes(frame)) }
}

function nsPerOp(run: () => unknown): number {
  const probe = performance.now()
  let probed = 0
  do {
    run()
    probed++
  } while (performance.now() - probe < 20)
  const iterations = Math.max(5, Math.ceil((probed * 150) / Math.max(1, performance.now() - probe)))
  const samples: number[] = []
  for (let r = 0; r < RUNS; r++) {
    const start = performance.now()
    for (let i = 0; i < iterations; i++) run()
    samples.push(((performance.now() - start) * 1e6) / iterations)
  }
  return samples.sort((a, b) => a - b)[Math.floor(RUNS / 2)]!
}

const native = loadNativeEgressScan()
const rows: { label: string; string: number; js: number; native?: number }[] = []
for (const size of sizes) {
  const { string, bytes } = frameOf(size.bytes)
  rows.push({
    label: size.label,
    string: nsPerOp(() => decodeFrame(string)),
    js: nsPerOp(() => jsDecoder.decode(bytes)),
    ...(native === undefined ? {} : { native: nsPerOp(() => native.scan(bytes)) }),
  })
}

const fmt = (n: number | undefined) => (n === undefined ? "n/a" : Math.round(n).toLocaleString())
console.log(["frame".padEnd(14), "string ns".padStart(12), "js ns".padStart(12), "native ns".padStart(12), "native/js".padStart(10)].join(" "))
let failed = false
for (const [index, row] of rows.entries()) {
  const ratio = row.native === undefined ? undefined : row.native / row.js
  const limit = index === 0 ? 0.5 : 1.0
  const verdict = ratio === undefined ? "" : ratio <= limit ? `PASS (<= ${limit})` : `FAIL (> ${limit})`
  if (ratio !== undefined && ratio > limit) failed = true
  console.log(
    [row.label.padEnd(14), fmt(row.string).padStart(12), fmt(row.js).padStart(12), fmt(row.native).padStart(12), (ratio?.toFixed(2) ?? "n/a").padStart(10), verdict].join(" "),
  )
}
console.log(native === undefined ? "native addon unavailable: bar skipped" : failed ? "FAIL" : "PASS")
if (failed) process.exit(1)
