import { loadNativeEgressScan } from "@/session/egress/native-scan"

const native = loadNativeEgressScan()

self.onmessage = (message: MessageEvent<ArrayBuffer>) => {
  postMessage(native === undefined ? { ok: false, code: "not-loaded" } : native.scan(new Uint8Array(message.data)))
}
