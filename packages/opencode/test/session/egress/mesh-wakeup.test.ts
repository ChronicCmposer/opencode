// The hub mesh carries position wakeups only: a durable commit from one worker
// wakes that worker alone, and the control frame has no event body.

import { describe, expect, test } from "bun:test"
import { encodeFrame } from "@/session/egress/frame"
import { eventFrame } from "@/session/worker/frame"
import { buildProductionPortBackends } from "@/session/worker/production-hub"
import { recordingPublisher } from "../../lib/recorders"
import { pollUntil } from "../../lib/poll"

/** A worker end of the egress + control ports, recording raw control messages. */
function connect(backends: ReturnType<typeof buildProductionPortBackends>, workerID: number) {
  const data = new MessageChannel()
  const control = new MessageChannel()
  const raw: unknown[] = []
  control.port2.onmessage = (message) => void raw.push(message.data)
  control.port2.start()
  const dispose = backends.egress!(workerID, data.port1, control.port1)
  control.port2.postMessage({ type: "listen", id: 1, pattern: "durable.wakeup" })
  return {
    raw,
    commit: (seq: number) =>
      data.port2.postMessage(
        encodeFrame(
          eventFrame({
            event: { id: `evt_${workerID}_${seq}`, type: "session.message", properties: { secret: "body" } },
            aggregateID: `ses_${workerID}`,
            seq,
          }),
        ),
      ),
    wakeups: () => raw.filter((message) => (message as { type?: string }).type === "durable"),
    close: () => {
      dispose()
      for (const port of [data.port1, data.port2, control.port1, control.port2]) port.close()
    },
  }
}

describe("mesh position wakeups", () => {
  test("a durable commit wakes its own worker once and never another, with no event body", async () => {
    const { publisher } = recordingPublisher()
    const backends = buildProductionPortBackends({ events: publisher })
    const a = connect(backends, 1)
    const b = connect(backends, 2)
    try {
      await pollUntil(() => backends.egressMesh.subscriptions().length === 2, { message: "workers never subscribed" })

      a.commit(1)
      await pollUntil(() => a.wakeups().length === 1, { message: "owner never woken" })
      // Let any (incorrect) fan-out to B land before asserting its silence.
      await new Promise((resolve) => setTimeout(resolve, 50))

      expect(a.wakeups()).toEqual([{ type: "durable", meta: { aggregateID: "ses_1", seq: 1 } }])
      expect(b.wakeups()).toEqual([])
      expect(JSON.stringify(a.raw)).not.toContain("body")
    } finally {
      a.close()
      b.close()
    }
  })

  test("the hub never serialises the event for the mesh", async () => {
    const { publisher } = recordingPublisher()
    const backends = buildProductionPortBackends({ events: publisher })
    const sent: unknown[] = []
    const publish = backends.egressMesh.publish
    backends.egressMesh.publish = (workerId, wakeup) => {
      sent.push(wakeup)
      return publish(workerId, wakeup)
    }
    const a = connect(backends, 1)
    try {
      await pollUntil(() => backends.egressMesh.subscriptions().length === 1, { message: "worker never subscribed" })
      a.commit(1)
      await pollUntil(() => sent.length === 1, { message: "mesh never published" })
      expect(sent).toEqual([{ aggregateID: "ses_1", seq: 1 }])
      expect(Object.keys(sent[0] as object).sort()).toEqual(["aggregateID", "seq"])
    } finally {
      a.close()
    }
  })
})
