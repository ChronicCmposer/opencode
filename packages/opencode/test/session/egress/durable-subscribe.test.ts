// W2 — worker durable-subscription cleanup on a failed `listen` (egress-client
// finding). A `subscribeDurable` whose mesh `listen` request fails must not leave
// a stale local subscription behind: the entry is removed so a later durable push
// can never reach a subscriber the hub never registered.

import { describe, expect, test } from "bun:test"
import { attachMeshWorker, createEgressMesh, type DurableCommit } from "@/session/egress/mesh"
import { createWorkerEgressClient } from "@/session/worker/egress-client"
import { durable } from "../../lib/egress-fixtures"
import { pollUntil } from "../../lib/poll"

/**
 * A real control port whose `listen` push throws, so `subscribeDurable`'s listen
 * request rejects while every other frame still crosses. The receiver is
 * forwarded to the real port so hub pushes (durable/resync) still arrive.
 */
function listenFailingPort(real: MessagePort): MessagePort {
  let handler: ((event: MessageEvent) => void) | undefined
  const wrapper = {
    get onmessage() {
      return handler ?? null
    },
    set onmessage(next: ((event: MessageEvent) => void) | null) {
      handler = next ?? undefined
    },
    start: () => real.start(),
    close: () => real.close(),
    postMessage: (message: unknown) => {
      if (typeof message === "object" && message !== null && (message as { type?: unknown }).type === "listen") {
        throw new Error("listen transport failure")
      }
      real.postMessage(message)
    },
  }
  real.onmessage = (event: MessageEvent) => handler?.(event)
  return wrapper as unknown as MessagePort
}

describe("worker durable subscription failure", () => {
  test("a failed listen removes the local subscription so a later push cannot reach it", async () => {
    const egressChannel = new MessageChannel()
    const controlChannel = new MessageChannel()
    const mesh = createEgressMesh()
    const attachment = attachMeshWorker(mesh, "w1", controlChannel.port2)
    const client = createWorkerEgressClient(egressChannel.port1, {
      controlPort: listenFailingPort(controlChannel.port1),
    })

    const commits: DurableCommit[] = []
    client.subscribeDurable({ pattern: "message.updated", onDurable: (commit) => commits.push(commit) })

    // The listen failed, but simulate the hub having registered the pattern
    // independently: the durable push must not reach the stale subscription.
    mesh.listen("w1", "message.updated")
    mesh.publish({ meta: { generation: 1, ordinal: 1 }, event: durable("d1") })
    // The push is still acknowledged by the mesh wiring, so outstanding drains.
    await pollUntil(() => mesh.outstanding("w1") === 0)

    expect(commits).toEqual([])
    client.dispose()
    attachment.dispose()
  })
})
