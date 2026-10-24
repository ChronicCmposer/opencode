// Phase 8.1 worker entry point (plan task 8.1).
//
// Creates a NEW dedicated `MessageChannel` for egress, a SEPARATE one for the
// generic control link, and a THIRD for the EventV2 mesh control plane, then
// transfers the hub ends to the parent. The default worker channel stays free
// for commands ("delta" / "durable" / "flood" / "end" / "mesh.*").
//
//   * egress        — the production egress sender (data plane).
//   * control       — the generic control link: hub `{type:"ping",id}` -> pong,
//                     plus flood bookkeeping.
//   * egressControl — the production mesh client: `listen` requests, `durable`
//                     wakeups and overflow `resync` pushes, with `durable.ack` /
//                     `resync.ack` acknowledgements travelling back to the hub.
//
// The mesh client never touches the parent's default channel directly; durable
// wakes and resync pushes are forwarded there so a test can observe them, and
// acks are driven by explicit `mesh.ack` / `mesh.resync.ack` commands.

import { createControlLink } from "../../src/session/egress/control"
import { createMeshWorkerClient } from "../../src/session/egress/mesh"
import { createEgressSender } from "../../src/session/egress/sender"
import { onMessage, postToParent } from "./worker-scope"

interface Command {
  readonly type: string
  readonly id?: string
  readonly ordinal?: number
  readonly count?: number
  readonly pattern?: string
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

const egressChannel = new MessageChannel()
const controlChannel = new MessageChannel()
const egressControlChannel = new MessageChannel()
const sender = createEgressSender({ port: egressChannel.port1, generation: 1 })
const control = createControlLink(controlChannel.port1)
const mesh = createMeshWorkerClient(egressControlChannel.port1)

control.onMessage((frame) => {
  if (frame.type === "ping" && typeof frame.id === "number") {
    control.notify({ type: "pong", id: frame.id })
  }
})

mesh.onDurable((commit) => {
  postToParent({ type: "mesh.durable", id: commit.event.id })
})
mesh.onResync((pattern) => {
  postToParent({ type: "mesh.resync", pattern })
})

const handleCommand = (message: Command): void => {
  switch (message.type) {
    case "delta":
      sender.pushDelta({ id: message.id ?? "", type: "message.part.updated", properties: { id: message.id } })
      return
    case "durable":
      sender.pushDurable(
        { id: message.id ?? "", type: "message.updated", properties: { id: message.id } },
        { generation: 1, ordinal: message.ordinal ?? 0 },
      )
      return
    case "flood": {
      const count = message.count ?? 0
      control.notify({ type: "flood.start" })
      for (let index = 0; index < count; index++) {
        sender.pushDelta({ id: `f${index}`, type: "message.part.updated", properties: { index } })
      }
      control.notify({ type: "flood.done", count })
      return
    }
    case "mesh.listen": {
      const pattern = message.pattern ?? "*"
      void mesh.listen(pattern).then(
        () => postToParent({ type: "mesh.listen.ok", pattern }),
        (error) => postToParent({ type: "mesh.listen.error", pattern, message: messageOf(error) }),
      )
      return
    }
    case "mesh.ack":
      mesh.acknowledgeDelivery(message.count)
      return
    case "mesh.resync.ack":
      mesh.acknowledgeResync(message.pattern ?? "*")
      return
    case "end":
      sender.end()
      return
  }
}

onMessage<Command>(handleCommand)

postToParent(
  {
    type: "ports",
    egress: egressChannel.port2,
    control: controlChannel.port2,
    egressControl: egressControlChannel.port2,
  },
  [egressChannel.port2, controlChannel.port2, egressControlChannel.port2],
)
