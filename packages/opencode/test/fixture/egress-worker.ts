// Phase 8.1 worker entry point (plan task 8.1).
//
// Creates a NEW dedicated `MessageChannel` for egress and a SEPARATE one for
// control, transfers the hub ends to the parent, and drives the production
// egress sender + control link. The default worker channel stays free for
// commands ("delta" / "durable" / "flood" / "end").
//
// control port in : hub -> worker `{ type: "ping", id }` -> worker replies pong.

import { createControlLink } from "../../src/session/egress/control"
import { createEgressSender } from "../../src/session/egress/sender"

interface WorkerScope {
  postMessage(message: unknown, transfer?: readonly Transferable[]): void
  onmessage: ((event: { data: unknown }) => void) | null
}

interface Command {
  readonly type: string
  readonly id?: string
  readonly ordinal?: number
  readonly count?: number
}

const scope = globalThis as unknown as WorkerScope

const egressChannel = new MessageChannel()
const controlChannel = new MessageChannel()
const sender = createEgressSender({ port: egressChannel.port1, generation: 1 })
const control = createControlLink(controlChannel.port1)

control.onMessage((frame) => {
  if (frame.type === "ping" && typeof frame.id === "number") {
    control.notify({ type: "pong", id: frame.id })
  }
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
    case "end":
      sender.end()
      return
  }
}

scope.onmessage = (event) => handleCommand(event.data as Command)

scope.postMessage({ type: "ports", egress: egressChannel.port2, control: controlChannel.port2 }, [
  egressChannel.port2,
  controlChannel.port2,
])
