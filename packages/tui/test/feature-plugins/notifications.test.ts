import { expect, test } from "bun:test"
import type { TuiAttentionSoundName, TuiPluginApi, TuiPluginMeta, TuiToast } from "@opencode-ai/plugin/tui"
import notifications from "../../src/feature-plugins/system/notifications"
import { createTuiPluginApi } from "../fixture/tui-plugin"

type BusEvent = { properties: Record<string, unknown> }

type Bus = {
  on: (type: string, handler: (event: BusEvent) => void) => () => void
  emit: (type: string, event: BusEvent) => void
}

function createEventBus(): Bus {
  const handlers = new Map<string, Set<(event: BusEvent) => void>>()
  return {
    on(type, handler) {
      const set = handlers.get(type) ?? new Set()
      set.add(handler)
      handlers.set(type, set)
      return () => void set.delete(handler)
    },
    emit(type, event) {
      for (const handler of handlers.get(type) ?? []) handler(event)
    },
  }
}

type NotifyCall = { message: string; sound: TuiAttentionSoundName }
type ToastCall = { message: string; variant: TuiToast["variant"] }

function harness() {
  const bus = createEventBus()
  const notifications_: NotifyCall[] = []
  const toasts: ToastCall[] = []
  const sessions = new Map<string, { parentID?: string; title?: string }>()

  const api = createTuiPluginApi({
    event: bus as unknown as TuiPluginApi["event"],
    attention: {
      notify: async (input) => {
        const sound = typeof input.sound === "object" ? input.sound?.name : undefined
        notifications_.push({ message: input.message, sound: sound ?? "default" })
        return { ok: true, notification: false, sound: false, skipped: "blurred" }
      },
    },
    ui: {
      toast: (input) => void toasts.push({ message: input.message, variant: input.variant }),
    },
    state: {
      session: {
        get: (sessionID: string) => {
          const session = sessions.get(sessionID)
          return session ? ({ ...session, id: sessionID } as never) : undefined
        },
      },
    },
  })

  void notifications.tui(api, undefined, pluginMeta)

  const status = (sessionID: string, value: Record<string, unknown>) =>
    bus.emit("session.status", { properties: { sessionID, status: value } })
  const cancelled = (sessionID: string, mode: "cooperative" | "terminated" | "completed") =>
    bus.emit("session.cancelled", { properties: { sessionID, mode, durationMs: 1, acknowledged: true } })

  return { notifications: notifications_, toasts, sessions, status, cancelled }
}

const session = "ses_1"

const pluginMeta = {
  id: "notifications",
  source: "internal",
  spec: "notifications",
  target: "notifications",
  first_time: 0,
  last_time: 0,
  time_changed: 0,
  load_count: 1,
  fingerprint: "test",
  state: "same",
} satisfies TuiPluginMeta

test("a cooperative cancel surfaces its mode and suppresses the terminal idle", () => {
  const h = harness()
  h.status(session, { type: "busy" })
  h.cancelled(session, "cooperative")
  h.status(session, { type: "idle" })

  expect(h.toasts).toEqual([{ message: "Session cancelled", variant: "warning" }])
  expect(h.notifications).toEqual([])
})

test("a terminated cancel names the mode", () => {
  const h = harness()
  h.cancelled(session, "terminated")

  expect(h.toasts).toEqual([{ message: "Session terminated", variant: "warning" }])
})

test("a subagent cancel stays silent", () => {
  const h = harness()
  h.sessions.set(session, { parentID: "ses_parent" })
  h.status(session, { type: "busy" })
  h.cancelled(session, "cooperative")

  expect(h.toasts).toEqual([])
  expect(h.notifications).toEqual([])
})

test("an ordinary idle still announces completion", () => {
  const h = harness()
  h.status(session, { type: "busy" })
  h.status(session, { type: "idle" })

  expect(h.notifications).toEqual([{ message: "Session done", sound: "done" }])
  expect(h.toasts).toEqual([])
})

test("a cancel resting on an earlier idle still surfaces its mode", () => {
  // The hub can publish the terminal idle before `session.cancelled`; the cancel
  // outcome must still reach the user, not be swallowed by the idle's "done".
  const h = harness()
  h.status(session, { type: "busy" })
  h.status(session, { type: "idle" })
  h.cancelled(session, "cooperative")

  expect(h.notifications).toEqual([{ message: "Session done", sound: "done" }])
  expect(h.toasts).toEqual([{ message: "Session cancelled", variant: "warning" }])
})
