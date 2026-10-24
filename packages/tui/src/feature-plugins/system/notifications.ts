import type { Event } from "@opencode-ai/sdk/v2"
import type { TuiAttentionSoundName, TuiPlugin, TuiPluginApi } from "@opencode-ai/plugin/tui"
import type { BuiltinTuiPlugin } from "../builtins"

const id = "internal:notifications"

type SessionError = Extract<Event, { type: "session.error" }>["properties"]["error"]

function notify(api: TuiPluginApi, sessionID: string | undefined, message: string, sound: TuiAttentionSoundName) {
  const session = sessionID ? api.state.session.get(sessionID) : undefined
  const isSubagent = session?.parentID !== undefined
  void api.attention.notify({
    title: session?.title,
    message,
    notification: isSubagent ? false : { when: "blurred" },
    sound: { name: sound, when: "always" },
  })
}

function sessionErrorMessage(error: SessionError) {
  if (error?.name === "MessageAbortedError") return "Session aborted"
  const data = error?.data
  if (data && typeof data === "object" && "message" in data && data.message === "SSE read timed out") {
    return "Model stopped responding"
  }
  return "Session error"
}

type CancelMode = Extract<Event, { type: "session.cancelled" }>["properties"]["mode"]

function sessionCancelledMessage(mode: CancelMode) {
  if (mode === "terminated") return "Session terminated"
  return "Session cancelled"
}

const tui: TuiPlugin = async (api) => {
  const active = new Set<string>()
  const errored = new Set<string>()
  const cancelled = new Set<string>()
  const questions = new Set<string>()
  const permissions = new Set<string>()
  // Completion is announced one turn late so a cancel published alongside the
  // terminal idle can supersede it (see `session.cancelled`).
  const pendingCompletion = new Map<string, ReturnType<typeof setTimeout>>()

  const dropPendingCompletion = (sessionID: string) => {
    const timer = pendingCompletion.get(sessionID)
    if (timer === undefined) return
    clearTimeout(timer)
    pendingCompletion.delete(sessionID)
  }

  const announceCompletion = (sessionID: string) => {
    pendingCompletion.delete(sessionID)
    const session = api.state.session.get(sessionID)
    notify(api, sessionID, "Session done", session?.parentID ? "subagent_done" : "done")
  }

  api.event.on("question.asked", (event) => {
    if (questions.has(event.properties.id)) return
    questions.add(event.properties.id)
    notify(api, event.properties.sessionID, "Question needs input", "question")
  })

  api.event.on("question.replied", (event) => {
    questions.delete(event.properties.requestID)
  })

  api.event.on("question.rejected", (event) => {
    questions.delete(event.properties.requestID)
  })

  api.event.on("permission.asked", (event) => {
    if (permissions.has(event.properties.id)) return
    permissions.add(event.properties.id)
    notify(api, event.properties.sessionID, "Permission needs input", "permission")
  })

  api.event.on("permission.replied", (event) => {
    permissions.delete(event.properties.requestID)
  })

  api.event.on("session.cancelled", (event) => {
    const sessionID = event.properties.sessionID
    if (cancelled.has(sessionID)) return
    cancelled.add(sessionID)
    // A cancel is terminal: drop the run indicators and surface the outcome.
    // The hub publishes this alongside the terminal idle, so a cancel that
    // follows that idle supersedes the pending completion; whichever order the
    // two arrive in, this toast is the authoritative cancel signal.
    active.delete(sessionID)
    errored.delete(sessionID)
    dropPendingCompletion(sessionID)
    const session = api.state.session.get(sessionID)
    if (session?.parentID !== undefined) return
    api.ui.toast({ variant: "warning", message: sessionCancelledMessage(event.properties.mode) })
  })

  api.event.on("session.status", (event) => {
    const sessionID = event.properties.sessionID
    if (event.properties.status.type === "busy" || event.properties.status.type === "retry") {
      active.add(sessionID)
      errored.delete(sessionID)
      cancelled.delete(sessionID)
      dropPendingCompletion(sessionID)
      return
    }

    if (event.properties.status.type !== "idle") return
    if (!active.has(sessionID)) return
    active.delete(sessionID)

    if (errored.has(sessionID)) {
      errored.delete(sessionID)
      return
    }

    if (cancelled.delete(sessionID)) return

    pendingCompletion.set(
      sessionID,
      setTimeout(() => announceCompletion(sessionID), 0),
    )
  })

  api.event.on("session.error", (event) => {
    const sessionID = event.properties.sessionID
    if (!sessionID) return
    if (!active.has(sessionID)) return
    errored.add(sessionID)
    dropPendingCompletion(sessionID)
    notify(api, sessionID, sessionErrorMessage(event.properties.error), "error")
  })
}

const plugin: BuiltinTuiPlugin = {
  id,
  tui,
}

export default plugin
