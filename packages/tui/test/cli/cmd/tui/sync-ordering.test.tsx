/** @jsxImportSource @opentui/solid */
import { expect, test } from "bun:test"
import type { GlobalEvent } from "@opencode-ai/sdk/v2"
import { tmpdir } from "../../../fixture/fixture"
import { json, mount, wait } from "./sync-fixture"

const sessionID = "ses_ordering"
const directory = "/tmp/opencode/packages/opencode"
const session = {
  id: sessionID,
  title: "ordering",
  time: { created: 0, updated: 0 },
  version: "1.15.13",
  directory,
}
const assistant = {
  id: "msg_seed",
  sessionID,
  role: "assistant" as const,
  agent: "build",
  modelID: "model",
  providerID: "test",
  mode: "build",
  parentID: "msg_user",
  path: { cwd: directory, root: directory },
  cost: 0,
  tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
  time: { created: 1, completed: 2 },
}

function global(payload: GlobalEvent["payload"]): GlobalEvent {
  return { directory: "/tmp/other", project: "proj_test", payload }
}

function message(id: string, created: number) {
  return { ...assistant, id, time: { created, completed: created + 1 } }
}

async function setup() {
  await using tmp = await tmpdir()
  await Bun.write(`${tmp.path}/kv.json`, "{}")
  return mount(undefined, tmp.path)
}

test("live messages order by creation time when timestamp digit widths differ", async () => {
  const { app, emit, sync } = await setup()

  try {
    for (const info of [message("msg_cc", 100), message("msg_bb", 9), message("msg_dd", 1000)]) {
      emit(global({ id: `evt_${info.id}`, type: "message.updated", properties: { sessionID, info } }))
    }
    await wait(() => sync.data.message[sessionID]?.length === 3)

    expect(sync.data.message[sessionID].map((item) => item.id)).toEqual(["msg_bb", "msg_cc", "msg_dd"])
  } finally {
    app.renderer.destroy()
  }
})

test("live message updates keep their slot when timestamp digit widths differ", async () => {
  const { app, emit, sync } = await setup()
  const older = message("msg_bb", 9)
  const newer = message("msg_cc", 100)

  try {
    emit(global({ id: "evt_newer", type: "message.updated", properties: { sessionID, info: newer } }))
    emit(global({ id: "evt_older", type: "message.updated", properties: { sessionID, info: older } }))
    await wait(() => sync.data.message[sessionID]?.length === 2)

    const updated = { ...older, time: { created: 9, completed: 11 } }
    emit(global({ id: "evt_updated", type: "message.updated", properties: { sessionID, info: updated } }))
    await wait(() => {
      const head = sync.data.message[sessionID]?.[0]
      return head !== undefined && "completed" in head.time && head.time.completed === 11
    })

    expect(sync.data.message[sessionID].map((item) => item.id)).toEqual(["msg_bb", "msg_cc"])
    expect(sync.data.message[sessionID]).toHaveLength(2)
  } finally {
    app.renderer.destroy()
  }
})

test("permission requests stay id-ordered under out-of-order events", async () => {
  const { app, emit, sync } = await setup()
  const request = (id: string) => ({
    id,
    sessionID,
    permission: "edit",
    patterns: [] as string[],
    metadata: {},
    always: [] as string[],
  })

  try {
    for (const id of ["prt_c", "prt_a", "prt_b"]) {
      emit(global({ id: `evt_${id}`, type: "permission.asked", properties: request(id) }))
    }
    await wait(() => sync.data.permission[sessionID]?.length === 3)
    expect(sync.data.permission[sessionID].map((item) => item.id)).toEqual(["prt_a", "prt_b", "prt_c"])

    emit(
      global({
        id: "evt_reply",
        type: "permission.replied",
        properties: { sessionID, requestID: "prt_a", reply: "once" },
      }),
    )
    await wait(() => sync.data.permission[sessionID]?.length === 2)
    expect(sync.data.permission[sessionID].map((item) => item.id)).toEqual(["prt_b", "prt_c"])
  } finally {
    app.renderer.destroy()
  }
})

test("question requests stay id-ordered under out-of-order events", async () => {
  const { app, emit, sync } = await setup()
  const request = (id: string) => ({ id, sessionID, questions: [] })

  try {
    for (const id of ["qst_c", "qst_a", "qst_b"]) {
      emit(global({ id: `evt_${id}`, type: "question.asked", properties: request(id) }))
    }
    await wait(() => sync.data.question[sessionID]?.length === 3)
    expect(sync.data.question[sessionID].map((item) => item.id)).toEqual(["qst_a", "qst_b", "qst_c"])

    emit(
      global({
        id: "evt_reply",
        type: "question.replied",
        properties: { sessionID, requestID: "qst_a", answers: [] },
      }),
    )
    await wait(() => sync.data.question[sessionID]?.length === 2)
    expect(sync.data.question[sessionID].map((item) => item.id)).toEqual(["qst_b", "qst_c"])
  } finally {
    app.renderer.destroy()
  }
})

test("hydration and live inserts share one message ordering", async () => {
  let resolveMessages!: (response: Response) => void
  const messages = new Promise<Response>((resolve) => {
    resolveMessages = resolve
  })
  let requested = false
  await using tmp = await tmpdir()
  await Bun.write(`${tmp.path}/kv.json`, "{}")
  const { app, emit, sync } = await mount((url) => {
    if (url.pathname === `/session/${sessionID}`) return json(session)
    if (url.pathname === `/session/${sessionID}/message`) {
      requested = true
      return messages
    }
    if (url.pathname === `/session/${sessionID}/todo` || url.pathname === `/session/${sessionID}/diff`) return json([])
    return undefined
  }, tmp.path)

  try {
    const hydrate = sync.session.sync(sessionID)
    await wait(() => requested)

    const live = message("msg_live", 1000)
    emit(global({ id: "evt_live", type: "message.updated", properties: { sessionID, info: live } }))
    await wait(() => sync.data.message[sessionID]?.some((item) => item.id === live.id) ?? false)

    resolveMessages(
      json([
        { info: message("msg_hydrated", 9), parts: [] },
        { info: message("msg_hydrated_late", 100), parts: [] },
      ]),
    )
    await hydrate

    expect(sync.data.message[sessionID].map((item) => item.id)).toEqual([
      "msg_hydrated",
      "msg_hydrated_late",
      "msg_live",
    ])
  } finally {
    app.renderer.destroy()
  }
})
