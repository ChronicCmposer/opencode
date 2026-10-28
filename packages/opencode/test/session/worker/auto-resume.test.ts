import { describe, expect, test } from "bun:test"
import {
  AUTO_RESUME_CAP,
  createAutoResumeController,
  decideAutoResume,
} from "@/session/ledger/resume"

const base = {
  sessionID: "ses_1",
  phase: "stream" as const,
  attempts: 0,
  lastCommittedMessageID: "msg_1",
}

describe("auto-resume policy (D9)", () => {
  test("resumes from the last committed message when no side-effecting call started", () => {
    expect(decideAutoResume({ ...base, indeterminate: [{ callID: "read_1", sideEffecting: false }] })).toEqual({
      resume: true,
      sessionID: "ses_1",
      attempt: 1,
      phase: "stream",
      fromMessageID: "msg_1",
    })
  })

  test("an indeterminate side-effecting tool blocks resume", () => {
    expect(decideAutoResume({ ...base, indeterminate: [{ callID: "write_1", sideEffecting: true }] })).toMatchObject({
      resume: false,
      reason: "indeterminate",
    })
  })

  test("cancel never resumes", () => {
    expect(decideAutoResume({ ...base, cancelled: true })).toMatchObject({ resume: false, reason: "cancelled" })
  })

  test("boot has no run to resume and no committed message blocks the rest", () => {
    expect(decideAutoResume({ ...base, phase: "boot" })).toMatchObject({ resume: false, reason: "phase" })
    expect(decideAutoResume({ ...base, lastCommittedMessageID: null })).toMatchObject({
      resume: false,
      reason: "no-committed-message",
    })
  })

  test("the cap is ten auto-resumes per run", () => {
    expect(AUTO_RESUME_CAP).toBe(10)
    expect(decideAutoResume({ ...base, attempts: AUTO_RESUME_CAP })).toMatchObject({ resume: false, reason: "cap" })
    const controller = createAutoResumeController()
    for (let i = 1; i <= AUTO_RESUME_CAP; i++) {
      expect(controller.decide(base).resume).toBe(true)
      expect(controller.record("ses_1")).toBe(i)
    }
    expect(controller.decide(base)).toMatchObject({ resume: false, reason: "cap" })
    controller.reset("ses_1")
    expect(controller.decide(base).resume).toBe(true)
  })
})
