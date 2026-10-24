// POSIX process-tree helpers (plan tasks 3.8 and 3.9).
//
// Enumerates descendants, reads process-group ids, spawns a child detached as
// its own group leader, and kills whole process groups. These are the exact
// semantics later phases depend on: the MCP broker/hub register a child's PGID
// before the spawn returns and reap the group on teardown, rather than walking
// and killing individual PIDs (`src/mcp/index.ts` finalizer).
//
// PGIDs are a POSIX concept; `posixSupport()` reports whether the current
// platform supports them and why the checks are skipped otherwise.

import { existsSync, readdirSync, readFileSync } from "node:fs"

export interface PosixSupport {
  readonly supported: boolean
  /** Human-readable reason the POSIX checks are skipped; empty when supported. */
  readonly reason: string
}

export function posixSupport(platform: NodeJS.Platform = process.platform): PosixSupport {
  if (platform === "win32") {
    return {
      supported: false,
      reason: "process-group/PGID semantics are POSIX-only; Windows is tracked separately (ProcessReaper TODO)",
    }
  }
  return { supported: true, reason: "" }
}

/** POSIX platforms support process-group/PGID semantics; Windows does not. */
export function isPosix(platform: string = process.platform): boolean {
  return posixSupport(platform as NodeJS.Platform).supported
}

/** Total descendants for a full `fanout`-ary tree of the given depth below the root. */
export function expectedDescendantCount(fanout: number, depth: number): number {
  if (!Number.isInteger(fanout) || fanout < 1) throw new Error(`fanout must be a positive integer, got ${fanout}`)
  if (!Number.isInteger(depth) || depth < 0) throw new Error(`depth must be a non-negative integer, got ${depth}`)
  let total = 0
  let level = 1
  for (let index = 0; index < depth; index++) {
    level *= fanout
    total += level
  }
  return total
}

interface StatFields {
  readonly ppid: number
  readonly pgrp: number
}

function readStat(pid: number): StatFields | undefined {
  try {
    const stat = readFileSync(`/proc/${pid}/stat`, "utf8")
    // The comm field is wrapped in parens and may contain spaces, so parse
    // from the last ')' to keep field indexes stable.
    const close = stat.lastIndexOf(")")
    if (close === -1) return undefined
    const fields = stat.slice(close + 2).split(" ")
    const ppid = Number.parseInt(fields[1] ?? "", 10)
    const pgrp = Number.parseInt(fields[2] ?? "", 10)
    if (!Number.isInteger(ppid) || !Number.isInteger(pgrp)) return undefined
    return { ppid, pgrp }
  } catch {
    return undefined
  }
}

function hasProcfs(): boolean {
  return process.platform === "linux" && existsSync("/proc/self/stat")
}

function procDescendants(pid: number): number[] {
  const childrenOf = new Map<number, number[]>()
  for (const entry of readdirSync("/proc")) {
    if (!/^\d+$/.test(entry)) continue
    const childPid = Number.parseInt(entry, 10)
    const info = readStat(childPid)
    if (!info) continue
    const children = childrenOf.get(info.ppid) ?? []
    children.push(childPid)
    childrenOf.set(info.ppid, children)
  }

  const descendants: number[] = []
  const queue = [pid]
  for (let index = 0; index < queue.length; index++) {
    for (const child of childrenOf.get(queue[index]!) ?? []) {
      descendants.push(child)
      queue.push(child)
    }
  }
  return descendants.sort((left, right) => left - right)
}

function pgrepDescendants(pid: number): number[] {
  const descendants: number[] = []
  const queue = [pid]
  for (let index = 0; index < queue.length; index++) {
    let output = ""
    try {
      output = Bun.spawnSync(["pgrep", "-P", String(queue[index]!)]).stdout.toString()
    } catch {
      continue
    }
    for (const token of output.split("\n")) {
      const childPid = Number.parseInt(token, 10)
      if (Number.isInteger(childPid) && !descendants.includes(childPid)) {
        descendants.push(childPid)
        queue.push(childPid)
      }
    }
  }
  return descendants.sort((left, right) => left - right)
}

/** All descendants of `pid`, depth-first flattened, excluding `pid`. */
export function listDescendants(pid: number): number[] {
  if (!Number.isInteger(pid) || pid <= 0) throw new Error(`invalid pid: ${pid}`)
  return hasProcfs() ? procDescendants(pid) : pgrepDescendants(pid)
}

function psField(pid: number, field: string): number | undefined {
  try {
    const result = Bun.spawnSync(["ps", "-o", `${field}=`, "-p", String(pid)])
    const value = Number.parseInt(result.stdout.toString().trim(), 10)
    return Number.isInteger(value) && value > 0 ? value : undefined
  } catch {
    return undefined
  }
}

/** Process-group id of `pid`, or `undefined` if it could not be read. */
export function processGroupId(pid: number): number | undefined {
  if (!Number.isInteger(pid) || pid <= 0) throw new Error(`invalid pid: ${pid}`)
  const info = hasProcfs() ? readStat(pid) : undefined
  if (info && info.pgrp > 0) return info.pgrp
  return psField(pid, "pgid")
}

/** True when `pid` exists (including when it is owned by another user). */
export function isAlive(pid: number): boolean {
  try {
    process.kill(pid, 0)
    return true
  } catch (error) {
    return (error as NodeJS.ErrnoException).code !== "ESRCH"
  }
}

/** True when any process remains in the group `pgid`. */
export function isGroupAlive(pgid: number): boolean {
  if (!Number.isInteger(pgid) || pgid <= 0) throw new Error(`invalid process group: ${pgid}`)
  try {
    process.kill(-pgid, 0)
    return true
  } catch (error) {
    return (error as NodeJS.ErrnoException).code !== "ESRCH"
  }
}

/** Signal a whole process group; a vanished group (`ESRCH`) is not an error. */
export function killProcessGroup(pgid: number, signal: NodeJS.Signals = "SIGTERM"): void {
  if (!Number.isInteger(pgid) || pgid <= 0) throw new Error(`invalid process group: ${pgid}`)
  try {
    process.kill(-pgid, signal)
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error
  }
}

export interface WaitForOptions {
  readonly timeoutMs?: number
  readonly intervalMs?: number
  /** Failure message used when the probe never produces a value. */
  readonly message: string
}

/** Options for {@link waitForDeath}; `message` is derived from the pids when omitted. */
export interface WaitForDeathOptions {
  readonly timeoutMs?: number
  readonly intervalMs?: number
  readonly message?: string
}

/** Poll `probe` until it returns a non-`undefined` value, or fail loudly. */
export async function waitFor<T>(
  probe: () => T | undefined | Promise<T | undefined>,
  options: WaitForOptions,
): Promise<T> {
  const timeoutMs = options.timeoutMs ?? 5000
  const intervalMs = options.intervalMs ?? 25
  const deadline = Date.now() + timeoutMs
  for (;;) {
    const value = await probe()
    if (value !== undefined) return value
    if (Date.now() >= deadline) throw new Error(`${options.message} (after ${timeoutMs}ms)`)
    await new Promise((resolve) => setTimeout(resolve, intervalMs))
  }
}

/** Resolve `true` once every pid has exited; `false` on timeout. */
export async function waitForDeath(pids: ReadonlyArray<number>, options: WaitForDeathOptions = {}): Promise<boolean> {
  const message = options.message ?? `processes did not exit: ${pids.join(",")}`
  try {
    await waitFor(
      () => (pids.every((pid) => !isAlive(pid)) ? true : undefined),
      { ...options, message },
    )
    return true
  } catch {
    return false
  }
}

/** Default per-read deadline for {@link StreamReader}. */
export const DEFAULT_READ_TIMEOUT_MS = 5000

export interface StreamReader {
  /** Read one line; `signal` aborts the wait and cancels the underlying stream. */
  readLine(timeoutMs?: number, signal?: AbortSignal): Promise<string>
  readExact(length: number, timeoutMs?: number, signal?: AbortSignal): Promise<string>
}

export interface SpawnDetachedOptions {
  readonly cwd?: string
  /** Environment overlay; merged over `process.env`. */
  readonly env?: Record<string, string | undefined>
  readonly stdin?: "pipe" | "ignore"
  readonly stdout?: "pipe" | "ignore"
}

export interface DetachedChild {
  readonly pid: number
  /** Equal to `pid`: a detached spawn makes the child its own group leader. */
  readonly pgid: number
  readonly process: Bun.Subprocess
  /** Write to the child's stdin and resolve once the bytes are flushed. */
  write(text: string): Promise<void>
  readonly reader: StreamReader
  kill(signal?: NodeJS.Signals): void
  killGroup(signal?: NodeJS.Signals): void
  readonly exited: Promise<number>
}

function concat(left: Uint8Array, right: Uint8Array): Uint8Array {
  const joined = new Uint8Array(left.length + right.length)
  joined.set(left, 0)
  joined.set(right, left.length)
  return joined
}

function unpipedReader(): StreamReader {
  const fail = () => Promise.reject(new Error("child stdout is not piped; pass stdout: \"pipe\" to read it"))
  return { readLine: fail, readExact: fail }
}

function createReader(stream: ReadableStream<Uint8Array>): StreamReader {
  const source = stream.getReader()
  const decoder = new TextDecoder()
  let buffer: Uint8Array = new Uint8Array(0)
  let closed = false
  // The single outstanding `read()`. A per-call timeout must NOT abandon it: an
  // abandoned read keeps consuming the next chunk off the stream and would
  // silently drop that data. Instead the in-flight read is retained and reused
  // by the next `fill`, so a timeout loses no bytes and leaks no read. Only an
  // explicit abort is terminal, and it cancels the stream so the read settles.
  let pending: ReturnType<typeof source.read> | undefined

  const fill = async (deadline: number, signal?: AbortSignal): Promise<void> => {
    if (closed) throw new Error("child output stream closed")
    signal?.throwIfAborted()
    const remaining = deadline - Date.now()
    if (remaining <= 0) throw new Error("timed out reading child output")

    if (pending === undefined) pending = source.read()
    const read = pending

    let timer: ReturnType<typeof setTimeout> | undefined
    const timeout = new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error("timed out reading child output")), remaining)
    })
    let rejectAbort: ((error: unknown) => void) | undefined
    const aborted = new Promise<never>((_, reject) => {
      rejectAbort = reject
    })
    const onAbort = (): void => {
      closed = true
      pending = undefined
      // Reject before cancelling so the caller observes the abort reason rather
      // than the "stream closed" result that `cancel()` makes the read produce.
      rejectAbort?.(signal?.reason instanceof Error ? signal.reason : new Error("read aborted"))
      void source.cancel().catch(() => undefined)
    }
    signal?.addEventListener("abort", onAbort, { once: true })

    try {
      const result = await Promise.race([read, timeout, aborted])
      pending = undefined
      if (result.done) throw new Error("child output stream closed")
      buffer = concat(buffer, result.value)
    } finally {
      if (timer) clearTimeout(timer)
      signal?.removeEventListener("abort", onAbort)
    }
  }

  return {
    readLine: async (timeoutMs = DEFAULT_READ_TIMEOUT_MS, signal?: AbortSignal) => {
      const deadline = Date.now() + timeoutMs
      for (;;) {
        signal?.throwIfAborted()
        const newline = buffer.indexOf(0x0a)
        if (newline >= 0) {
          const line = decoder.decode(buffer.slice(0, newline)).replace(/\r$/, "")
          buffer = buffer.slice(newline + 1)
          return line
        }
        await fill(deadline, signal)
      }
    },
    readExact: async (length, timeoutMs = DEFAULT_READ_TIMEOUT_MS, signal?: AbortSignal) => {
      const deadline = Date.now() + timeoutMs
      while (buffer.length < length) await fill(deadline, signal)
      const value = decoder.decode(buffer.slice(0, length))
      buffer = buffer.slice(length)
      return value
    },
  }
}

/** Spawn `command` detached so it becomes the leader of a fresh process group. */
export function spawnDetached(command: ReadonlyArray<string>, options: SpawnDetachedOptions = {}): DetachedChild {
  const env: Record<string, string> = {}
  for (const [key, value] of Object.entries({ ...process.env, ...options.env })) {
    if (value !== undefined) env[key] = value
  }

  const process_ = Bun.spawn(command as string[], {
    cwd: options.cwd,
    env,
    detached: true,
    stdin: options.stdin ?? "ignore",
    stdout: options.stdout ?? "ignore",
    stderr: "ignore",
  })

  if (process_.pid === undefined) throw new Error(`failed to spawn detached child: ${command.join(" ")}`)
  const pid = process_.pid
  const reader = options.stdout === "pipe" ? createReader(process_.stdout as ReadableStream<Uint8Array>) : unpipedReader()

  return {
    pid,
    pgid: pid,
    process: process_,
    write: async (text) => {
      const sink = process_.stdin as Bun.FileSink | undefined
      if (!sink) throw new Error("child stdin is not piped")
      await sink.write(text)
      await sink.flush()
    },
    reader,
    kill: (signal = "SIGTERM") => {
      process.kill(pid, signal)
    },
    killGroup: (signal = "SIGTERM") => {
      killProcessGroup(pid, signal)
    },
    exited: process_.exited as Promise<number>,
  }
}
