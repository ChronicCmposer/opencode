/** AI SDK packages a session worker can construct from serialized options alone. */
export const WORKER_SDK_PACKAGES = [
  "@ai-sdk/anthropic",
  "@ai-sdk/google",
  "@ai-sdk/openai",
  "@ai-sdk/openai-compatible",
] as const

export type WorkerSdkPackage = (typeof WORKER_SDK_PACKAGES)[number]

export const isWorkerSdkPackage = (npm: string): npm is WorkerSdkPackage =>
  (WORKER_SDK_PACKAGES as readonly string[]).includes(npm)
