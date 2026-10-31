/**
 * A JSON value that stays as its validated UTF-8 bytes until something reads it. Pass-through consumers (SSE,
 * the TUI forwarder) splice `raw` into their output; readers call `value`, which parses once and caches.
 */
export class LazyJson {
  #value: unknown
  #parsed = false
  #text: string | undefined

  constructor(readonly raw: Uint8Array) {}

  static is(input: unknown): input is LazyJson {
    return input instanceof LazyJson
  }

  /** Wrap an already-parsed value (hub-originated events), so every consumer sees one shape. */
  static of(value: unknown): LazyJson {
    const lazy = new LazyJson(new TextEncoder().encode(JSON.stringify(value)))
    lazy.#value = value
    lazy.#parsed = true
    return lazy
  }

  /** The raw JSON text, decoded once. */
  get text(): string {
    this.#text ??= new TextDecoder().decode(this.raw)
    return this.#text
  }

  get value(): unknown {
    if (!this.#parsed) {
      this.#value = JSON.parse(this.text)
      this.#parsed = true
    }
    return this.#value
  }

  toJSON(): unknown {
    return this.value
  }
}

/** The plain value of event data that may be lazy. */
export const valueOf = (data: unknown): unknown => (LazyJson.is(data) ? data.value : data)
