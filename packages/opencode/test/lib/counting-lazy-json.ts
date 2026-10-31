import { LazyJson } from "../../src/util/lazy-json"

/** A LazyJson that counts `.value` reads, to prove pass-through paths never parse. */
export class CountingLazyJson extends LazyJson {
  reads = 0
  serialised = 0
  constructor(input: unknown) {
    super(new TextEncoder().encode(JSON.stringify(input)))
  }
  override get value(): unknown {
    this.reads++
    return super.value
  }
  override toJSON(): unknown {
    this.serialised++
    return super.toJSON()
  }
}
