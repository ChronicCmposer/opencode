import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

export default {
  id: "20261027032116_add_worker_lease",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`ALTER TABLE \`event_sequence\` ADD \`worker_owner_id\` text;`)
      yield* tx.run(`ALTER TABLE \`event_sequence\` ADD \`worker_heartbeat_at\` integer;`)
      yield* tx.run(`ALTER TABLE \`event_sequence\` ADD \`worker_expires_at\` integer;`)
    })
  },
} satisfies DatabaseMigration.Migration
