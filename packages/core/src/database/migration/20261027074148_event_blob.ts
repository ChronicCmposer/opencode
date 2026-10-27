import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

export default {
  id: "20261027074148_event_blob",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`
        CREATE TABLE \`event_blob\` (
          \`aggregate_id\` text NOT NULL,
          \`seq\` integer NOT NULL,
          \`type\` text NOT NULL,
          \`hash\` text NOT NULL,
          \`size\` integer NOT NULL,
          \`data\` text,
          \`file_path\` text,
          CONSTRAINT \`event_blob_pk\` PRIMARY KEY(\`aggregate_id\`, \`seq\`),
          CONSTRAINT \`fk_event_blob_aggregate_id_event_sequence_aggregate_id_fk\` FOREIGN KEY (\`aggregate_id\`) REFERENCES \`event_sequence\`(\`aggregate_id\`) ON DELETE CASCADE
        );
      `)
      yield* tx.run(`CREATE INDEX \`event_blob_hash_idx\` ON \`event_blob\` (\`hash\`);`)
    })
  },
} satisfies DatabaseMigration.Migration
