import { PrismaClient } from "@rising-intelligence/db";
import type { CliFlags } from "../../lib/args.js";
import { getBooleanFlag, getStringFlag } from "../../lib/flags.js";
import {
  parseNonNegativeIntegerStrict,
} from "../../lib/number.js";
import { resolveTopicsDatabaseUrl } from "./database-url.js";

export async function topicsList(flags: CliFlags): Promise<void> {
  const databaseUrl = resolveTopicsDatabaseUrl(flags);
  const showCounts = getBooleanFlag(flags, "counts");
  const minCountRaw = getStringFlag(flags, "min-count");
  if (minCountRaw && !showCounts) {
    throw new Error("--min-count requires --counts");
  }
  const minCount = minCountRaw
    ? parseNonNegativeIntegerStrict(minCountRaw, "--min-count")
    : 0;

  const prisma = new PrismaClient({
    datasources: {
      db: {
        url: databaseUrl,
      },
    },
  });

  try {
    // Query to get distinct topics using raw SQL since we need to unnest the array
    const query = showCounts
      ? `
        SELECT topic, COUNT(*) as count
        FROM raw_events, UNNEST(topics) AS topic
        GROUP BY topic
        HAVING COUNT(*) >= $1
        ORDER BY count DESC, topic ASC
      `
      : `
        SELECT DISTINCT topic
        FROM raw_events, UNNEST(topics) AS topic
        WHERE topic IS NOT NULL
        ORDER BY topic ASC
      `;

    interface TopicRow {
      topic: string;
      count?: bigint;
    }

    const results = await prisma.$queryRawUnsafe<TopicRow[]>(
      query,
      ...(showCounts ? [minCount] : [])
    );

    console.log(`\nTopics in raw_events (${results.length}):\n`);

    if (results.length === 0) {
      console.log(`  (no topics found)`);
    } else {
      for (const row of results) {
        if (showCounts) {
          const count = typeof row.count === "bigint" ? Number(row.count) : row.count;
          console.log(`  ${row.topic.padEnd(40)} ${count}`);
        } else {
          console.log(`  ${row.topic}`);
        }
      }
    }

    console.log();
  } finally {
    await prisma.$disconnect();
  }
}
