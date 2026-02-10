import { PrismaClient } from "@rising-intelligence/db";
import {
  getEnvString,
  getSecretValue,
  resolveDatabaseUrl,
} from "@rising-intelligence/shared";

type Flags = Record<string, string | boolean>;

function getStringFlag(flags: Flags, name: string): string | undefined {
  const value = flags[name];
  return typeof value === "string" ? value : undefined;
}

function getBooleanFlag(flags: Flags, name: string): boolean {
  return flags[name] === true;
}

function getDatabaseUrl(flags: Flags): string {
  const databaseUrlFlag = getStringFlag(flags, "database-url");
  const databaseUrlEnv = getEnvString("DATABASE_URL");

  const host = getStringFlag(flags, "postgres-host") || getEnvString("POSTGRES_HOST") || "localhost";
  const port = getStringFlag(flags, "postgres-port") || getEnvString("POSTGRES_PORT") || "5432";
  const db = getStringFlag(flags, "postgres-db") || getEnvString("POSTGRES_DB") || "rising_intelligence";
  const user = getStringFlag(flags, "postgres-user") || getEnvString("POSTGRES_USER") || "rising";

  let password = getStringFlag(flags, "postgres-password") || getEnvString("POSTGRES_PASSWORD");
  if (!password) {
    const secret = getSecretValue("POSTGRES_PASSWORD");
    if (secret) {
      password = secret;
    } else {
      password = "rising"; // Default password from docker-compose.yml
    }
  }

  return resolveDatabaseUrl(databaseUrlFlag || databaseUrlEnv, {
    host,
    port: parseInt(port, 10),
    db,
    user,
    password,
  });
}

export async function topicsList(flags: Flags): Promise<void> {
  const databaseUrl = getDatabaseUrl(flags);
  const showCounts = getBooleanFlag(flags, "counts");
  const minCount = parseInt(getStringFlag(flags, "min-count") || "0", 10);

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

    // eslint-disable-next-line no-console
    console.log(`\nTopics in raw_events (${results.length}):\n`);

    if (results.length === 0) {
      // eslint-disable-next-line no-console
      console.log(`  (no topics found)`);
    } else {
      for (const row of results) {
        if (showCounts) {
          const count = typeof row.count === 'bigint' ? Number(row.count) : row.count;
          // eslint-disable-next-line no-console
          console.log(`  ${row.topic.padEnd(40)} ${count}`);
        } else {
          // eslint-disable-next-line no-console
          console.log(`  ${row.topic}`);
        }
      }
    }

    // eslint-disable-next-line no-console
    console.log();
  } finally {
    await prisma.$disconnect();
  }
}
