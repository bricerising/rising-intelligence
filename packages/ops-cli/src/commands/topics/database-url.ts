import {
  getEnvString,
  getSecretValue,
} from "@rising-intelligence/shared/config";
import { resolveDatabaseUrl } from "@rising-intelligence/shared/database";
import type { CliFlags } from "../../lib/args.js";
import { getStringFlag } from "../../lib/flags.js";
import { parsePositiveIntegerStrict } from "../../lib/number.js";

export function resolveTopicsDatabaseUrl(flags: CliFlags): string {
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
    port: parsePositiveIntegerStrict(port, "--postgres-port"),
    db,
    user,
    password,
  });
}
