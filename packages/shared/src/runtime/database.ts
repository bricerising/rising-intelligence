import { getSecretValue } from "./secrets.js";

/**
 * Resolve a Postgres password from env or _FILE secret.
 * Falls back to the provided default (or "rising").
 */
export function resolvePostgresPassword(
  env: Record<string, string | undefined>,
  fallback = "rising"
): string {
  if (env.POSTGRES_PASSWORD && env.POSTGRES_PASSWORD.trim().length > 0) {
    return env.POSTGRES_PASSWORD;
  }

  const secret = getSecretValue("POSTGRES_PASSWORD");
  if (secret && secret.trim().length > 0) {
    return secret;
  }

  return fallback;
}

/**
 * Build a postgresql:// connection URL from components.
 */
export function buildPostgresUrl(opts: {
  host: string;
  port: number;
  db: string;
  user: string;
  password: string;
}): string {
  const username = encodeURIComponent(opts.user);
  const password = encodeURIComponent(opts.password);
  const database = encodeURIComponent(opts.db);

  return `postgresql://${username}:${password}@${opts.host}:${opts.port}/${database}`;
}

/**
 * Resolve DATABASE_URL: use explicit value if non-empty, otherwise build from components.
 */
export function resolveDatabaseUrl(
  explicitUrl: string | undefined,
  components: { host: string; port: number; db: string; user: string; password: string }
): string {
  if (explicitUrl && explicitUrl.trim().length > 0) {
    return explicitUrl;
  }
  return buildPostgresUrl(components);
}
