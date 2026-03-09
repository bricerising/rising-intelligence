import { z } from "zod";

export class ConfigValidationError extends Error {
  readonly issues: ReadonlyArray<z.ZodIssue>;

  constructor(issues: ReadonlyArray<z.ZodIssue>) {
    const details = issues
      .map((issue) => `${issue.path.join(".") || "<root>"}: ${issue.message}`)
      .join("\n  ");
    super(`Configuration validation failed:\n  ${details}`);
    this.name = "ConfigValidationError";
    this.issues = issues;
  }
}

/**
 * Parse and validate a Zod config schema against env vars.
 * Throws ConfigValidationError on validation failure.
 */
export function parseConfig<TOutput, TDef extends z.ZodTypeDef, TInput>(
  schema: z.ZodType<TOutput, TDef, TInput>,
  env: Record<string, string | undefined> = process.env as Record<string, string | undefined>
): TOutput {
  const result = schema.safeParse(env);
  if (!result.success) {
    throw new ConfigValidationError(result.error.issues);
  }
  return result.data;
}

/**
 * Zod helper for boolean env vars that arrive as "true"/"false" strings.
 */
export function zBooleanEnv(defaultValue: "true" | "false" = "true") {
  return z
    .string()
    .default(defaultValue)
    .transform((v) => v === "true");
}
