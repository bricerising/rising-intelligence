import { z } from "zod";

const BriefFailurePayloadSchema = z.object({
  error_code: z.string().min(1),
  error_message: z.string().min(1),
  retryable: z.boolean(),
});

const BriefResultPayloadSchema = z
  .object({
    request_id: z.string().min(1),
    produced_at: z.string().datetime({ offset: true }),
    failure: BriefFailurePayloadSchema.optional(),
  })
  .passthrough();

export type BriefResultPayload = z.infer<typeof BriefResultPayloadSchema>;

export function parseBriefResultPayload(value: unknown): BriefResultPayload {
  const parsed = BriefResultPayloadSchema.safeParse(value);
  if (parsed.success) {
    return parsed.data;
  }

  const issue = parsed.error.issues[0];
  const path = issue.path.length > 0 ? issue.path.join(".") : "root";
  throw new Error(`Persisted brief result payload is invalid at '${path}': ${issue.message}`);
}

export function buildFailureBriefResultPayload(
  requestId: string,
  producedAt: Date,
  code: string,
  message: string,
  retryable: boolean
): BriefResultPayload {
  return {
    request_id: requestId,
    produced_at: producedAt.toISOString(),
    failure: {
      error_code: code,
      error_message: message,
      retryable,
    },
  };
}
