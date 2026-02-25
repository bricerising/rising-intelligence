const NO_COVERAGE_ERROR_CODE = "no_coverage";

export class LlmGenerationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "LlmGenerationError";
  }
}

export class NonRetryableProcessingError extends Error {
  public readonly code: string;

  constructor(message: string, code = "grounding_error") {
    super(message);
    this.name = "NonRetryableProcessingError";
    this.code = code;
  }
}

export function toGroundingError(message: string): NonRetryableProcessingError {
  return new NonRetryableProcessingError(message);
}

export function toNoCoverageError(message: string): NonRetryableProcessingError {
  return new NonRetryableProcessingError(message, NO_COVERAGE_ERROR_CODE);
}

export function classifyRetryableFailureCode(error: LlmGenerationError): "llm_error" | "timeout" {
  const message = error.message.toLowerCase();
  if (
    message.includes("timeout") ||
    message.includes("timed out") ||
    message.includes("etimedout") ||
    message.includes("abort")
  ) {
    return "timeout";
  }

  return "llm_error";
}
