export interface SerializedError {
  message: string;
  name?: string;
  stack?: string;
}

export interface SerializeErrorOptions {
  includeStack?: boolean;
}

function shouldIncludeStackByDefault(): boolean {
  if (process.env.LOG_ERROR_STACKS === "true") {
    return true;
  }
  if (process.env.LOG_ERROR_STACKS === "false") {
    return false;
  }

  return process.env.NODE_ENV === "development" || process.env.NODE_ENV === "test";
}

export function serializeError(
  error: unknown,
  options: SerializeErrorOptions = {}
): SerializedError | string {
  const includeStack = options.includeStack ?? shouldIncludeStackByDefault();

  if (error instanceof Error) {
    return {
      message: error.message,
      name: error.name,
      ...(includeStack ? { stack: error.stack } : {}),
    };
  }

  return String(error);
}
