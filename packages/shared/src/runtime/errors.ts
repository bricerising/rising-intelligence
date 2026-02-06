export interface SerializedError {
  message: string;
  name?: string;
  stack?: string;
}

export function serializeError(error: unknown): SerializedError | string {
  if (error instanceof Error) {
    return {
      message: error.message,
      name: error.name,
      stack: error.stack,
    };
  }

  return String(error);
}
