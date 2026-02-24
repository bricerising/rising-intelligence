export type AsyncChainNext<TResult> = () => Promise<TResult>;

export interface AsyncChainStep<TContext, TResult> {
  readonly name: string;
  execute(context: TContext, next: AsyncChainNext<TResult>): Promise<TResult>;
}

export interface RunAsyncChainOptions<TContext, TResult> {
  onEnd(context: TContext, index: number): TResult | Promise<TResult>;
  duplicateNextError?: (stepName: string) => Error;
}

function defaultDuplicateNextError(stepName: string): Error {
  return new Error(`Pipeline step "${stepName}" called next() multiple times`);
}

export async function runAsyncChain<TContext, TResult>(
  steps: readonly AsyncChainStep<TContext, TResult>[],
  context: TContext,
  options: RunAsyncChainOptions<TContext, TResult>
): Promise<TResult> {
  const duplicateNextError = options.duplicateNextError ?? defaultDuplicateNextError;

  const dispatch = async (index: number): Promise<TResult> => {
    const step = steps[index];
    if (!step) {
      return options.onEnd(context, index);
    }

    let nextCalled = false;
    const next: AsyncChainNext<TResult> = async () => {
      if (nextCalled) {
        throw duplicateNextError(step.name);
      }
      nextCalled = true;
      return dispatch(index + 1);
    };

    return step.execute(context, next);
  };

  return dispatch(0);
}
