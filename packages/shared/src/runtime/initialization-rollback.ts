import type { Logger } from "pino";
import { runShutdownSteps, type ShutdownStep } from "./lifecycle.js";

export interface InitializationRollbackBuilder {
  register(step: ShutdownStep): void;
  rollback(logger: Logger): Promise<void>;
}

/**
 * Builder for startup rollback steps.
 * Steps are executed in reverse registration order so partial initialization
 * unwinds in LIFO order.
 */
export function createInitializationRollbackBuilder(): InitializationRollbackBuilder {
  const steps: ShutdownStep[] = [];

  return {
    register(step: ShutdownStep): void {
      steps.unshift(step);
    },
    async rollback(logger: Logger): Promise<void> {
      if (steps.length === 0) {
        return;
      }

      await runShutdownSteps(logger, steps);
    },
  };
}
