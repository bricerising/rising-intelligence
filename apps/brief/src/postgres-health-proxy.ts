import type { HealthContext } from "./health.js";

type AsyncMethod = (...args: unknown[]) => Promise<unknown>;

type AsyncMethodKey<T extends object> = {
  [K in keyof T]: T[K] extends (...args: infer _Args) => Promise<unknown> ? K : never;
}[keyof T];

function isTrackedMethod(
  property: PropertyKey,
  trackedMethods: ReadonlySet<PropertyKey>
): boolean {
  return trackedMethods.has(property);
}

/**
 * Proxy decorator that keeps Postgres health status aligned with tracked async methods.
 */
export function createPostgresHealthProxy<T extends object>(
  delegate: T,
  healthContext: HealthContext,
  trackedMethods: readonly AsyncMethodKey<T>[]
): T {
  const trackedMethodSet = new Set<PropertyKey>(trackedMethods as readonly PropertyKey[]);
  const wrappedMethodCache = new Map<PropertyKey, AsyncMethod>();

  return new Proxy(delegate, {
    get(target, property, receiver) {
      const value = Reflect.get(target, property, receiver);
      if (
        typeof value !== "function" ||
        !isTrackedMethod(property, trackedMethodSet)
      ) {
        return value;
      }

      const cachedMethod = wrappedMethodCache.get(property);
      if (cachedMethod) {
        return cachedMethod;
      }

      const wrappedMethod: AsyncMethod = async (...args) => {
        try {
          const result = await Reflect.apply(value as AsyncMethod, target, args);
          healthContext.postgresHealthy = true;
          return result;
        } catch (error) {
          healthContext.postgresHealthy = false;
          throw error;
        }
      };

      wrappedMethodCache.set(property, wrappedMethod);
      return wrappedMethod;
    },
  });
}
