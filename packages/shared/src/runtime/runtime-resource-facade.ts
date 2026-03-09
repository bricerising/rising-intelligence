import type { Logger } from "pino";
import {
  createStartupFacade,
  createStartupResourceConnector,
  type StartupFacade,
  type StartupResourceConnector,
  type StartupResourceSpec,
} from "./startup-facade.js";

export interface RuntimeResourceFacade {
  connect<TResource>(spec: StartupResourceSpec<TResource>): Promise<TResource>;
}

export interface RuntimeCompositionRoot {
  startup: StartupFacade;
  resources: RuntimeResourceFacade;
}

class DefaultRuntimeResourceFacade implements RuntimeResourceFacade {
  constructor(private readonly resources: StartupResourceConnector) {}

  connect<TResource>(spec: StartupResourceSpec<TResource>): Promise<TResource> {
    return this.resources.connect(spec);
  }
}

/**
 * Create a resource spec for a health server.
 */
export function healthServerSpec<TServer>(
  connect: () => Promise<TServer> | TServer,
  disconnect: (server: TServer) => Promise<void> | void
): StartupResourceSpec<TServer> {
  return { name: "health-server", connect, disconnect, rollbackAction: "close" };
}

/**
 * Create a resource spec for a Postgres client.
 */
export function postgresSpec<TClient>(
  connect: () => Promise<TClient> | TClient,
  disconnect: (client: TClient) => Promise<void> | void
): StartupResourceSpec<TClient> {
  return { name: "postgres", connect, disconnect, rollbackAction: "disconnect" };
}

/**
 * Create a resource spec for a Redis client.
 */
export function redisSpec<TClient>(
  connect: () => Promise<TClient> | TClient,
  disconnect: (client: TClient) => Promise<void> | void
): StartupResourceSpec<TClient> {
  return { name: "redis", connect, disconnect, rollbackAction: "disconnect" };
}

/**
 * Create a resource spec for a Kafka consumer.
 */
export function kafkaConsumerSpec<TContext>(
  connect: () => Promise<TContext> | TContext,
  disconnect: (context: TContext) => Promise<void> | void
): StartupResourceSpec<TContext> {
  return { name: "kafka-consumer", connect, disconnect, rollbackAction: "disconnect" };
}

/**
 * Create a resource spec for a Kafka producer.
 */
export function kafkaProducerSpec<TContext>(
  connect: () => Promise<TContext> | TContext,
  disconnect: (context: TContext) => Promise<void> | void
): StartupResourceSpec<TContext> {
  return { name: "kafka-producer", connect, disconnect, rollbackAction: "disconnect" };
}

/**
 * Facade over startup resource connector with canonical names and rollback
 * actions for service runtime resources.
 */
export function createRuntimeResourceFacade(
  resources: StartupResourceConnector
): RuntimeResourceFacade {
  return new DefaultRuntimeResourceFacade(resources);
}

/**
 * Factory for the standard service composition root:
 * startup rollback orchestrator + named runtime resource connector facade.
 */
export function createRuntimeCompositionRoot(
  logger: Logger,
  startup: StartupFacade = createStartupFacade(logger),
  connector: StartupResourceConnector = createStartupResourceConnector(startup)
): RuntimeCompositionRoot {
  return {
    startup,
    resources: createRuntimeResourceFacade(connector),
  };
}
