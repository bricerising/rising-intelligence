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
  connectHealthServer<TServer>(
    connect: () => Promise<TServer> | TServer,
    disconnect: (server: TServer) => Promise<void> | void
  ): Promise<TServer>;
  connectPostgres<TClient>(
    connect: () => Promise<TClient> | TClient,
    disconnect: (client: TClient) => Promise<void> | void
  ): Promise<TClient>;
  connectRedis<TClient>(
    connect: () => Promise<TClient> | TClient,
    disconnect: (client: TClient) => Promise<void> | void
  ): Promise<TClient>;
  connectKafkaConsumer<TContext>(
    connect: () => Promise<TContext> | TContext,
    disconnect: (context: TContext) => Promise<void> | void
  ): Promise<TContext>;
  connectKafkaProducer<TContext>(
    connect: () => Promise<TContext> | TContext,
    disconnect: (context: TContext) => Promise<void> | void
  ): Promise<TContext>;
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

  connectHealthServer<TServer>(
    connect: () => Promise<TServer> | TServer,
    disconnect: (server: TServer) => Promise<void> | void
  ): Promise<TServer> {
    return this.connect({
      name: "health-server",
      connect,
      disconnect,
      rollbackAction: "close",
    });
  }

  connectPostgres<TClient>(
    connect: () => Promise<TClient> | TClient,
    disconnect: (client: TClient) => Promise<void> | void
  ): Promise<TClient> {
    return this.connect({
      name: "postgres",
      connect,
      disconnect,
      rollbackAction: "disconnect",
    });
  }

  connectRedis<TClient>(
    connect: () => Promise<TClient> | TClient,
    disconnect: (client: TClient) => Promise<void> | void
  ): Promise<TClient> {
    return this.connect({
      name: "redis",
      connect,
      disconnect,
      rollbackAction: "disconnect",
    });
  }

  connectKafkaConsumer<TContext>(
    connect: () => Promise<TContext> | TContext,
    disconnect: (context: TContext) => Promise<void> | void
  ): Promise<TContext> {
    return this.connect({
      name: "kafka-consumer",
      connect,
      disconnect,
      rollbackAction: "disconnect",
    });
  }

  connectKafkaProducer<TContext>(
    connect: () => Promise<TContext> | TContext,
    disconnect: (context: TContext) => Promise<void> | void
  ): Promise<TContext> {
    return this.connect({
      name: "kafka-producer",
      connect,
      disconnect,
      rollbackAction: "disconnect",
    });
  }
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
