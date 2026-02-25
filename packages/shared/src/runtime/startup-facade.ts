import type { Logger } from "pino";
import {
  createInitializationResourceBuilder,
  type InitializationResourceBuilder,
} from "./initialization-rollback.js";

export interface StartupResourceConnection<TResource> {
  readonly name: string;
  connect(): Promise<TResource> | TResource;
  disconnect(resource: TResource): Promise<void> | void;
  readonly disconnectErrorMessage?: string;
}

export interface StartupResourceSpec<TResource> {
  readonly name: string;
  connect(): Promise<TResource> | TResource;
  disconnect(resource: TResource): Promise<void> | void;
  readonly disconnectErrorMessage?: string;
  readonly rollbackAction?: string;
}

export interface StartupFacade {
  connect<TResource>(resource: StartupResourceConnection<TResource>): Promise<TResource>;
  run<TResult>(initialize: () => Promise<TResult>): Promise<TResult>;
  rollback(): Promise<void>;
}

export interface StartupResourceConnectionFactory {
  create<TResource>(spec: StartupResourceSpec<TResource>): StartupResourceConnection<TResource>;
}

export interface StartupResourceConnector {
  connect<TResource>(spec: StartupResourceSpec<TResource>): Promise<TResource>;
}

function defaultDisconnectErrorMessage(name: string, rollbackAction = "cleanup"): string {
  return `${name} ${rollbackAction} failed during initialization rollback`;
}

class DefaultStartupFacade implements StartupFacade {
  constructor(
    private readonly logger: Logger,
    private readonly resourceBuilder: InitializationResourceBuilder
  ) {}

  connect<TResource>(resource: StartupResourceConnection<TResource>): Promise<TResource> {
    return this.resourceBuilder.create({
      name: resource.name,
      create: resource.connect,
      rollback: resource.disconnect,
      rollbackErrorMessage:
        resource.disconnectErrorMessage ?? defaultDisconnectErrorMessage(resource.name),
    });
  }

  async run<TResult>(initialize: () => Promise<TResult>): Promise<TResult> {
    try {
      return await initialize();
    } catch (error) {
      await this.rollback();
      throw error;
    }
  }

  rollback(): Promise<void> {
    return this.resourceBuilder.rollback(this.logger);
  }
}

class DefaultStartupResourceConnectionFactory implements StartupResourceConnectionFactory {
  create<TResource>(spec: StartupResourceSpec<TResource>): StartupResourceConnection<TResource> {
    return {
      name: spec.name,
      connect: spec.connect,
      disconnect: spec.disconnect,
      disconnectErrorMessage:
        spec.disconnectErrorMessage
        ?? defaultDisconnectErrorMessage(spec.name, spec.rollbackAction ?? "cleanup"),
    };
  }
}

class DefaultStartupResourceConnector implements StartupResourceConnector {
  constructor(
    private readonly startup: StartupFacade,
    private readonly connectionFactory: StartupResourceConnectionFactory
  ) {}

  connect<TResource>(spec: StartupResourceSpec<TResource>): Promise<TResource> {
    return this.startup.connect(this.connectionFactory.create(spec));
  }
}

export function createStartupFacade(
  logger: Logger,
  resourceBuilder: InitializationResourceBuilder = createInitializationResourceBuilder()
): StartupFacade {
  return new DefaultStartupFacade(logger, resourceBuilder);
}

export function createStartupResourceConnectionFactory(): StartupResourceConnectionFactory {
  return new DefaultStartupResourceConnectionFactory();
}

export function createStartupResourceConnector(
  startup: StartupFacade,
  connectionFactory: StartupResourceConnectionFactory = createStartupResourceConnectionFactory()
): StartupResourceConnector {
  return new DefaultStartupResourceConnector(startup, connectionFactory);
}
