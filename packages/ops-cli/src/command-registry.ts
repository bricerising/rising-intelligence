import type { CliFlags } from "./lib/args.js";

export type CliCommandHandler = (flags: CliFlags) => Promise<void>;

export interface CliCommandDefinition {
  readonly group: string;
  readonly command: string;
  readonly aliases?: readonly string[];
  readonly summary: string;
  run: CliCommandHandler;
}

export interface CliCommand extends CliCommandDefinition {
  readonly key: string;
  readonly aliases: readonly string[];
}

export function toCommandKey(group: string, command: string): string {
  return `${group}:${command}`;
}

type LazyCommandModule = Record<string, unknown>;

export interface CreateLazyCommandDefinitionInput {
  readonly group: string;
  readonly command: string;
  readonly aliases?: readonly string[];
  readonly summary: string;
  readonly loadModule: () => Promise<LazyCommandModule>;
  readonly exportName: string;
}

function resolveLazyCommandHandler(
  module: LazyCommandModule,
  exportName: string,
  commandKey: string
): CliCommandHandler {
  const candidate = module[exportName];
  if (typeof candidate !== "function") {
    throw new Error(
      `Invalid command module for ${commandKey}: export '${exportName}' is not a function`
    );
  }

  return candidate as CliCommandHandler;
}

export function createLazyCommandDefinition(
  input: CreateLazyCommandDefinitionInput
): CliCommandDefinition {
  return {
    group: input.group,
    command: input.command,
    aliases: input.aliases,
    summary: input.summary,
    run: async (flags) => {
      const module = await input.loadModule();
      const commandKey = toCommandKey(input.group, input.command);
      const handler = resolveLazyCommandHandler(module, input.exportName, commandKey);
      await handler(flags);
    },
  };
}

export class CommandRegistry {
  private readonly commands: readonly CliCommand[];
  private readonly byKey = new Map<string, CliCommand>();

  constructor(definitions: readonly CliCommandDefinition[]) {
    this.commands = definitions.map((definition) => {
      const key = toCommandKey(definition.group, definition.command);
      const aliases = [key, ...(definition.aliases ?? [])];
      const command: CliCommand = {
        ...definition,
        key,
        aliases,
      };

      for (const alias of aliases) {
        if (this.byKey.has(alias)) {
          throw new Error(`Duplicate CLI command alias: ${alias}`);
        }
        this.byKey.set(alias, command);
      }

      return command;
    });
  }

  list(): readonly CliCommand[] {
    return this.commands;
  }

  resolve(group: string, command: string): CliCommand | null {
    return this.byKey.get(toCommandKey(group, command)) ?? null;
  }
}

const LGTM_URLS = {
  grafana: "http://localhost:3001",
  loki: "http://localhost:3100",
  tempo: "http://localhost:3200",
  mimir: "http://localhost:9009",
  otlpGrpc: "localhost:4317",
  otlpHttp: "http://localhost:4318",
  kafka: "localhost:9092",
  schemaRegistry: "http://localhost:8081",
} as const;

const DEFAULT_COMMANDS: readonly CliCommandDefinition[] = [
  createLazyCommandDefinition({
    group: "schema-registry",
    command: "publish-protos",
    aliases: ["schema-registry:publish"],
    summary: "Publish protobuf schemas + subjects",
    loadModule: () => import("./commands/schema-registry/publish-protos.js"),
    exportName: "schemaRegistryPublishProtos",
  }),
  createLazyCommandDefinition({
    group: "kafka",
    command: "create-topics",
    summary: "Create required Kafka topics",
    loadModule: () => import("./commands/kafka/create-topics.js"),
    exportName: "kafkaCreateTopics",
  }),
  createLazyCommandDefinition({
    group: "kafka",
    command: "topics",
    aliases: ["kafka:list"],
    summary: "List all Kafka topics",
    loadModule: () => import("./commands/kafka/topics.js"),
    exportName: "kafkaTopics",
  }),
  createLazyCommandDefinition({
    group: "kafka",
    command: "ensure-topics",
    aliases: ["kafka:ensure"],
    summary: "Ensure topics exist and leaders are ready",
    loadModule: () => import("./commands/kafka/ensure-topics.js"),
    exportName: "kafkaEnsureTopics",
  }),
  {
    group: "lgtm",
    command: "urls",
    aliases: ["lgtm:endpoints"],
    summary: "Print local dev endpoints",
    async run() {
      for (const [name, endpoint] of Object.entries(LGTM_URLS)) {
        console.log(`${name}: ${endpoint}`);
      }
    },
  },
  createLazyCommandDefinition({
    group: "brief",
    command: "trigger",
    aliases: ["brief:publish-summary-request", "brief:generate-summary-request"],
    summary: "Publish a manual SummaryRequest to Kafka",
    loadModule: () => import("./commands/brief/trigger.js"),
    exportName: "briefTrigger",
  }),
  createLazyCommandDefinition({
    group: "brief",
    command: "diagnose",
    aliases: ["brief:triage"],
    summary: "Trigger and diagnose brief request execution path",
    loadModule: () => import("./commands/brief/diagnose.js"),
    exportName: "briefDiagnose",
  }),
  createLazyCommandDefinition({
    group: "topics",
    command: "list",
    summary: "List distinct topics from raw_events table",
    loadModule: () => import("./commands/topics/list.js"),
    exportName: "topicsList",
  }),
  createLazyCommandDefinition({
    group: "topics",
    command: "retag",
    aliases: ["topics:reclassify"],
    summary: "Recompute raw_events tags/topics from allowlist rules",
    loadModule: () => import("./commands/topics/retag.js"),
    exportName: "topicsRetag",
  }),
  createLazyCommandDefinition({
    group: "events",
    command: "enrich",
    aliases: ["events:backfill"],
    summary: "Retrofit raw_events tags/topics and quality metadata",
    loadModule: () => import("./commands/events/enrich.js"),
    exportName: "eventsEnrich",
  }),
  createLazyCommandDefinition({
    group: "db",
    command: "snapshot",
    aliases: ["db:backup"],
    summary: "Create Postgres snapshot backups (single-run or scheduled loop)",
    loadModule: () => import("./commands/db/snapshot.js"),
    exportName: "dbSnapshot",
  }),
  createLazyCommandDefinition({
    group: "db",
    command: "test-bootstrap-check",
    aliases: ["db:test-bootstrap"],
    summary: "Check test DB bootstrap tables and Prisma migrations",
    loadModule: () => import("./commands/db/test-bootstrap-check.js"),
    exportName: "dbTestBootstrapCheck",
  }),
  createLazyCommandDefinition({
    group: "e2e",
    command: "brief-run",
    aliases: ["e2e:brief"],
    summary: "Run brief compose e2e with isolated project/ports",
    loadModule: () => import("./commands/e2e/brief-run.js"),
    exportName: "e2eBriefRun",
  }),
];

export function createDefaultCommandRegistry(): CommandRegistry {
  return new CommandRegistry(DEFAULT_COMMANDS);
}
