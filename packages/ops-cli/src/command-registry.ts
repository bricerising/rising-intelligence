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

interface CommandTokenResolver {
  readonly name: string;
  resolve(
    commandTokens: readonly string[],
    commandLookup: CommandLookup
  ): CliCommand | null;
}

interface CommandLookup {
  get(commandKey: string): CliCommand | null;
}

function normalizeCommandTokenForLookup(value: string): string | null {
  const normalized = value.trim();
  return normalized.length === 0 ? null : normalized;
}

function normalizeCommandTokenForRegistration(value: string, field: string): string {
  const normalized = value.trim();
  if (normalized.length === 0) {
    throw new Error(`CLI command ${field} cannot be empty`);
  }
  return normalized;
}

function normalizeRegisteredAliases(
  key: string,
  aliases: readonly string[] | undefined
): string[] {
  const normalized = new Set<string>([key]);
  for (const alias of aliases ?? []) {
    normalized.add(normalizeCommandTokenForRegistration(alias, "alias"));
  }
  return [...normalized];
}

function createCommand(definition: CliCommandDefinition): CliCommand {
  const group = normalizeCommandTokenForRegistration(definition.group, "group");
  const command = normalizeCommandTokenForRegistration(definition.command, "command");
  const key = toCommandKey(group, command);
  return {
    ...definition,
    group,
    command,
    key,
    aliases: normalizeRegisteredAliases(key, definition.aliases),
  };
}

class CommandAliasIndex implements CommandLookup {
  private readonly byAlias = new Map<string, CliCommand>();

  register(command: CliCommand): void {
    for (const alias of command.aliases) {
      const existing = this.byAlias.get(alias);
      if (existing) {
        throw new Error(`Duplicate CLI command alias: ${alias}`);
      }
      this.byAlias.set(alias, command);
    }
  }

  get(commandKey: string): CliCommand | null {
    const normalized = normalizeCommandTokenForLookup(commandKey);
    if (!normalized) {
      return null;
    }
    return this.byAlias.get(normalized) ?? null;
  }
}

const TWO_TOKEN_COMMAND_RESOLVER: CommandTokenResolver = {
  name: "two-token",
  resolve(commandTokens, commandLookup): CliCommand | null {
    if (commandTokens.length !== 2) {
      return null;
    }

    const [rawGroup, rawCommand] = commandTokens;
    const group = normalizeCommandTokenForLookup(rawGroup);
    const command = normalizeCommandTokenForLookup(rawCommand);
    if (!group || !command) {
      return null;
    }

    return commandLookup.get(toCommandKey(group, command));
  },
};

const SINGLE_TOKEN_KEY_RESOLVER: CommandTokenResolver = {
  name: "single-token-key",
  resolve(commandTokens, commandLookup): CliCommand | null {
    if (commandTokens.length !== 1) {
      return null;
    }

    const [rawKey] = commandTokens;
    return commandLookup.get(rawKey);
  },
};

const DEFAULT_COMMAND_TOKEN_RESOLVERS: readonly CommandTokenResolver[] = [
  TWO_TOKEN_COMMAND_RESOLVER,
  SINGLE_TOKEN_KEY_RESOLVER,
];

export interface CreateLazyCommandDefinitionInput {
  readonly group: string;
  readonly command: string;
  readonly aliases?: readonly string[];
  readonly summary: string;
  readonly loadModule: () => Promise<LazyCommandModule>;
  readonly exportName: string;
}

function normalizeCommandGroupAndName(
  group: string,
  command: string
): { group: string; command: string } {
  return {
    group: normalizeCommandTokenForRegistration(group, "group"),
    command: normalizeCommandTokenForRegistration(command, "command"),
  };
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
  const { group, command } = normalizeCommandGroupAndName(input.group, input.command);
  const exportName = normalizeCommandTokenForRegistration(input.exportName, "export name");

  return {
    group,
    command,
    aliases: input.aliases,
    summary: input.summary,
    run: async (flags) => {
      const module = await input.loadModule();
      const commandKey = toCommandKey(group, command);
      const handler = resolveLazyCommandHandler(module, exportName, commandKey);
      await handler(flags);
    },
  };
}

function capitalizeWord(value: string): string {
  return value.charAt(0).toUpperCase() + value.slice(1);
}

function toCamelCase(value: string): string {
  const words = value
    .split(/[^A-Za-z0-9]+/)
    .map((word) => word.trim())
    .filter((word) => word.length > 0);

  return words
    .map((word, index) => {
      const lower = word.toLowerCase();
      return index === 0 ? lower : capitalizeWord(lower);
    })
    .join("");
}

export function toConventionalLazyCommandExportName(group: string, command: string): string {
  const normalized = normalizeCommandGroupAndName(group, command);
  return `${toCamelCase(normalized.group)}${capitalizeWord(toCamelCase(normalized.command))}`;
}

export function toConventionalLazyCommandModulePath(group: string, command: string): string {
  const normalized = normalizeCommandGroupAndName(group, command);
  return `./commands/${normalized.group}/${normalized.command}.js`;
}

export interface CreateConventionalLazyCommandDefinitionInput {
  readonly group: string;
  readonly command: string;
  readonly aliases?: readonly string[];
  readonly summary: string;
  readonly modulePath?: string;
  readonly exportName?: string;
}

export function createConventionalLazyCommandDefinition(
  input: CreateConventionalLazyCommandDefinitionInput
): CliCommandDefinition {
  const { group, command } = normalizeCommandGroupAndName(input.group, input.command);
  const modulePath = input.modulePath ?? toConventionalLazyCommandModulePath(group, command);
  const exportName = input.exportName ?? toConventionalLazyCommandExportName(group, command);

  return createLazyCommandDefinition({
    group,
    command,
    aliases: input.aliases,
    summary: input.summary,
    loadModule: () => import(modulePath),
    exportName,
  });
}

interface CommandGroupStaticInput {
  readonly command: string;
  readonly aliases?: readonly string[];
  readonly summary: string;
  run: CliCommandHandler;
}

interface CommandGroupLazyInput {
  readonly command: string;
  readonly aliases?: readonly string[];
  readonly summary: string;
  readonly modulePath?: string;
  readonly exportName?: string;
}

class CommandGroupBuilder {
  constructor(
    private readonly catalogBuilder: CommandCatalogBuilder,
    private readonly group: string
  ) {}

  lazy(input: CommandGroupLazyInput): this {
    this.catalogBuilder.add(
      createConventionalLazyCommandDefinition({
        group: this.group,
        command: input.command,
        aliases: input.aliases,
        summary: input.summary,
        modulePath: input.modulePath,
        exportName: input.exportName,
      })
    );
    return this;
  }

  command(input: CommandGroupStaticInput): this {
    this.catalogBuilder.add({
      group: this.group,
      command: input.command,
      aliases: input.aliases,
      summary: input.summary,
      run: input.run,
    });
    return this;
  }
}

class CommandCatalogBuilder {
  private readonly definitions: CliCommandDefinition[] = [];

  group(group: string): CommandGroupBuilder {
    return new CommandGroupBuilder(this, group);
  }

  add(definition: CliCommandDefinition): this {
    this.definitions.push(definition);
    return this;
  }

  build(): readonly CliCommandDefinition[] {
    return [...this.definitions];
  }
}

export class CommandRegistry {
  private readonly commands: readonly CliCommand[];
  private readonly commandLookup: CommandLookup;
  private readonly commandTokenResolvers: readonly CommandTokenResolver[];

  constructor(
    definitions: readonly CliCommandDefinition[],
    commandTokenResolvers: readonly CommandTokenResolver[] = DEFAULT_COMMAND_TOKEN_RESOLVERS
  ) {
    this.commandTokenResolvers = commandTokenResolvers;
    const commandAliasIndex = new CommandAliasIndex();
    this.commands = definitions.map((definition) => {
      const command = createCommand(definition);
      commandAliasIndex.register(command);
      return command;
    });
    this.commandLookup = commandAliasIndex;
  }

  list(): readonly CliCommand[] {
    return this.commands;
  }

  resolveInput(commandTokens: readonly string[]): CliCommand | null {
    for (const resolver of this.commandTokenResolvers) {
      const resolved = resolver.resolve(commandTokens, this.commandLookup);
      if (resolved) {
        return resolved;
      }
    }

    return null;
  }

  resolve(group: string, command: string): CliCommand | null {
    return this.resolveInput([group, command]);
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

function createLgtmUrlsCommandDefinition(): CliCommandDefinition {
  return {
    group: "lgtm",
    command: "urls",
    aliases: ["lgtm:endpoints"],
    summary: "Print local dev endpoints",
    async run() {
      for (const [name, endpoint] of Object.entries(LGTM_URLS)) {
        // eslint-disable-next-line no-console
        console.log(`${name}: ${endpoint}`);
      }
    },
  };
}

function createDefaultCommandDefinitions(): readonly CliCommandDefinition[] {
  const builder = new CommandCatalogBuilder();

  builder.group("schema-registry")
    .lazy({
      command: "publish-protos",
      aliases: ["schema-registry:publish"],
      summary: "Publish protobuf schemas + subjects",
    });

  builder.group("kafka")
    .lazy({
      command: "create-topics",
      summary: "Create required Kafka topics",
    })
    .lazy({
      command: "topics",
      aliases: ["kafka:list"],
      summary: "List all Kafka topics",
    })
    .lazy({
      command: "ensure-topics",
      aliases: ["kafka:ensure"],
      summary: "Ensure topics exist and leaders are ready",
    });

  builder.add(createLgtmUrlsCommandDefinition());

  builder.group("brief")
    .lazy({
      command: "trigger",
      aliases: ["brief:publish-summary-request", "brief:generate-summary-request"],
      summary: "Publish a manual SummaryRequest to Kafka",
    })
    .lazy({
      command: "diagnose",
      aliases: ["brief:triage"],
      summary: "Trigger and diagnose brief request execution path",
    });

  builder.group("topics")
    .lazy({
      command: "list",
      summary: "List distinct topics from raw_events table",
    })
    .lazy({
      command: "retag",
      aliases: ["topics:reclassify"],
      summary: "Recompute raw_events tags/topics from allowlist rules",
    });

  builder.group("events")
    .lazy({
      command: "enrich",
      aliases: ["events:backfill"],
      summary: "Retrofit raw_events tags/topics and quality metadata",
    });

  builder.group("db")
    .lazy({
      command: "snapshot",
      aliases: ["db:backup"],
      summary: "Create Postgres snapshot backups (single-run or scheduled loop)",
    })
    .lazy({
      command: "test-bootstrap-check",
      aliases: ["db:test-bootstrap"],
      summary: "Check test DB bootstrap tables and Prisma migrations",
    });

  builder.group("e2e")
    .lazy({
      command: "brief-run",
      aliases: ["e2e:brief"],
      summary: "Run brief compose e2e with isolated project/ports",
    });

  return builder.build();
}

const DEFAULT_COMMANDS = createDefaultCommandDefinitions();

export function createDefaultCommandRegistry(): CommandRegistry {
  return new CommandRegistry(DEFAULT_COMMANDS);
}
