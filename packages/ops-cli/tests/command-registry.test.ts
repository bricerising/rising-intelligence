import { describe, expect, it, vi } from "vitest";
import {
  CommandRegistry,
  createConventionalLazyCommandDefinition,
  createDefaultCommandRegistry,
  createLazyCommandDefinition,
  toConventionalLazyCommandExportName,
  toConventionalLazyCommandModulePath,
} from "../src/command-registry.js";

describe("CommandRegistry", () => {
  it("resolves canonical and alias command keys to the same handler", () => {
    const run = vi.fn(async () => {});
    const registry = new CommandRegistry([
      {
        group: "kafka",
        command: "topics",
        aliases: ["kafka:list"],
        summary: "List Kafka topics",
        run,
      },
    ]);

    const canonical = registry.resolve("kafka", "topics");
    const alias = registry.resolve("kafka", "list");

    expect(canonical).not.toBeNull();
    expect(alias).not.toBeNull();
    expect(canonical).toBe(alias);
  });

  it("resolves single-token canonical and alias keys", () => {
    const run = vi.fn(async () => {});
    const registry = new CommandRegistry([
      {
        group: "kafka",
        command: "topics",
        aliases: ["kafka:list"],
        summary: "List Kafka topics",
        run,
      },
    ]);

    const canonical = registry.resolveInput(["kafka:topics"]);
    const alias = registry.resolveInput(["kafka:list"]);

    expect(canonical).not.toBeNull();
    expect(alias).not.toBeNull();
    expect(canonical).toBe(alias);
  });

  it("returns null when a command is not registered", () => {
    const registry = new CommandRegistry([
      {
        group: "kafka",
        command: "topics",
        summary: "List Kafka topics",
        run: async () => {},
      },
    ]);

    expect(registry.resolve("kafka", "missing")).toBeNull();
    expect(registry.resolveInput(["kafka:missing"])).toBeNull();
    expect(registry.resolveInput(["kafka", "topics", "extra"])).toBeNull();
  });

  it("throws when duplicate aliases are configured", () => {
    expect(() =>
      new CommandRegistry([
        {
          group: "kafka",
          command: "topics",
          aliases: ["kafka:list"],
          summary: "List Kafka topics",
          run: async () => {},
        },
        {
          group: "lgtm",
          command: "urls",
          aliases: ["kafka:list"],
          summary: "List endpoints",
          run: async () => {},
        },
      ])
    ).toThrow(/duplicate cli command alias/i);
  });

  it("normalizes command group/command aliases and lookup tokens", () => {
    const run = vi.fn(async () => {});
    const registry = new CommandRegistry([
      {
        group: " kafka ",
        command: " topics ",
        aliases: ["  kafka:list  "],
        summary: "List Kafka topics",
        run,
      },
    ]);

    const canonical = registry.resolve("kafka", "topics");
    const twoTokenSpaced = registry.resolveInput([" kafka ", " topics "]);
    const alias = registry.resolve("kafka", "list");
    const aliasSpaced = registry.resolveInput(["  kafka:list  "]);

    expect(canonical).not.toBeNull();
    expect(twoTokenSpaced).toBe(canonical);
    expect(alias).toBe(canonical);
    expect(aliasSpaced).toBe(canonical);
    expect(canonical?.aliases).toEqual(["kafka:topics", "kafka:list"]);
  });

  it("throws when group, command, or aliases are empty after trimming", () => {
    expect(() =>
      new CommandRegistry([
        {
          group: " ",
          command: "topics",
          summary: "List Kafka topics",
          run: async () => {},
        },
      ])
    ).toThrow(/command group cannot be empty/i);

    expect(() =>
      new CommandRegistry([
        {
          group: "kafka",
          command: " ",
          summary: "List Kafka topics",
          run: async () => {},
        },
      ])
    ).toThrow(/command command cannot be empty/i);

    expect(() =>
      new CommandRegistry([
        {
          group: "kafka",
          command: "topics",
          aliases: ["  "],
          summary: "List Kafka topics",
          run: async () => {},
        },
      ])
    ).toThrow(/command alias cannot be empty/i);
  });

  it("creates lazy command definitions that resolve and invoke module handlers", async () => {
    const run = vi.fn(async () => {});
    const command = createLazyCommandDefinition({
      group: "kafka",
      command: "topics",
      summary: "List topics",
      loadModule: async () => ({ kafkaTopics: run }),
      exportName: "kafkaTopics",
    });

    await command.run({ counts: true });

    expect(run).toHaveBeenCalledOnce();
    expect(run).toHaveBeenCalledWith({ counts: true });
  });

  it("throws when lazy command module export is missing or invalid", async () => {
    const command = createLazyCommandDefinition({
      group: "topics",
      command: "list",
      summary: "List topics",
      loadModule: async () => ({ notAFunction: "nope" }),
      exportName: "topicsList",
    });

    await expect(command.run({})).rejects.toThrow(
      /invalid command module for topics:list/i
    );
  });

  it("normalizes lazy command identifiers before executing handlers", async () => {
    const command = createLazyCommandDefinition({
      group: " topics ",
      command: " list ",
      summary: "List topics",
      loadModule: async () => ({ notAFunction: "nope" }),
      exportName: "topicsList",
    });

    await expect(command.run({})).rejects.toThrow(
      /invalid command module for topics:list/i
    );
  });

  it("derives conventional lazy command module path and export names", () => {
    expect(
      toConventionalLazyCommandModulePath(" schema-registry ", " publish-protos ")
    ).toBe("./commands/schema-registry/publish-protos.js");
    expect(
      toConventionalLazyCommandExportName(" schema-registry ", " publish-protos ")
    ).toBe("schemaRegistryPublishProtos");
    expect(toConventionalLazyCommandExportName("e2e", "brief-run")).toBe("e2eBriefRun");
  });

  it("allows conventional lazy command definitions to override module conventions", async () => {
    const command = createConventionalLazyCommandDefinition({
      group: "custom",
      command: "run",
      summary: "Custom command",
      exportName: "customRun",
      modulePath:
        "data:text/javascript,export const customRun = async (flags) => { globalThis.__riCustomRunFlags = flags; }",
    });

    await command.run({ dryRun: true });
    expect((globalThis as Record<string, unknown>).__riCustomRunFlags).toEqual({
      dryRun: true,
    });
    delete (globalThis as Record<string, unknown>).__riCustomRunFlags;
  });

  it("registers topics retag in the default command registry", () => {
    const registry = createDefaultCommandRegistry();
    const command = registry.resolve("topics", "retag");
    const alias = registry.resolve("topics", "reclassify");

    expect(command).not.toBeNull();
    expect(alias).toBe(command);
  });

  it("registers events enrich in the default command registry", () => {
    const registry = createDefaultCommandRegistry();
    const command = registry.resolve("events", "enrich");
    const alias = registry.resolve("events", "backfill");

    expect(command).not.toBeNull();
    expect(alias).toBe(command);
  });

  it("registers db snapshot in the default command registry", () => {
    const registry = createDefaultCommandRegistry();
    const command = registry.resolve("db", "snapshot");
    const alias = registry.resolve("db", "backup");

    expect(command).not.toBeNull();
    expect(alias).toBe(command);
  });

  it("registers kafka ensure-topics in the default command registry", () => {
    const registry = createDefaultCommandRegistry();
    const command = registry.resolve("kafka", "ensure-topics");
    const alias = registry.resolve("kafka", "ensure");

    expect(command).not.toBeNull();
    expect(alias).toBe(command);
  });

  it("registers brief diagnose in the default command registry", () => {
    const registry = createDefaultCommandRegistry();
    const command = registry.resolve("brief", "diagnose");
    const alias = registry.resolve("brief", "triage");

    expect(command).not.toBeNull();
    expect(alias).toBe(command);
  });

  it("registers db test-bootstrap-check in the default command registry", () => {
    const registry = createDefaultCommandRegistry();
    const command = registry.resolve("db", "test-bootstrap-check");
    const alias = registry.resolve("db", "test-bootstrap");

    expect(command).not.toBeNull();
    expect(alias).toBe(command);
  });

  it("registers e2e brief-run in the default command registry", () => {
    const registry = createDefaultCommandRegistry();
    const command = registry.resolve("e2e", "brief-run");
    const alias = registry.resolve("e2e", "brief");

    expect(command).not.toBeNull();
    expect(alias).toBe(command);
  });

  it("resolves default commands from single-token keys", () => {
    const registry = createDefaultCommandRegistry();
    const canonical = registry.resolveInput(["topics:retag"]);
    const alias = registry.resolveInput(["topics:reclassify"]);

    expect(canonical).not.toBeNull();
    expect(alias).toBe(canonical);
  });
});
