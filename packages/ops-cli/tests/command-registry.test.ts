import { describe, expect, it, vi } from "vitest";
import {
  CommandRegistry,
  createDefaultCommandRegistry,
  createLazyCommandDefinition,
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
});
