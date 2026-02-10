#!/usr/bin/env node

import { loadDotEnv } from "@rising-intelligence/shared";
import { printHelp } from "./help.js";
import { parseArgs } from "./lib/args.js";

async function main() {
  loadDotEnv();

  const parsed = parseArgs(process.argv.slice(2));
  if (parsed.kind === "help") {
    printHelp();
    return;
  }

  const { command, flags } = parsed;

  if (command.length === 0) {
    printHelp();
    process.exitCode = 1;
    return;
  }

  const [group, subcommand] = command;

  if (group === "schema-registry" && (subcommand === "publish-protos" || subcommand === "publish")) {
    const { schemaRegistryPublishProtos } = await import("./commands/schema-registry/publish-protos.js");
    await schemaRegistryPublishProtos(flags);
    return;
  }

  if (group === "lgtm" && (subcommand === "urls" || subcommand === "endpoints")) {
    const urls = {
      grafana: "http://localhost:3001",
      loki: "http://localhost:3100",
      tempo: "http://localhost:3200",
      mimir: "http://localhost:9009",
      otlpGrpc: "localhost:4317",
      otlpHttp: "http://localhost:4318",
      kafka: "localhost:9092",
      schemaRegistry: "http://localhost:8081",
    };

    for (const [key, value] of Object.entries(urls)) {
      // eslint-disable-next-line no-console
      console.log(`${key}: ${value}`);
    }
    return;
  }

  if (
    group === "brief" &&
    (subcommand === "trigger" ||
      subcommand === "publish-summary-request" ||
      subcommand === "generate-summary-request")
  ) {
    const { briefTrigger } = await import("./commands/brief/trigger.js");
    await briefTrigger(flags);
    return;
  }

  if (group === "kafka" && (subcommand === "topics" || subcommand === "list")) {
    const { kafkaTopics } = await import("./commands/kafka/topics.js");
    await kafkaTopics(flags);
    return;
  }

  if (group === "kafka" && subcommand === "create-topics") {
    const { kafkaCreateTopics } = await import("./commands/kafka/create-topics.js");
    await kafkaCreateTopics(flags);
    return;
  }

  if (group === "topics" && subcommand === "list") {
    const { topicsList } = await import("./commands/topics/list.js");
    await topicsList(flags);
    return;
  }

  printHelp(`Unknown command: ${command.join(" ")}`);
  process.exitCode = 1;
}

main().catch((error: unknown) => {
  // eslint-disable-next-line no-console
  console.error(error);
  process.exitCode = 1;
});
