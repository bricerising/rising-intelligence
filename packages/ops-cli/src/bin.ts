#!/usr/bin/env node

import { loadDotEnv } from "@rising-intelligence/shared";
import { briefTrigger } from "./commands/brief/trigger.js";
import { kafkaTopics } from "./commands/kafka/topics.js";
import { topicsList } from "./commands/topics/list.js";
import { schemaRegistryPublishProtos } from "./commands/schema-registry/publish-protos.js";
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
    await briefTrigger(flags);
    return;
  }

  if (group === "kafka" && (subcommand === "topics" || subcommand === "list")) {
    await kafkaTopics(flags);
    return;
  }

  if (group === "topics" && subcommand === "list") {
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
