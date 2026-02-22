#!/usr/bin/env node

import { loadDotEnv } from "@rising-intelligence/shared";
import { createDefaultCommandRegistry } from "./command-registry.js";
import { printHelp } from "./help.js";
import { parseArgs } from "./lib/args.js";

function formatCommandInput(command: string[]): string {
  if (command.length === 0) {
    return "(none)";
  }

  return command.join(" ");
}

function buildCommandErrorMessage(command: string[]): string {
  const input = formatCommandInput(command);
  if (command.length > 2) {
    return `Expected command format: <group> <command> or <group:command>. Received: ${input}`;
  }

  return `Unknown command: ${input}`;
}

async function main() {
  loadDotEnv();

  const commandRegistry = createDefaultCommandRegistry();
  const parsed = parseArgs(process.argv.slice(2));
  if (parsed.kind === "help") {
    printHelp(commandRegistry);
    return;
  }

  const { command, flags } = parsed;

  if (command.length === 0) {
    printHelp(commandRegistry);
    process.exitCode = 1;
    return;
  }

  const resolvedCommand = commandRegistry.resolveInput(command);
  if (resolvedCommand) {
    await resolvedCommand.run(flags);
    return;
  }

  printHelp(commandRegistry, buildCommandErrorMessage(command));
  process.exitCode = 1;
}

main().catch((error: unknown) => {
  // eslint-disable-next-line no-console
  console.error(error);
  process.exitCode = 1;
});
