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

  if (command.length !== 2) {
    printHelp(
      commandRegistry,
      `Expected command format: <group> <command>. Received: ${formatCommandInput(command)}`
    );
    process.exitCode = 1;
    return;
  }

  const [group, subcommand] = command;
  const resolvedCommand = commandRegistry.resolve(group, subcommand);
  if (resolvedCommand) {
    await resolvedCommand.run(flags);
    return;
  }

  printHelp(commandRegistry, `Unknown command: ${command.join(" ")}`);
  process.exitCode = 1;
}

main().catch((error: unknown) => {
  // eslint-disable-next-line no-console
  console.error(error);
  process.exitCode = 1;
});

