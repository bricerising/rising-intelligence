export type ParsedArgs =
  | { kind: "help" }
  | {
      kind: "command";
      command: string[];
      flags: Record<string, string | boolean>;
    };

export function parseArgs(argv: string[]): ParsedArgs {
  if (argv.length === 0) {
    return { kind: "help" };
  }

  if (argv.includes("-h") || argv.includes("--help")) {
    return { kind: "help" };
  }

  const command: string[] = [];
  const flags: Record<string, string | boolean> = {};

  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i];

    if (!token.startsWith("--")) {
      command.push(token);
      continue;
    }

    const [rawKey, rawValue] = token.slice(2).split("=", 2);
    const key = rawKey.trim();
    if (!key) {
      continue;
    }

    if (rawValue !== undefined) {
      flags[key] = rawValue;
      continue;
    }

    const next = argv[i + 1];
    if (next && !next.startsWith("--")) {
      flags[key] = next;
      i += 1;
      continue;
    }

    flags[key] = true;
  }

  return { kind: "command", command, flags };
}
