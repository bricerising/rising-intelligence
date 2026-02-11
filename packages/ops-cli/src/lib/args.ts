export type CliFlagValue = string | boolean | string[];
export type CliFlags = Record<string, CliFlagValue>;

export type ParsedArgs =
  | { kind: "help" }
  | {
      kind: "command";
      command: string[];
      flags: CliFlags;
    };

export function parseArgs(argv: string[]): ParsedArgs {
  if (argv.length === 0) {
    return { kind: "help" };
  }

  if (argv.includes("-h") || argv.includes("--help")) {
    return { kind: "help" };
  }

  const command: string[] = [];
  const flags: CliFlags = {};

  const setFlag = (key: string, value: string | boolean) => {
    const existing = flags[key];
    if (existing === undefined) {
      flags[key] = value;
      return;
    }

    if (typeof value === "string") {
      if (Array.isArray(existing)) {
        flags[key] = [...existing, value];
        return;
      }
      if (typeof existing === "string") {
        flags[key] = [existing, value];
        return;
      }
      flags[key] = value;
      return;
    }

    if (typeof existing !== "string" && !Array.isArray(existing)) {
      flags[key] = value;
    }
  };

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
      setFlag(key, rawValue);
      continue;
    }

    const next = argv[i + 1];
    if (next && !next.startsWith("--")) {
      setFlag(key, next);
      i += 1;
      continue;
    }

    setFlag(key, true);
  }

  return { kind: "command", command, flags };
}
