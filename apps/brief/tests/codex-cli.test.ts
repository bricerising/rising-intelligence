import { chmod, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import pino from "pino";
import { afterEach, describe, expect, it } from "vitest";
import type { Config } from "../src/contract.js";
import { executeCodexCli } from "../src/llm/codex-cli.js";

const logger = pino({ level: "silent" });
const tempDirs: string[] = [];

interface RecordedCall {
  args: string[];
  stdinLength: number;
  promptFilePath: string | null;
  promptFileLength: number;
}

async function createFakeCodexScript(
  mode:
    | "recover-on-retry"
    | "always-empty"
    | "always-success"
    | "prompt-file-error-then-stdin-recovery"
    | "invalid-payload-then-stdin-recovery"
): Promise<{
  commandPath: string;
  callsPath: string;
}> {
  const tempDir = await mkdtemp(join(tmpdir(), "brief-codex-cli-test-"));
  tempDirs.push(tempDir);
  const commandPath = join(tempDir, "fake-codex.cjs");
  const callsPath = join(tempDir, "calls.json");
  const modePath = join(tempDir, "mode.txt");
  await writeFile(modePath, mode, "utf-8");

  const script = `#!/usr/bin/env node
const fs = require("node:fs");
const mode = fs.readFileSync(process.argv[2], "utf8").trim();
const callsPath = process.argv[3];
const args = process.argv.slice(4);
let stdinData = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk) => {
  stdinData += chunk;
});
process.stdin.on("end", () => {
  const promptArg = args.at(-1) ?? "";
  const promptPathLine = promptArg
    .split(/\\r?\\n/)
    .find((line) => line.startsWith("PROMPT_FILE_PATH:"));
  const promptFilePath = promptPathLine
    ? promptPathLine.slice("PROMPT_FILE_PATH:".length).trim()
    : null;
  const promptFileLength = promptFilePath && fs.existsSync(promptFilePath)
    ? fs.readFileSync(promptFilePath, "utf8").length
    : 0;
  const existing = fs.existsSync(callsPath)
    ? JSON.parse(fs.readFileSync(callsPath, "utf8"))
    : [];
  existing.push({
    args,
    stdinLength: stdinData.length,
    promptFilePath,
    promptFileLength
  });
  fs.writeFileSync(callsPath, JSON.stringify(existing), "utf8");
  const callCount = existing.length;
  const usesStdinPrompt = args.at(-1) === "-";
  if (mode === "recover-on-retry" && callCount === 1) {
    process.exit(0);
  }
  if (mode === "prompt-file-error-then-stdin-recovery" && !usesStdinPrompt) {
    process.stdout.write(JSON.stringify({
      status: "error",
      error: "cannot_read_prompt_file",
      details: "Cannot access PROMPT_FILE_PATH in sandbox"
    }));
    return;
  }
  if (mode === "invalid-payload-then-stdin-recovery" && !usesStdinPrompt) {
    process.stdout.write(JSON.stringify({
      status: "ok",
      message: "non-schema payload"
    }));
    return;
  }
  if (mode === "always-empty") {
    process.exit(0);
  }
  process.stdout.write(JSON.stringify({
    title: "Recovered Brief",
    highlights: [
      {
        topic: "aws.bedrock",
        what_happened: "A release shipped.",
        why_it_matters: "Performance improved.",
        suggested_action: "Validate defaults.",
        citations: ["https://example.com/release"]
      }
    ],
    notes: "Recovered on retry.",
    usage: { prompt_tokens: 10, completion_tokens: 5 },
    meta: { provider: "codex-cli", model: "fake-codex", estimated_cost_usd: 0 }
  }));
});
process.stdin.resume();
`;
  await writeFile(commandPath, script, "utf-8");
  await chmod(commandPath, 0o755);

  const wrapperPath = join(tempDir, "codex-wrapper.sh");
  await writeFile(
    wrapperPath,
    `#!/usr/bin/env sh
exec "${commandPath}" "${modePath}" "${callsPath}" "$@"
`,
    "utf-8"
  );
  await chmod(wrapperPath, 0o755);

  return { commandPath: wrapperPath, callsPath };
}

function makeConfig(command: string): Config {
  return {
    LLM_CODEX_CLI_COMMAND: command,
    LLM_CODEX_TIMEOUT_MS: 5000,
    LLM_CODEX_MODEL: "",
    LLM_CODEX_PROFILE: "",
  } as Config;
}

afterEach(async () => {
  while (tempDirs.length > 0) {
    const dir = tempDirs.pop();
    if (dir) {
      await rm(dir, { recursive: true, force: true });
    }
  }
});

describe("executeCodexCli", () => {
  it("retries without output artifact when last-message file is missing", async () => {
    const fake = await createFakeCodexScript("recover-on-retry");
    const result = await executeCodexCli(
      makeConfig(fake.commandPath),
      "return a valid JSON brief",
      logger
    );

    expect(result).toMatchObject({
      title: "Recovered Brief",
      meta: {
        provider: "codex-cli",
        model: "fake-codex",
      },
    });

    const rawCalls = await readFile(fake.callsPath, "utf-8");
    const calls = JSON.parse(rawCalls) as RecordedCall[];
    expect(calls).toHaveLength(2);
    expect(calls[0].args).toContain("--output-last-message");
    expect(calls[1].args).not.toContain("--output-last-message");
    expect(calls[0].args.at(-1)).toContain("PROMPT_FILE_PATH:");
    expect(calls[1].args.at(-1)).toContain("PROMPT_FILE_PATH:");
    expect(calls[0].stdinLength).toBe(0);
    expect(calls[1].stdinLength).toBe(0);
    expect(calls[0].promptFilePath).toBeTruthy();
    expect(calls[1].promptFilePath).toBeTruthy();
    expect(calls[0].promptFileLength).toBe("return a valid JSON brief".length);
    expect(calls[1].promptFileLength).toBe("return a valid JSON brief".length);
  });

  it("preserves ENOENT classification when retry also fails", async () => {
    const fake = await createFakeCodexScript("always-empty");

    await expect(
      executeCodexCli(makeConfig(fake.commandPath), "return a valid JSON brief", logger)
    ).rejects.toThrow(/ENOENT/);

    const rawCalls = await readFile(fake.callsPath, "utf-8");
    const calls = JSON.parse(rawCalls) as RecordedCall[];
    expect(calls).toHaveLength(2);
    expect(calls[0].args).toContain("--output-last-message");
    expect(calls[1].args).not.toContain("--output-last-message");
    expect(calls[0].args.at(-1)).toContain("PROMPT_FILE_PATH:");
    expect(calls[1].args.at(-1)).toContain("PROMPT_FILE_PATH:");
    expect(calls[0].stdinLength).toBe(0);
    expect(calls[1].stdinLength).toBe(0);
  });

  it("writes very large prompts to a file and references that file in argv", async () => {
    const fake = await createFakeCodexScript("always-success");
    const largePrompt = "x".repeat(400_000);

    const result = await executeCodexCli(makeConfig(fake.commandPath), largePrompt, logger);

    expect(result).toMatchObject({
      title: "Recovered Brief",
      meta: {
        provider: "codex-cli",
        model: "fake-codex",
      },
    });

    const rawCalls = await readFile(fake.callsPath, "utf-8");
    const calls = JSON.parse(rawCalls) as RecordedCall[];
    expect(calls).toHaveLength(1);
    expect(calls[0].args).toContain("--output-last-message");
    expect(calls[0].args.at(-1)).toContain("PROMPT_FILE_PATH:");
    expect(calls[0].stdinLength).toBe(0);
    expect(calls[0].promptFilePath).toBeTruthy();
    expect(calls[0].promptFileLength).toBe(largePrompt.length);
  });

  it("falls back to stdin when prompt file access is blocked by sandbox", async () => {
    const fake = await createFakeCodexScript("prompt-file-error-then-stdin-recovery");
    const prompt = "return a valid JSON brief";

    const result = await executeCodexCli(makeConfig(fake.commandPath), prompt, logger);
    expect(result).toMatchObject({
      title: "Recovered Brief",
      meta: {
        provider: "codex-cli",
        model: "fake-codex",
      },
    });

    const rawCalls = await readFile(fake.callsPath, "utf-8");
    const calls = JSON.parse(rawCalls) as RecordedCall[];
    expect(calls).toHaveLength(2);
    expect(calls[0].args.at(-1)).toContain("PROMPT_FILE_PATH:");
    expect(calls[0].stdinLength).toBe(0);
    expect(calls[1].args.at(-1)).toBe("-");
    expect(calls[1].stdinLength).toBe(prompt.length);
  });

  it("falls back to stdin when file-mode JSON misses required brief fields", async () => {
    const fake = await createFakeCodexScript("invalid-payload-then-stdin-recovery");
    const prompt = "return a valid JSON brief";

    const result = await executeCodexCli(makeConfig(fake.commandPath), prompt, logger);
    expect(result).toMatchObject({
      title: "Recovered Brief",
      meta: {
        provider: "codex-cli",
        model: "fake-codex",
      },
    });

    const rawCalls = await readFile(fake.callsPath, "utf-8");
    const calls = JSON.parse(rawCalls) as RecordedCall[];
    expect(calls).toHaveLength(2);
    expect(calls[0].args.at(-1)).toContain("PROMPT_FILE_PATH:");
    expect(calls[0].stdinLength).toBe(0);
    expect(calls[1].args.at(-1)).toBe("-");
    expect(calls[1].stdinLength).toBe(prompt.length);
  });
});
