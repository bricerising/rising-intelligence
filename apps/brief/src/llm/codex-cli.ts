import { execFile as execFileCallback } from "node:child_process";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import type pino from "pino";
import type { Config } from "../config.js";

const execFile = promisify(execFileCallback);
const EXEC_MAX_BUFFER_BYTES = 4 * 1024 * 1024;
const TEMP_DIR_PREFIX = "brief-codex-cli-";

function parseJsonResponse(message: string): unknown {
  const trimmed = message.trim();
  if (trimmed.length === 0) {
    throw new Error("Codex CLI returned an empty response");
  }

  const fencedMatch = trimmed.match(/```(?:json)?\s*([\s\S]*?)```/i);
  if (fencedMatch?.[1]) {
    return JSON.parse(fencedMatch[1].trim());
  }

  try {
    return JSON.parse(trimmed);
  } catch {
    const firstBrace = trimmed.indexOf("{");
    const lastBrace = trimmed.lastIndexOf("}");
    if (firstBrace >= 0 && lastBrace > firstBrace) {
      return JSON.parse(trimmed.slice(firstBrace, lastBrace + 1));
    }
    throw new Error("Codex CLI response did not contain a JSON object");
  }
}

function describeExecError(error: unknown): string {
  if (error && typeof error === "object") {
    const details = error as {
      message?: string;
      code?: string | number;
      signal?: string;
      stdout?: string | Buffer;
      stderr?: string | Buffer;
    };
    const message = details.message ?? "unknown error";
    const code = details.code !== undefined ? ` code=${String(details.code)}` : "";
    const signal = details.signal ? ` signal=${details.signal}` : "";
    const stderr =
      typeof details.stderr === "string"
        ? details.stderr.trim().slice(0, 400)
        : details.stderr?.toString().trim().slice(0, 400) ?? "";
    const stdout =
      typeof details.stdout === "string"
        ? details.stdout.trim().slice(0, 200)
        : details.stdout?.toString().trim().slice(0, 200) ?? "";
    const output = stderr || stdout;
    return output ? `${message}${code}${signal}: ${output}` : `${message}${code}${signal}`;
  }
  return "unknown error";
}

function buildCodexExecArgs(config: Config, outputPath: string, prompt: string): string[] {
  const args = [
    "exec",
    "--skip-git-repo-check",
    "--sandbox",
    "read-only",
    "--color",
    "never",
    "-C",
    process.cwd(),
    "--output-last-message",
    outputPath,
  ];

  const profile = config.LLM_CODEX_PROFILE.trim();
  if (profile.length > 0) {
    args.push("-p", profile);
  }

  const model = config.LLM_CODEX_MODEL.trim();
  if (model.length > 0) {
    args.push("-m", model);
  }

  args.push(prompt);
  return args;
}

export async function executeCodexCli(
  config: Config,
  prompt: string,
  logger: pino.Logger
): Promise<unknown> {
  const tempDir = await mkdtemp(join(tmpdir(), TEMP_DIR_PREFIX));
  const outputPath = join(tempDir, "last-message.txt");
  const args = buildCodexExecArgs(config, outputPath, prompt);

  try {
    const { stderr } = await execFile(config.LLM_CODEX_CLI_COMMAND, args, {
      timeout: config.LLM_CODEX_TIMEOUT_MS,
      maxBuffer: EXEC_MAX_BUFFER_BYTES,
    });
    if (stderr.trim().length > 0) {
      logger.debug({ stderr: stderr.trim().slice(0, 400) }, "Codex CLI emitted stderr output");
    }

    const message = await readFile(outputPath, "utf-8");
    return parseJsonResponse(message);
  } catch (error) {
    throw new Error(`Codex CLI execution failed: ${describeExecError(error)}`);
  } finally {
    await rm(tempDir, { recursive: true, force: true });
  }
}
