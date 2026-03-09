import { execFile as execFileCallback } from "node:child_process";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { homedir, tmpdir } from "node:os";
import { join } from "node:path";
import type pino from "pino";
import type { Config } from "../config.js";

const EXEC_MAX_BUFFER_BYTES = 4 * 1024 * 1024;
const TEMP_DIR_PREFIX = "brief-codex-cli-";
const PROMPT_FILE_NAME = "prompt.txt";
const PROMPT_FILE_PATH_PREFIX = "PROMPT_FILE_PATH:";

interface ExecFileResult {
  stdout: string;
  stderr: string;
}

async function execFileWithInput(
  command: string,
  args: string[],
  timeoutMs: number,
  maxBuffer: number,
  stdinInput: string
): Promise<ExecFileResult> {
  return new Promise((resolve, reject) => {
    const child = execFileCallback(
      command,
      args,
      {
        timeout: timeoutMs,
        maxBuffer,
        encoding: "utf8",
      },
      (error, stdout, stderr) => {
        if (error) {
          const err = error as NodeJS.ErrnoException & {
            stdout?: string | Buffer;
            stderr?: string | Buffer;
          };
          err.stdout = err.stdout ?? stdout;
          err.stderr = err.stderr ?? stderr;
          reject(err);
          return;
        }
        resolve({ stdout, stderr });
      }
    );

    const stdin = child.stdin;
    if (!stdin) {
      return;
    }

    stdin.on("error", (error) => {
      const code = error && typeof error === "object" && "code" in error
        ? String((error as NodeJS.ErrnoException).code)
        : "";
      if (code === "EPIPE" || code === "ERR_STREAM_DESTROYED") {
        return;
      }
    });
    stdin.end(stdinInput);
  });
}

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
    const rawMessage = details.message?.trim() ?? "unknown error";
    const message = rawMessage.startsWith("Command failed:")
      ? "command failed"
      : rawMessage.split("\n")[0].slice(0, 200);
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

function buildPromptFileInstruction(promptPath: string): string {
  return [
    "Read the full brief generation instructions from the prompt file path below.",
    `${PROMPT_FILE_PATH_PREFIX} ${promptPath}`,
    "Follow those instructions exactly and return valid JSON only.",
  ].join("\n");
}

function buildCodexExecArgs(
  config: Config,
  outputPath: string | null,
  promptPath: string,
  promptDir: string
): string[] {
  const args = [
    "exec",
    "--skip-git-repo-check",
    "--sandbox",
    "read-only",
    "--color",
    "never",
    "-C",
    process.cwd(),
    "--add-dir",
    promptDir,
  ];

  if (outputPath && outputPath.trim().length > 0) {
    args.push("--output-last-message", outputPath);
  }

  const profile = config.LLM_CODEX_PROFILE.trim();
  if (profile.length > 0) {
    args.push("-p", profile);
  }

  const model = config.LLM_CODEX_MODEL.trim();
  if (model.length > 0) {
    args.push("-m", model);
  }

  // Keep argv small by sending only a prompt file reference.
  args.push(buildPromptFileInstruction(promptPath));
  return args;
}

function buildCodexStdinExecArgs(config: Config, outputPath: string | null): string[] {
  const args = [
    "exec",
    "--skip-git-repo-check",
    "--sandbox",
    "read-only",
    "--color",
    "never",
    "-C",
    process.cwd(),
  ];

  if (outputPath && outputPath.trim().length > 0) {
    args.push("--output-last-message", outputPath);
  }

  const profile = config.LLM_CODEX_PROFILE.trim();
  if (profile.length > 0) {
    args.push("-p", profile);
  }

  const model = config.LLM_CODEX_MODEL.trim();
  if (model.length > 0) {
    args.push("-m", model);
  }

  args.push("-");
  return args;
}

function isPromptFileAccessErrorPayload(value: unknown): boolean {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return false;
  }

  const record = value as Record<string, unknown>;
  const fields = [record.error, record.details, record.message, record.required_input, record.status];
  const text = fields
    .filter((field): field is string => typeof field === "string")
    .join(" ")
    .toLowerCase();

  return (
    text.includes("prompt_file_path") ||
    text.includes("cannot_read_prompt_file") ||
    (text.includes("prompt file") && text.includes("cannot"))
  );
}

function hasExpectedBriefShape(value: unknown): boolean {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return false;
  }
  const record = value as Record<string, unknown>;
  return typeof record.title === "string" && Array.isArray(record.highlights);
}

function shouldRetryWithStdinFallback(value: unknown): boolean {
  return isPromptFileAccessErrorPayload(value) || !hasExpectedBriefShape(value);
}

function tryParseOutputMessage(message: string): { ok: true; value: unknown } | { ok: false; error: Error } {
  try {
    return { ok: true, value: parseJsonResponse(message) };
  } catch (error) {
    return {
      ok: false,
      error: error instanceof Error ? error : new Error("Failed to parse Codex CLI response"),
    };
  }
}

async function createCodexTempDir(logger: pino.Logger): Promise<string> {
  const candidateRoots = [tmpdir(), join(process.cwd(), ".tmp"), join(homedir(), ".codex-tmp")];
  let lastError: unknown;
  for (const candidateRoot of candidateRoots) {
    try {
      await mkdir(candidateRoot, { recursive: true });
      return await mkdtemp(join(candidateRoot, TEMP_DIR_PREFIX));
    } catch (error) {
      lastError = error;
      logger.warn(
        {
          candidateRoot,
          error: describeExecError(error),
        },
        "Codex CLI temp dir root unavailable; trying next fallback"
      );
    }
  }

  throw lastError ?? new Error("Unable to allocate Codex CLI temp dir");
}

async function executeCodexCliViaStdin(
  config: Config,
  prompt: string,
  outputPath: string | null,
  logger: pino.Logger
): Promise<unknown> {
  const args = buildCodexStdinExecArgs(config, outputPath);
  const { stdout, stderr } = await execFileWithInput(
    config.LLM_CODEX_CLI_COMMAND,
    args,
    config.LLM_CODEX_TIMEOUT_MS,
    EXEC_MAX_BUFFER_BYTES,
    prompt
  );
  if (stderr.trim().length > 0) {
    logger.debug({ stderr: stderr.trim().slice(0, 400) }, "Codex CLI stdin fallback emitted stderr output");
  }

  const stdoutText = stdout.trim();
  if (stdoutText.length > 0) {
    const parsedStdout = tryParseOutputMessage(stdout);
    if (parsedStdout.ok) {
      return parsedStdout.value;
    }
    logger.warn(
      {
        error: parsedStdout.error.message,
        stdoutPreview: stdoutText.slice(0, 300),
      },
      "Codex CLI stdin fallback produced non-parseable stdout"
    );
  }

  if (!outputPath) {
    throw new Error("Codex CLI stdin fallback produced no parseable output");
  }

  try {
    const message = await readFile(outputPath, "utf-8");
    const parsedFile = tryParseOutputMessage(message);
    if (parsedFile.ok) {
      return parsedFile.value;
    }
    throw parsedFile.error;
  } catch (error) {
    const code =
      error && typeof error === "object" && "code" in error
        ? String((error as { code?: string }).code)
        : "";
    if (code === "ENOENT" && stdoutText.length > 0) {
      const parsedStdout = tryParseOutputMessage(stdout);
      if (parsedStdout.ok) {
        return parsedStdout.value;
      }
    }
    throw error;
  }
}

export async function executeCodexCli(
  config: Config,
  prompt: string,
  logger: pino.Logger
): Promise<unknown> {
  let tempDir: string | null = null;
  let promptPath: string | null = null;
  let outputPath: string | null = null;
  try {
    tempDir = await createCodexTempDir(logger);
    promptPath = join(tempDir, PROMPT_FILE_NAME);
    outputPath = join(tempDir, "last-message.txt");
  } catch (error) {
    logger.warn(
      { error: describeExecError(error) },
      "Codex CLI temp dir unavailable; falling back to stdin-only invocation"
    );
    try {
      return await executeCodexCliViaStdin(config, prompt, null, logger);
    } catch (stdinFallbackError) {
      throw new Error(`Codex CLI stdin fallback failed: ${describeExecError(stdinFallbackError)}`);
    }
  }

  try {
    await writeFile(promptPath, prompt, "utf-8");
  } catch (error) {
    logger.warn(
      { promptPath, error: describeExecError(error) },
      "Codex CLI prompt file write failed; falling back to stdin-only invocation"
    );
    try {
      return await executeCodexCliViaStdin(config, prompt, null, logger);
    } catch (stdinFallbackError) {
      throw new Error(`Codex CLI stdin fallback failed: ${describeExecError(stdinFallbackError)}`);
    }
  }
  const args = buildCodexExecArgs(config, outputPath, promptPath, tempDir);

  try {
    const { stdout, stderr } = await execFileWithInput(
      config.LLM_CODEX_CLI_COMMAND,
      args,
      config.LLM_CODEX_TIMEOUT_MS,
      EXEC_MAX_BUFFER_BYTES,
      ""
    );
    if (stderr.trim().length > 0) {
      logger.debug({ stderr: stderr.trim().slice(0, 400) }, "Codex CLI emitted stderr output");
    }

    const stdoutText = stdout.trim();
    if (stdoutText.length > 0) {
      const parsedStdout = tryParseOutputMessage(stdout);
      if (parsedStdout.ok) {
        if (shouldRetryWithStdinFallback(parsedStdout.value)) {
          logger.warn(
            { stdoutPreview: stdoutText.slice(0, 300) },
            "Codex CLI file-mode output was unusable for brief schema; retrying once with stdin prompt fallback"
          );
          return await executeCodexCliViaStdin(config, prompt, outputPath, logger);
        }
        return parsedStdout.value;
      }
      logger.debug(
        {
          error: parsedStdout.error.message,
          stdoutPreview: stdoutText.slice(0, 300),
        },
        "Failed to parse Codex CLI stdout as JSON; attempting output file artifact"
      );
    }

    try {
      const message = await readFile(outputPath, "utf-8");
      const parsedFile = tryParseOutputMessage(message);
      if (parsedFile.ok) {
        if (shouldRetryWithStdinFallback(parsedFile.value)) {
          logger.warn(
            { outputPath },
            "Codex CLI file-mode output artifact was unusable for brief schema; retrying once with stdin prompt fallback"
          );
          return await executeCodexCliViaStdin(config, prompt, outputPath, logger);
        }
        return parsedFile.value;
      }
      throw parsedFile.error;
    } catch (error) {
      const code =
        error && typeof error === "object" && "code" in error
          ? String((error as { code?: string }).code)
          : "";
      if (code === "ENOENT" && stdoutText.length > 0) {
        logger.warn(
          { outputPath, stdoutPreview: stdoutText.slice(0, 300) },
          "Codex CLI output file missing; falling back to stdout content"
        );
        const parsedStdout = tryParseOutputMessage(stdout);
        if (parsedStdout.ok) {
          if (shouldRetryWithStdinFallback(parsedStdout.value)) {
            logger.warn(
              { stdoutPreview: stdoutText.slice(0, 300) },
              "Codex CLI fallback from output-file miss was unusable for brief schema; retrying once with stdin prompt"
            );
            return await executeCodexCliViaStdin(config, prompt, outputPath, logger);
          }
          return parsedStdout.value;
        }
      }

      if (code === "ENOENT") {
        logger.warn(
          { outputPath, hasStdout: stdoutText.length > 0 },
          "Codex CLI output file missing; retrying once without output artifact"
        );

        const retryArgs = buildCodexExecArgs(config, null, promptPath, tempDir);
        try {
          const { stdout: retryStdout, stderr: retryStderr } = await execFileWithInput(
            config.LLM_CODEX_CLI_COMMAND,
            retryArgs,
            config.LLM_CODEX_TIMEOUT_MS,
            EXEC_MAX_BUFFER_BYTES,
            ""
          );
          if (retryStderr.trim().length > 0) {
            logger.debug(
              { stderr: retryStderr.trim().slice(0, 400) },
              "Codex CLI retry emitted stderr output"
            );
          }

          const parsedRetryStdout = tryParseOutputMessage(retryStdout);
          if (parsedRetryStdout.ok) {
            if (shouldRetryWithStdinFallback(parsedRetryStdout.value)) {
              logger.warn(
                { outputPath },
                "Codex CLI retry output was unusable for brief schema; retrying once with stdin prompt fallback"
              );
              return await executeCodexCliViaStdin(config, prompt, outputPath, logger);
            }
            logger.warn(
              { outputPath },
              "Codex CLI recovered via stdout-only retry after missing output artifact"
            );
            return parsedRetryStdout.value;
          }

          logger.warn(
            {
              outputPath,
              error: parsedRetryStdout.error.message,
              stdoutPreview: retryStdout.trim().slice(0, 300),
            },
            "Codex CLI retry did not produce parseable JSON output"
          );
        } catch (retryError) {
          logger.warn(
            {
              outputPath,
              error: describeExecError(retryError),
            },
            "Codex CLI retry failed after missing output artifact"
          );
        }

        const artifactError = new Error(
          `Codex CLI output file missing and retry output was not parseable: ${outputPath}`
        ) as NodeJS.ErrnoException;
        artifactError.code = "ENOENT";
        throw artifactError;
      }

      throw error;
    }
  } catch (error) {
    throw new Error(`Codex CLI execution failed: ${describeExecError(error)}`);
  } finally {
    if (tempDir) {
      await rm(tempDir, { recursive: true, force: true });
    }
  }
}
