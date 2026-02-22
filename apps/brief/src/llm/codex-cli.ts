import { execFile as execFileCallback } from "node:child_process";
import { mkdir, mkdtemp, readFile, rm } from "node:fs/promises";
import { homedir, tmpdir } from "node:os";
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

function buildCodexExecArgs(config: Config, outputPath: string | null, prompt: string): string[] {
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

  args.push(prompt);
  return args;
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
  try {
    return await mkdtemp(join(tmpdir(), TEMP_DIR_PREFIX));
  } catch (error) {
    const code =
      error && typeof error === "object" && "code" in error
        ? String((error as { code?: string }).code)
        : "";
    if (code !== "ENOSPC") {
      throw error;
    }
    const fallbackRoots = [join(homedir(), ".codex-tmp"), join(process.cwd(), ".tmp")];
    logger.warn(
      { fallbackRoots },
      "Codex CLI temp dir allocation failed in /tmp (ENOSPC); retrying alternative temp roots"
    );

    let lastFallbackError: unknown = error;
    for (const fallbackRoot of fallbackRoots) {
      try {
        await mkdir(fallbackRoot, { recursive: true });
        return await mkdtemp(join(fallbackRoot, TEMP_DIR_PREFIX));
      } catch (fallbackError) {
        lastFallbackError = fallbackError;
        logger.warn(
          {
            fallbackRoot,
            error: describeExecError(fallbackError),
          },
          "Codex CLI temp dir fallback root unavailable"
        );
      }
    }

    throw lastFallbackError;
  }
}

export async function executeCodexCli(
  config: Config,
  prompt: string,
  logger: pino.Logger
): Promise<unknown> {
  const tempDir = await createCodexTempDir(logger);
  const outputPath = join(tempDir, "last-message.txt");
  const args = buildCodexExecArgs(config, outputPath, prompt);

  try {
    const { stdout, stderr } = await execFile(config.LLM_CODEX_CLI_COMMAND, args, {
      timeout: config.LLM_CODEX_TIMEOUT_MS,
      maxBuffer: EXEC_MAX_BUFFER_BYTES,
    });
    if (stderr.trim().length > 0) {
      logger.debug({ stderr: stderr.trim().slice(0, 400) }, "Codex CLI emitted stderr output");
    }

    const stdoutText = stdout.trim();
    if (stdoutText.length > 0) {
      const parsedStdout = tryParseOutputMessage(stdout);
      if (parsedStdout.ok) {
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
          return parsedStdout.value;
        }
      }

      if (code === "ENOENT") {
        logger.warn(
          { outputPath, hasStdout: stdoutText.length > 0 },
          "Codex CLI output file missing; retrying once without output artifact"
        );

        const retryArgs = buildCodexExecArgs(config, null, prompt);
        try {
          const { stdout: retryStdout, stderr: retryStderr } = await execFile(
            config.LLM_CODEX_CLI_COMMAND,
            retryArgs,
            {
              timeout: config.LLM_CODEX_TIMEOUT_MS,
              maxBuffer: EXEC_MAX_BUFFER_BYTES,
            }
          );
          if (retryStderr.trim().length > 0) {
            logger.debug(
              { stderr: retryStderr.trim().slice(0, 400) },
              "Codex CLI retry emitted stderr output"
            );
          }

          const parsedRetryStdout = tryParseOutputMessage(retryStdout);
          if (parsedRetryStdout.ok) {
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
    await rm(tempDir, { recursive: true, force: true });
  }
}
