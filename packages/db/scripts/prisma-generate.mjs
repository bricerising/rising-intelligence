import { spawn } from "node:child_process";
import fs from "node:fs/promises";
import { createRequire } from "node:module";
import os from "node:os";
import path from "node:path";

const DEFAULT_TMP_HOME = path.join(os.tmpdir(), "rising-intelligence-home");

const require = createRequire(import.meta.url);

async function assertWritablePrismaCacheDir(cacheHome) {
  const prismaDir = path.join(cacheHome, "prisma");
  await fs.mkdir(prismaDir, { recursive: true });

  const probePath = path.join(
    prismaDir,
    `.write-probe-${process.pid}-${Date.now()}.tmp`,
  );
  await fs.writeFile(probePath, "ok", { encoding: "utf8" });
  await fs.unlink(probePath);
}

async function resolveEnginesFromNodeModules() {
  // Prisma may attempt to download engines at build time. In restricted network
  // environments (like this sandbox), prefer the engines already installed in
  // `node_modules/@prisma/engines` and point Prisma at them explicitly.
  let enginesDir;
  try {
    const enginesPackageJson = require.resolve("@prisma/engines/package.json");
    enginesDir = path.dirname(enginesPackageJson);
  } catch {
    return null;
  }

  const entries = await fs.readdir(enginesDir);

  const queryEngineLibrary =
    entries.find((name) =>
      /^libquery_engine-.*\.(dylib\.node|so\.node|dll\.node)$/.test(name),
    ) ?? entries.find((name) => name === "query_engine.dll.node");

  const schemaEngineBinary = entries.find((name) => name.startsWith("schema-engine-"));

  if (!queryEngineLibrary || !schemaEngineBinary) {
    return null;
  }

  return {
    queryEngineLibraryPath: path.join(enginesDir, queryEngineLibrary),
    schemaEngineBinaryPath: path.join(enginesDir, schemaEngineBinary),
  };
}

async function resolveWritableHomeAndCacheDir() {
  const currentHome = os.homedir();
  const requestedXdgCacheHome = process.env.XDG_CACHE_HOME?.trim();
  const candidateXdgCacheHome =
    requestedXdgCacheHome && requestedXdgCacheHome.length > 0
      ? requestedXdgCacheHome
      : path.join(currentHome, ".cache");

  try {
    await assertWritablePrismaCacheDir(candidateXdgCacheHome);
    return {
      home: process.env.HOME ?? currentHome,
      xdgCacheHome: candidateXdgCacheHome,
    };
  } catch {
    const tmpHome = process.env.RI_PRISMA_HOME?.trim();
    const home = tmpHome && tmpHome.length > 0 ? tmpHome : DEFAULT_TMP_HOME;
    const xdgCacheHome = path.join(home, ".cache");

    await assertWritablePrismaCacheDir(xdgCacheHome);
    return { home, xdgCacheHome };
  }
}

const { home, xdgCacheHome } = await resolveWritableHomeAndCacheDir();
const engines = await resolveEnginesFromNodeModules();

const env = {
  ...process.env,
  HOME: home,
  XDG_CACHE_HOME: xdgCacheHome,

  ...(engines
    ? {
        PRISMA_CLI_QUERY_ENGINE_TYPE:
          process.env.PRISMA_CLI_QUERY_ENGINE_TYPE ?? "library",
        PRISMA_QUERY_ENGINE_LIBRARY:
          process.env.PRISMA_QUERY_ENGINE_LIBRARY ?? engines.queryEngineLibraryPath,
        PRISMA_SCHEMA_ENGINE_BINARY:
          process.env.PRISMA_SCHEMA_ENGINE_BINARY ?? engines.schemaEngineBinaryPath,
      }
    : {}),
};

const child = spawn("prisma", ["generate", "--schema=prisma/schema.prisma"], {
  stdio: "inherit",
  env,
});

child.on("exit", (code) => {
  process.exit(code ?? 1);
});

child.on("error", (error) => {
  console.error("Failed to run prisma generate:", error);
  process.exit(1);
});
