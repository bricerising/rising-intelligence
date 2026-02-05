import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";

const distRuntimeDir = dirname(fileURLToPath(import.meta.url));

export const SHARED_PACKAGE_ROOT = resolve(distRuntimeDir, "..", "..");
export const REPO_ROOT = resolve(SHARED_PACKAGE_ROOT, "..", "..");

export const PROTO_ROOT = resolve(SHARED_PACKAGE_ROOT, "contracts", "proto");
export const CONTRACTS_PROTO_PATH = resolve(
  PROTO_ROOT,
  "rising_intelligence",
  "v1",
  "contracts.proto",
);
export const SERVICES_PROTO_PATH = resolve(
  PROTO_ROOT,
  "rising_intelligence",
  "v1",
  "services.proto",
);

