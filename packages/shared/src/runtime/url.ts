import { isIP } from "node:net";

export function redactUrlPassword(url: string, invalidPlaceholder = "<invalid-url>"): string {
  try {
    const parsed = new URL(url);
    if (parsed.password) {
      parsed.password = "***";
    }

    return parsed.toString();
  } catch {
    return invalidPlaceholder;
  }
}

export interface UrlSafetyFacade {
  canonicalizeHttpUrl(value: string): string | null;
  isAllowedFetchUrl(value: string): boolean;
}

export interface UrlSafetyFacadeOptions {
  allowPrivateHosts?: boolean;
}

function normalizeHostname(hostname: string): string {
  const normalized = hostname.trim().toLowerCase();
  if (normalized.startsWith("[") && normalized.endsWith("]")) {
    return normalized.slice(1, -1);
  }
  return normalized;
}

function isPrivateOrLoopbackIpv4(hostname: string): boolean {
  const parts = hostname.split(".");
  if (parts.length !== 4) {
    return false;
  }

  const octets = parts.map((part) => Number.parseInt(part, 10));
  if (octets.some((octet) => Number.isNaN(octet) || octet < 0 || octet > 255)) {
    return false;
  }

  return (
    octets[0] === 10 ||
    octets[0] === 127 ||
    (octets[0] === 169 && octets[1] === 254) ||
    (octets[0] === 172 && octets[1] >= 16 && octets[1] <= 31) ||
    (octets[0] === 192 && octets[1] === 168)
  );
}

function isPrivateOrLoopbackIpv6(hostname: string): boolean {
  // Treat the IPv6 special-use ::/8 range as disallowed for outbound fetches.
  // This blocks unspecified/loopback and mapped/compatibility forms that can
  // otherwise bypass IPv4 private-host checks.
  if (hostname.startsWith("::")) {
    return true;
  }
  if (hostname.startsWith("fc") || hostname.startsWith("fd")) {
    return true;
  }

  const firstHextet = hostname.split(":")[0];
  return /^fe[89ab][0-9a-f]{0,2}$/i.test(firstHextet);
}

function isDisallowedHostname(hostname: string): boolean {
  const normalized = normalizeHostname(hostname);
  if (
    normalized === "localhost" ||
    normalized.endsWith(".localhost") ||
    normalized === "0.0.0.0"
  ) {
    return true;
  }

  const ipVersion = isIP(normalized);
  if (ipVersion === 4) {
    return isPrivateOrLoopbackIpv4(normalized);
  }
  if (ipVersion === 6) {
    return isPrivateOrLoopbackIpv6(normalized);
  }
  return false;
}

function parseHttpUrl(value: string): URL | null {
  try {
    const parsed = new URL(value);
    if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
      return null;
    }
    return parsed;
  } catch {
    return null;
  }
}

function normalizeHttpUrl(parsed: URL): string {
  parsed.hash = "";
  parsed.hostname = normalizeHostname(parsed.hostname);
  if (
    (parsed.protocol === "http:" && parsed.port === "80") ||
    (parsed.protocol === "https:" && parsed.port === "443")
  ) {
    parsed.port = "";
  }
  if (parsed.pathname.length > 1 && parsed.pathname.endsWith("/")) {
    parsed.pathname = parsed.pathname.slice(0, -1);
  }
  return parsed.toString();
}

export function createUrlSafetyFacade(
  options: UrlSafetyFacadeOptions = {}
): UrlSafetyFacade {
  const allowPrivateHosts = options.allowPrivateHosts ?? false;

  const hasDisallowedHost = (parsed: URL): boolean =>
    !allowPrivateHosts && isDisallowedHostname(parsed.hostname);

  return {
    canonicalizeHttpUrl(value: string): string | null {
      const trimmed = value.trim();
      if (trimmed.length === 0) {
        return null;
      }

      const parsed = parseHttpUrl(trimmed);
      if (!parsed || hasDisallowedHost(parsed)) {
        return null;
      }

      return normalizeHttpUrl(parsed);
    },
    isAllowedFetchUrl(value: string): boolean {
      const parsed = parseHttpUrl(value);
      if (!parsed || hasDisallowedHost(parsed)) {
        return false;
      }
      return true;
    },
  };
}
