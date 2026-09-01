import { createHash } from "crypto";
import { chmodSync, mkdirSync, readFileSync, realpathSync } from "fs";
import { isAbsolute, join, resolve, win32 } from "path";
import { homedir } from "os";

export const INTERCOM_DIR_MODE = 0o700;
export const INTERCOM_RUNTIME_FILE_MODE = 0o600;
export const INTERCOM_TCP_HOST = "127.0.0.1";
export const INTERCOM_PROTOCOL_NAME = "pi-intercom";
export const INTERCOM_PROTOCOL_VERSION = 1;

export interface BrokerTcpEndpoint {
  transport: "tcp";
  host: string;
  port: number;
  stateId?: string;
}

export type BrokerConnectTarget = string | BrokerTcpEndpoint;

// Canonical Windows spelling used for both the readable pipe segment and its collision-resistant
// suffix. On Windows, realpath adopts the filesystem's canonical component spelling: ordinary
// case-insensitive aliases converge while per-directory case-sensitive names remain distinct.
// Explicit win32 lexical fallback keeps missing paths and cross-platform callers deterministic.
function normalizeWindowsAgentDir(agentDir: string, platform: NodeJS.Platform = process.platform): string {
  let canonicalInput = agentDir;
  if (platform === "win32") {
    try {
      canonicalInput = realpathSync.native(agentDir);
    } catch {
      // The agent directory may not have been created yet; lexical normalization remains stable.
    }
  }
  const normalized = win32.normalize(canonicalInput);
  const root = win32.parse(normalized).root;
  const canonical = root.toLowerCase() + normalized.slice(root.length);
  return canonical.length > root.length ? canonical.replace(/[\\/]+$/, "") : canonical;
}

function pipeAgentDirHash(normalizedAgentDir: string): string {
  // Hex is a Windows-valid pipe token; do not expose the drive-letter colon in the identity suffix.
  return createHash("sha256").update(normalizedAgentDir).digest("hex").slice(0, 16);
}

function sanitizePipeSegment(value: string): string {
  return value
    .replace(/[^a-zA-Z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .toLowerCase() || "default";
}

export function getAgentDirPath(
  env: NodeJS.ProcessEnv = process.env,
  homeDir: string = homedir(),
  cwd: string = process.cwd(),
): string {
  const configured = env.PI_CODING_AGENT_DIR?.trim();
  if (!configured) {
    return join(homeDir, ".pi/agent");
  }

  return isAbsolute(configured) ? configured : resolve(cwd, configured);
}

export function getIntercomDirPath(agentDir: string = getAgentDirPath()): string {
  return join(agentDir, "intercom");
}

export function shouldUseWindowsTcpTransport(
  platform: NodeJS.Platform = process.platform,
  env: NodeJS.ProcessEnv = process.env,
): boolean {
  if (platform !== "win32") {
    return false;
  }

  const transport = env.PI_INTERCOM_TRANSPORT?.trim().toLowerCase();
  if (transport === "tcp") {
    return true;
  }

  const legacyOptIn = env.PI_INTERCOM_TCP?.trim().toLowerCase();
  return legacyOptIn === "1" || legacyOptIn === "true";
}

export function getBrokerPortFilePath(intercomDir: string = getIntercomDirPath()): string {
  return join(intercomDir, "broker.port.json");
}

export function getBrokerSocketPath(
  platform: NodeJS.Platform = process.platform,
  agentDir: string = getAgentDirPath(),
): string {
  if (platform === "win32") {
    const normalizedAgentDir = normalizeWindowsAgentDir(agentDir, platform);
    const readable = sanitizePipeSegment(normalizedAgentDir).slice(0, 128).replace(/-+$/, "") || "default";
    return `\\\\.\\pipe\\pi-intercom-${readable}-${pipeAgentDirHash(normalizedAgentDir)}`;
  }

  return join(getIntercomDirPath(agentDir), "broker.sock");
}

export function getBrokerConnectTarget(
  platform: NodeJS.Platform = process.platform,
  env: NodeJS.ProcessEnv = process.env,
  intercomDir: string = getIntercomDirPath(getAgentDirPath(env)),
): BrokerConnectTarget {
  if (shouldUseWindowsTcpTransport(platform, env)) {
    const endpointFile = getBrokerPortFilePath(intercomDir);
    const raw = readFileSync(endpointFile, "utf-8");
    const parsed: unknown = JSON.parse(raw);
    if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
      throw new Error(`Invalid intercom TCP endpoint at ${endpointFile}: expected a JSON object`);
    }
    const endpoint = parsed as Record<string, unknown>;
    if (
      endpoint.transport !== "tcp"
      || endpoint.host !== INTERCOM_TCP_HOST
      || typeof endpoint.port !== "number"
      || !Number.isSafeInteger(endpoint.port)
      || endpoint.port <= 0
      || endpoint.port > 65535
      || typeof endpoint.stateId !== "string"
      || endpoint.stateId.length === 0
    ) {
      throw new Error(`Invalid intercom TCP endpoint at ${endpointFile}`);
    }
    return { transport: "tcp", host: endpoint.host, port: endpoint.port, stateId: endpoint.stateId };
  }

  return getBrokerSocketPath(platform, getAgentDirPath(env));
}

export function getBrokerListenTarget(
  platform: NodeJS.Platform = process.platform,
  env: NodeJS.ProcessEnv = process.env,
): BrokerConnectTarget {
  if (shouldUseWindowsTcpTransport(platform, env)) {
    return { transport: "tcp", host: INTERCOM_TCP_HOST, port: 0 };
  }

  return getBrokerSocketPath(platform, getAgentDirPath(env));
}

export function ensureIntercomRuntimeDir(
  intercomDir: string = getIntercomDirPath(),
  platform: NodeJS.Platform = process.platform,
): void {
  mkdirSync(intercomDir, { recursive: true, mode: INTERCOM_DIR_MODE });
  if (platform !== "win32") {
    chmodSync(intercomDir, INTERCOM_DIR_MODE);
  }
}

export function restrictIntercomRuntimeFile(
  filePath: string,
  platform: NodeJS.Platform = process.platform,
): void {
  if (platform !== "win32") {
    chmodSync(filePath, INTERCOM_RUNTIME_FILE_MODE);
  }
}
