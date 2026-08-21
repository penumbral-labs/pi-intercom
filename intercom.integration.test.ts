import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import path from "node:path";
import { tmpdir } from "node:os";
import { EventEmitter, once } from "node:events";
import { execFileSync, spawn, type ChildProcess } from "node:child_process";
import { ReplyTracker } from "./reply-tracker.ts";
import { MAX_PENDING_ASK_EDGES_PER_SESSION } from "./broker/ask-edges.ts";
import { BROKER_SESSION_ID, type BrokerMessage, type Message, type SessionInfo } from "./types.ts";
import { INTERCOM_EXTENSION_REGISTER_EVENT, type IntercomExtensionChannel } from "./extension-api.ts";

const repoDir = process.cwd();
const childEnvKeys = [
  "PI_SUBAGENT_ORCHESTRATOR_TARGET",
  "PI_SUBAGENT_ORCHESTRATOR_SESSION_ID",
  "PI_INTERCOM_SESSION_ID",
  "PI_INTERCOM_NAME_POLL_MS",
  "PI_SUBAGENT_RUN_ID",
  "PI_SUBAGENT_CHILD_AGENT",
  "PI_SUBAGENT_CHILD_INDEX",
  "PI_SUBAGENT_INTERCOM_SESSION_NAME",
  "PI_SUBAGENT_SUPERVISOR_CHANNEL_DIR",
] as const;
const inheritedIntercomEnv = new Map<string, string>();
for (const [key, value] of Object.entries(process.env)) {
  if ((key.startsWith("PI_SUBAGENT_") || key.startsWith("PI_INTERCOM_")) && value !== undefined) {
    inheritedIntercomEnv.set(key, value);
    delete process.env[key];
  }
}
const sharedHomeDir = mkdtempSync(path.join(tmpdir(), "pi-intercom-home-"));
const previousHome = process.env.HOME;
const previousUserProfile = process.env.USERPROFILE;
process.env.HOME = sharedHomeDir;
process.env.USERPROFILE = sharedHomeDir;
const { IntercomClient } = await import("./broker/client.ts");
const { getBrokerLaunchSpec, getTsxCliPath } = await import("./broker/spawn.ts");
test.after(() => {
  for (const key of Object.keys(process.env)) {
    if (key.startsWith("PI_SUBAGENT_") || key.startsWith("PI_INTERCOM_")) delete process.env[key];
  }
  for (const [key, value] of inheritedIntercomEnv) process.env[key] = value;
  process.env.HOME = previousHome;
  process.env.USERPROFILE = previousUserProfile;
  rmSync(sharedHomeDir, { recursive: true, force: true });
});

async function waitForBrokerReady(broker: ChildProcess): Promise<void> {
  const stdout = broker.stdout;
  if (!stdout) throw new Error("Broker stdout is unavailable");

  const ready = new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(() => {
      cleanup();
      reject(new Error("Broker startup timed out"));
    }, 10000);
    const onStdout = (chunk: Buffer) => {
      if (chunk.toString().includes("Intercom broker started")) {
        cleanup();
        resolve();
      }
    };
    const onExit = (code: number | null, signal: NodeJS.Signals | null) => {
      cleanup();
      reject(new Error(`Broker exited before startup (code=${code}, signal=${signal})`));
    };
    const cleanup = () => {
      clearTimeout(timeout);
      stdout.off("data", onStdout);
      broker.off("exit", onExit);
    };

    stdout.on("data", onStdout);
    broker.once("exit", onExit);
  });

  await ready;
}

async function withChildOrchestratorEnv<T>(metadata: {
  orchestratorTarget?: string;
  orchestratorSessionId?: string;
  inheritedIntercomSessionId?: string;
  namePollMs?: string;
  runId?: string;
  agent?: string;
  index?: string;
  sessionName?: string;
  supervisorChannelDir?: string;
}, fn: () => T | Promise<T>): Promise<T> {
  const previous = new Map<string, string | undefined>();
  for (const key of childEnvKeys) {
    previous.set(key, process.env[key]);
    delete process.env[key];
  }
  if (metadata.orchestratorTarget !== undefined) process.env.PI_SUBAGENT_ORCHESTRATOR_TARGET = metadata.orchestratorTarget;
  if (metadata.orchestratorSessionId !== undefined) process.env.PI_SUBAGENT_ORCHESTRATOR_SESSION_ID = metadata.orchestratorSessionId;
  if (metadata.inheritedIntercomSessionId !== undefined) process.env.PI_INTERCOM_SESSION_ID = metadata.inheritedIntercomSessionId;
  if (metadata.namePollMs !== undefined) process.env.PI_INTERCOM_NAME_POLL_MS = metadata.namePollMs;
  if (metadata.runId !== undefined) process.env.PI_SUBAGENT_RUN_ID = metadata.runId;
  if (metadata.agent !== undefined) process.env.PI_SUBAGENT_CHILD_AGENT = metadata.agent;
  if (metadata.index !== undefined) process.env.PI_SUBAGENT_CHILD_INDEX = metadata.index;
  if (metadata.sessionName !== undefined) process.env.PI_SUBAGENT_INTERCOM_SESSION_NAME = metadata.sessionName;
  if (metadata.supervisorChannelDir !== undefined) process.env.PI_SUBAGENT_SUPERVISOR_CHANNEL_DIR = metadata.supervisorChannelDir;
  try {
    return await fn();
  } finally {
    for (const key of childEnvKeys) {
      const value = previous.get(key);
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
  }
}

interface CapturedToolResult {
  content: Array<{ type: string; text: string }>;
  details?: Record<string, unknown>;
}

interface RenderToolResult {
  content: Array<{ type: string; text: string }>;
  details?: Record<string, unknown>;
}

interface RenderedComponent {
  render(width: number): string[];
}

interface RenderTheme {
  fg(name: string, text: string): string;
  bold(text: string): string;
}

interface CapturedTool {
  name: string;
  parameters?: unknown;
  execute: (toolCallId: string, params: Record<string, unknown>, signal: AbortSignal, onUpdate: unknown, ctx: unknown) => Promise<CapturedToolResult>;
  renderCall?: (args: Record<string, unknown>, theme: RenderTheme, context: Record<string, unknown>) => RenderedComponent;
  renderResult?: (result: RenderToolResult, options: { expanded?: boolean; isPartial?: boolean }, theme: RenderTheme, context: Record<string, unknown>) => RenderedComponent;
}

const renderTheme: RenderTheme = {
  fg: (_name, text) => text,
  bold: (text) => text,
};

function renderToText(component: RenderedComponent): string {
  return component.render(120).map((line) => line.trimEnd()).join("\n");
}

function createExtensionHarness(sessionName: string | (() => string) = "child-worker", options: {
  abort?: () => void;
  hasUI?: boolean;
  isIdle?: () => boolean;
  mode?: "tui" | "rpc" | "json" | "print";
  ui?: unknown;
  sessionId?: string | (() => string);
} = {}) {
  const events = new EventEmitter();
  const lifecycleHandlers = new Map<string, Array<(event: unknown, ctx: unknown) => unknown>>();
  const commands = new Map<string, (args: string, ctx: unknown) => unknown>();
  const tools: CapturedTool[] = [];
  const entries: Array<{ type: string; data: unknown }> = [];
  const sentMessages: Array<{ message: { customType?: string; content?: string; details?: unknown }; options?: { triggerTurn?: boolean; deliverAs?: string } }> = [];
  const pi = {
    getSessionName: () => typeof sessionName === "function" ? sessionName() : sessionName,
    events: {
      on: (channel: string, handler: (payload: unknown) => void) => {
        events.on(channel, handler);
        return () => events.off(channel, handler);
      },
      emit: (channel: string, payload: unknown) => events.emit(channel, payload),
    },
    on: (event: string, handler: (payload: unknown, ctx: unknown) => unknown) => {
      const handlers = lifecycleHandlers.get(event) ?? [];
      handlers.push(handler);
      lifecycleHandlers.set(event, handlers);
    },
    registerMessageRenderer: () => undefined,
    registerTool: (tool: CapturedTool) => {
      tools.push(tool);
    },
    registerCommand: (name: string, command: { handler: (args: string, ctx: unknown) => unknown }) => {
      commands.set(name, command.handler);
    },
    registerShortcut: () => undefined,
    sendMessage: (message: { customType?: string; content?: string; details?: unknown }, options?: { triggerTurn?: boolean; deliverAs?: string }) => {
      sentMessages.push({ message, options });
    },
    appendEntry: (type: string, data: unknown) => entries.push({ type, data }),
  };
  const ctx = {
    cwd: repoDir,
    mode: options.mode ?? (options.hasUI ? "tui" : "print"),
    model: { id: "child-model" },
    sessionManager: { getSessionId: () => typeof options.sessionId === "function" ? options.sessionId() : options.sessionId ?? "session-child-test" },
    isIdle: options.isIdle ?? (() => true),
    hasUI: options.hasUI ?? false,
    abort: options.abort ?? (() => undefined),
    ui: options.ui,
  };
  return {
    pi,
    ctx,
    tools,
    commands,
    entries,
    sentMessages,
    async emitLifecycle(event: string, payload: unknown = {}, eventContext: unknown = ctx) {
      for (const handler of lifecycleHandlers.get(event) ?? []) {
        await handler(payload, eventContext);
      }
    },
    async emitLifecycleResults(event: string, payload: unknown = {}, eventContext: unknown = ctx) {
      const results: unknown[] = [];
      for (const handler of lifecycleHandlers.get(event) ?? []) {
        results.push(await handler(payload, eventContext));
      }
      return results;
    },
  };
}

async function connectRawRegistered(sessionId: string, name: string, sessionOverrides: Record<string, unknown> = {}) {
  const net = await import("node:net");
  const { getBrokerSocketPath } = await import("./broker/paths.ts");
  const { createMessageReader, writeMessage } = await import("./broker/framing.ts");
  const socket = net.connect(getBrokerSocketPath());
  await once(socket, "connect");
  const registered = new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error("Raw register timed out")), 2000);
    const reader = createMessageReader((msg) => {
      if (typeof msg === "object" && msg !== null && "type" in msg && msg.type === "registered") {
        clearTimeout(timeout);
        socket.off("data", reader);
        resolve();
      }
    }, reject);
    socket.on("data", reader);
  });
  writeMessage(socket, {
    type: "register",
    sessionId,
    session: {
      name,
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
      ...sessionOverrides,
    },
  });
  await registered;
  return { socket, writeMessage };
}

test("opt-in TCP broker requires endpoint state for health and registration", { concurrency: false }, async () => {
  const net = await import("node:net");
  const { readFileSync } = await import("node:fs");
  const { createMessageReader, writeMessage } = await import("./broker/framing.ts");
  const agentDir = mkdtempSync(path.join(tmpdir(), "pi-intercom-tcp-agent-"));
  const broker = spawn(process.execPath, [
    getTsxCliPath(),
    "-e",
    "Object.defineProperty(process, 'platform', { value: 'win32' }); import('./broker/broker.ts').catch((error) => { console.error(error); process.exit(1); });",
  ], {
    cwd: repoDir,
    env: {
      ...process.env,
      HOME: agentDir,
      USERPROFILE: agentDir,
      PI_CODING_AGENT_DIR: agentDir,
      PI_INTERCOM_TRANSPORT: "tcp",
    },
    stdio: ["ignore", "pipe", "pipe"],
  });

  const exchange = async (message: unknown, waitForResponse: boolean): Promise<unknown[]> => {
    const socket = net.connect({ host, port });
    const messages: unknown[] = [];
    return await new Promise((resolve, reject) => {
      let settled = false;
      const finish = () => {
        if (settled) {
          return;
        }
        settled = true;
        clearTimeout(timeout);
        socket.off("data", reader);
        socket.off("close", finish);
        socket.off("error", onSocketError);
        socket.destroy();
        resolve(messages);
      };
      const onSocketError = () => finish();
      const reader = createMessageReader((received) => {
        messages.push(received);
        if (waitForResponse) {
          finish();
        }
      }, reject);
      const timeout = setTimeout(finish, 500);
      socket.once("connect", () => writeMessage(socket, message));
      socket.on("data", reader);
      socket.once("close", finish);
      socket.once("error", onSocketError);
    });
  };

  let host = "";
  let port = 0;
  let stateId = "";
  try {
    await waitForBrokerReady(broker);
    const endpoint: unknown = JSON.parse(readFileSync(path.join(agentDir, "intercom", "broker.port.json"), "utf-8"));
    if (typeof endpoint !== "object" || endpoint === null || Array.isArray(endpoint)) {
      throw new Error("Invalid TCP endpoint fixture");
    }
    const endpointRecord = endpoint as Record<string, unknown>;
    if (endpointRecord.host !== "127.0.0.1" || typeof endpointRecord.port !== "number" || typeof endpointRecord.stateId !== "string") {
      throw new Error(`Invalid TCP endpoint fixture: ${JSON.stringify(endpointRecord)}`);
    }
    host = endpointRecord.host;
    port = endpointRecord.port;
    stateId = endpointRecord.stateId;

    assert.deepEqual(await exchange({ type: "health", requestId: "unauthorized-health" }, false), []);
    assert.deepEqual(await exchange({
      type: "register",
      sessionId: "unauthorized-tcp-client",
      session: {
        name: "unauthorized",
        cwd: repoDir,
        model: "test-model",
        pid: process.pid,
        startedAt: Date.now(),
        lastActivity: Date.now(),
      },
    }, false), []);

    const healthMessages = await exchange({ type: "health", requestId: "authorized-health", stateId }, true);
    assert.deepEqual(healthMessages, [{
      type: "health_ok",
      requestId: "authorized-health",
      protocol: "pi-intercom",
      version: 1,
    }]);

    const registerMessages = await exchange({
      type: "register",
      sessionId: "authorized-tcp-client",
      stateId,
      session: {
        name: "authorized",
        cwd: repoDir,
        model: "test-model",
        pid: process.pid,
        startedAt: Date.now(),
        lastActivity: Date.now(),
      },
    }, true);
    assert.equal(registerMessages.length, 1);
    assert.deepEqual(registerMessages[0], {
      type: "registered",
      sessionId: "authorized-tcp-client",
      features: [
        "extension-bus-v1",
        "correlated-operations-v1",
        "exact-send-v1",
        "extension-state-refresh-v1",
        "opaque-dispatch-v1",
      ],
      brokerEpoch: (registerMessages[0] as { brokerEpoch: string }).brokerEpoch,
    });
    assert.match((registerMessages[0] as { brokerEpoch: string }).brokerEpoch, /^[0-9a-f-]{36}$/);
  } finally {
    if (broker.exitCode === null && broker.signalCode === null) {
      broker.kill("SIGTERM");
      await once(broker, "exit").catch(() => undefined);
    }
    rmSync(agentDir, { recursive: true, force: true });
  }
});

test("reconciliation baseline and production broker launch remain pinned", () => {
  assert.equal(
    execFileSync("git", ["merge-base", "0685e199", "HEAD"], { cwd: repoDir, encoding: "utf8" }).trim(),
    execFileSync("git", ["rev-parse", "0685e199"], { cwd: repoDir, encoding: "utf8" }).trim(),
  );
  const launch = getBrokerLaunchSpec(
    path.join(repoDir, "broker", "broker.ts"),
    "npx",
    ["--no-install", "tsx"],
  );
  assert.ok(launch.command.length > 0);
  assert.ok(launch.args.some((arg) => arg.endsWith(path.join("broker", "broker.ts"))));
});

async function setupClients() {
  const broker = spawn(process.execPath, [getTsxCliPath(), path.join(repoDir, "broker", "broker.ts")], {
    cwd: repoDir,
    env: { ...process.env, HOME: sharedHomeDir, USERPROFILE: sharedHomeDir },
    stdio: ["ignore", "pipe", "pipe"],
  });

  try {
    await waitForBrokerReady(broker);
    const planner = new IntercomClient();
    const orchestrator = new IntercomClient();

    await planner.connect({
      name: "planner",
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    });
    await orchestrator.connect({
      name: "orchestrator",
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    });

    return {
      planner,
      orchestrator,
      cleanup: async () => {
        await planner.disconnect().catch(() => undefined);
        await orchestrator.disconnect().catch(() => undefined);
        broker.kill("SIGTERM");
        await once(broker, "exit").catch(() => undefined);
      },
    };
  } catch (error) {
    broker.kill("SIGTERM");
    await once(broker, "exit").catch(() => undefined);
    throw error;
  }
}

function waitForReply(client: InstanceType<typeof IntercomClient>, replyTo: string, timeoutMs = 5000): Promise<{ from: SessionInfo; message: Message; }> {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      client.off("message", handler);
      reject(new Error(`Timed out waiting for reply to ${replyTo}`));
    }, timeoutMs);
    const handler = (from: SessionInfo, message: Message) => {
      if (message.replyTo !== replyTo) {
        return;
      }
      clearTimeout(timeout);
      client.off("message", handler);
      resolve({ from, message });
    };
    client.on("message", handler);
  });
}

async function waitForSessionByName(client: InstanceType<typeof IntercomClient>, name: string): Promise<SessionInfo> {
  const deadline = Date.now() + 2000;
  while (Date.now() < deadline) {
    const session = (await client.listSessions()).find((candidate) => candidate.name === name);
    if (session) {
      return session;
    }
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  const sessions = await client.listSessions();
  throw new Error(`Timed out waiting for ${name}; saw ${JSON.stringify(sessions.map((session) => session.name))}`);
}

async function waitForSessionStatus(client: InstanceType<typeof IntercomClient>, name: string, status: string): Promise<SessionInfo> {
  const deadline = Date.now() + 2000;
  while (Date.now() < deadline) {
    const session = (await client.listSessions()).find((candidate) => candidate.name === name);
    if (session?.status === status) {
      return session;
    }
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  const sessions = await client.listSessions();
  throw new Error(`Timed out waiting for ${name} status ${status}; saw ${JSON.stringify(sessions.map((session) => ({ name: session.name, status: session.status })))}`);
}

async function waitForSessionModel(client: InstanceType<typeof IntercomClient>, name: string, model: string): Promise<SessionInfo> {
  const deadline = Date.now() + 2000;
  while (Date.now() < deadline) {
    const session = (await client.listSessions()).find((candidate) => candidate.name === name);
    if (session?.model === model) {
      return session;
    }
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  const sessions = await client.listSessions();
  throw new Error(`Timed out waiting for ${name} model ${model}; saw ${JSON.stringify(sessions.map((session) => ({ name: session.name, model: session.model })))}`);
}

async function withConfirmSendEnabled<T>(fn: () => T | Promise<T>): Promise<T> {
  const { getIntercomDirPath } = await import("./broker/paths.ts");
  const { getConfigPath } = await import("./config.ts");
  const { mkdirSync, writeFileSync, existsSync, rmSync: removeSync } = await import("node:fs");
  const intercomDir = getIntercomDirPath();
  mkdirSync(intercomDir, { recursive: true });
  const configPath = getConfigPath(intercomDir);
  const existed = existsSync(configPath);
  writeFileSync(configPath, JSON.stringify({ confirmSend: true }), "utf-8");
  try {
    return await fn();
  } finally {
    if (existed) {
      writeFileSync(configPath, JSON.stringify({ confirmSend: false }), "utf-8");
    } else {
      removeSync(configPath, { force: true });
    }
  }
}

async function withAskTimeoutMs<T>(timeoutMs: number, fn: () => T | Promise<T>): Promise<T> {
  const previous = process.env.PI_INTERCOM_ASK_TIMEOUT_MS;
  process.env.PI_INTERCOM_ASK_TIMEOUT_MS = String(timeoutMs);
  try {
    return await fn();
  } finally {
    if (previous === undefined) delete process.env.PI_INTERCOM_ASK_TIMEOUT_MS;
    else process.env.PI_INTERCOM_ASK_TIMEOUT_MS = previous;
  }
}

async function waitForSessionId(client: InstanceType<typeof IntercomClient>, sessionId: string): Promise<SessionInfo> {
  const deadline = Date.now() + 2000;
  while (Date.now() < deadline) {
    const session = (await client.listSessions()).find((candidate) => candidate.id === sessionId);
    if (session) {
      return session;
    }
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  const sessions = await client.listSessions();
  throw new Error(`Timed out waiting for ${sessionId}; saw ${JSON.stringify(sessions.map((session) => session.id))}`);
}

async function waitForNoSessionId(client: InstanceType<typeof IntercomClient>, sessionId: string): Promise<void> {
  const deadline = Date.now() + 2000;
  while (Date.now() < deadline) {
    if (!(await client.listSessions()).some((candidate) => candidate.id === sessionId)) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  throw new Error(`Timed out waiting for ${sessionId} to leave`);
}

test("broker accepts caller supplied stable IDs across reconnect", { concurrency: false }, async () => {
  const { planner, cleanup } = await setupClients();
  const worker = new IntercomClient();

  try {
    await worker.connect({
      name: "stable-worker",
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    }, "stable-session-id");
    assert.equal(worker.sessionId, "stable-session-id");
    await waitForSessionId(planner, "stable-session-id");
    await worker.disconnect();
    await waitForNoSessionId(planner, "stable-session-id");

    const reconnected = new IntercomClient();
    await reconnected.connect({
      name: "stable-worker",
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    }, "stable-session-id");
    assert.equal(reconnected.sessionId, "stable-session-id");
    await waitForSessionId(planner, "stable-session-id");
    await reconnected.disconnect();
  } finally {
    await worker.disconnect().catch(() => undefined);
    await cleanup();
  }
});

test("broker owns local trust metadata instead of trusting registration payloads", { concurrency: false }, async () => {
  const { planner, cleanup } = await setupClients();
  const raw = await connectRawRegistered("trust-metadata-worker-id", "trust-metadata-worker", {
    peerUid: 0,
    trustedLocal: false,
  });

  try {
    const session = await waitForSessionId(planner, "trust-metadata-worker-id");
    assert.equal(session.trustedLocal, process.platform !== "win32");
    assert.equal(session.peerUid, undefined);
  } finally {
    raw.socket.destroy();
    await cleanup();
  }
});

test("broker rejects unknown replyTo values instead of delivering forged replies", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();

  try {
    const result = await planner.send(orchestrator.sessionId!, {
      text: "This is not a real reply.",
      replyTo: "not-a-pending-ask",
    });
    assert.equal(result.delivered, false);
    assert.match(result.reason ?? "", /pending ask/i);
  } finally {
    await cleanup();
  }
});

test("broker rotates endpoint epochs and replays same message ids without duplicate delivery", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  const replacement = new IntercomClient();
  const received: Message[] = [];
  orchestrator.on("message", (_from: SessionInfo, message: Message) => received.push(message));

  try {
    const firstEndpoint = await waitForSessionByName(planner, "orchestrator");
    assert.equal(typeof firstEndpoint.endpointEpoch, "string");

    const messageId = "endpoint-epoch-replay";
    const first = await planner.send(orchestrator.sessionId!, { text: "deliver once", messageId });
    const replay = await planner.send(orchestrator.sessionId!, { text: "deliver once", messageId });
    assert.deepEqual([first.delivery, replay.delivery], ["socket_delivered", "socket_delivered"]);
    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.equal(received.filter((message) => message.id === messageId).length, 1);

    await replacement.connect({
      name: "orchestrator-replacement",
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    }, orchestrator.sessionId!);
    const replacedEndpoint = await waitForSessionId(planner, orchestrator.sessionId!);
    assert.notEqual(replacedEndpoint.endpointEpoch, firstEndpoint.endpointEpoch);
  } finally {
    await replacement.disconnect().catch(() => undefined);
    await cleanup();
  }
});

test("delivery records keep colon-containing sender and message IDs distinct", { concurrency: false }, async () => {
  const { orchestrator, cleanup } = await setupClients();
  const first = new IntercomClient();
  const second = new IntercomClient();
  const received: Message[] = [];
  orchestrator.on("message", (_from: SessionInfo, message: Message) => received.push(message));

  try {
    await first.connect({ name: "record-key-first", cwd: repoDir, model: "test-model", pid: process.pid, startedAt: Date.now(), lastActivity: Date.now() }, "a:b");
    await second.connect({ name: "record-key-second", cwd: repoDir, model: "test-model", pid: process.pid, startedAt: Date.now(), lastActivity: Date.now() }, "a");

    assert.equal((await first.send(orchestrator.sessionId!, { messageId: "c", text: "same fingerprint" })).delivered, true);
    assert.equal((await second.send(orchestrator.sessionId!, { messageId: "b:c", text: "same fingerprint" })).delivered, true);
    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.deepEqual(received.map((message) => message.id).sort(), ["b:c", "c"]);
  } finally {
    await first.disconnect().catch(() => undefined);
    await second.disconnect().catch(() => undefined);
    await cleanup();
  }
});

test("client re-resolves a rebound exact target once with the same message id", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  const replacement = new IntercomClient();
  const replacementReceived = once(replacement, "message") as Promise<[SessionInfo, Message]>;
  const listSessions = planner.listSessions.bind(planner);
  let listCalls = 0;

  try {
    (planner as unknown as { listSessions: () => Promise<SessionInfo[]> }).listSessions = async () => {
      const sessions = await listSessions();
      listCalls += 1;
      if (listCalls === 1) {
        await replacement.connect({
          name: "orchestrator-replacement",
          cwd: repoDir,
          model: "test-model",
          pid: process.pid,
          startedAt: Date.now(),
          lastActivity: Date.now(),
        }, orchestrator.sessionId!);
      }
      return sessions;
    };

    const result = await planner.send(orchestrator.sessionId!, { text: "retry after rebound", messageId: "endpoint-rebound-retry" });
    assert.equal(result.delivered, true);
    assert.equal(result.delivery, "socket_delivered");
    const [, message] = await replacementReceived;
    assert.equal(message.id, "endpoint-rebound-retry");
    assert.equal(listCalls, 2);
  } finally {
    await replacement.disconnect().catch(() => undefined);
    await cleanup();
  }
});

test("broker rejects malformed exact target fields instead of falling back to name routing", { concurrency: false }, async () => {
  const { orchestrator, cleanup } = await setupClients();
  const raw = await connectRawRegistered("malformed-exact-sender", "malformed-exact-sender");
  const { createMessageReader } = await import("./broker/framing.ts");

  try {
    const delivery = new Promise<Record<string, unknown>>((resolve, reject) => {
      const reader = createMessageReader((received) => {
        if (typeof received === "object" && received !== null && "type" in received && received.type === "delivery_failed") {
          raw.socket.off("data", reader);
          resolve(received as Record<string, unknown>);
        }
      }, reject);
      raw.socket.on("data", reader);
    });
    raw.writeMessage(raw.socket, {
      type: "send",
      to: orchestrator.sessionId,
      targetId: "",
      targetEpoch: "",
      message: {
        id: "malformed-exact-target",
        timestamp: Date.now(),
        content: { text: "must not reach orchestrator" },
      },
    });
    const result = await delivery;
    assert.equal(result.code, "E_INVALID_TARGET");
  } finally {
    raw.socket.destroy();
    await cleanup();
  }
});

test("broker rejects changed message content after a rebound exact-target failure", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  const raw = await connectRawRegistered("rebound-reuse-sender", "rebound-reuse-sender");
  const replacement = new IntercomClient();
  const { createMessageReader } = await import("./broker/framing.ts");

  try {
    const targetId = orchestrator.sessionId!;
    const oldTarget = await waitForSessionId(planner, targetId);
    await replacement.connect({
      name: "rebound-reuse-replacement",
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    }, targetId);
    const receiveDelivery = () => new Promise<Record<string, unknown>>((resolve, reject) => {
      const reader = createMessageReader((received) => {
        if (typeof received === "object" && received !== null && "type" in received && (received.type === "delivered" || received.type === "delivery_failed")) {
          raw.socket.off("data", reader);
          resolve(received as Record<string, unknown>);
        }
      }, reject);
      raw.socket.on("data", reader);
    });
    const send = (text: string) => raw.writeMessage(raw.socket, {
      type: "send",
      to: targetId,
      targetId,
      targetEpoch: oldTarget.endpointEpoch,
      message: { id: "rebound-id-reuse", timestamp: Date.now(), content: { text } },
    });

    const firstDelivery = receiveDelivery();
    send("first content");
    assert.equal((await firstDelivery).code, "E_TARGET_REBOUND");
    const secondDelivery = receiveDelivery();
    send("changed content");
    assert.equal((await secondDelivery).code, "E_MESSAGE_ID_REUSE");
  } finally {
    raw.socket.destroy();
    await replacement.disconnect().catch(() => undefined);
    await cleanup();
  }
});


test("broker disconnects a connection that exceeds the local rate limit", { concurrency: false }, async () => {
  const { cleanup } = await setupClients();
  const raw = await connectRawRegistered("rate-limit-worker-id", "rate-limit-worker");

  try {
    raw.socket.on("error", () => undefined);
    const closed = once(raw.socket, "close");
    for (let i = 0; i < 600; i += 1) {
      raw.writeMessage(raw.socket, { type: "list", requestId: `flood-${i}` });
    }
    await closed;
    assert.equal(raw.socket.destroyed, true);
  } finally {
    raw.socket.destroy();
    await cleanup();
  }
});

test("broker idle raw connections cannot block legitimate registration", { concurrency: false }, async () => {
  const net = await import("node:net");
  const { getBrokerSocketPath } = await import("./broker/paths.ts");
  const { cleanup } = await setupClients();
  const sockets: ReturnType<typeof net.connect>[] = [];
  const legitimate = new IntercomClient();

  try {
    for (let i = 0; i < 140; i += 1) {
      const socket = net.connect(getBrokerSocketPath());
      socket.on("error", () => undefined);
      sockets.push(socket);
      await Promise.race([
        once(socket, "connect").catch(() => undefined),
        once(socket, "close").catch(() => undefined),
        new Promise((resolve) => setTimeout(resolve, 200)),
      ]);
    }

    await legitimate.connect({
      name: "legitimate-after-idle-flood",
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    });
    assert.equal(legitimate.isConnected(), true);
  } finally {
    for (const socket of sockets) {
      socket.destroy();
    }
    await legitimate.disconnect().catch(() => undefined);
    await cleanup();
  }
});

test("broker times out sockets that unregister and go idle", { concurrency: false }, async () => {
  const { cleanup } = await setupClients();
  const raws: Array<Awaited<ReturnType<typeof connectRawRegistered>>> = [];
  const legitimate = new IntercomClient();

  try {
    for (let i = 0; i < 40; i += 1) {
      const raw = await connectRawRegistered(`unregister-idle-${i}`, `unregister-idle-${i}`);
      raw.socket.on("error", () => undefined);
      raw.writeMessage(raw.socket, { type: "unregister" });
      raws.push(raw);
    }
    await new Promise((resolve) => setTimeout(resolve, 1200));

    await legitimate.connect({
      name: "legitimate-after-unregister-idle-flood",
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    });
    assert.equal(legitimate.isConnected(), true);
  } finally {
    for (const raw of raws) {
      raw.socket.destroy();
    }
    await legitimate.disconnect().catch(() => undefined);
    await cleanup();
  }
});

test("unnamed sessions use a collision-resistant runtime alias", { concurrency: false }, async () => {
  const { planner, cleanup } = await setupClients();
  const { default: piIntercomExtension } = await import("./index.ts");
  const firstSessionId = "019fe418-248e-7447-9379-fdce6e91dcba";
  const secondSessionId = "019fe418-248e-7abc-8123-111111111111";
  const firstHarness = createExtensionHarness("", { sessionId: firstSessionId });
  const secondHarness = createExtensionHarness("", { sessionId: secondSessionId });

  try {
    piIntercomExtension(firstHarness.pi as never);
    piIntercomExtension(secondHarness.pi as never);
    await firstHarness.emitLifecycle("session_start");
    await secondHarness.emitLifecycle("session_start");
    const first = await waitForSessionId(planner, firstSessionId);
    const second = await waitForSessionId(planner, secondSessionId);
    assert.equal(first.name, "subagent-chat-019fe418-248e-7447");
    assert.equal(second.name, "subagent-chat-019fe418-248e-7abc");
    assert.equal(first.runtimeFallbackAlias, true);
    assert.equal(second.runtimeFallbackAlias, true);
    assert.notEqual(first.name, second.name);
  } finally {
    await firstHarness.emitLifecycle("session_shutdown");
    await secondHarness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("broker coalesces no-op presence floods", { concurrency: false }, async () => {
  const { planner, cleanup } = await setupClients();
  const worker = new IntercomClient();
  const updates: SessionInfo[] = [];
  planner.on("presence_update", (session: SessionInfo) => {
    if (session.name === "presence-worker") {
      updates.push(session);
    }
  });

  try {
    await worker.connect({
      name: "presence-worker",
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    });
    worker.updatePresence({ status: "idle" });
    for (let i = 0; i < 20; i += 1) {
      worker.updatePresence({ status: "idle" });
    }
    await new Promise((resolve) => setTimeout(resolve, 150));
    assert.equal(updates.length, 1);
  } finally {
    await worker.disconnect().catch(() => undefined);
    await cleanup();
  }
});

test("old stable-ID socket cannot mutate the replacement session", { concurrency: false }, async () => {
  const { planner, cleanup } = await setupClients();
  const first = await connectRawRegistered("replaceable-session-id", "replaceable-worker-old");
  const replacement = new IntercomClient();

  try {
    await replacement.connect({
      name: "replaceable-worker-new",
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    }, "replaceable-session-id");

    first.writeMessage(first.socket, { type: "presence", name: "stale-name" });
    first.writeMessage(first.socket, { type: "unregister" });
    await new Promise((resolve) => setTimeout(resolve, 50));
    const replacementSession = await waitForSessionId(planner, "replaceable-session-id");
    assert.equal(replacementSession.name, "replaceable-worker-new");

    const received = once(replacement, "message") as Promise<[SessionInfo, Message]>;
    const sent = await planner.send("replaceable-session-id", { text: "still there" });
    assert.equal(sent.delivered, true);
    const [, message] = await received;
    assert.equal(message.content.text, "still there");
  } finally {
    first.socket.destroy();
    await replacement.disconnect().catch(() => undefined);
    await cleanup();
  }
});

test("stable-ID replacement clears old ask edges and ignores stale cancels", { concurrency: false }, async () => {
  const { orchestrator, cleanup } = await setupClients();
  const first = await connectRawRegistered("replaceable-asker-id", "replaceable-asker-old");
  const replacement = new IntercomClient();

  try {
    first.writeMessage(first.socket, {
      type: "send",
      to: orchestrator.sessionId,
      message: { id: "old-ask-edge", timestamp: Date.now(), expectsReply: true, content: { text: "Old ask" } },
    });
    await new Promise((resolve) => setTimeout(resolve, 50));

    await replacement.connect({
      name: "replaceable-asker-new",
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    }, "replaceable-asker-id");

    const reverseAfterReplace = await orchestrator.send("replaceable-asker-id", {
      messageId: "reverse-after-replace",
      text: "Can I ask the replacement?",
      expectsReply: true,
    });
    assert.equal(reverseAfterReplace.delivered, true);
    assert.equal((await replacement.send(orchestrator.sessionId!, {
      text: "Replacement answered.",
      replyTo: "reverse-after-replace",
    })).delivered, true);

    const replacementAsk = await replacement.send(orchestrator.sessionId!, {
      messageId: "replacement-ask-edge",
      text: "Replacement ask",
      expectsReply: true,
    });
    assert.equal(replacementAsk.delivered, true);
    first.writeMessage(first.socket, { type: "cancel_ask", messageId: "replacement-ask-edge" });
    await new Promise((resolve) => setTimeout(resolve, 50));

    const reverseWhileReplacementWaits = await orchestrator.send("replaceable-asker-id", {
      messageId: "reverse-while-replacement-waits",
      text: "Can I ask while replacement waits?",
      expectsReply: true,
    });
    assert.equal(reverseWhileReplacementWaits.delivered, false);
    assert.match(reverseWhileReplacementWaits.reason ?? "", /Mutual ask refused/);
  } finally {
    first.socket.destroy();
    await replacement.disconnect().catch(() => undefined);
    await cleanup();
  }
});

test("broker resolves unique short IDs and rejects ambiguous prefixes", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  const first = new IntercomClient();
  const second = new IntercomClient();
  const evilPrefix = new IntercomClient();

  try {
    await first.connect({ name: "short-id-one", cwd: repoDir, model: "test-model", pid: process.pid, startedAt: Date.now(), lastActivity: Date.now() }, "abcdef12-session");
    await second.connect({ name: "short-id-two", cwd: repoDir, model: "test-model", pid: process.pid, startedAt: Date.now(), lastActivity: Date.now() }, "abcdef99-session");
    await evilPrefix.connect({ name: "evil-prefix", cwd: repoDir, model: "test-model", pid: process.pid, startedAt: Date.now(), lastActivity: Date.now() }, "orchestrator-evil");

    const received = once(first, "message") as Promise<[SessionInfo, Message]>;
    const unique = await planner.send("abcdef12", { text: "prefix works" });
    assert.equal(unique.delivered, true);
    const [, message] = await received;
    assert.equal(message.content.text, "prefix works");

    const ambiguous = await planner.send("abcdef", { text: "ambiguous" });
    assert.equal(ambiguous.delivered, false);
    assert.match(ambiguous.reason ?? "", /Multiple sessions/);

    const exactNameReceived = once(orchestrator, "message") as Promise<[SessionInfo, Message]>;
    const exactName = await planner.send("orchestrator", { text: "exact name wins" });
    assert.equal(exactName.delivered, true);
    const [, exactNameMessage] = await exactNameReceived;
    assert.equal(exactNameMessage.content.text, "exact name wins");
  } finally {
    await first.disconnect().catch(() => undefined);
    await second.disconnect().catch(() => undefined);
    await evilPrefix.disconnect().catch(() => undefined);
    await cleanup();
  }
});

test("intercom tool prefers exact names over ID prefixes", { concurrency: false }, async () => {
  const { orchestrator, cleanup } = await setupClients();
  const { default: piIntercomExtension } = await import("./index.ts");
  const evilPrefix = new IntercomClient();
  const harness = createExtensionHarness("exact-name-worker");

  try {
    await evilPrefix.connect({ name: "evil-prefix", cwd: repoDir, model: "test-model", pid: process.pid, startedAt: Date.now(), lastActivity: Date.now() }, "orchestrator-evil");
    piIntercomExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");

    const intercomTool = harness.tools.find((tool) => tool.name === "intercom")!;
    const exactNameReceived = Promise.race([
      once(orchestrator, "message") as Promise<[SessionInfo, Message]>,
      new Promise<null>((resolve) => setTimeout(() => resolve(null), 500)),
    ]);
    const result = await intercomTool.execute("send-exact-name", { action: "send", to: "orchestrator", message: "exact name wins" }, new AbortController().signal, undefined, harness.ctx);
    assert.notEqual(result.details?.error, true);

    const received = await exactNameReceived;
    assert.notEqual(received, null);
    assert.equal(received![1].content.text, "exact name wins");
    await harness.emitLifecycle("session_shutdown");
  } finally {
    await evilPrefix.disconnect().catch(() => undefined);
    await cleanup();
  }
});

test("extension can pin a restart-stable intercom session id", { concurrency: false }, async () => {
  const { planner, cleanup } = await setupClients();
  const { default: piIntercomExtension } = await import("./index.ts");
  const previousStableId = process.env.PI_INTERCOM_STABLE_ID;
  const previousPublishedId = process.env.PI_INTERCOM_SESSION_ID;
  process.env.PI_INTERCOM_STABLE_ID = "pinned-worker-session";
  const harness = createExtensionHarness("pinned-worker", { sessionId: "transient-pi-session" });

  try {
    piIntercomExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    const session = await waitForSessionId(planner, "pinned-worker-session");
    assert.equal(session.name, "pinned-worker");
    assert.equal(process.env.PI_INTERCOM_SESSION_ID, "pinned-worker-session");
    await harness.emitLifecycle("session_shutdown");
  } finally {
    if (previousStableId === undefined) delete process.env.PI_INTERCOM_STABLE_ID;
    else process.env.PI_INTERCOM_STABLE_ID = previousStableId;
    if (previousPublishedId === undefined) delete process.env.PI_INTERCOM_SESSION_ID;
    else process.env.PI_INTERCOM_SESSION_ID = previousPublishedId;
    await cleanup();
  }
});

test("intercom-id inserts a stable handoff snippet into the editor", { concurrency: false }, async () => {
  const { cleanup } = await setupClients();
  const { default: piIntercomExtension } = await import("./index.ts");
  let editorText = "Existing note";
  const notifications: string[] = [];
  const harness = createExtensionHarness("handoff-worker", {
    hasUI: true,
    ui: {
      getEditorText: () => editorText,
      setEditorText: (text: string) => { editorText = text; },
      notify: (message: string) => { notifications.push(message); },
    },
  });

  try {
    piIntercomExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    await harness.commands.get("intercom-id")!("", harness.ctx);
    assert.match(editorText, /Existing note\n\nUse pi-intercom: intercom\(\{ action: "send", to: "session-child-test", message: "\.\.\." \}\)/);
    assert.match(notifications.at(-1) ?? "", /Inserted intercom contact target: session-child-test/);
    await harness.emitLifecycle("session_shutdown");
  } finally {
    await cleanup();
  }
});

test("intercom tool shows unique ID prefixes when names collide", { concurrency: false }, async () => {
  const { cleanup } = await setupClients();
  const { default: piIntercomExtension } = await import("./index.ts");
  const twinA = new IntercomClient();
  const twinB = new IntercomClient();
  const harness = createExtensionHarness("collision-sender");

  try {
    await twinA.connect({ name: "twin", cwd: repoDir, model: "test-model", pid: process.pid, startedAt: Date.now(), lastActivity: Date.now() }, "019fc92c-066f-755e-95d8-50ebb030d40d");
    await twinB.connect({ name: "twin", cwd: `${repoDir}/other`, model: "test-model", pid: process.pid, startedAt: Date.now(), lastActivity: Date.now() }, "019fc92c-b5f7-7536-b715-e41a4a6e9eb5");
    piIntercomExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");

    const intercomTool = harness.tools.find((tool) => tool.name === "intercom")!;
    const listed = await intercomTool.execute("list-twin", { action: "list" }, new AbortController().signal, undefined, harness.ctx);
    const listText = listed.content.map((part) => (part as { text?: string }).text ?? "").join("");
    assert.match(listText, /019fc92c-066f/);
    assert.match(listText, /019fc92c-b5f7/);

    const listedCwd = await intercomTool.execute("list-cwd-twin", { action: "list-cwd" }, new AbortController().signal, undefined, harness.ctx);
    const listCwdText = listedCwd.content.map((part) => (part as { text?: string }).text ?? "").join("");
    assert.match(listCwdText, /019fc92c-066f/);
    assert.doesNotMatch(listCwdText, /019fc92c-b5f7/);

    const result = await intercomTool.execute("send-twin", { action: "send", to: "twin", message: "which one?" }, new AbortController().signal, undefined, harness.ctx);

    assert.equal(result.details?.error, true);
    const text = result.content.map((part) => (part as { text?: string }).text ?? "").join("");
    assert.match(text, /parentheses/);
    assert.match(text, /019fc92c-066f/);
    assert.match(text, /019fc92c-b5f7/);
    await harness.emitLifecycle("session_shutdown");
  } finally {
    await twinA.disconnect().catch(() => undefined);
    await twinB.disconnect().catch(() => undefined);
    await cleanup();
  }
});

test("invalid extension registrations invoke onUnavailable without onReady", async () => {
  const { default: piIntercomExtension } = await import("./index.ts");
  const harness = createExtensionHarness();
  const unavailable: string[] = [];
  let ready = false;

  piIntercomExtension(harness.pi as never);
  harness.pi.events.emit(INTERCOM_EXTENSION_REGISTER_EVENT, {
    namespace: "invalid opaque namespace",
    ownerEligible: false,
    opaqueDispatch: { version: 1, roles: ["receive"] },
    onReady: () => { ready = true; },
    onEvent: () => undefined,
    onUnavailable: (reason: string) => unavailable.push(reason),
  });

  assert.deepEqual(unavailable, ["unsupported_host"]);
  assert.equal(ready, false);
});

test("extension channels register locally without creating conversation messages", async () => {
  const { default: piIntercomExtension } = await import("./index.ts");
  const harness = createExtensionHarness();
  let channel: IntercomExtensionChannel | undefined;
  const extensionEvents: unknown[] = [];

  piIntercomExtension(harness.pi as never);
  harness.pi.events.emit(INTERCOM_EXTENSION_REGISTER_EVENT, {
    namespace: "test-extension/v1",
    ownerEligible: true,
    onReady: (value: IntercomExtensionChannel) => { channel = value; },
    onEvent: (event: unknown) => extensionEvents.push(event),
  });

  assert.equal(channel?.namespace, "test-extension/v1");
  assert.deepEqual(channel?.snapshot(), {
    connected: false,
    capabilities: { extensionBus: false },
  });
  assert.deepEqual(extensionEvents, []);
  assert.deepEqual(harness.sentMessages, []);
  assert.deepEqual(harness.entries, []);
});

test("opaque claim releases receiver reservation before dispose", { concurrency: false }, async () => {
  const { default: piIntercomExtension } = await import("./index.ts");
  const { cleanup } = await setupClients();
  const sender = new IntercomClient();
  const harness = createExtensionHarness("opaque-claim-receiver", { hasUI: true, sessionId: "opaque-claim-receiver" });
  let receiverChannel: IntercomExtensionChannel | undefined;
  let claim: (() => Promise<unknown>) | undefined;
  try {
    piIntercomExtension(harness.pi as never);
    harness.pi.events.emit(INTERCOM_EXTENSION_REGISTER_EVENT, {
      namespace: "opaque/claim-receiver",
      ownerEligible: false,
      opaqueDispatch: {
        version: 1,
        roles: ["receive"],
        onReserve: (_event: unknown, reservation: { claim(): Promise<unknown> }) => {
          claim = () => reservation.claim();
          return "reserved";
        },
      },
      onReady: (channel: IntercomExtensionChannel) => { receiverChannel = channel; },
      onEvent: () => undefined,
    });
    await harness.emitLifecycle("session_start");
    await sender.connect({
      name: "opaque-claim-sender", cwd: repoDir, model: "test-model", pid: process.pid, startedAt: Date.now(), lastActivity: Date.now(),
      extensions: [{ namespace: "opaque/claim-sender", ownerEligible: false, opaqueDispatch: { version: 1, roles: ["send"] } }],
    }, "opaque-claim-sender");
    await waitForSessionId(sender, "opaque-claim-receiver");
    const accepted = await sender.sendOpaqueDispatch("opaque/claim-sender", {
      requestId: "claim-release-request", toSessionId: "opaque-claim-receiver", recipientNamespace: "opaque/claim-receiver", payload: null,
    });
    assert.equal(accepted.accepted, true);
    assert.ok(claim);
    assert.deepEqual(await claim(), { claimed: true });
    assert.deepEqual(await claim(), { claimed: true });

    const frames: BrokerMessage[] = [];
    const stop = sender.onOpaqueDispatch((frame) => frames.push(frame));
    receiverChannel?.dispose();
    await new Promise((resolve) => setTimeout(resolve, 25));
    stop();
    assert.equal(frames.some((frame) => frame.type === "opaque_dispatch_v1_receipt" && frame.receipt.status === "failed_closed"), false);
  } finally {
    receiverChannel?.dispose();
    await sender.disconnect().catch(() => undefined);
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("opaque consumer refusal and callback failures retain typed terminal reasons", { concurrency: false }, async () => {
  const cases = [
    { name: "refused", expected: "consumer_refused", onReserve: () => "refused" },
    { name: "threw", expected: "consumer_threw", onReserve: () => { throw new Error("consumer failed privately"); } },
    { name: "malformed", expected: "malformed_consumer_result", onReserve: () => "invalid" },
  ] as const;
  for (const entry of cases) {
    const { default: piIntercomExtension } = await import("./index.ts");
    const { cleanup } = await setupClients();
    const sender = new IntercomClient();
    const sessionId = `opaque-${entry.name}-receiver`;
    const namespace = `opaque/${entry.name}-receiver`;
    const harness = createExtensionHarness(sessionId, { hasUI: true, sessionId });
    let channel: IntercomExtensionChannel | undefined;
    try {
      piIntercomExtension(harness.pi as never);
      harness.pi.events.emit(INTERCOM_EXTENSION_REGISTER_EVENT, {
        namespace,
        ownerEligible: false,
        opaqueDispatch: { version: 1, roles: ["receive"], onReserve: entry.onReserve },
        onReady: (value: IntercomExtensionChannel) => { channel = value; },
        onEvent: () => undefined,
      });
      await harness.emitLifecycle("session_start");
      await sender.connect({
        name: `opaque-${entry.name}-sender`, cwd: repoDir, model: "test-model", pid: process.pid, startedAt: Date.now(), lastActivity: Date.now(),
        extensions: [{ namespace: "opaque/failure-sender", ownerEligible: false, opaqueDispatch: { version: 1, roles: ["send"] } }],
      }, `opaque-${entry.name}-sender`);
      await waitForSessionId(sender, sessionId);
      const result = await sender.sendOpaqueDispatch("opaque/failure-sender", {
        requestId: `${entry.name}-request`, toSessionId: sessionId, recipientNamespace: namespace, payload: null,
      });
      assert.equal(result.accepted, false);
      if (!result.accepted) assert.equal(result.code, entry.expected);
    } finally {
      channel?.dispose();
      await sender.disconnect().catch(() => undefined);
      await harness.emitLifecycle("session_shutdown");
      await cleanup();
    }
  }
});

test("opaque extension sends consumer_unloaded before dispose removes capability", { concurrency: false }, async () => {
  const { default: piIntercomExtension } = await import("./index.ts");
  const { cleanup } = await setupClients();
  const sender = new IntercomClient();
  const harness = createExtensionHarness("opaque-receiver", { hasUI: true, sessionId: "opaque-receiver" });
  let receiverChannel: IntercomExtensionChannel | undefined;
  let senderChannel: IntercomExtensionChannel | undefined;
  try {
    piIntercomExtension(harness.pi as never);
    harness.pi.events.emit(INTERCOM_EXTENSION_REGISTER_EVENT, {
      namespace: "opaque/receiver",
      ownerEligible: false,
      opaqueDispatch: {
        version: 1,
        roles: ["receive"],
        onReserve: () => "reserved",
      },
      onReady: (channel: IntercomExtensionChannel) => { receiverChannel = channel; },
      onEvent: () => undefined,
    });
    await harness.emitLifecycle("session_start");
    await sender.connect({
      name: "opaque-sender",
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
      extensions: [{ namespace: "opaque/sender", ownerEligible: false, opaqueDispatch: { version: 1, roles: ["send"] } }],
    }, "opaque-sender");
    const receiverSession = await waitForSessionId(sender, "opaque-receiver");
    assert.equal(receiverSession.opaqueDispatch?.namespaces.some((entry) => entry.namespace === "opaque/receiver" && entry.roles.includes("receive")), true);
    senderChannel = {
      namespace: "opaque/sender",
      snapshot: () => ({ connected: true, capabilities: { extensionBus: true, opaqueDispatchVersion: 1 } }),
      publish: () => undefined,
      commitState: () => undefined,
      refreshState: async () => ({ ok: false, code: "connection_lost" }),
      listSessions: () => sender.listSessions(),
      peerCapability: (sessionId, recipientNamespace) => sender.peerCapability(sessionId, recipientNamespace),
      sendOpaqueDispatch: (input) => sender.sendOpaqueDispatch("opaque/sender", input),
      cancelMessage: (messageId) => sender.cancelOpaqueDispatch("opaque/sender", messageId),
      reconcileClaim: (input) => sender.reconcileOpaqueClaim("opaque/sender", input),
      dispose: () => undefined,
    };
    const terminalReceipt = new Promise<Extract<BrokerMessage, { type: "opaque_dispatch_v1_receipt" }>>((resolve, reject) => {
      const timeout = setTimeout(() => {
        stop();
        reject(new Error("consumer_unloaded receipt timeout"));
      }, 2_000);
      const stop = sender.onOpaqueDispatch((frame) => {
        if (frame.type !== "opaque_dispatch_v1_receipt" || frame.receipt.status !== "failed_closed") return;
        clearTimeout(timeout);
        stop();
        resolve(frame);
      });
    });
    const result = await senderChannel.sendOpaqueDispatch({
      requestId: "dispose-request",
      toSessionId: "opaque-receiver",
      recipientNamespace: "opaque/receiver",
      payload: { private: true },
    });
    assert.equal(result.accepted, true);
    await new Promise((resolve) => setTimeout(resolve, 20));
    receiverChannel?.dispose();
    assert.equal((await terminalReceipt).receipt.reason, "consumer_unloaded");
    assert.equal(sender.isConnected(), true);
    assert.equal((await sender.listSessions()).some((session) => session.id === "opaque-receiver"), true);
  } finally {
    receiverChannel?.dispose();
    senderChannel?.dispose();
    await sender.disconnect().catch(() => undefined);
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("late extension registration advertises before an onReady publish", { concurrency: false }, async () => {
  const { cleanup } = await setupClients();
  const observer = new IntercomClient();
  const observerMessages: BrokerMessage[] = [];
  const harness = createExtensionHarness("late-extension-worker");
  const extensionEvents: unknown[] = [];

  try {
    observer.onBrokerMessage((message) => observerMessages.push(message));
    observer.on("error", () => {});
    await observer.connect({
      name: "extension-observer",
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
      extensions: [{ namespace: "late-extension/v1", ownerEligible: false }],
    });

    const { default: piIntercomExtension } = await import("./index.ts");
    piIntercomExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    await waitForSessionByName(observer, "late-extension-worker");

    harness.pi.events.emit(INTERCOM_EXTENSION_REGISTER_EVENT, {
      namespace: "late-extension/v1",
      ownerEligible: true,
      onReady: (channel: IntercomExtensionChannel) => {
        channel.publish({ probe: "onReady" }, { audience: "capable" });
      },
      onEvent: (event: unknown) => extensionEvents.push(event),
    });

    const deadline = Date.now() + 3000;
    while (
      Date.now() < deadline
      && !observerMessages.some((message) => message.type === "extension_message"
        && (message.payload as { probe?: string }).probe === "onReady")
    ) {
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    assert.equal(
      observerMessages.some((message) => message.type === "extension_message"
        && (message.payload as { probe?: string }).probe === "onReady"),
      true,
    );
    assert.equal(
      extensionEvents.some((event) => typeof event === "object" && event !== null
        && (event as { type?: string }).type === "connection"
        && (event as { connected?: boolean }).connected === true),
      true,
    );
    assert.deepEqual(harness.sentMessages, []);
    assert.deepEqual(harness.entries, []);
    await harness.emitLifecycle("session_shutdown");
  } finally {
    await observer.disconnect().catch(() => undefined);
    await cleanup();
  }
});

test("intercom tool renders compact call and result rows", async () => {
  const { default: piIntercomExtension } = await import("./index.ts");
  const harness = createExtensionHarness();

  piIntercomExtension(harness.pi as never);
  const intercomTool = harness.tools.find((tool) => tool.name === "intercom")!;

  assert.ok(intercomTool.renderCall);
  assert.ok(intercomTool.renderResult);
  assert.match(renderToText(intercomTool.renderCall({
    action: "ask",
    to: "planner",
    message: "Need a decision before I continue with this implementation.",
    attachments: [{ type: "snippet", name: "note.ts", content: "const ok = true;" }],
  }, renderTheme, {})), /intercom ask → planner \(1 attachment\)\n {2}Need a decision/);

  const resultText = renderToText(intercomTool.renderResult({
    content: [{ type: "text", text: "Message sent to planner" }],
    details: { delivered: true, messageId: "abcdef123456" },
  }, { isPartial: false, expanded: false }, renderTheme, { isError: false, expanded: false }));
  assert.match(resultText, /✓ Message sent to planner \(abcdef12\)/);

  const errorText = renderToText(intercomTool.renderResult({
    content: [{ type: "text", text: "Missing 'to' or 'message' parameter" }],
    details: { error: true, reason: "Missing target" },
  }, { isPartial: false, expanded: true }, renderTheme, { isError: false, expanded: true }));
  assert.match(errorText, /✗ Missing 'to' or 'message' parameter/);
  assert.match(errorText, /Reason: Missing target/);
});

test("intercom tool result hook marks failed details as errors", async () => {
  const { default: piIntercomExtension } = await import("./index.ts");
  const harness = createExtensionHarness();
  piIntercomExtension(harness.pi as never);

  const errorResults = await harness.emitLifecycleResults("tool_result", {
    toolName: "intercom",
    details: { error: true },
  });
  assert.deepEqual(errorResults.filter(Boolean), [{ isError: true }]);

  const deliveryResults = await harness.emitLifecycleResults("tool_result", {
    toolName: "contact_supervisor",
    details: { delivered: false },
  });
  assert.deepEqual(deliveryResults.filter(Boolean), [{ isError: true }]);

  const okResults = await harness.emitLifecycleResults("tool_result", {
    toolName: "intercom",
    details: { delivered: true },
  });
  assert.deepEqual(okResults.filter(Boolean), []);
});

test("contact supervisor tool renders reason and reply state", async () => {
  const { default: piIntercomExtension } = await import("./index.ts");

  await withChildOrchestratorEnv({
    orchestratorTarget: "orchestrator",
    runId: "78f659a3",
    agent: "worker",
    index: "0",
  }, () => {
    const harness = createExtensionHarness();
    piIntercomExtension(harness.pi as never);
    const supervisorTool = harness.tools.find((tool) => tool.name === "contact_supervisor")!;

    assert.ok(supervisorTool.renderCall);
    assert.ok(supervisorTool.renderResult);
    assert.match(renderToText(supervisorTool.renderCall({
      reason: "interview_request",
      message: "Please answer these before I continue.",
      interview: { title: "API migration", questions: [] },
    }, renderTheme, {})), /contact_supervisor interview_request API migration\n {2}Please answer/);

    const warningText = renderToText(supervisorTool.renderResult({
      content: [{ type: "text", text: "Reply from supervisor:\nUse stable API" }],
      details: { structuredReplyParseError: "reply JSON must include a responses array" },
    }, { isPartial: false }, renderTheme, { isError: false }));
    assert.match(warningText, /⚠ Reply from supervisor:\nUse stable API/);
    assert.match(warningText, /Structured reply parse issue: reply JSON must include a responses array/);

    const failureText = renderToText(supervisorTool.renderResult({
      content: [{ type: "text", text: "Invalid reason" }],
      details: { error: true },
    }, { isPartial: false }, renderTheme, { isError: false }));
    assert.match(failureText, /✗ Invalid reason/);
  });
});

test("sessions publish automatic lifecycle status", { concurrency: false }, async () => {
  const { default: piIntercomExtension } = await import("./index.ts");
  const { planner, cleanup } = await setupClients();
  const harness = createExtensionHarness("status-worker", { hasUI: true });

  try {
    piIntercomExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");

    await waitForSessionStatus(planner, "status-worker", "idle");

    const freshEventContext = {
      ...harness.ctx,
      model: { id: "fresh-model" },
      sessionManager: { getSessionId: () => "session-child-test" },
    };
    await harness.emitLifecycle("model_select", { model: { id: "fresh-model" } }, freshEventContext);
    await waitForSessionModel(planner, "status-worker", "fresh-model");

    await harness.emitLifecycle("agent_start");
    await waitForSessionStatus(planner, "status-worker", "thinking");

    await harness.emitLifecycle("tool_execution_start", { toolCallId: "tool-1", toolName: "bash" });
    await waitForSessionStatus(planner, "status-worker", "tool:bash");
    await harness.emitLifecycle("tool_execution_start", { toolCallId: "tool-2", toolName: "read" });

    await harness.emitLifecycle("tool_execution_end", { toolCallId: "tool-1", toolName: "bash" });
    await waitForSessionStatus(planner, "status-worker", "tool:read");

    await harness.emitLifecycle("tool_execution_end", { toolCallId: "tool-2", toolName: "read" });
    await waitForSessionStatus(planner, "status-worker", "thinking");

    await harness.emitLifecycle("agent_end");
    await waitForSessionStatus(planner, "status-worker", "idle");
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("idle name poll propagates /name changes without other activity", { concurrency: false }, async () => {
  const { planner, cleanup } = await setupClients();
  let sessionName = "idle-name-before";
  const harness = createExtensionHarness(() => sessionName, { hasUI: true });

  try {
    await withChildOrchestratorEnv({ namePollMs: "25" }, async () => {
      const { default: piIntercomExtension } = await import("./index.ts");
      piIntercomExtension(harness.pi as never);
      await harness.emitLifecycle("session_start");
      await waitForSessionByName(planner, "idle-name-before");
      sessionName = "idle-name-after";
      await waitForSessionByName(planner, "idle-name-after");
    });
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("turn_start re-registers when Pi replaces the session context", { concurrency: false }, async () => {
  const { planner, cleanup } = await setupClients();
  let sessionName = "fork-before";
  let sessionId = "session-fork-before";
  const harness = createExtensionHarness(() => sessionName, { hasUI: true, sessionId: () => sessionId });

  try {
    const { default: piIntercomExtension } = await import("./index.ts");
    piIntercomExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    await waitForSessionId(planner, "session-fork-before");

    sessionName = "fork-after";
    sessionId = "session-fork-after";
    await harness.emitLifecycle("turn_start");
    const replaced = await waitForSessionId(planner, "session-fork-after");
    assert.equal(replaced.name, "fork-after");
    await waitForNoSessionId(planner, "session-fork-before");
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("busy interactive sessions steer top-level asks without aborting", { concurrency: false }, async () => {
  const { default: piIntercomExtension } = await import("./index.ts");
  const { planner, cleanup } = await setupClients();
  let abortCount = 0;
  let idle = false;
  const harness = createExtensionHarness("interactive-worker", {
    abort: () => { abortCount += 1; },
    hasUI: true,
    isIdle: () => idle,
  });

  try {
    piIntercomExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");

    const target = await waitForSessionByName(planner, "interactive-worker");

    const delivered = await planner.send(target.id, {
      messageId: 'interactive-busy-"ask',
      text: "Can you respond after your current turn?",
      expectsReply: true,
    });
    assert.equal(delivered.delivered, true);
    await new Promise((resolve) => setTimeout(resolve, 20));
    assert.equal(abortCount, 0);
    assert.equal(harness.sentMessages.length, 1);
    assert.equal(harness.sentMessages[0]?.message.customType, "intercom_message");
    assert.equal(harness.sentMessages[0]?.options?.deliverAs, "steer");
    assert.match(harness.sentMessages[0]?.message.content ?? "", /Can you respond after your current turn/);
    assert.match(harness.sentMessages[0]?.message.content ?? "", /replyTo: "interactive-busy-\\"ask"/);

    await harness.emitLifecycle("turn_end");
    assert.equal(harness.sentMessages.length, 1, "turn end must not inject the steered message again");

    idle = true;
    await harness.emitLifecycle("agent_end");
    await new Promise((resolve) => setTimeout(resolve, 20));

    assert.equal(abortCount, 0);
    assert.equal(harness.sentMessages.length, 1, "agent end must not inject the steered message again");
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("idle interactive sessions trigger a new turn immediately", { concurrency: false }, async () => {
	const { default: piIntercomExtension } = await import("./index.ts");
	const { planner, cleanup } = await setupClients();
	const harness = createExtensionHarness("idle-trigger-worker", {
		hasUI: true,
		isIdle: () => true,
	});

	try {
		piIntercomExtension(harness.pi as never);
		await harness.emitLifecycle("session_start");
		const worker = await waitForSessionByName(planner, "idle-trigger-worker");

		assert.equal((await planner.send(worker.id, { messageId: "idle-trigger", text: "Handle this now" })).delivered, true);
		await new Promise((resolve) => setTimeout(resolve, 20));

		assert.equal(harness.sentMessages.length, 1);
		assert.equal(harness.sentMessages[0]?.options?.triggerTurn, true);
		assert.equal(harness.sentMessages[0]?.options?.deliverAs, undefined);
	} finally {
		await harness.emitLifecycle("session_shutdown");
		await cleanup();
	}
});

test("duplicate inbound message IDs inject once with visible delivery metadata", { concurrency: false }, async () => {
  const { default: piIntercomExtension } = await import("./index.ts");
  const { planner, cleanup } = await setupClients();
  const harness = createExtensionHarness("dedupe-worker", { hasUI: true });

  try {
    piIntercomExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    const worker = await waitForSessionByName(planner, "dedupe-worker");
    const receipts: string[] = [];
    const unsubscribeReceipts = planner.onMessageReceipt((_from, receipt) => {
      if (receipt.messageId === "duplicate-inbound") receipts.push(receipt.detail ? `${receipt.status}:${receipt.detail}` : receipt.status);
    });

    try {
      assert.equal((await planner.send(worker.id, { messageId: "duplicate-inbound", text: "First copy" })).delivered, true);
      assert.equal((await planner.send(worker.id, { messageId: "duplicate-inbound", text: "First copy" })).delivered, true);
      await new Promise((resolve) => setTimeout(resolve, 100));
    } finally {
      unsubscribeReceipts();
    }

    assert.equal(harness.sentMessages.length, 1);
    assert.ok(receipts.includes("receiver_received"));
    assert.ok(receipts.includes("acknowledged:accepted by receiver"));
    assert.ok(receipts.includes("injected"));
    assert.equal(receipts.includes("acknowledged:duplicate message id suppressed"), false);
    const sent = harness.sentMessages[0]!;
    assert.match(sent.message.content ?? "", /id duplicate-inbound/);
    assert.match(sent.message.content ?? "", /seq 1/);
    assert.match(sent.message.content ?? "", /broker delivered/);
    assert.match(sent.message.content ?? "", /receiver received/);
    assert.match(sent.message.content ?? "", /injected/);
    const details = sent.message.details as { message?: Message };
    assert.equal(details.message?.id, "duplicate-inbound");
    assert.equal(details.message?.senderSequence, 1);
    assert.equal(typeof details.message?.brokerReceivedAt, "number");
    assert.equal(typeof details.message?.brokerDeliveredAt, "number");
    assert.equal(typeof details.message?.receiverReceivedAt, "number");
    assert.equal(typeof details.message?.injectedAt, "number");
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("busy interactive sessions steer same-sender messages in sequence order", { concurrency: false }, async () => {
  const { default: piIntercomExtension } = await import("./index.ts");
  const { planner, cleanup } = await setupClients();
  let idle = false;
  const harness = createExtensionHarness("sequence-worker", {
    hasUI: true,
    isIdle: () => idle,
  });

  try {
    piIntercomExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    const worker = await waitForSessionByName(planner, "sequence-worker");
    const receipts = new Map<string, string[]>();
    const unsubscribeReceipts = planner.onMessageReceipt((_from, receipt) => {
      const statuses = receipts.get(receipt.messageId) ?? [];
      statuses.push(receipt.status);
      receipts.set(receipt.messageId, statuses);
    });

    assert.equal((await planner.send(worker.id, { messageId: "sequence-1", text: "First steered message" })).delivered, true);
    assert.equal((await planner.send(worker.id, { messageId: "sequence-2", text: "Second steered message" })).delivered, true);
    await new Promise((resolve) => setTimeout(resolve, 20));
    assert.equal(harness.sentMessages.length, 2);
    assert.match(harness.sentMessages[0]?.message.content ?? "", /First steered message/);
    assert.match(harness.sentMessages[1]?.message.content ?? "", /Second steered message/);
    assert.equal(harness.sentMessages[0]?.options?.deliverAs, "steer");
    assert.equal(harness.sentMessages[1]?.options?.deliverAs, "steer");

    idle = true;
    await harness.emitLifecycle("agent_end");
    await new Promise((resolve) => setTimeout(resolve, 20));
    assert.equal(harness.sentMessages.length, 2, "steered messages must not be injected again at idle");
    const firstDetails = harness.sentMessages[0]?.message.details as { message?: Message } | undefined;
    const secondDetails = harness.sentMessages[1]?.message.details as { message?: Message } | undefined;
    assert.equal(firstDetails?.message?.senderSequence, 1);
    assert.equal(secondDetails?.message?.senderSequence, 2);
    assert.deepEqual(receipts.get("sequence-1"), ["receiver_received", "acknowledged", "injected"]);
    assert.deepEqual(receipts.get("sequence-2"), ["receiver_received", "acknowledged", "injected"]);
    unsubscribeReceipts();
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("explicit cancel acknowledges that a steered inbound message may already be processed", { concurrency: false }, async () => {
  const { default: piIntercomExtension } = await import("./index.ts");
  const { planner, cleanup } = await setupClients();
  let idle = false;
  const harness = createExtensionHarness("cancel-worker", {
    hasUI: true,
    isIdle: () => idle,
  });

  try {
    piIntercomExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    const worker = await waitForSessionByName(planner, "cancel-worker");
    const receipts: string[] = [];
    const unsubscribeReceipts = planner.onMessageReceipt((_from, receipt) => {
      if (receipt.messageId === "cancel-steered") receipts.push(receipt.status);
    });

    assert.equal((await planner.send(worker.id, { messageId: "cancel-steered", text: "Cancel after steering", expectsReply: true })).delivered, true);
    await new Promise((resolve) => setTimeout(resolve, 20));
    assert.equal(harness.sentMessages.length, 1);
    assert.equal(harness.sentMessages[0]?.options?.deliverAs, "steer");
    assert.equal((await planner.cancelMessage("cancel-steered")).delivered, true);
    await new Promise((resolve) => setTimeout(resolve, 20));

    idle = true;
    await harness.emitLifecycle("agent_end");
    await new Promise((resolve) => setTimeout(resolve, 20));

    assert.equal(harness.sentMessages.length, 1);
    assert.deepEqual(receipts, ["receiver_received", "acknowledged", "injected", "cancellation_requested"]);
    unsubscribeReceipts();
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("intercom cancel action requests cancellation for a sent message", { concurrency: false }, async () => {
  const { default: piIntercomExtension } = await import("./index.ts");
  const { planner, cleanup } = await setupClients();
  let idle = false;
  const senderHarness = createExtensionHarness("cancel-sender", { sessionId: "session-cancel-sender" });
  const receiverHarness = createExtensionHarness("cancel-tool-worker", {
    hasUI: true,
    isIdle: () => idle,
    sessionId: "session-cancel-tool-worker",
  });

  try {
    piIntercomExtension(senderHarness.pi as never);
    piIntercomExtension(receiverHarness.pi as never);
    await senderHarness.emitLifecycle("session_start");
    await receiverHarness.emitLifecycle("session_start");
    await waitForSessionByName(planner, "cancel-tool-worker");
    const intercomTool = senderHarness.tools.find((tool) => tool.name === "intercom")!;

    const sendResult = await intercomTool.execute("send-before-cancel", { action: "send", to: "cancel-tool-worker", message: "Cancel this through the tool" }, new AbortController().signal, undefined, senderHarness.ctx);
    const messageId = String(sendResult.details?.messageId);
    assert.equal(sendResult.details?.delivered, true);
    assert.notEqual(messageId, "undefined");
    await new Promise((resolve) => setTimeout(resolve, 20));
    assert.equal(receiverHarness.sentMessages.length, 1);
    assert.equal(receiverHarness.sentMessages[0]?.options?.deliverAs, "steer");

    const cancelResult = await intercomTool.execute("cancel-message", { action: "cancel", messageId }, new AbortController().signal, undefined, senderHarness.ctx);
    assert.equal(cancelResult.details?.delivered, true);
    assert.match(cancelResult.content[0]?.text ?? "", /Cancellation requested/);

    idle = true;
    await receiverHarness.emitLifecycle("agent_end");
    await new Promise((resolve) => setTimeout(resolve, 20));
    assert.equal(receiverHarness.sentMessages.length, 1);
  } finally {
    await senderHarness.emitLifecycle("session_shutdown");
    await receiverHarness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("same-sender supersede reports an already-steered inbound message", { concurrency: false }, async () => {
  const { default: piIntercomExtension } = await import("./index.ts");
  const { planner, cleanup } = await setupClients();
  let idle = false;
  const harness = createExtensionHarness("supersede-worker", {
    hasUI: true,
    isIdle: () => idle,
  });

  try {
    piIntercomExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    const worker = await waitForSessionByName(planner, "supersede-worker");
    const receipts = new Map<string, string[]>();
    const unsubscribeReceipts = planner.onMessageReceipt((_from, receipt) => {
      const statuses = receipts.get(receipt.messageId) ?? [];
      statuses.push(receipt.status);
      receipts.set(receipt.messageId, statuses);
    });

    assert.equal((await planner.send(worker.id, { messageId: "superseded-message", text: "Old steered message", expectsReply: true })).delivered, true);
    await new Promise((resolve) => setTimeout(resolve, 20));
    const replacement = await planner.send(worker.id, { messageId: "replacement-message", text: "Replacement message", supersedes: "superseded-message" });
    assert.equal(replacement.delivered, true);
    await new Promise((resolve) => setTimeout(resolve, 20));

    assert.equal(harness.sentMessages.length, 2);
    assert.match(harness.sentMessages[0]?.message.content ?? "", /Old steered message/);
    assert.match(harness.sentMessages[1]?.message.content ?? "", /Replacement message/);
    assert.equal(harness.sentMessages[0]?.options?.deliverAs, "steer");
    assert.equal(harness.sentMessages[1]?.options?.deliverAs, "steer");
    const details = harness.sentMessages[1]?.message.details as { message?: Message } | undefined;
    assert.equal(details?.message?.supersedes, "superseded-message");

    idle = true;
    await harness.emitLifecycle("agent_end");
    await new Promise((resolve) => setTimeout(resolve, 20));
    assert.equal(harness.sentMessages.length, 2);
    assert.deepEqual(receipts.get("superseded-message"), ["receiver_received", "acknowledged", "injected", "superseded"]);
    assert.deepEqual(receipts.get("replacement-message"), ["receiver_received", "acknowledged", "injected"]);
    unsubscribeReceipts();
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("supersede is scoped to the same sender and receiver", { concurrency: false }, async () => {
  const { default: piIntercomExtension } = await import("./index.ts");
  const { planner, orchestrator, cleanup } = await setupClients();
  const idle = false;
  const firstHarness = createExtensionHarness("supersede-first", {
    hasUI: true,
    isIdle: () => idle,
  });
  const secondHarness = createExtensionHarness("supersede-second", {
    hasUI: true,
    isIdle: () => idle,
    sessionId: "session-supersede-second",
  });

  try {
    piIntercomExtension(firstHarness.pi as never);
    piIntercomExtension(secondHarness.pi as never);
    await firstHarness.emitLifecycle("session_start");
    await secondHarness.emitLifecycle("session_start");
    const first = await waitForSessionByName(planner, "supersede-first");
    const second = await waitForSessionByName(planner, "supersede-second");

    assert.equal((await planner.send(first.id, { messageId: "wrong-target-old", text: "Old target" })).delivered, true);
    const wrongReceiver = await planner.send(second.id, { messageId: "wrong-target-new", text: "Wrong receiver", supersedes: "wrong-target-old" });
    assert.equal(wrongReceiver.delivered, false);
    assert.match(wrongReceiver.reason ?? "", /same sender and receiver|previous message/);

    const wrongSender = await orchestrator.send(first.id, { messageId: "wrong-sender-new", text: "Wrong sender", supersedes: "wrong-target-old" });
    assert.equal(wrongSender.delivered, false);
    assert.match(wrongSender.reason ?? "", /same sender and receiver|previous message/);
  } finally {
    await firstHarness.emitLifecycle("session_shutdown");
    await secondHarness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("replied steered asks are not injected again after the current turn", { concurrency: false }, async () => {
  const { default: piIntercomExtension } = await import("./index.ts");
  const { planner, cleanup } = await setupClients();
  let idle = false;
  const harness = createExtensionHarness("reply-while-busy-worker", {
    hasUI: true,
    isIdle: () => idle,
  });

  try {
    piIntercomExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    const worker = await waitForSessionByName(planner, "reply-while-busy-worker");

    const askId = "reply-while-busy-ask";
    const replyReceived = waitForReply(planner, askId);
    assert.equal((await planner.send(worker.id, {
      messageId: askId,
      text: "Can you answer before this turn ends?",
      expectsReply: true,
    })).delivered, true);
    await new Promise((resolve) => setTimeout(resolve, 20));
    assert.equal(harness.sentMessages.length, 1);
    assert.equal(harness.sentMessages[0]?.options?.deliverAs, "steer");

    const intercomTool = harness.tools.find((tool) => tool.name === "intercom")!;
    const result = await intercomTool.execute("reply-while-busy", {
      action: "reply",
      message: "Answered during the current turn.",
      replyTo: askId,
    }, new AbortController().signal, undefined, harness.ctx);
    assert.equal(result.details?.delivered, true);
    assert.equal((await replyReceived).message.replyTo, askId);

    idle = true;
    await harness.emitLifecycle("agent_end");
    await new Promise((resolve) => setTimeout(resolve, 20));
    assert.equal(harness.sentMessages.length, 1);
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("deferred startup connect is cancelled on shutdown", { concurrency: false }, async () => {
  const { default: piIntercomExtension } = await import("./index.ts");
  const { planner, cleanup } = await setupClients();
  const harness = createExtensionHarness("shutdown-before-start", { hasUI: true });

  try {
    piIntercomExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    await harness.emitLifecycle("session_shutdown");
    await new Promise((resolve) => setTimeout(resolve, 50));

    const sessions = await planner.listSessions();
    assert.equal(sessions.some((session) => session.name === "shutdown-before-start"), false);
  } finally {
    await cleanup();
  }
});

test("stale overlay work stops after same-session restart", { concurrency: false }, async () => {
  const { default: piIntercomExtension } = await import("./index.ts");
  const { planner, cleanup } = await setupClients();
  let customCalls = 0;
  let resolveFirstCustom: ((value: unknown) => void) | undefined;
  const ui = {
    notify: () => undefined,
    custom: async () => {
      customCalls += 1;
      if (customCalls > 1) {
        return { sent: false };
      }
      return new Promise((resolve) => {
        resolveFirstCustom = resolve;
      });
    },
  };
  const harness = createExtensionHarness("overlay-worker", { hasUI: true, ui });

  try {
    piIntercomExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    await waitForSessionByName(planner, "overlay-worker");

    const overlayPromise = Promise.resolve(harness.commands.get("intercom")!("", harness.ctx));
    const deadline = Date.now() + 2000;
    while (!resolveFirstCustom && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 25));
    }
    assert.ok(resolveFirstCustom, "overlay should reach the session picker");

    const plannerSession = await waitForSessionByName(planner, "planner");
    await harness.emitLifecycle("session_shutdown");
    await harness.emitLifecycle("session_start");
    resolveFirstCustom(plannerSession);
    await overlayPromise;

    assert.equal(customCalls, 1);
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("steered inbound messages are not reinjected after shutdown", { concurrency: false }, async () => {
  const { default: piIntercomExtension } = await import("./index.ts");
  const { planner, cleanup } = await setupClients();
  let idle = false;
  const harness = createExtensionHarness("disposed-worker", {
    hasUI: true,
    isIdle: () => idle,
  });

  try {
    piIntercomExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    const target = await waitForSessionByName(planner, "disposed-worker");
    const receipts: string[] = [];
    const unsubscribeReceipts = planner.onMessageReceipt((_from, receipt) => {
      if (receipt.messageId === "disposed-ask") receipts.push(receipt.status);
    });

    const delivered = await planner.send(target.id, {
      messageId: "disposed-ask",
      text: "This should be steered before shutdown.",
      expectsReply: true,
    });
    assert.equal(delivered.delivered, true);
    await new Promise((resolve) => setTimeout(resolve, 20));
    assert.equal(harness.sentMessages.length, 1);
    assert.equal(harness.sentMessages[0]?.options?.deliverAs, "steer");

    await harness.emitLifecycle("session_shutdown");
    idle = true;
    await harness.emitLifecycle("agent_end");
    await new Promise((resolve) => setTimeout(resolve, 20));

    assert.equal(harness.sentMessages.length, 1);
    assert.deepEqual(receipts, ["receiver_received", "acknowledged", "injected"]);
    unsubscribeReceipts();
  } finally {
    await cleanup();
  }
});

test("busy non-interactive sessions ignore plain sends without emitting a reply-shaped notice", { concurrency: false }, async () => {
  const { default: piIntercomExtension } = await import("./index.ts");
  const { planner, cleanup } = await setupClients();
  const harness = createExtensionHarness("plain-pipe-worker", {
    hasUI: false,
    isIdle: () => false,
  });

  try {
    piIntercomExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    const target = await waitForSessionByName(planner, "plain-pipe-worker");
    const messages: Message[] = [];
    const onMessage = (_from: SessionInfo, message: Message) => messages.push(message);
    planner.on("message", onMessage);

    assert.equal((await planner.send(target.id, {
      messageId: "plain-pipe-send",
      text: "FYI while busy",
    })).delivered, true);
    await new Promise((resolve) => setTimeout(resolve, 100));

    planner.off("message", onMessage);
    assert.deepEqual(messages, []);
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("busy non-interactive sessions auto-reply to top-level asks without aborting", { concurrency: false }, async () => {
  const { default: piIntercomExtension } = await import("./index.ts");
  const { planner, cleanup } = await setupClients();
  let abortCount = 0;
  const harness = createExtensionHarness("pipe-worker", {
    abort: () => { abortCount += 1; },
    hasUI: false,
    isIdle: () => false,
  });

  try {
    piIntercomExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");

    const target = await waitForSessionByName(planner, "pipe-worker");

    const askId = "pipe-mode-ask";
    const replyPromise = waitForReply(planner, askId, 1000);
    const delivered = await planner.send(target.id, {
      messageId: askId,
      text: "Can you respond while busy?",
      expectsReply: true,
    });
    assert.equal(delivered.delivered, true);

    const reply = await replyPromise;
    assert.equal(reply.message.replyTo, askId);
    assert.match(reply.message.content.text, /non-interactive|cannot respond/i);
    assert.equal(abortCount, 0);

  } finally {
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("supervisor tool registers only when child metadata is present", async () => {
  const { default: piIntercomExtension } = await import("./index.ts");

  await withChildOrchestratorEnv({}, () => {
    const harness = createExtensionHarness();
    piIntercomExtension(harness.pi as never);
    assert.deepEqual(harness.tools.map((tool) => tool.name), ["intercom"]);
  });

  await withChildOrchestratorEnv({
    orchestratorTarget: "orchestrator",
    runId: "78f659a3",
    agent: "worker",
    index: "0",
    sessionName: "subagent-worker-78f659a3-1",
  }, () => {
    const harness = createExtensionHarness();
    piIntercomExtension(harness.pi as never);
    assert.deepEqual(harness.tools.map((tool) => tool.name), ["contact_supervisor", "intercom"]);
    const supervisorTool = harness.tools.find((tool) => tool.name === "contact_supervisor");
    assert.match(JSON.stringify(supervisorTool?.parameters), /interview_request/);
    assert.match(JSON.stringify(supervisorTool?.parameters), /questions/);
  });

  await withChildOrchestratorEnv({
    orchestratorTarget: "orchestrator",
    runId: "78f659a3",
    agent: "worker",
    index: "0",
    supervisorChannelDir: path.join(sharedHomeDir, "native-supervisor-channel"),
  }, () => {
    const harness = createExtensionHarness();
    piIntercomExtension(harness.pi as never);
    assert.deepEqual(harness.tools.map((tool) => tool.name), ["intercom"]);
  });
});

test("child supervisor tool resolves target and includes run metadata", { concurrency: false }, async () => {
  const { default: piIntercomExtension } = await import("./index.ts");
  const { orchestrator, cleanup } = await setupClients();

  try {
    await withChildOrchestratorEnv({
      orchestratorTarget: "orchestrator",
      runId: "78f659a3",
      agent: "worker",
      index: "0",
      sessionName: "subagent-worker-78f659a3-1",
    }, async () => {
      const harness = createExtensionHarness("subagent-worker-78f659a3-1");
      piIntercomExtension(harness.pi as never);
      await harness.emitLifecycle("session_start");

      const supervisorTool = harness.tools.find((tool) => tool.name === "contact_supervisor")!;

      const askReceived = once(orchestrator, "message") as Promise<[SessionInfo, Message]>;
      const askResultPromise = supervisorTool.execute("ask-1", { reason: "need_decision", message: "Which API should I use?" }, new AbortController().signal, undefined, harness.ctx);
      const [askFrom, askMessage] = await askReceived;
      assert.equal(askMessage.expectsReply, true);
      assert.match(askMessage.content.text, /Subagent needs a supervisor decision/);
      assert.match(askMessage.content.text, /Run: 78f659a3/);
      assert.match(askMessage.content.text, /Agent: worker/);
      assert.match(askMessage.content.text, /Child index: 0/);
      assert.match(askMessage.content.text, /Which API should I use\?/);

      const reply = await orchestrator.send(askFrom.id, { text: "Use the stable API.", replyTo: askMessage.id });
      assert.equal(reply.delivered, true);
      const askResult = await askResultPromise;
      assert.notEqual(askResult.details?.error, true);
      assert.match(askResult.content[0]?.text ?? "", /Use the stable API/);

      const updateReceived = once(orchestrator, "message") as Promise<[SessionInfo, Message]>;
      const updateResult = await supervisorTool.execute("update-1", { reason: "progress_update", message: "Found a schema mismatch." }, new AbortController().signal, undefined, harness.ctx);
      const [_updateFrom, updateMessage] = await updateReceived;
      assert.equal(updateMessage.expectsReply, undefined);
      assert.match(updateMessage.content.text, /Subagent progress update/);
      assert.match(updateMessage.content.text, /Run: 78f659a3/);
      assert.match(updateMessage.content.text, /Agent: worker/);
      assert.match(updateMessage.content.text, /Found a schema mismatch/);
      assert.notEqual(updateResult.details?.error, true);

      const interviewReceived = once(orchestrator, "message") as Promise<[SessionInfo, Message]>;
      const interview = {
        title: "API migration choices",
        description: "Choose the implementation path before edits continue.",
        questions: [
          { id: "context", type: "info", question: "Migration context", context: "Use the existing auth boundary." },
          { id: "api", type: "single", question: "Which API should I target?", options: [" Stable API ", "Experimental API"] },
          { id: "notes", type: "text", question: "Any constraints to preserve?" },
        ],
      };
      const interviewResultPromise = supervisorTool.execute("interview-1", {
        reason: "interview_request",
        message: "Please answer both so I can continue safely.",
        interview,
      }, new AbortController().signal, undefined, harness.ctx);
      const [interviewFrom, interviewMessage] = await interviewReceived;
      assert.equal(interviewMessage.expectsReply, true);
      assert.match(interviewMessage.content.text, /Subagent requests a structured supervisor interview/);
      assert.match(interviewMessage.content.text, /Interview: API migration choices/);
      assert.match(interviewMessage.content.text, /\[context\] \(info\) Migration context/);
      assert.match(interviewMessage.content.text, /Info questions are context-only/);
      assert.match(interviewMessage.content.text, /\[api\] \(single\) Which API should I target\?/);
      assert.match(interviewMessage.content.text, / {3}- Stable API/);
      assert.match(interviewMessage.content.text, /\[notes\] \(text\) Any constraints to preserve\?/);
      assert.match(interviewMessage.content.text, /"responses"/);
      assert.doesNotMatch(interviewMessage.content.text, /"id": "context"/);

      const structuredReply = {
        responses: [
          { id: "api", value: "Stable API" },
          { id: "notes", value: "Keep the public error shape unchanged." },
        ],
      };
      const interviewReply = await orchestrator.send(interviewFrom.id, {
        text: `\`\`\`json\n${JSON.stringify(structuredReply, null, 2)}\n\`\`\``,
        replyTo: interviewMessage.id,
      });
      assert.equal(interviewReply.delivered, true);
      const interviewResult = await interviewResultPromise;
      assert.notEqual(interviewResult.details?.error, true);
      assert.match(interviewResult.content[0]?.text ?? "", /Stable API/);
      assert.deepEqual(interviewResult.details?.structuredReply, structuredReply);

      const invalidReplyReceived = once(orchestrator, "message") as Promise<[SessionInfo, Message]>;
      const invalidReplyResultPromise = supervisorTool.execute("interview-invalid-reply", {
        reason: "interview_request",
        interview,
      }, new AbortController().signal, undefined, harness.ctx);
      const [invalidReplyFrom, invalidReplyMessage] = await invalidReplyReceived;
      const invalidReply = await orchestrator.send(invalidReplyFrom.id, {
        text: '{"responses":[{"id":"api","value":"Removed API"}]}',
        replyTo: invalidReplyMessage.id,
      });
      assert.equal(invalidReply.delivered, true);
      const invalidReplyResult = await invalidReplyResultPromise;
      assert.notEqual(invalidReplyResult.details?.error, true);
      assert.equal(invalidReplyResult.details?.structuredReply, undefined);
      assert.match(String(invalidReplyResult.details?.structuredReplyParseError), /must match one of the question options/);

      await harness.emitLifecycle("session_shutdown");
    });
  } finally {
    await cleanup();
  }
});

test("child supervisor tool uses stable supervisor ID when names are duplicated", { concurrency: false }, async () => {
  const { orchestrator, cleanup } = await setupClients();
  const duplicate = new IntercomClient();

  try {
    await duplicate.connect({
      name: "orchestrator",
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    }, "duplicate-orchestrator-id");

    await withChildOrchestratorEnv({
      orchestratorTarget: "orchestrator",
      orchestratorSessionId: orchestrator.sessionId!,
      runId: "78f659a3",
      agent: "worker",
      index: "0",
    }, async () => {
      const { default: piIntercomExtension } = await import("./index.ts");
      const harness = createExtensionHarness("duplicate-name-child");
      piIntercomExtension(harness.pi as never);
      await harness.emitLifecycle("session_start");
      const supervisorTool = harness.tools.find((tool) => tool.name === "contact_supervisor")!;

      const received = once(orchestrator, "message") as Promise<[SessionInfo, Message]>;
      const result = await supervisorTool.execute("update-duplicate", { reason: "progress_update", message: "Stable ID route." }, new AbortController().signal, undefined, harness.ctx);
      const [, message] = await received;
      assert.notEqual(result.details?.error, true);
      assert.match(message.content.text, /Stable ID route/);
      await harness.emitLifecycle("session_shutdown");
    });
  } finally {
    await duplicate.disconnect().catch(() => undefined);
    await cleanup();
  }
});

test("child supervisor tool rejects invalid reasons and interview payloads", async () => {
  const { default: piIntercomExtension } = await import("./index.ts");

  await withChildOrchestratorEnv({
    orchestratorTarget: "orchestrator",
    runId: "78f659a3",
    agent: "worker",
    index: "0",
  }, async () => {
    const harness = createExtensionHarness();
    piIntercomExtension(harness.pi as never);
    const supervisorTool = harness.tools.find((tool) => tool.name === "contact_supervisor")!;
    const result = await supervisorTool.execute("invalid-1", { reason: "done", message: "Finished." }, new AbortController().signal, undefined, harness.ctx);
    assert.equal(result.details?.error, true);
    assert.match(result.content[0]?.text ?? "", /Invalid reason/);

    const missingMessageResult = await supervisorTool.execute("invalid-message", { reason: "need_decision" }, new AbortController().signal, undefined, harness.ctx);
    assert.equal(missingMessageResult.details?.error, true);
    assert.match(missingMessageResult.content[0]?.text ?? "", /Missing 'message'/);

    const invalidInterviewResult = await supervisorTool.execute("invalid-interview", { reason: "interview_request", interview: { title: "Bad" } }, new AbortController().signal, undefined, harness.ctx);
    assert.equal(invalidInterviewResult.details?.error, true);
    assert.match(invalidInterviewResult.content[0]?.text ?? "", /interview\.questions must be a non-empty array/);

    const invalidInfoOptionsResult = await supervisorTool.execute("invalid-info-options", {
      reason: "interview_request",
      interview: {
        questions: [{ id: "context", type: "info", question: "Context", options: ["Not an answer"] }],
      },
    }, new AbortController().signal, undefined, harness.ctx);
    assert.equal(invalidInfoOptionsResult.details?.error, true);
    assert.match(invalidInfoOptionsResult.content[0]?.text ?? "", /options is only valid for single and multi questions/);
  });
});

test("child supervisor blocking requests fail fast when the supervisor is disconnected", { concurrency: false }, async () => {
  const { default: piIntercomExtension } = await import("./index.ts");
  const { cleanup } = await setupClients();

  try {
    await withChildOrchestratorEnv({
      orchestratorTarget: "missing-orchestrator",
      runId: "78f659a3",
      agent: "worker",
      index: "0",
    }, async () => {
      const harness = createExtensionHarness();
      piIntercomExtension(harness.pi as never);
      await harness.emitLifecycle("session_start");
      const supervisorTool = harness.tools.find((tool) => tool.name === "contact_supervisor")!;
      const updateResult = await supervisorTool.execute("update-1", { reason: "progress_update", message: "Blocked." }, new AbortController().signal, undefined, harness.ctx);
      assert.equal(updateResult.details?.delivered, false);
      assert.match(updateResult.content[0]?.text ?? "", /Session not found/);
      assert.equal(updateResult.details?.reason, "Session not found");

      const askResult = await supervisorTool.execute("ask-1", { reason: "need_decision", message: "Which path?" }, new AbortController().signal, undefined, harness.ctx);
      assert.equal(askResult.details?.error, true);
      assert.match(askResult.content[0]?.text ?? "", /not currently connected/);

      const secondAskResult = await supervisorTool.execute("ask-2", { reason: "need_decision", message: "Still blocked." }, new AbortController().signal, undefined, harness.ctx);
      assert.equal(secondAskResult.details?.error, true);
      assert.match(secondAskResult.content[0]?.text ?? "", /not currently connected/);
      assert.doesNotMatch(secondAskResult.content[0]?.text ?? "", /Already waiting/);
      await harness.emitLifecycle("session_shutdown");
    });
  } finally {
    await cleanup();
  }
});

test("regular intercom asks fail safely when started concurrently", { concurrency: false }, async () => {
  const { default: piIntercomExtension } = await import("./index.ts");
  const { orchestrator, cleanup } = await setupClients();

  try {
    const harness = createExtensionHarness("regular-ask-worker");
    piIntercomExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    await waitForSessionByName(orchestrator, "regular-ask-worker");
    const intercomTool = harness.tools.find((tool) => tool.name === "intercom")!;

    const firstMessage = once(orchestrator, "message") as Promise<[SessionInfo, Message]>;
    const firstAsk = intercomTool.execute("ask-1", { action: "ask", to: "orchestrator", message: "First?" }, new AbortController().signal, undefined, harness.ctx);
    const secondAsk = intercomTool.execute("ask-2", { action: "ask", to: "orchestrator", message: "Second?" }, new AbortController().signal, undefined, harness.ctx);
    const [from, askMessage] = await firstMessage;
    assert.equal(askMessage.expectsReply, true);

    const earlyResults = await Promise.race([
      Promise.all([firstAsk, secondAsk]),
      new Promise<null>((resolve) => setTimeout(() => resolve(null), 100)),
    ]);
    assert.equal(earlyResults, null);

    const pendingResult = await Promise.race([firstAsk, secondAsk]);
    assert.equal(pendingResult.details?.error, true);
    assert.match(pendingResult.content[0]?.text ?? "", /Already waiting/);

    const reply = await orchestrator.send(from.id, { text: "First answer.", replyTo: askMessage.id });
    assert.equal(reply.delivered, true);

    const results = await Promise.all([firstAsk, secondAsk]);
    assert.equal(results.filter((result) => result.details?.error === true).length, 1);
    assert.equal(results.filter((result) => /First answer/.test(result.content[0]?.text ?? "")).length, 1);
    await harness.emitLifecycle("session_shutdown");
  } finally {
    await cleanup();
  }
});

test("broker refuses reverse mutual asks until the original ask is answered", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();

  try {
    const askToOrchestrator = await planner.send(orchestrator.sessionId!, {
      messageId: "planner-to-orchestrator",
      text: "Can you decide?",
      expectsReply: true,
    });
    assert.equal(askToOrchestrator.delivered, true);

    const reverseAsk = await orchestrator.send(planner.sessionId!, {
      messageId: "orchestrator-to-planner",
      text: "Can you decide instead?",
      expectsReply: true,
    });
    assert.equal(reverseAsk.delivered, false);
    assert.match(reverseAsk.reason ?? "", /Mutual ask refused/);

    const plainSend = await orchestrator.send(planner.sessionId!, { text: "Plain update still works." });
    assert.equal(plainSend.delivered, true);

    const reply = await orchestrator.send(planner.sessionId!, {
      text: "Answered.",
      replyTo: "planner-to-orchestrator",
    });
    assert.equal(reply.delivered, true);

    const nextAsk = await orchestrator.send(planner.sessionId!, {
      messageId: "orchestrator-to-planner-after-reply",
      text: "Now can I ask?",
      expectsReply: true,
    });
    assert.equal(nextAsk.delivered, true);
  } finally {
    await cleanup();
  }
});

test("a reply can start a new reverse ask", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();

  try {
    const askToOrchestrator = await planner.send(orchestrator.sessionId!, {
      messageId: "planner-to-orchestrator-transition",
      text: "Can you decide?",
      expectsReply: true,
    });
    assert.equal(askToOrchestrator.delivered, true);

    const replyAndAsk = await orchestrator.send(planner.sessionId!, {
      messageId: "orchestrator-reply-and-ask",
      text: "I answered; can you decide the next thing?",
      replyTo: "planner-to-orchestrator-transition",
      expectsReply: true,
    });
    assert.equal(replyAndAsk.delivered, true);

    const duplicateReverseAsk = await orchestrator.send(planner.sessionId!, {
      messageId: "orchestrator-duplicate-reverse-ask",
      text: "Can I ask another before the first is answered?",
      expectsReply: true,
    });
    assert.equal(duplicateReverseAsk.delivered, true);

    const plannerReverseAsk = await planner.send(orchestrator.sessionId!, {
      messageId: "planner-reverse-while-orchestrator-waits",
      text: "Can I ask while you wait?",
      expectsReply: true,
    });
    assert.equal(plannerReverseAsk.delivered, false);
    assert.match(plannerReverseAsk.reason ?? "", /Mutual ask refused/);
  } finally {
    await cleanup();
  }
});

test("failed replies do not clear broker mutual-ask edges", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();

  try {
    const askToOrchestrator = await planner.send(orchestrator.sessionId!, {
      messageId: "planner-to-orchestrator-missing-reply",
      text: "Can you decide?",
      expectsReply: true,
    });
    assert.equal(askToOrchestrator.delivered, true);

    const missingReply = await orchestrator.send("missing-session", {
      messageId: "reply-to-missing-session",
      text: "Answered, maybe?",
      replyTo: "planner-to-orchestrator-missing-reply",
    });
    assert.equal(missingReply.delivered, false);
    assert.match(missingReply.reason ?? "", /Session not found/);

    const reverseAsk = await orchestrator.send(planner.sessionId!, {
      messageId: "reverse-after-missing-reply",
      text: "Can I ask now?",
      expectsReply: true,
    });
    assert.equal(reverseAsk.delivered, false);
    assert.match(reverseAsk.reason ?? "", /Mutual ask refused/);

    const deliveredReply = await orchestrator.send(planner.sessionId!, {
      messageId: "reply-to-planner",
      text: "Actually answered.",
      replyTo: "planner-to-orchestrator-missing-reply",
    });
    assert.equal(deliveredReply.delivered, true);

    const nextAsk = await orchestrator.send(planner.sessionId!, {
      messageId: "reverse-after-delivered-reply",
      text: "Now can I ask?",
      expectsReply: true,
    });
    assert.equal(nextAsk.delivered, true);
  } finally {
    await cleanup();
  }
});

test("regular intercom ask timeout reports message id and delivery state", { concurrency: false }, async () => {
  const previousTimeout = process.env.PI_INTERCOM_ASK_TIMEOUT_MS;
  process.env.PI_INTERCOM_ASK_TIMEOUT_MS = "50";
  const { default: piIntercomExtension } = await import("./index.ts");
  const { planner, cleanup } = await setupClients();
  const senderHarness = createExtensionHarness("timeout-worker", { sessionId: "session-timeout-worker" });
  const receiverHarness = createExtensionHarness("timeout-target", { sessionId: "session-timeout-target", hasUI: true });

  try {
    piIntercomExtension(senderHarness.pi as never);
    piIntercomExtension(receiverHarness.pi as never);
    await senderHarness.emitLifecycle("session_start");
    await receiverHarness.emitLifecycle("session_start");
    await waitForSessionByName(planner, "timeout-target");
    const intercomTool = senderHarness.tools.find((tool) => tool.name === "intercom")!;

    const result = await intercomTool.execute("ask-timeout", { action: "ask", to: "timeout-target", message: "Will this time out?" }, new AbortController().signal, undefined, senderHarness.ctx);

    assert.equal(result.details?.error, true);
    assert.equal(result.details?.deliveryState, "injected");
    assert.match(result.content[0]?.text ?? "", new RegExp(String(result.details?.messageId)));
    assert.match(result.content[0]?.text ?? "", /Last known delivery state: injected/);
    assert.match(result.content[0]?.text ?? "", /not cancellation/);
    assert.equal(receiverHarness.sentMessages.length, 1);
  } finally {
    if (previousTimeout === undefined) {
      delete process.env.PI_INTERCOM_ASK_TIMEOUT_MS;
    } else {
      process.env.PI_INTERCOM_ASK_TIMEOUT_MS = previousTimeout;
    }
    await senderHarness.emitLifecycle("session_shutdown");
    await receiverHarness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("extension applies cancelled, superseded, and timed-out stale-reply tiers", { concurrency: false }, async () => {
  const previousTimeout = process.env.PI_INTERCOM_ASK_TIMEOUT_MS;
  process.env.PI_INTERCOM_ASK_TIMEOUT_MS = "50";
  const { default: piIntercomExtension } = await import("./index.ts");
  const { planner, cleanup } = await setupClients();
  const harness = createExtensionHarness("stale-tier-worker", { sessionId: "session-stale-tier-worker", hasUI: true });
  const originalOn = EventEmitter.prototype.on;
  let inboundMessageHandler: ((from: SessionInfo, message: Message) => void) | undefined;
  EventEmitter.prototype.on = function (eventName: string | symbol, listener: (...args: any[]) => void) {
    if (eventName === "message") inboundMessageHandler = listener;
    return originalOn.call(this, eventName, listener);
  };

  try {
    piIntercomExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    await waitForSessionByName(planner, "stale-tier-worker");
    EventEmitter.prototype.on = originalOn;
    const plannerSession = await waitForSessionByName(planner, "planner");
    const intercomTool = harness.tools.find((tool) => tool.name === "intercom")!;
    assert.ok(inboundMessageHandler);
    const emitLateReply = (id: string, replyTo: string, text: string) => {
      inboundMessageHandler!(plannerSession, {
        id,
        timestamp: Date.now(),
        replyTo,
        content: { text },
      });
    };

    const cancelledController = new AbortController();
    const cancelledMessage = once(planner, "message") as Promise<[SessionInfo, Message]>;
    const cancelledAsk = intercomTool.execute("stale-cancelled", {
      action: "ask",
      to: "planner",
      message: "This ask will be cancelled.",
    }, cancelledController.signal, undefined, harness.ctx);
    const [, cancelledQuestion] = await cancelledMessage;
    cancelledController.abort();
    assert.equal((await cancelledAsk).details?.error, true);
    emitLateReply("late-cancelled-reply", cancelledQuestion.id, "Too late after cancellation.");
    await new Promise((resolve) => setTimeout(resolve, 20));
    assert.equal(harness.sentMessages.length, 0, "cancelled late reply must be dropped");

    const timedOutMessage = once(planner, "message") as Promise<[SessionInfo, Message]>;
    const timedOutAsk = intercomTool.execute("stale-timed-out", {
      action: "ask",
      to: "planner",
      message: "This ask will time out.",
    }, new AbortController().signal, undefined, harness.ctx);
    const [, timedOutQuestion] = await timedOutMessage;
    assert.equal((await timedOutAsk).details?.error, true);
    emitLateReply("late-timeout-reply", timedOutQuestion.id, "Visible after timeout.");
    await new Promise((resolve) => setTimeout(resolve, 20));
    assert.equal(harness.sentMessages.length, 1);
    assert.match(harness.sentMessages[0]?.message.content ?? "", /Late reply to abandoned ask/);
    assert.match(harness.sentMessages[0]?.message.content ?? "", /Visible after timeout/);

    const supersededMessage = once(planner, "message") as Promise<[SessionInfo, Message]>;
    const supersededAsk = intercomTool.execute("stale-before-supersede", {
      action: "ask",
      to: "planner",
      message: "This ask will be replaced.",
    }, new AbortController().signal, undefined, harness.ctx);
    const [, supersededQuestion] = await supersededMessage;
    assert.equal((await supersededAsk).details?.error, true);

    const replacementMessage = once(planner, "message") as Promise<[SessionInfo, Message]>;
    const replacementAsk = intercomTool.execute("stale-superseding", {
      action: "ask",
      to: "planner",
      message: "This replaces the prior ask.",
      supersedes: supersededQuestion.id,
    }, new AbortController().signal, undefined, harness.ctx);
    const [, replacementQuestion] = await replacementMessage;
    emitLateReply("late-superseded-reply", supersededQuestion.id, "Too late after supersession.");
    await new Promise((resolve) => setTimeout(resolve, 20));
    assert.equal(harness.sentMessages.length, 1, "superseded late reply must be dropped");

    emitLateReply("replacement-reply", replacementQuestion.id, "Current answer.");
    const replacementResult = await replacementAsk;
    assert.notEqual(replacementResult.details?.error, true);
    assert.match(replacementResult.content[0]?.text ?? "", /Current answer/);
  } finally {
    EventEmitter.prototype.on = originalOn;
    if (previousTimeout === undefined) {
      delete process.env.PI_INTERCOM_ASK_TIMEOUT_MS;
    } else {
      process.env.PI_INTERCOM_ASK_TIMEOUT_MS = previousTimeout;
    }
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("regular intercom ask cancellation clears broker mutual-ask edge", { concurrency: false }, async () => {
  const { default: piIntercomExtension } = await import("./index.ts");
  const { orchestrator, cleanup } = await setupClients();

  try {
    const harness = createExtensionHarness("cancel-cleanup-worker");
    piIntercomExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    const worker = await waitForSessionByName(orchestrator, "cancel-cleanup-worker");
    const intercomTool = harness.tools.find((tool) => tool.name === "intercom")!;

    const controller = new AbortController();
    const cancelledMessage = once(orchestrator, "message") as Promise<[SessionInfo, Message]>;
    const cancelledResultPromise = intercomTool.execute("ask-cancelled", { action: "ask", to: "orchestrator", message: "Should I continue?" }, controller.signal, undefined, harness.ctx);
    await cancelledMessage;
    controller.abort();
    const cancelledResult = await cancelledResultPromise;
    assert.equal(cancelledResult.details?.error, true);
    assert.match(cancelledResult.content[0]?.text ?? "", /Cancelled/);

    const reverseAsk = await orchestrator.send(worker.id, {
      messageId: "reverse-after-cancel",
      text: "Can I ask after your cancellation?",
      expectsReply: true,
    });
    assert.equal(reverseAsk.delivered, true);
    await harness.emitLifecycle("session_shutdown");
  } finally {
    await cleanup();
  }
});

test("child supervisor tool clears reply waiter when cancelled", { concurrency: false }, async () => {
  const { default: piIntercomExtension } = await import("./index.ts");
  const { orchestrator, cleanup } = await setupClients();

  try {
    await withChildOrchestratorEnv({
      orchestratorTarget: "orchestrator",
      runId: "78f659a3",
      agent: "worker",
      index: "0",
      sessionName: "subagent-worker-78f659a3-1",
    }, async () => {
      const harness = createExtensionHarness("subagent-worker-78f659a3-1");
      piIntercomExtension(harness.pi as never);
      await harness.emitLifecycle("session_start");
      const supervisorTool = harness.tools.find((tool) => tool.name === "contact_supervisor")!;

      const controller = new AbortController();
      const cancelledMessage = once(orchestrator, "message") as Promise<[SessionInfo, Message]>;
      const cancelledResultPromise = supervisorTool.execute("ask-cancelled", { reason: "need_decision", message: "Should I continue?" }, controller.signal, undefined, harness.ctx);
      await cancelledMessage;
      controller.abort();
      const cancelledResult = await cancelledResultPromise;
      assert.equal(cancelledResult.details?.error, true);
      assert.match(cancelledResult.content[0]?.text ?? "", /Cancelled/);

      const nextMessage = once(orchestrator, "message") as Promise<[SessionInfo, Message]>;
      const nextResultPromise = supervisorTool.execute("ask-next", { reason: "need_decision", message: "Can I ask again?" }, new AbortController().signal, undefined, harness.ctx);
      const [from, message] = await nextMessage;
      assert.match(message.content.text, /Can I ask again/);
      const reply = await orchestrator.send(from.id, { text: "Yes.", replyTo: message.id });
      assert.equal(reply.delivered, true);
      const nextResult = await nextResultPromise;
      assert.notEqual(nextResult.details?.error, true);
      assert.match(nextResult.content[0]?.text ?? "", /Yes\./);
      await harness.emitLifecycle("session_shutdown");
    });
  } finally {
    await cleanup();
  }
});

test("full ask/reply round-trip works with reply target resolved from current turn context", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  const replyTracker = new ReplyTracker();

  try {
    const askId = "ask-current-turn";
    const askPromise = once(orchestrator, "message") as Promise<[SessionInfo, Message]>;
    const replyPromise = waitForReply(planner, askId);

    const delivered = await planner.send(orchestrator.sessionId!, {
      messageId: askId,
      text: "What should I do next?",
      expectsReply: true,
    });
    assert.equal(delivered.delivered, true);

    const [from, message] = await askPromise;
    const context = replyTracker.recordIncomingMessage(from, message, Date.now());
    replyTracker.queueTurnContext(context);
    replyTracker.beginTurn(Date.now());

    const target = replyTracker.resolveReplyTarget({}, Date.now());
    const sent = await orchestrator.send(target.from.id, {
      text: "Ship it.",
      replyTo: target.message.id,
    });
    assert.equal(sent.delivered, true);
    replyTracker.markReplied(target.message.id);

    const reply = await replyPromise;
    assert.equal(reply.message.content.text, "Ship it.");
    assert.equal(reply.message.replyTo, askId);
    assert.deepEqual(replyTracker.listPending(Date.now()), []);
  } finally {
    await cleanup();
  }
});

test("intercom reply targets exact replyTo when multiple asks are pending", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  const { default: piIntercomExtension } = await import("./index.ts");
  const harness = createExtensionHarness("reply-target-worker");

  try {
    piIntercomExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    const worker = await waitForSessionByName(planner, "reply-target-worker");

    assert.equal((await planner.send(worker.id, { messageId: "reply-target-1", text: "First?", expectsReply: true })).delivered, true);
    assert.equal((await orchestrator.send(worker.id, { messageId: "reply-target-2", text: "Second?", expectsReply: true })).delivered, true);
    await new Promise((resolve) => setTimeout(resolve, 50));

    const intercomTool = harness.tools.find((tool) => tool.name === "intercom")!;
    const replyReceived = waitForReply(orchestrator, "reply-target-2");
    const result = await intercomTool.execute("reply-exact", {
      action: "reply",
      message: "Second answer.",
      replyTo: "reply-target-2",
    }, new AbortController().signal, undefined, harness.ctx);
    assert.notEqual(result.details?.error, true);
    const reply = await replyReceived;
    assert.equal(reply.message.content.text, "Second answer.");

    const pending = await intercomTool.execute("pending-after-exact", { action: "pending" }, new AbortController().signal, undefined, harness.ctx);
    assert.match(pending.content[0]?.text ?? "", /reply-target-1/);
    assert.doesNotMatch(pending.content[0]?.text ?? "", /reply-target-2/);
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("intercom reply sends attachments", { concurrency: false }, async () => {
  const { planner, cleanup } = await setupClients();
  const { default: piIntercomExtension } = await import("./index.ts");
  const harness = createExtensionHarness("reply-attachment-worker");

  try {
    piIntercomExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    const worker = await waitForSessionByName(planner, "reply-attachment-worker");

    const askId = "reply-attachment-ask";
    assert.equal((await planner.send(worker.id, { messageId: askId, text: "Send details?", expectsReply: true })).delivered, true);
    await new Promise((resolve) => setTimeout(resolve, 50));

    const intercomTool = harness.tools.find((tool) => tool.name === "intercom")!;
    const attachments = [{ type: "snippet" as const, name: "details.md", content: "attached details", language: "md" }];
    const replyReceived = waitForReply(planner, askId);
    const result = await intercomTool.execute("reply-with-attachment", {
      action: "reply",
      message: "Here are details.",
      attachments,
    }, new AbortController().signal, undefined, harness.ctx);

    assert.equal(result.details?.delivered, true);
    const reply = await replyReceived;
    assert.equal(reply.message.content.text, "Here are details.");
    assert.deepEqual(reply.message.content.attachments, attachments);

    const sentEntry = harness.entries.find((entry) => entry.type === "intercom_sent");
    assert.deepEqual((sentEntry?.data as { message?: { attachments?: unknown } }).message?.attachments, attachments);
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("intercom reply targets one of multiple pending asks by short session ID", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  const { default: piIntercomExtension } = await import("./index.ts");
  const harness = createExtensionHarness("reply-short-id-worker");

  try {
    piIntercomExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    const worker = await waitForSessionByName(planner, "reply-short-id-worker");

    assert.equal((await planner.send(worker.id, { messageId: "reply-short-id-1", text: "First?", expectsReply: true })).delivered, true);
    assert.equal((await orchestrator.send(worker.id, { messageId: "reply-short-id-2", text: "Second?", expectsReply: true })).delivered, true);
    await new Promise((resolve) => setTimeout(resolve, 50));

    const intercomTool = harness.tools.find((tool) => tool.name === "intercom")!;
    const replyReceived = waitForReply(planner, "reply-short-id-1");
    const result = await intercomTool.execute("reply-short-id", {
      action: "reply",
      to: planner.sessionId!.slice(0, 8),
      message: "First answer.",
    }, new AbortController().signal, undefined, harness.ctx);
    assert.equal(result.details?.delivered, true);
    assert.equal((await replyReceived).message.content.text, "First answer.");

    const pending = await intercomTool.execute("pending-after-short-id", { action: "pending" }, new AbortController().signal, undefined, harness.ctx);
    assert.doesNotMatch(pending.content[0]?.text ?? "", /reply-short-id-1/);
    assert.match(pending.content[0]?.text ?? "", /reply-short-id-2/);
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("a short-ID reply unblocks the original ask when another ask is pending", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  const { default: piIntercomExtension } = await import("./index.ts");
  const askerHarness = createExtensionHarness("short-id-asker", { sessionId: "asker123-session" });
  const replierHarness = createExtensionHarness("short-id-replier", { sessionId: "replier-session" });

  try {
    piIntercomExtension(askerHarness.pi as never);
    piIntercomExtension(replierHarness.pi as never);
    await askerHarness.emitLifecycle("session_start");
    await replierHarness.emitLifecycle("session_start");
    const asker = await waitForSessionByName(planner, "short-id-asker");
    const replier = await waitForSessionByName(planner, "short-id-replier");
    const askerTool = askerHarness.tools.find((tool) => tool.name === "intercom")!;
    const replierTool = replierHarness.tools.find((tool) => tool.name === "intercom")!;

    const originalAsk = askerTool.execute("ask-for-work", {
      action: "ask",
      to: replier.id,
      message: "Is any work pending?",
    }, new AbortController().signal, undefined, askerHarness.ctx);
    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.equal((await orchestrator.send(replier.id, {
      messageId: "another-pending-ask",
      text: "A separate pending question",
      expectsReply: true,
    })).delivered, true);
    await new Promise((resolve) => setTimeout(resolve, 50));

    const reply = await replierTool.execute("reply-to-work-ask", {
      action: "reply",
      to: asker.id.slice(0, 8),
      message: "No work is pending.",
    }, new AbortController().signal, undefined, replierHarness.ctx);
    assert.equal(reply.details?.delivered, true, reply.content.map((part) => part.text).join("\n"));

    const result = await originalAsk;
    assert.doesNotMatch(result.content[0]?.text ?? "", /No reply from/);
    assert.match(result.content[0]?.text ?? "", /No work is pending/);

    const pending = await replierTool.execute("remaining-pending", { action: "pending" }, new AbortController().signal, undefined, replierHarness.ctx);
    assert.match(pending.content[0]?.text ?? "", /another-pending-ask/);
  } finally {
    await askerHarness.emitLifecycle("session_shutdown");
    await replierHarness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("broker queues replies to recently disconnected named senders", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  const replacement = new IntercomClient();

  try {
    const originalPlannerId = planner.sessionId!;
    const receivedAsk = once(orchestrator, "message") as Promise<[SessionInfo, Message]>;
    assert.equal((await planner.send(orchestrator.sessionId!, { messageId: "ephemeral-cli-ask", text: "Can you answer later?", expectsReply: true })).delivered, true);
    await receivedAsk;
    await planner.disconnect();

    const queuedReply = once(replacement, "message") as Promise<[SessionInfo, Message]>;
    const reply = await orchestrator.send(originalPlannerId, {
      messageId: "queued-reply-to-ephemeral",
      text: "Queued answer.",
      replyTo: "ephemeral-cli-ask",
    });
    assert.equal(reply.delivered, true);

    await replacement.connect({
      name: "planner",
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    });
    const [from, message] = await queuedReply;
    assert.equal(from.id, orchestrator.sessionId);
    assert.equal(message.id, "queued-reply-to-ephemeral");
    assert.equal(message.replyTo, "ephemeral-cli-ask");
    assert.equal(message.content.text, "Queued answer.");
    assert.equal(typeof message.brokerReceivedAt, "number");
    assert.equal(typeof message.brokerDeliveredAt, "number");
  } finally {
    await replacement.disconnect().catch(() => undefined);
    await cleanup();
  }
});

test("broker reports queued and cancelled mailbox receipts without closing the sender socket", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();

  try {
    const disconnectedId = planner.sessionId!;
    await planner.disconnect();
    const receipts: Array<{ from: string; status: string }> = [];
    const unsubscribeReceipts = orchestrator.onMessageReceipt((from, receipt) => {
      if (receipt.messageId === "queued-then-cancelled") {
        receipts.push({ from: from.name ?? from.id, status: receipt.status });
      }
    });

    assert.equal((await orchestrator.send(disconnectedId, {
      messageId: "queued-then-cancelled",
      text: "Cancel this while it is still queued.",
    })).delivered, true);
    assert.equal((await orchestrator.cancelMessage("queued-then-cancelled")).delivered, true);
    assert.deepEqual(receipts, [
      { from: "pi-intercom-broker", status: "queued" },
      { from: "pi-intercom-broker", status: "cancelled" },
    ]);

    const sessions = await orchestrator.listSessions();
    assert.ok(sessions.some((session) => session.id === orchestrator.sessionId));
    unsubscribeReceipts();
  } finally {
    await cleanup();
  }
});

test("broker wire-observes an expired receipt when mailbox capacity evicts a message", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();

  try {
    const disconnectedId = planner.sessionId!;
    await planner.disconnect();
    const observed: Array<{ from: SessionInfo; status: string; timestamp: number }> = [];
    const unsubscribeReceipts = orchestrator.onMessageReceipt((from, receipt) => {
      if (receipt.messageId === "mailbox-capacity-0") {
        observed.push({ from, status: receipt.status, timestamp: receipt.timestamp });
      }
    });

    for (let index = 0; index < 256; index += 1) {
      assert.equal((await orchestrator.send(disconnectedId, {
        messageId: `mailbox-capacity-${index}`,
        text: `Queued mailbox message ${index}`,
      })).delivered, true);
      if (index === 199) {
        // The broker permits a 240-message burst and refills 120 tokens/s.
        await new Promise((resolve) => setTimeout(resolve, 500));
      }
    }
    assert.equal((await orchestrator.send(disconnectedId, {
      messageId: "mailbox-capacity-overflow",
      text: "Evict the oldest queued mailbox message.",
    })).delivered, true);

    assert.deepEqual(observed.map(({ status }) => status), ["queued", "expired"]);
    const expired = observed[1]!;
    assert.deepEqual(Object.keys(expired.from).sort(), [
      "cwd",
      "id",
      "lastActivity",
      "model",
      "name",
      "pid",
      "startedAt",
      "status",
      "trustedLocal",
    ]);
    assert.equal(expired.from.id, BROKER_SESSION_ID);
    assert.equal(expired.from.name, "pi-intercom-broker");
    assert.equal(expired.from.cwd, "");
    assert.equal(expired.from.model, "broker");
    assert.equal(expired.from.status, "broker");
    assert.equal(expired.from.lastActivity, expired.timestamp);
    assert.equal(expired.from.trustedLocal, process.platform !== "win32");
    assert.equal(orchestrator.isConnected(), true);
    unsubscribeReceipts();
  } finally {
    await cleanup();
  }
});

test("broker rejects the reserved broker session ID on the registration wire", { concurrency: false }, async () => {
  const net = await import("node:net");
  const { getBrokerSocketPath } = await import("./broker/paths.ts");
  const { createMessageReader, writeMessage } = await import("./broker/framing.ts");
  const { orchestrator, cleanup } = await setupClients();
  const socket = net.connect(getBrokerSocketPath());
  const received: unknown[] = [];

  try {
    await once(socket, "connect");
    const closed = once(socket, "close");
    socket.on("error", () => undefined);
    socket.on("data", createMessageReader((message) => received.push(message), (error) => socket.destroy(error)));
    writeMessage(socket, {
      type: "register",
      sessionId: BROKER_SESSION_ID,
      session: {
        name: "reserved-id-collision",
        cwd: repoDir,
        model: "test-model",
        pid: process.pid,
        startedAt: Date.now(),
        lastActivity: Date.now(),
      },
    });

    await closed;
    assert.deepEqual(received, [], "reserved identity must not receive a registered frame");
    assert.equal(socket.destroyed, true);
    const sessions = await orchestrator.listSessions();
    assert.equal(sessions.some((session) => session.id === BROKER_SESSION_ID), false);
  } finally {
    socket.destroy();
    await cleanup();
  }
});

test("broker rejects blocking asks to disconnected targets", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();

  try {
    const disconnectedId = planner.sessionId!;
    await planner.disconnect();
    const result = await orchestrator.send(disconnectedId, {
      messageId: "offline-broker-ask",
      text: "Do not queue this blocking request.",
      expectsReply: true,
    });
    assert.equal(result.delivered, false);
    assert.match(result.reason ?? "", /not currently connected/);
    assert.match(result.reason ?? "", /not queued/);
  } finally {
    await cleanup();
  }
});

test("broker never remaps a disconnected mailbox back to the sending session", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  const sender = new IntercomClient();
  const replacement = new IntercomClient();

  try {
    const disconnectedId = planner.sessionId!;
    await planner.disconnect();
    await sender.connect({
      name: "planner",
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    });
    const senderId = sender.sessionId!;

    const selfDeliveries: Message[] = [];
    sender.on("message", (_from: SessionInfo, message: Message) => selfDeliveries.push(message));
    const result = await sender.send(disconnectedId, {
      messageId: "no-self-mailbox-remap",
      text: "Queue this for the disconnected session.",
    });
    assert.equal(result.delivered, true);
    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.deepEqual(selfDeliveries, []);

    await sender.disconnect();
    await sender.connect({
      name: "planner",
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    }, senderId);
    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.deepEqual(selfDeliveries, []);

    const queuedMessage = once(replacement, "message") as Promise<[SessionInfo, Message]>;
    await replacement.connect({
      name: "planner",
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    }, disconnectedId);
    const [, message] = await queuedMessage;
    assert.equal(message.id, "no-self-mailbox-remap");
  } finally {
    await sender.disconnect().catch(() => undefined);
    await replacement.disconnect().catch(() => undefined);
    await cleanup();
  }
});

test("broker does not treat runtime fallback aliases as reconnect identities", { concurrency: false }, async () => {
  const { orchestrator, cleanup } = await setupClients();
  const original = new IntercomClient();
  const unrelated = new IntercomClient();
  const replacement = new IntercomClient();
  const fallbackAlias = "subagent-chat-019fe418-248e-7447";
  const originalId = "runtime-fallback-original";

  try {
    await original.connect({
      name: fallbackAlias,
      runtimeFallbackAlias: true,
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    }, originalId);
    await original.disconnect();

    const unrelatedDeliveries: Message[] = [];
    unrelated.on("message", (_from: SessionInfo, message: Message) => unrelatedDeliveries.push(message));
    await unrelated.connect({
      name: fallbackAlias,
      runtimeFallbackAlias: true,
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    });

    assert.equal((await orchestrator.send(originalId, {
      messageId: "fallback-alias-mail",
      text: "Only the original session should receive this.",
    })).delivered, true);
    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.deepEqual(unrelatedDeliveries, []);

    const queuedMessage = once(replacement, "message") as Promise<[SessionInfo, Message]>;
    await replacement.connect({
      name: fallbackAlias,
      runtimeFallbackAlias: true,
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    }, originalId);
    const [, message] = await queuedMessage;
    assert.equal(message.id, "fallback-alias-mail");
  } finally {
    await original.disconnect().catch(() => undefined);
    await unrelated.disconnect().catch(() => undefined);
    await replacement.disconnect().catch(() => undefined);
    await cleanup();
  }
});

test("broker does not deliver explicit mailbox mail to a matching fallback alias", { concurrency: false }, async () => {
  const { orchestrator, cleanup } = await setupClients();
  const original = new IntercomClient();
  const fallback = new IntercomClient();
  const replacement = new IntercomClient();
  const sharedName = "subagent-chat-shared-worker";
  const originalId = "explicit-mailbox-original";

  try {
    await original.connect({
      name: sharedName,
      runtimeFallbackAlias: false,
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    }, originalId);
    await original.disconnect();

    const fallbackDeliveries: Message[] = [];
    fallback.on("message", (_from: SessionInfo, message: Message) => fallbackDeliveries.push(message));
    await fallback.connect({
      name: sharedName,
      runtimeFallbackAlias: true,
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    });

    assert.equal((await orchestrator.send(originalId, {
      messageId: "explicit-mailbox-not-fallback",
      text: "Keep this message for the explicit identity.",
    })).delivered, true);
    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.deepEqual(fallbackDeliveries, []);

    const queuedMessage = once(replacement, "message") as Promise<[SessionInfo, Message]>;
    await replacement.connect({
      name: sharedName,
      runtimeFallbackAlias: false,
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    }, originalId);
    const [, message] = await queuedMessage;
    assert.equal(message.id, "explicit-mailbox-not-fallback");
  } finally {
    await original.disconnect().catch(() => undefined);
    await fallback.disconnect().catch(() => undefined);
    await replacement.disconnect().catch(() => undefined);
    await cleanup();
  }
});

test("broker preserves mailbox reconnects for explicit subagent-chat names", { concurrency: false }, async () => {
  const { orchestrator, cleanup } = await setupClients();
  const original = new IntercomClient();
  const replacement = new IntercomClient();
  const explicitName = "subagent-chat-explicit-worker";
  const originalId = "explicit-subagent-chat-original";

  try {
    await original.connect({
      name: explicitName,
      runtimeFallbackAlias: false,
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    }, originalId);
    await original.disconnect();

    const delivered = once(replacement, "message") as Promise<[SessionInfo, Message]>;
    await replacement.connect({
      name: explicitName,
      runtimeFallbackAlias: false,
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    });
    assert.equal((await orchestrator.send(originalId, {
      messageId: "explicit-subagent-chat-mail",
      text: "Explicit names keep mailbox reconnect semantics.",
    })).delivered, true);
    const [, message] = await delivered;
    assert.equal(message.id, "explicit-subagent-chat-mail");
  } finally {
    await original.disconnect().catch(() => undefined);
    await replacement.disconnect().catch(() => undefined);
    await cleanup();
  }
});

test("broker delivers old-id replies to an already reconnected same-name sender", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  const replacement = new IntercomClient();

  try {
    const originalPlannerId = planner.sessionId!;
    const receivedAsk = once(orchestrator, "message") as Promise<[SessionInfo, Message]>;
    assert.equal((await planner.send(orchestrator.sessionId!, { messageId: "reconnected-cli-ask", text: "Can you answer after reconnect?", expectsReply: true })).delivered, true);
    await receivedAsk;
    await planner.disconnect();

    await replacement.connect({
      name: "planner",
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    });
    const deliveredReply = once(replacement, "message") as Promise<[SessionInfo, Message]>;
    const reply = await orchestrator.send(originalPlannerId, {
      messageId: "reply-after-cli-reconnect",
      text: "Immediate answer after reconnect.",
      replyTo: "reconnected-cli-ask",
    });
    assert.equal(reply.delivered, true);
    const [, message] = await deliveredReply;
    assert.equal(message.id, "reply-after-cli-reconnect");
    assert.equal(message.replyTo, "reconnected-cli-ask");
    assert.equal(message.content.text, "Immediate answer after reconnect.");
  } finally {
    await replacement.disconnect().catch(() => undefined);
    await cleanup();
  }
});

test("broker keeps queued mail away from a same-name session in another cwd", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  const otherProject = new IntercomClient();

  try {
    const originalPlannerId = planner.sessionId!;
    const receivedAsk = once(orchestrator, "message") as Promise<[SessionInfo, Message]>;
    assert.equal((await planner.send(orchestrator.sessionId!, { messageId: "cross-cwd-ask", text: "Answer later?", expectsReply: true })).delivered, true);
    await receivedAsk;
    await planner.disconnect();

    assert.equal((await orchestrator.send(originalPlannerId, {
      messageId: "cross-cwd-answer",
      text: "Answer for the original project.",
      replyTo: "cross-cwd-ask",
    })).delivered, true);

    const received: Message[] = [];
    otherProject.on("message", (_from: SessionInfo, message: Message) => received.push(message));
    await otherProject.connect({
      name: "planner",
      cwd: path.join(repoDir, "other-project"),
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    });
    await new Promise((resolve) => setTimeout(resolve, 200));

    assert.deepEqual(received, []);
  } finally {
    await otherProject.disconnect().catch(() => undefined);
    await cleanup();
  }
});

test("broker delivers queued mail to a relaunch reporting the same cwd differently", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  const replacement = new IntercomClient();

  try {
    const originalPlannerId = planner.sessionId!;
    const receivedAsk = once(orchestrator, "message") as Promise<[SessionInfo, Message]>;
    assert.equal((await planner.send(orchestrator.sessionId!, { messageId: "cwd-variant-ask", text: "Answer later?", expectsReply: true })).delivered, true);
    await receivedAsk;
    await planner.disconnect();

    assert.equal((await orchestrator.send(originalPlannerId, {
      messageId: "cwd-variant-answer",
      text: "Answer for the same project.",
      replyTo: "cwd-variant-ask",
    })).delivered, true);

    const queuedReply = once(replacement, "message") as Promise<[SessionInfo, Message]>;
    await replacement.connect({
      name: "planner",
      // Same directory as setupClients(), spelled with a ".." segment and a
      // trailing separator. Built by concatenation because path.join would
      // collapse the ".." before the broker ever sees it.
      cwd: `${repoDir}${path.sep}ui${path.sep}..${path.sep}`,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    });

    const [, message] = await queuedReply;
    assert.equal(message.id, "cwd-variant-answer");
    assert.equal(message.content.text, "Answer for the same project.");
  } finally {
    await replacement.disconnect().catch(() => undefined);
    await cleanup();
  }
});

test("broker does not reroute an id-addressed message to a same-name session in another cwd", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  const otherProject = new IntercomClient();

  try {
    const originalPlannerId = planner.sessionId!;
    const receivedAsk = once(orchestrator, "message") as Promise<[SessionInfo, Message]>;
    assert.equal((await planner.send(orchestrator.sessionId!, { messageId: "cross-cwd-live-ask", text: "Answer later?", expectsReply: true })).delivered, true);
    await receivedAsk;
    await planner.disconnect();

    const received: Message[] = [];
    otherProject.on("message", (_from: SessionInfo, message: Message) => received.push(message));
    await otherProject.connect({
      name: "planner",
      cwd: path.join(repoDir, "other-project"),
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    });

    assert.equal((await orchestrator.send(originalPlannerId, {
      messageId: "cross-cwd-live-answer",
      text: "Answer for the original project.",
      replyTo: "cross-cwd-live-ask",
    })).delivered, true);
    await new Promise((resolve) => setTimeout(resolve, 200));

    assert.deepEqual(received, []);
  } finally {
    await otherProject.disconnect().catch(() => undefined);
    await cleanup();
  }
});

test("intercom reply queues mail for a disconnected named sender", { concurrency: false }, async () => {
  const { planner, cleanup } = await setupClients();
  const { default: piIntercomExtension } = await import("./index.ts");
  const harness = createExtensionHarness("stale-reply-worker");
  const replacement = new IntercomClient();

  try {
    piIntercomExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    const worker = await waitForSessionByName(planner, "stale-reply-worker");
    assert.equal((await planner.send(worker.id, { messageId: "stale-reply-ask", text: "Still there?", expectsReply: true })).delivered, true);
    await new Promise((resolve) => setTimeout(resolve, 50));
    await planner.disconnect();

    const intercomTool = harness.tools.find((tool) => tool.name === "intercom")!;
    const result = await intercomTool.execute("reply-stale", {
      action: "reply",
      message: "No sender remains.",
      replyTo: "stale-reply-ask",
    }, new AbortController().signal, undefined, harness.ctx);
    assert.equal(result.details?.delivered, true);

    const pending = await intercomTool.execute("pending-after-stale", { action: "pending" }, new AbortController().signal, undefined, harness.ctx);
    assert.match(pending.content[0]?.text ?? "", /No unresolved inbound asks/);

    const queuedReply = once(replacement, "message") as Promise<[SessionInfo, Message]>;
    await replacement.connect({
      name: "planner",
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    });
    const [, message] = await queuedReply;
    assert.equal(message.replyTo, "stale-reply-ask");
    assert.equal(message.content.text, "No sender remains.");
  } finally {
    await replacement.disconnect().catch(() => undefined);
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("subagent control intercom events wake the current orchestrator session", async () => {
  const { default: piIntercomExtension } = await import("./index.ts");
  const harness = createExtensionHarness("orchestrator");

  piIntercomExtension(harness.pi as never);
  await harness.emitLifecycle("session_start");
  harness.pi.events.emit("subagent:control-intercom", {
    to: "orchestrator",
    message: "subagent needs attention\n\nworker needs attention in run 78f659a3.",
  });
  await new Promise((resolve) => setImmediate(resolve));

  assert.equal(harness.sentMessages.length, 1);
  assert.equal(harness.sentMessages[0]?.message.customType, "intercom_message");
  assert.match(harness.sentMessages[0]?.message.content ?? "", /From subagent-control/);
  assert.match(harness.sentMessages[0]?.message.content ?? "", /worker needs attention in run 78f659a3/);
  assert.equal(harness.sentMessages[0]?.options?.triggerTurn, true);
  await harness.emitLifecycle("session_shutdown");
});

test("subagent result intercom events wake the current orchestrator session", async () => {
  const { default: piIntercomExtension } = await import("./index.ts");
  const harness = createExtensionHarness("orchestrator");
  const deliveryAcks: unknown[] = [];
  harness.pi.events.on("subagent:result-intercom-delivery", (payload) => deliveryAcks.push(payload));

  piIntercomExtension(harness.pi as never);
  await harness.emitLifecycle("session_start");
  harness.pi.events.emit("subagent:result-intercom", {
    to: "orchestrator",
    requestId: "result-1",
    message: "subagent result\n\nRun: 78f659a3\nAgent: worker\nStatus: completed",
  });
  await new Promise((resolve) => setImmediate(resolve));

  assert.equal(harness.sentMessages.length, 1);
  assert.equal(harness.sentMessages[0]?.message.customType, "intercom_message");
  assert.match(harness.sentMessages[0]?.message.content ?? "", /From subagent-result/);
  assert.match(harness.sentMessages[0]?.message.content ?? "", /Status: completed/);
  assert.equal(harness.sentMessages[0]?.options?.triggerTurn, true);
  assert.deepEqual(deliveryAcks, [{ requestId: "result-1", delivered: true }]);
  await harness.emitLifecycle("session_shutdown");
});

test("subagent result relay reports a negative acknowledgement before an inactive runtime dispatch", async () => {
  const { default: piIntercomExtension } = await import("./index.ts");
  const harness = createExtensionHarness("orchestrator");
  const deliveryAcks: unknown[] = [];
  harness.pi.events.on("subagent:result-intercom-delivery", (payload) => deliveryAcks.push(payload));

  piIntercomExtension(harness.pi as never);
  harness.pi.events.emit("subagent:result-intercom", {
    to: "orchestrator",
    requestId: "inactive-result",
    message: "must not dispatch",
  });
  await new Promise((resolve) => setImmediate(resolve));

  assert.equal(harness.sentMessages.length, 0);
  assert.deepEqual(deliveryAcks, [{
    requestId: "inactive-result",
    delivered: false,
    error: "Intercom runtime is not active",
  }]);
});

test("async ask can be replied to later from the single pending ask fallback", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  const replyTracker = new ReplyTracker();

  try {
    const askId = "ask-later";
    const askPromise = once(orchestrator, "message") as Promise<[SessionInfo, Message]>;
    const replyPromise = waitForReply(planner, askId);

    const delivered = await planner.send(orchestrator.sessionId!, {
      messageId: askId,
      text: "Need an answer later.",
      expectsReply: true,
    });
    assert.equal(delivered.delivered, true);

    const [from, message] = await askPromise;
    replyTracker.recordIncomingMessage(from, message, Date.now());

    const target = replyTracker.resolveReplyTarget({}, Date.now());
    const sent = await orchestrator.send(target.from.id, {
      text: "Answering later worked.",
      replyTo: target.message.id,
    });
    assert.equal(sent.delivered, true);
    replyTracker.markReplied(target.message.id);

    const reply = await replyPromise;
    assert.equal(reply.message.content.text, "Answering later worked.");
    assert.equal(reply.message.replyTo, askId);
  } finally {
    await cleanup();
  }
});

test("presence carries context usage to peers, and an explicit null clears a stale value", { concurrency: false }, async () => {
  // Peers should see each other's live context-window usage without a separate
  // query, and a post-compaction null must CLEAR the value rather than leave a
  // stale-high percentage frozen in the list.
  const { planner, orchestrator, cleanup } = await setupClients();
  try {
    planner.updatePresence({ contextPct: 50, contextTokens: 100000, contextWindow: 200000 });
    // Flush barrier: a round-trip on planner's OWN socket guarantees the broker
    // processed the presence (FIFO per socket) before the peer probes.
    await planner.send(orchestrator.sessionId!, { text: "flush" });
    let sessions = await orchestrator.listSessions();
    let p = sessions.find(s => s.id === planner.sessionId);
    assert.equal(p?.contextPct, 50);
    assert.equal(p?.contextTokens, 100000);
    assert.equal(p?.contextWindow, 200000);

    // Post-compaction: null contextPct/tokens must CLEAR (not freeze the old %).
    planner.updatePresence({ contextPct: null, contextTokens: null });
    await planner.send(orchestrator.sessionId!, { text: "flush" });
    sessions = await orchestrator.listSessions();
    p = sessions.find(s => s.id === planner.sessionId);
    assert.equal(p?.contextPct, undefined, "null contextPct must CLEAR the field, not freeze the old value");
    assert.equal(p?.contextTokens, undefined);
    // contextWindow (the denominator, not nulled here) is retained.
    assert.equal(p?.contextWindow, 200000);
  } finally {
    await cleanup();
  }
});

test("public send infers a reply from the sole pending ask, resolves the waiter, and dismisses it", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  const { default: piIntercomExtension } = await import("./index.ts");
  const harness = createExtensionHarness("infer-send-worker");

  try {
    piIntercomExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    const worker = await waitForSessionByName(orchestrator, "infer-send-worker");

    const askId = "infer-ask-1";
    assert.equal((await planner.send(worker.id, { messageId: askId, text: "What's next?", expectsReply: true })).delivered, true);
    await new Promise((resolve) => setTimeout(resolve, 50));

    const intercomTool = harness.tools.find((tool) => tool.name === "intercom")!;
    const replyReceived = waitForReply(planner, askId);
    const result = await intercomTool.execute("infer-send", {
      action: "send",
      to: "planner",
      message: "Ship it.",
    }, new AbortController().signal, undefined, harness.ctx);

    assert.notEqual(result.details?.error, true);
    assert.equal(result.details?.delivered, true);
    assert.equal(result.details?.replyTo, askId);
    assert.match(result.content[0]?.text ?? "", /Reply sent to planner \(inferred from pending ask\)/);

    const reply = await replyReceived;
    assert.equal(reply.message.content.text, "Ship it.");
    assert.equal(reply.message.replyTo, askId);

    const sentEntry = harness.entries.find((entry) => entry.type === "intercom_sent");
    assert.equal((sentEntry?.data as { message: { replyTo?: string } }).message.replyTo, askId);

    const pending = await intercomTool.execute("pending-after-infer", { action: "pending" }, new AbortController().signal, undefined, harness.ctx);
    assert.equal(pending.content[0]?.text, "No unresolved inbound asks.");
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("an intentional unrelated public notification during a sole pending ask is still inferred as its reply", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  const { default: piIntercomExtension } = await import("./index.ts");
  const harness = createExtensionHarness("notify-during-ask-worker");

  try {
    piIntercomExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    const worker = await waitForSessionByName(orchestrator, "notify-during-ask-worker");

    const askId = "notify-ask-1";
    assert.equal((await planner.send(worker.id, { messageId: askId, text: "Can you take this task?", expectsReply: true })).delivered, true);
    await new Promise((resolve) => setTimeout(resolve, 50));

    const intercomTool = harness.tools.find((tool) => tool.name === "intercom")!;
    const replyReceived = waitForReply(planner, askId);
    // Unrelated to the ask's content — proves the accepted trade-off: any public
    // send to the sole pending asker is reclassified as its answer.
    const result = await intercomTool.execute("notify-during-ask", {
      action: "send",
      to: "planner",
      message: "Deploying build 42 now.",
    }, new AbortController().signal, undefined, harness.ctx);

    assert.equal(result.details?.replyTo, askId);
    assert.match(result.content[0]?.text ?? "", /inferred from pending ask/);
    const reply = await replyReceived;
    assert.equal(reply.message.content.text, "Deploying build 42 now.");
    assert.equal(reply.message.replyTo, askId);
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("confirmSend still gates an inferred reply; cancellation preserves the pending ask", { concurrency: false }, async () => {
  await withConfirmSendEnabled(async () => {
    const { planner, orchestrator, cleanup } = await setupClients();
    const { default: piIntercomExtension } = await import("./index.ts");
    const confirmCalls: Array<[string, string]> = [];
    const harness = createExtensionHarness("confirm-infer-worker", {
      hasUI: true,
      ui: {
        confirm: async (title: string, text: string) => {
          confirmCalls.push([title, text]);
          return false;
        },
      },
    });

    try {
      piIntercomExtension(harness.pi as never);
      await harness.emitLifecycle("session_start");
      const worker = await waitForSessionByName(orchestrator, "confirm-infer-worker");

      const askId = "confirm-infer-ask-1";
      assert.equal((await planner.send(worker.id, { messageId: askId, text: "Ready?", expectsReply: true })).delivered, true);
      await new Promise((resolve) => setTimeout(resolve, 50));

      const intercomTool = harness.tools.find((tool) => tool.name === "intercom")!;
      const result = await intercomTool.execute("confirm-infer", {
        action: "send",
        to: "planner",
        message: "Yes, ready.",
      }, new AbortController().signal, undefined, harness.ctx);

      assert.equal(confirmCalls.length, 1);
      assert.equal(result.content[0]?.text, "Message cancelled by user");
      assert.equal(result.details?.delivered, undefined);

      const pending = await intercomTool.execute("pending-after-cancel", { action: "pending" }, new AbortController().signal, undefined, harness.ctx);
      assert.match(pending.content[0]?.text ?? "", /confirm-infer-ask-1/);
    } finally {
      await harness.emitLifecycle("session_shutdown");
      await cleanup();
    }
  });
});

test("contact_supervisor progress_update stays unthreaded despite a pending ask; a later inferred send resolves it", { concurrency: false }, async () => {
  const { orchestrator, cleanup } = await setupClients();

  try {
    await withChildOrchestratorEnv({
      orchestratorTarget: "orchestrator",
      runId: "aa11bb22",
      agent: "worker",
      index: "0",
      sessionName: "subagent-worker-aa11bb22-1",
    }, async () => {
      const { default: piIntercomExtension } = await import("./index.ts");
      const harness = createExtensionHarness("subagent-worker-aa11bb22-1");
      piIntercomExtension(harness.pi as never);
      await harness.emitLifecycle("session_start");
      const worker = await waitForSessionByName(orchestrator, "subagent-worker-aa11bb22-1");

      const askId = "boundary-ask-1";
      assert.equal((await orchestrator.send(worker.id, { messageId: askId, text: "Any blockers?", expectsReply: true })).delivered, true);
      await new Promise((resolve) => setTimeout(resolve, 50));

      const supervisorTool = harness.tools.find((tool) => tool.name === "contact_supervisor")!;
      const updateReceived = once(orchestrator, "message") as Promise<[SessionInfo, Message]>;
      const updateResult = await supervisorTool.execute("update-1", { reason: "progress_update", message: "Still working." }, new AbortController().signal, undefined, harness.ctx);
      const [, updateMessage] = await updateReceived;
      assert.notEqual(updateResult.details?.error, true);
      assert.equal(updateMessage.replyTo, undefined);

      const intercomTool = harness.tools.find((tool) => tool.name === "intercom")!;
      const pendingAfterUpdate = await intercomTool.execute("pending-after-update", { action: "pending" }, new AbortController().signal, undefined, harness.ctx);
      assert.match(pendingAfterUpdate.content[0]?.text ?? "", /boundary-ask-1/);

      const replyReceived = waitForReply(orchestrator, askId);
      const sendResult = await intercomTool.execute("infer-after-update", { action: "send", to: "orchestrator", message: "No blockers." }, new AbortController().signal, undefined, harness.ctx);
      assert.equal(sendResult.details?.replyTo, askId);
      const reply = await replyReceived;
      assert.equal(reply.message.replyTo, askId);

      await harness.emitLifecycle("session_shutdown");
    });
  } finally {
    await cleanup();
  }
});

test("caller-supplied replyTo takes precedence over inference and skips the inferred result text", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  const { default: piIntercomExtension } = await import("./index.ts");
  const harness = createExtensionHarness("precedence-worker");

  try {
    piIntercomExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    const worker = await waitForSessionByName(orchestrator, "precedence-worker");

    const inferableAskId = "precedence-inferable-ask";
    assert.equal((await planner.send(worker.id, { messageId: inferableAskId, text: "Sole pending ask", expectsReply: true })).delivered, true);
    // A second, distinct pending ask that the caller explicitly targets below —
    // it must be a real ask (broker rejects a replyTo that isn't a pending ask;
    // see "broker rejects unknown replyTo values..."), and it proves the explicit
    // choice isn't just whatever inference would have picked.
    const explicitReplyTo = "precedence-explicit-target";
    assert.equal((await planner.send(worker.id, { messageId: explicitReplyTo, text: "Explicit target ask", expectsReply: true })).delivered, true);
    await new Promise((resolve) => setTimeout(resolve, 50));

    const intercomTool = harness.tools.find((tool) => tool.name === "intercom")!;
    const replyReceived = waitForReply(planner, explicitReplyTo);
    const result = await intercomTool.execute("precedence-send", {
      action: "send",
      to: "planner",
      message: "Explicit wins.",
      replyTo: explicitReplyTo,
    }, new AbortController().signal, undefined, harness.ctx);

    assert.equal(result.details?.replyTo, explicitReplyTo);
    assert.equal(result.content[0]?.text, "Message sent to planner");
    const reply = await replyReceived;
    assert.equal(reply.message.replyTo, explicitReplyTo);

    const pending = await intercomTool.execute("pending-after-precedence", { action: "pending" }, new AbortController().signal, undefined, harness.ctx);
    assert.match(pending.content[0]?.text ?? "", /precedence-inferable-ask/);
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("multiple pending asks from the same sender leave a send unthreaded and preserve every ask", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  const { default: piIntercomExtension } = await import("./index.ts");
  const harness = createExtensionHarness("ambiguous-worker");

  try {
    piIntercomExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    const worker = await waitForSessionByName(orchestrator, "ambiguous-worker");

    assert.equal((await planner.send(worker.id, { messageId: "ambiguous-ask-1", text: "First?", expectsReply: true })).delivered, true);
    assert.equal((await planner.send(worker.id, { messageId: "ambiguous-ask-2", text: "Second?", expectsReply: true })).delivered, true);
    await new Promise((resolve) => setTimeout(resolve, 50));

    const intercomTool = harness.tools.find((tool) => tool.name === "intercom")!;
    const result = await intercomTool.execute("ambiguous-send", {
      action: "send",
      to: "planner",
      message: "Unthreaded update.",
    }, new AbortController().signal, undefined, harness.ctx);

    assert.equal(result.details?.replyTo, undefined);
    assert.equal(result.content[0]?.text, "Message sent to planner");

    const pending = await intercomTool.execute("pending-after-ambiguous", { action: "pending" }, new AbortController().signal, undefined, harness.ctx);
    assert.match(pending.content[0]?.text ?? "", /ambiguous-ask-1/);
    assert.match(pending.content[0]?.text ?? "", /ambiguous-ask-2/);
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("an expired pending ask is not inferred; the send remains ordinary", { concurrency: false }, async () => {
  await withAskTimeoutMs(50, async () => {
    const { planner, orchestrator, cleanup } = await setupClients();
    const { default: piIntercomExtension } = await import("./index.ts");
    const harness = createExtensionHarness("expiry-worker");

    try {
      piIntercomExtension(harness.pi as never);
      await harness.emitLifecycle("session_start");
      const worker = await waitForSessionByName(orchestrator, "expiry-worker");

      assert.equal((await planner.send(worker.id, { messageId: "expiry-ask-1", text: "Answer soon?", expectsReply: true })).delivered, true);
      await new Promise((resolve) => setTimeout(resolve, 150));

      const intercomTool = harness.tools.find((tool) => tool.name === "intercom")!;
      const result = await intercomTool.execute("expiry-send", {
        action: "send",
        to: "planner",
        message: "Unrelated now.",
      }, new AbortController().signal, undefined, harness.ctx);

      assert.equal(result.details?.replyTo, undefined);
      assert.equal(result.content[0]?.text, "Message sent to planner");
    } finally {
      await harness.emitLifecycle("session_shutdown");
      await cleanup();
    }
  });
});

test("intercom ask fails fast when the target is not currently connected", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  const { default: piIntercomExtension } = await import("./index.ts");
  const harness = createExtensionHarness("offline-ask-worker");

  try {
    piIntercomExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    await waitForSessionByName(orchestrator, "offline-ask-worker");
    const disconnectedId = planner.sessionId!;
    await planner.disconnect();

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 250);
    const intercomTool = harness.tools.find((tool) => tool.name === "intercom")!;
    const result = await intercomTool.execute("ask-offline", {
      action: "ask",
      to: disconnectedId,
      message: "This must not wait in the disconnected mailbox.",
    }, controller.signal, undefined, harness.ctx);
    clearTimeout(timeout);

    assert.match(result.content[0]?.text ?? "", /not currently connected/i);
    assert.equal(result.details?.error, true);
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("send falls back to an exact stored ID or name for a disconnected asker but never guesses from an ID prefix", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  const { default: piIntercomExtension } = await import("./index.ts");
  const harness = createExtensionHarness("disconnected-asker-worker");

  try {
    piIntercomExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    const worker = await waitForSessionByName(orchestrator, "disconnected-asker-worker");

    const askId = "disconnected-ask-1";
    const originalPlannerId = planner.sessionId!;
    assert.equal((await planner.send(worker.id, { messageId: askId, text: "Any concerns before I disconnect?", expectsReply: true })).delivered, true);
    await new Promise((resolve) => setTimeout(resolve, 50));
    await planner.disconnect();

    const intercomTool = harness.tools.find((tool) => tool.name === "intercom")!;

    const prefixResult = await intercomTool.execute("send-prefix", {
      action: "send",
      to: originalPlannerId.slice(0, 8),
      message: "Should not infer from a bare prefix.",
    }, new AbortController().signal, undefined, harness.ctx);
    assert.doesNotMatch(prefixResult.content[0]?.text ?? "", /inferred from pending ask/);
    assert.equal(prefixResult.details?.replyTo, undefined);

    const pendingAfterPrefix = await intercomTool.execute("pending-after-prefix", { action: "pending" }, new AbortController().signal, undefined, harness.ctx);
    assert.match(pendingAfterPrefix.content[0]?.text ?? "", /disconnected-ask-1/);

    const replacement = new IntercomClient();
    const queuedReply = waitForReply(replacement, askId);

    const exactResult = await intercomTool.execute("send-exact-id", {
      action: "send",
      to: originalPlannerId,
      message: "Reconnect and see this.",
    }, new AbortController().signal, undefined, harness.ctx);
    assert.match(exactResult.content[0]?.text ?? "", /inferred from pending ask/);
    assert.equal(exactResult.details?.replyTo, askId);

    await replacement.connect({ name: "planner", cwd: repoDir, model: "test-model", pid: process.pid, startedAt: Date.now(), lastActivity: Date.now() });
    const queuedMessage = await queuedReply;
    assert.equal(queuedMessage.message.replyTo, askId);

    const pendingAfterExact = await intercomTool.execute("pending-after-exact-id", { action: "pending" }, new AbortController().signal, undefined, harness.ctx);
    assert.equal(pendingAfterExact.content[0]?.text, "No unresolved inbound asks.");

    await replacement.disconnect().catch(() => undefined);
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("failed delivery from an inferred reply preserves the pending ask", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  const impostor = new IntercomClient();
  const { default: piIntercomExtension } = await import("./index.ts");
  const harness = createExtensionHarness("delivery-failure-worker");

  try {
    piIntercomExtension(harness.pi as never);
    await harness.emitLifecycle("session_start");
    const worker = await waitForSessionByName(orchestrator, "delivery-failure-worker");

    const askId = "delivery-failure-ask-1";
    assert.equal((await planner.send(worker.id, { messageId: askId, text: "Ping before disconnect", expectsReply: true })).delivered, true);
    await new Promise((resolve) => setTimeout(resolve, 50));
    await planner.disconnect();

    await impostor.connect({ name: "planner", cwd: repoDir, model: "test-model", pid: process.pid, startedAt: Date.now(), lastActivity: Date.now() });
    await impostor.disconnect();

    const intercomTool = harness.tools.find((tool) => tool.name === "intercom")!;
    const result = await intercomTool.execute("send-ambiguous-disconnected", {
      action: "send",
      to: "planner",
      message: "Should not deliver.",
    }, new AbortController().signal, undefined, harness.ctx);

    assert.equal(result.details?.delivered, false);
    assert.match(result.content[0]?.text ?? "", /Multiple disconnected sessions named/);

    const pending = await intercomTool.execute("pending-after-failure", { action: "pending" }, new AbortController().signal, undefined, harness.ctx);
    assert.match(pending.content[0]?.text ?? "", /delivery-failure-ask-1/);
  } finally {
    await harness.emitLifecycle("session_shutdown");
    await cleanup();
  }
});

test("broker refuses a duplicate pending ask ID without displacing the original edge", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  try {
    const target = await waitForSessionByName(planner, "orchestrator");
    const askId = "duplicate-pending-ask-id";

    const first = await planner.send(target.id, { messageId: askId, text: "first ask", expectsReply: true });
    assert.equal(first.delivered, true);

    const second = await planner.send(target.id, { messageId: askId, text: "second ask", expectsReply: true });
    assert.equal(second.delivered, false);
    assert.match(second.reason ?? "", /Duplicate pending ask message ID/);

    // The original edge must survive: replying to it still resolves against a live ask.
    const reply = await orchestrator.send(
      (await waitForSessionByName(orchestrator, "planner")).id,
      { text: "answer", replyTo: askId },
    );
    assert.equal(reply.delivered, true, "original ask edge must still be pending after the refusal");
  } finally {
    await cleanup();
  }
});

test("broker caps concurrent pending asks per session and keeps other sessions unaffected", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  const other = await connectRawRegistered("ask-cap-other-id", "ask-cap-other");
  try {
    const target = await waitForSessionByName(planner, "orchestrator");
    for (let i = 0; i < 16; i += 1) {
      const result = await planner.send(target.id, { messageId: `cap-ask-${i}`, text: `ask ${i}`, expectsReply: true });
      assert.equal(result.delivered, true, `ask ${i + 1} of 16 should be accepted`);
    }

    const refused = await planner.send(target.id, { messageId: "cap-ask-overflow", text: "one too many", expectsReply: true });
    assert.equal(refused.delivered, false);
    assert.match(refused.reason ?? "", /from this session/);

    // The cap is per asker: an unrelated session is still allowed to ask.
    const { createMessageReader } = await import("./broker/framing.ts");
    const otherAsk = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error("no delivered/delivery_failed for other session")), 3000);
      const reader = createMessageReader((msg) => {
        const m = msg as { type?: string; messageId?: string };
        if (m.messageId !== "other-session-ask") return;
        clearTimeout(timeout);
        other.socket.off("data", reader);
        if (m.type === "delivered") resolve();
        else reject(new Error(`other session was refused: ${JSON.stringify(msg)}`));
      }, reject);
      other.socket.on("data", reader);
    });
    other.writeMessage(other.socket, {
      type: "send",
      to: target.id,
      message: { id: "other-session-ask", timestamp: Date.now(), expectsReply: true, content: { text: "from other" } },
    });
    await otherAsk;
  } finally {
    other.socket.destroy();
    await cleanup();
  }
});

test("broker refuses replacing a peer-owned ask when the sender is at its ask cap", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  const sink = new IntercomClient();
  try {
    await sink.connect({
      name: "ask-cap-sink",
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    });

    const peerAskId = "peer-owned-cap-ask";
    assert.equal((await orchestrator.send(planner.sessionId!, {
      messageId: peerAskId,
      text: "Answer and ask me something?",
      expectsReply: true,
    })).delivered, true);

    for (let i = 0; i < MAX_PENDING_ASK_EDGES_PER_SESSION; i += 1) {
      const result = await planner.send(sink.sessionId!, {
        messageId: `different-asker-cap-${i}`,
        text: `pending ask ${i}`,
        expectsReply: true,
      });
      assert.equal(result.delivered, true, `ask ${i + 1} should fill the sender's own capacity`);
    }

    const refused = await planner.send(orchestrator.sessionId!, {
      messageId: "different-asker-reply-and-ask",
      text: "Answering, with a follow-up.",
      replyTo: peerAskId,
      expectsReply: true,
    });
    assert.equal(refused.delivered, false);
    assert.match(refused.reason ?? "", /from this session/);

    assert.equal((await planner.send(orchestrator.sessionId!, {
      messageId: "different-asker-plain-reply",
      text: "Answering without adding another ask.",
      replyTo: peerAskId,
    })).delivered, true, "the refusal must leave the peer-owned ask available for a plain reply");
  } finally {
    await sink.disconnect().catch(() => undefined);
    await cleanup();
  }
});

test("broker allows replacing the sender's own ask when the sender is at its ask cap", { concurrency: false }, async () => {
  const { planner, cleanup } = await setupClients();
  const sink = new IntercomClient();
  try {
    await sink.connect({
      name: "same-asker-cap-sink",
      cwd: repoDir,
      model: "test-model",
      pid: process.pid,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    });

    for (let i = 0; i < MAX_PENDING_ASK_EDGES_PER_SESSION - 1; i += 1) {
      const result = await planner.send(sink.sessionId!, {
        messageId: `same-asker-cap-${i}`,
        text: `pending ask ${i}`,
        expectsReply: true,
      });
      assert.equal(result.delivered, true);
    }

    const ownAskId = "same-asker-self-ask";
    assert.equal((await planner.send(planner.sessionId!, {
      messageId: ownAskId,
      text: "Self-directed ask at the cap.",
      expectsReply: true,
    })).delivered, true);

    const replacement = await planner.send(planner.sessionId!, {
      messageId: "same-asker-reply-and-ask",
      text: "Replace my own pending ask.",
      replyTo: ownAskId,
      expectsReply: true,
    });
    assert.equal(replacement.delivered, true);
  } finally {
    await sink.disconnect().catch(() => undefined);
    await cleanup();
  }
});

test("oversize delivery is contained: sender is told, and neither connection dies", { concurrency: false }, async () => {
  const { planner, orchestrator, cleanup } = await setupClients();
  const raw = await connectRawRegistered("oversize-sender-id", "oversize-sender");
  try {
    const { createMessageReader, MAX_FRAME_BYTES } = await import("./broker/framing.ts");
    const target = await waitForSessionByName(planner, "orchestrator");

    // Hand-build the frame so it is exactly at the wire cap: legal inbound, but the broker's
    // added `from` plus brokerReceivedAt/brokerDeliveredAt push the outbound frame over it.
    // Going through client.send would be capped on the way out instead.
    const build = (padding: string) => JSON.stringify({
      type: "send",
      to: target.id,
      message: { id: "oversize-after-metadata", timestamp: 1, content: { text: padding } },
    });
    const overhead = Buffer.byteLength(build(""), "utf-8");
    const payload = build("x".repeat(MAX_FRAME_BYTES - overhead));
    assert.equal(Buffer.byteLength(payload, "utf-8"), MAX_FRAME_BYTES, "inbound frame must sit exactly at the cap");

    const outcome = new Promise<{ type?: string; reason?: string }>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error("no delivery outcome for the oversize send")), 5000);
      const reader = createMessageReader((msg) => {
        const m = msg as { type?: string; messageId?: string; reason?: string };
        if (m.messageId !== "oversize-after-metadata") return;
        clearTimeout(timeout);
        raw.socket.off("data", reader);
        resolve(m);
      }, reject);
      raw.socket.on("data", reader);
    });

    const header = Buffer.alloc(4);
    header.writeUInt32BE(Buffer.byteLength(payload, "utf-8"), 0);
    raw.socket.write(Buffer.concat([header, Buffer.from(payload, "utf-8")]));

    const result = await outcome;
    assert.equal(result.type, "delivery_failed");
    assert.match(result.reason ?? "", /too large after broker metadata/);

    // The point of containment: neither peer's connection was collateral damage.
    assert.equal(raw.socket.destroyed, false, "sender connection must survive an oversize refusal");
    const followUp = await planner.send(target.id, { text: "still connected" });
    assert.equal(followUp.delivered, true);
    const sessions = await orchestrator.listSessions();
    assert.ok(sessions.length >= 2, "target connection must survive an oversize refusal");
  } finally {
    raw.socket.destroy();
    await cleanup();
  }
});

test("a supersede whose replacement exceeds the frame cap applies neither frame", { concurrency: false }, async () => {
  const { cleanup } = await setupClients();
  const sender = await connectRawRegistered("supersede-atomic-sender-id", "supersede-atomic-sender");
  const receiver = await connectRawRegistered("supersede-atomic-receiver-id", "supersede-atomic-receiver");
  try {
    const { createMessageReader, MAX_FRAME_BYTES } = await import("./broker/framing.ts");

    // Record every frame the receiver sees, in arrival order.
    const received: Array<{ type?: string; message?: { id?: string }; control?: { action?: string; messageId?: string } }> = [];
    const receiverReader = createMessageReader((msg) => {
      received.push(msg as (typeof received)[number]);
    }, () => undefined);
    receiver.socket.on("data", receiverReader);

    const senderFrames: Array<{ type?: string; messageId?: string; reason?: string }> = [];
    const senderReader = createMessageReader((msg) => {
      senderFrames.push(msg as (typeof senderFrames)[number]);
    }, () => undefined);
    sender.socket.on("data", senderReader);

    const awaitSender = async (messageId: string, timeoutMs = 5000) => {
      const deadline = Date.now() + timeoutMs;
      while (Date.now() < deadline) {
        const hit = senderFrames.find((f) => f.messageId === messageId && (f.type === "delivered" || f.type === "delivery_failed"));
        if (hit) return hit;
        await new Promise((r) => setTimeout(r, 20));
      }
      throw new Error(`no outcome for ${messageId}: ${JSON.stringify(senderFrames)}`);
    };

    // 1. An ordinary message establishes the ID that will later be superseded.
    sender.writeMessage(sender.socket, {
      type: "send",
      to: "supersede-atomic-receiver-id",
      message: { id: "original-msg", timestamp: 1, content: { text: "original" } },
    });
    assert.equal((await awaitSender("original-msg")).type, "delivered");

    // 2. Supersede it with a replacement that only exceeds the cap once broker metadata is
    //    added. Hand-built so the inbound frame sits exactly at the cap and is legal on the way
    //    in; client.send would have refused it locally and never reached the broker.
    const build = (padding: string) => JSON.stringify({
      type: "send",
      to: "supersede-atomic-receiver-id",
      message: { id: "oversize-replacement", timestamp: 2, supersedes: "original-msg", content: { text: padding } },
    });
    const payload = build("x".repeat(MAX_FRAME_BYTES - Buffer.byteLength(build(""), "utf-8")));
    assert.equal(Buffer.byteLength(payload, "utf-8"), MAX_FRAME_BYTES, "inbound frame must sit exactly at the cap");
    const header = Buffer.alloc(4);
    header.writeUInt32BE(Buffer.byteLength(payload, "utf-8"), 0);
    const before = received.length;
    sender.socket.write(Buffer.concat([header, Buffer.from(payload, "utf-8")]));

    // 3. The sender is told, with the metadata-expansion reason.
    const failure = await awaitSender("oversize-replacement");
    assert.equal(failure.type, "delivery_failed");
    assert.match(failure.reason ?? "", /too large after broker metadata/);

    // 4. The receiver got NEITHER frame — no supersede control, no replacement. This is the
    //    regression: emitting the control first would have retired original-msg with no
    //    replacement ever arriving.
    await new Promise((r) => setTimeout(r, 250));
    const after = received.slice(before);
    assert.deepEqual(
      after.filter((f) => f.type === "message_control"),
      [],
      "no supersede control may reach the receiver when the replacement cannot be delivered",
    );
    assert.deepEqual(
      after.filter((f) => f.message?.id === "oversize-replacement"),
      [],
      "the oversized replacement must not be delivered",
    );

    // 5. The old ID is still actionable: a second, legal supersede of it still succeeds, which
    //    it could not if the failed attempt had consumed or retired it.
    const legalStart = received.length;
    sender.writeMessage(sender.socket, {
      type: "send",
      to: "supersede-atomic-receiver-id",
      message: { id: "legal-replacement", timestamp: 3, supersedes: "original-msg", content: { text: "replacement" } },
    });
    assert.equal((await awaitSender("legal-replacement")).type, "delivered");

    await new Promise((r) => setTimeout(r, 250));
    const legal = received.slice(legalStart);
    const controlIndex = legal.findIndex((f) => f.type === "message_control" && f.control?.messageId === "original-msg");
    const messageIndex = legal.findIndex((f) => f.message?.id === "legal-replacement");
    assert.ok(controlIndex >= 0, "a successful supersede still delivers the control");
    assert.ok(messageIndex >= 0, "a successful supersede still delivers the replacement");
    assert.ok(controlIndex < messageIndex, "wire ordering is preserved: control before message");

    // 6. Neither connection was collateral damage.
    assert.equal(sender.socket.destroyed, false, "sender socket must stay live");
    assert.equal(receiver.socket.destroyed, false, "receiver socket must stay live");
  } finally {
    sender.socket.destroy();
    receiver.socket.destroy();
    await cleanup();
  }
});
