import assert from "node:assert/strict";
import { once } from "node:events";
import { spawn, type ChildProcess } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import net from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { IntercomClient } from "./client.ts";
import { createMessageReader, writeMessage } from "./framing.ts";
import { ensureIntercomRuntimeDir, getBrokerSocketPath } from "./paths.ts";
import { getTsxCliPath } from "./spawn.ts";
import type { OpaqueDispatchBrokerFrame, SessionRegistration } from "../types.ts";

const senderNamespace = "wire/sender";
const receiverNamespace = "wire/receiver";
const sentinel = "OPAQUE_PRIVACY_SENTINEL_7f8c2e";

function registration(name: string, namespace: string, role: "send" | "receive"): SessionRegistration {
  return {
    name,
    cwd: process.cwd(),
    model: "wire-test",
    pid: process.pid,
    startedAt: Date.now(),
    lastActivity: Date.now(),
    extensions: [{ namespace, ownerEligible: false, opaqueDispatch: { version: 1, roles: [role] } }],
  };
}

function legacyRegistration(name: string): SessionRegistration {
  return {
    name,
    cwd: process.cwd(),
    model: "wire-test-v0.10",
    pid: process.pid,
    startedAt: Date.now(),
    lastActivity: Date.now(),
  };
}

async function connectRawOpaque(sessionId: string, namespace: string, role: "send" | "receive") {
  const socket = net.connect({ path: getBrokerSocketPath(), allowHalfOpen: true });
  const frames: Record<string, unknown>[] = [];
  socket.on("error", () => undefined);
  socket.on("data", createMessageReader((frame) => {
    if (typeof frame === "object" && frame !== null) frames.push(frame as Record<string, unknown>);
  }, (error) => socket.destroy(error)));
  await once(socket, "connect");
  writeMessage(socket, { type: "register", sessionId, session: registration(sessionId, namespace, role) });
  const registered = await waitForRawFrame(frames, (frame) => frame.type === "registered");
  return { socket, frames, endpointEpoch: registered.endpointEpoch as string };
}

async function waitForRawFrame(
  frames: Record<string, unknown>[],
  predicate: (frame: Record<string, unknown>) => boolean,
  timeoutMs = 2_000,
): Promise<Record<string, unknown>> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const frame = frames.find(predicate);
    if (frame) return frame;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error(`raw opaque frame timeout: ${JSON.stringify(frames)}`);
}

async function waitForSocketEnd(socket: net.Socket, timeoutMs = 2_000): Promise<void> {
  if (!socket.readable) return;
  await Promise.race([
    once(socket, "end").then(() => undefined),
    new Promise<never>((_resolve, reject) => setTimeout(() => reject(new Error("socket end timeout")), timeoutMs)),
  ]);
}

async function waitForBrokerReady(broker: ChildProcess): Promise<void> {
  const stdout = broker.stdout;
  if (!stdout) throw new Error("Broker stdout unavailable");
  await new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(() => finish(new Error("broker startup timeout")), 10_000);
    const finish = (error?: Error) => {
      clearTimeout(timeout);
      stdout.off("data", onData);
      broker.off("exit", onExit);
      error ? reject(error) : resolve();
    };
    const onData = (chunk: Buffer) => {
      if (chunk.toString().includes("Intercom broker started")) finish();
    };
    const onExit = () => finish(new Error("broker exited before startup"));
    stdout.on("data", onData);
    broker.once("exit", onExit);
  });
}

async function withWireClients(run: (sender: IntercomClient, receiver: IntercomClient) => Promise<void>): Promise<void> {
  const agentDir = mkdtempSync(join(tmpdir(), "pi-intercom-opaque-wire-"));
  const previousAgentDir = process.env.PI_CODING_AGENT_DIR;
  process.env.PI_CODING_AGENT_DIR = agentDir;
  const broker = spawn(process.execPath, [getTsxCliPath(), join(process.cwd(), "broker", "broker.ts")], {
    cwd: process.cwd(),
    env: { ...process.env, PI_CODING_AGENT_DIR: agentDir },
    stdio: ["ignore", "pipe", "pipe"],
  });
  const sender = new IntercomClient();
  const receiver = new IntercomClient();
  try {
    await waitForBrokerReady(broker);
    await receiver.connect(registration("wire-receiver", receiverNamespace, "receive"), "wire-receiver");
    await sender.connect(registration("wire-sender", senderNamespace, "send"), "wire-sender");
    await run(sender, receiver);
  } finally {
    await sender.disconnect().catch(() => undefined);
    await receiver.disconnect().catch(() => undefined);
    if (broker.exitCode === null && broker.signalCode === null) {
      broker.kill("SIGTERM");
      await once(broker, "exit").catch(() => undefined);
    }
    if (previousAgentDir === undefined) delete process.env.PI_CODING_AGENT_DIR;
    else process.env.PI_CODING_AGENT_DIR = previousAgentDir;
    rmSync(agentDir, { recursive: true, force: true });
  }
}

function nextOffer(client: IntercomClient): Promise<Extract<OpaqueDispatchBrokerFrame, { type: "opaque_dispatch_v1_offer" }>> {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      stop();
      reject(new Error("offer timeout"));
    }, 5_000);
    const stop = client.onOpaqueDispatch((frame) => {
      if (frame.type !== "opaque_dispatch_v1_offer") return;
      clearTimeout(timeout);
      stop();
      resolve(frame);
    });
  });
}

function nextReceipt(client: IntercomClient, status: "claimed" | "failed_closed"): Promise<Extract<OpaqueDispatchBrokerFrame, { type: "opaque_dispatch_v1_receipt" }>> {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      stop();
      reject(new Error("receipt timeout"));
    }, 5_000);
    const stop = client.onOpaqueDispatch((frame) => {
      if (frame.type !== "opaque_dispatch_v1_receipt" || frame.receipt.status !== status) return;
      clearTimeout(timeout);
      stop();
      resolve(frame);
    });
  });
}

function exhaustReceiverOpaqueTokens(client: IntercomClient): void {
  for (let index = 0; index < 59; index += 1) {
    client.ackOpaqueReceipt(receiverNamespace, "00000000-0000-4000-8000-000000000000", 1);
  }
}

test("new client refuses opaque writes to a featureless v0.10-style broker and keeps ordinary operations live", { concurrency: false }, async () => {
  const agentDir = mkdtempSync(join(tmpdir(), "pi-intercom-old-broker-"));
  const previousAgentDir = process.env.PI_CODING_AGENT_DIR;
  process.env.PI_CODING_AGENT_DIR = agentDir;
  ensureIntercomRuntimeDir(join(agentDir, "intercom"));
  const server = net.createServer((socket) => {
    socket.on("data", createMessageReader((message) => {
      if (typeof message !== "object" || message === null || !("type" in message)) return;
      const frame = message as Record<string, unknown>;
      if (frame.type === "register") writeMessage(socket, { type: "registered", sessionId: "new-client" });
      if (frame.type === "list") writeMessage(socket, { type: "sessions", requestId: frame.requestId, sessions: [] });
      if (frame.type === "send") {
        const outbound = frame.message as { id?: unknown };
        writeMessage(socket, { type: "delivered", messageId: outbound.id });
      }
    }, (error) => socket.destroy(error)));
  });
  const client = new IntercomClient();
  try {
    await new Promise<void>((resolve, reject) => server.listen(getBrokerSocketPath(process.platform, agentDir), resolve).once("error", reject));
    await client.connect(registration("new-client", senderNamespace, "send"), "new-client");
    assert.deepEqual(await client.sendOpaqueDispatch(senderNamespace, {
      requestId: "unsupported-request",
      toSessionId: "old-peer",
      recipientNamespace: receiverNamespace,
      payload: { secret: sentinel },
    }), { accepted: false, requestId: "unsupported-request", code: "unsupported_broker" });
    assert.deepEqual(await client.listSessions(), []);
    assert.equal((await client.send("old-peer", { text: "ordinary-still-live" })).delivered, true);
  } finally {
    await client.disconnect().catch(() => undefined);
    await new Promise<void>((resolve) => server.close(() => resolve()));
    if (previousAgentDir === undefined) delete process.env.PI_CODING_AGENT_DIR;
    else process.env.PI_CODING_AGENT_DIR = previousAgentDir;
    rmSync(agentDir, { recursive: true, force: true });
  }
});

test("v0.10-style client remains ordinary-wire compatible with the new broker", { concurrency: false }, async () => {
  await withWireClients(async (sender, receiver) => {
    const socket = net.connect(getBrokerSocketPath());
    const received: unknown[] = [];
    socket.on("data", createMessageReader((frame) => received.push(frame), (error) => socket.destroy(error)));
    await once(socket, "connect");
    const waitFor = async (predicate: (frame: unknown) => boolean): Promise<unknown> => {
      const deadline = Date.now() + 2_000;
      while (Date.now() < deadline) {
        const match = received.find(predicate);
        if (match !== undefined) return match;
        await new Promise((resolve) => setTimeout(resolve, 10));
      }
      throw new Error(`legacy wire response timeout: ${JSON.stringify(received)}`);
    };
    try {
      writeMessage(socket, { type: "register", sessionId: "legacy-client", session: legacyRegistration("legacy-client") });
      await waitFor((frame) => typeof frame === "object" && frame !== null && "type" in frame && frame.type === "registered");
      writeMessage(socket, { type: "list", requestId: "legacy-list" });
      const sessions = await waitFor((frame) => typeof frame === "object" && frame !== null && "type" in frame && frame.type === "sessions") as { sessions: Array<{ id: string }> };
      assert.equal(sessions.sessions.some((session) => session.id === "wire-receiver"), true);
      writeMessage(socket, {
        type: "send",
        to: "wire-receiver",
        message: { id: "legacy-message", timestamp: Date.now(), content: { text: "legacy ordinary message" } },
      });
      const delivered = await waitFor((frame) => typeof frame === "object" && frame !== null && "type" in frame && frame.type === "delivered") as { messageId: string };
      assert.equal(delivered.messageId, "legacy-message");

      const unsupported = await sender.sendOpaqueDispatch(senderNamespace, {
        requestId: "legacy-target-request",
        toSessionId: "legacy-client",
        recipientNamespace: "legacy/v1",
        payload: { sentinel },
      });
      assert.deepEqual(unsupported, { accepted: false, requestId: "legacy-target-request", code: "unsupported_target" });
      await new Promise((resolve) => setTimeout(resolve, 25));
      assert.equal(received.some((frame) => typeof frame === "object" && frame !== null && "type" in frame
        && String(frame.type).startsWith("opaque_dispatch_v1_")), false);
      writeMessage(socket, { type: "list", requestId: "legacy-list-after-opaque" });
      await waitFor((frame) => typeof frame === "object" && frame !== null && "type" in frame && frame.type === "sessions"
        && "requestId" in frame && frame.requestId === "legacy-list-after-opaque");
      assert.equal(receiver.isConnected(), true);
    } finally {
      socket.destroy();
    }
  });
});

test("retired origin socket cannot send as its replacement", { concurrency: false }, async () => {
  await withWireClients(async (_sender, receiver) => {
    const original = await connectRawOpaque("stale-send-origin", senderNamespace, "send");
    const replacement = await connectRawOpaque("stale-send-origin", senderNamespace, "send");

    const liveOffered = nextOffer(receiver);
    writeMessage(replacement.socket, {
      type: "opaque_dispatch_v1_send", operationId: "live-send", requestId: "live-request",
      senderNamespace, toSessionId: "wire-receiver", targetEpoch: receiver.endpointEpoch,
      recipientNamespace: receiverNamespace, payload: { live: true },
    });
    const liveOffer = await liveOffered;
    assert.deepEqual(liveOffer.payload, { live: true });

    const staleOffers: OpaqueDispatchBrokerFrame[] = [];
    const stop = receiver.onOpaqueDispatch((frame) => {
      if (frame.type === "opaque_dispatch_v1_offer" && frame.requestId === "stale-request") staleOffers.push(frame);
    });
    writeMessage(original.socket, {
      type: "opaque_dispatch_v1_send", operationId: "stale-send", requestId: "stale-request",
      senderNamespace, toSessionId: "wire-receiver", targetEpoch: receiver.endpointEpoch,
      recipientNamespace: receiverNamespace, payload: { stale: true },
    });
    await waitForSocketEnd(original.socket);
    stop();
    assert.equal(staleOffers.length, 0);
    replacement.socket.destroy();
  });
});

test("retired origin socket cannot cancel its replacement's dispatch", { concurrency: false }, async () => {
  await withWireClients(async (_sender, receiver) => {
    const original = await connectRawOpaque("stale-cancel-origin", senderNamespace, "send");
    const offered = nextOffer(receiver);
    writeMessage(original.socket, {
      type: "opaque_dispatch_v1_send", operationId: "original-send", requestId: "original-request",
      senderNamespace, toSessionId: "wire-receiver", targetEpoch: receiver.endpointEpoch,
      recipientNamespace: receiverNamespace, payload: { original: true },
    });
    const opaqueOffer = await offered;
    receiver.sendOpaqueReservationResult(opaqueOffer.messageId, opaqueOffer.reservationId, "reserved");
    await waitForRawFrame(original.frames, (frame) => frame.type === "opaque_dispatch_v1_ack");
    const replacement = await connectRawOpaque("stale-cancel-origin", senderNamespace, "send");
    writeMessage(original.socket, {
      type: "opaque_dispatch_v1_cancel", operationId: "stale-cancel", senderNamespace, messageId: opaqueOffer.messageId,
    });
    await new Promise((resolve) => setTimeout(resolve, 50));
    writeMessage(replacement.socket, {
      type: "opaque_dispatch_v1_cancel", operationId: "live-cancel", senderNamespace, messageId: opaqueOffer.messageId,
    });
    const result = await waitForRawFrame(replacement.frames, (frame) => frame.operationId === "live-cancel");
    assert.equal(result.cancelled, true);
    original.socket.destroy();
    replacement.socket.destroy();
  });
});

test("rate-limited retired origin socket cannot acknowledge replacement receipts", { concurrency: false }, async () => {
  await withWireClients(async (_sender, receiver) => {
    const original = await connectRawOpaque("stale-ack-origin", senderNamespace, "send");
    const offered = nextOffer(receiver);
    writeMessage(original.socket, {
      type: "opaque_dispatch_v1_send", operationId: "original-send", requestId: "original-request",
      senderNamespace, toSessionId: "wire-receiver", targetEpoch: receiver.endpointEpoch,
      recipientNamespace: receiverNamespace, payload: { original: true },
    });
    const opaqueOffer = await offered;
    receiver.sendOpaqueReservationResult(opaqueOffer.messageId, opaqueOffer.reservationId, "reserved");
    await waitForRawFrame(original.frames, (frame) => frame.type === "opaque_dispatch_v1_receipt");
    const fillerAck = {
      type: "opaque_dispatch_v1_receipt_ack" as const, senderNamespace,
      messageId: "00000000-0000-4000-8000-000000000000", sequence: 1,
    };
    for (let index = 0; index < 59; index += 1) writeMessage(original.socket, fillerAck);
    writeMessage(original.socket, {
      type: "opaque_dispatch_v1_cancel", operationId: "rate-limit-barrier", senderNamespace,
      messageId: "00000000-0000-4000-8000-000000000000",
    });
    const barrier = await waitForRawFrame(original.frames, (frame) => frame.operationId === "rate-limit-barrier");
    assert.equal(barrier.code, "rate_limited");

    const replacement = await connectRawOpaque("stale-ack-origin", senderNamespace, "send");
    await waitForRawFrame(replacement.frames, (frame) => frame.type === "opaque_dispatch_v1_receipt");
    const staleSocketClosed = once(original.socket, "close");
    writeMessage(original.socket, {
      type: "opaque_dispatch_v1_receipt_ack", senderNamespace, messageId: opaqueOffer.messageId, sequence: 1,
    });
    // End the stale socket after its ordered write and wait for close. Unlike a local event-loop
    // tick, this proves the broker consumed the socket stream before the next replay connection.
    original.socket.end();
    await staleSocketClosed;
    const nextReplacement = await connectRawOpaque("stale-ack-origin", senderNamespace, "send");
    const replay = await waitForRawFrame(nextReplacement.frames, (frame) => frame.type === "opaque_dispatch_v1_receipt");
    assert.equal((replay.receipt as { messageId: string }).messageId, opaqueOffer.messageId);
    nextReplacement.socket.destroy();
    replacement.socket.destroy();
  });
});

test("rate-limited claim returns its typed result immediately and terminalizes custody", { concurrency: false }, async () => {
  await withWireClients(async (sender, receiver) => {
    const offered = nextOffer(receiver);
    const accepted = sender.sendOpaqueDispatch(senderNamespace, {
      requestId: "rate-limited-claim-request", toSessionId: "wire-receiver",
      recipientNamespace: receiverNamespace, payload: { sentinel },
    });
    const offer = await offered;
    receiver.sendOpaqueReservationResult(offer.messageId, offer.reservationId, "reserved");
    assert.equal((await accepted).accepted, true);
    exhaustReceiverOpaqueTokens(receiver);
    const terminalReceipt = nextReceipt(sender, "failed_closed");
    const result = await Promise.race([
      receiver.claimOpaqueDispatch(receiverNamespace, offer.messageId, offer.reservationId),
      new Promise<never>((_resolve, reject) => setTimeout(() => reject(new Error("claim did not settle immediately")), 1_000)),
    ]);
    assert.deepEqual(result, { claimed: false, code: "rate_limited" });
    assert.equal((await terminalReceipt).receipt.reason, "rate_limited");
  });
});

test("rate-limited fail returns success immediately after terminalizing custody", { concurrency: false }, async () => {
  await withWireClients(async (sender, receiver) => {
    const offered = nextOffer(receiver);
    const accepted = sender.sendOpaqueDispatch(senderNamespace, {
      requestId: "rate-limited-fail-request", toSessionId: "wire-receiver",
      recipientNamespace: receiverNamespace, payload: { sentinel },
    });
    const offer = await offered;
    receiver.sendOpaqueReservationResult(offer.messageId, offer.reservationId, "reserved");
    assert.equal((await accepted).accepted, true);
    exhaustReceiverOpaqueTokens(receiver);
    const terminalReceipt = nextReceipt(sender, "failed_closed");
    const result = await Promise.race([
      receiver.failOpaqueDispatch(receiverNamespace, offer.messageId, offer.reservationId),
      new Promise<never>((_resolve, reject) => setTimeout(() => reject(new Error("fail did not settle immediately")), 1_000)),
    ]);
    assert.deepEqual(result, { failedClosed: true });
    assert.equal((await terminalReceipt).receipt.reason, "rate_limited");
  });
});

test("wire-level opaque flow remains private and ordinary traffic remains usable", { concurrency: false }, async () => {
  await withWireClients(async (sender, receiver) => {
    const ordinaryMessages: unknown[] = [];
    const genericBrokerMessages: unknown[] = [];
    receiver.on("message", (...args) => ordinaryMessages.push(args));
    receiver.onBrokerMessage((message) => genericBrokerMessages.push(message));

    const offered = nextOffer(receiver);
    const acceptedPromise = sender.sendOpaqueDispatch(senderNamespace, {
      requestId: "wire-request",
      toSessionId: "wire-receiver",
      recipientNamespace: receiverNamespace,
      payload: { sentinel },
    });
    const offer = await offered;
    assert.equal((offer.payload as { sentinel: string }).sentinel, sentinel);
    receiver.sendOpaqueReservationResult(offer.messageId, offer.reservationId, "reserved");
    const accepted = await acceptedPromise;
    assert.equal(accepted.accepted, true);

    const claimedReceipt = nextReceipt(sender, "claimed");
    assert.deepEqual(await receiver.claimOpaqueDispatch(receiverNamespace, offer.messageId, offer.reservationId), { claimed: true });
    const receipt = await claimedReceipt;
    sender.ackOpaqueReceipt(senderNamespace, receipt.receipt.messageId, receipt.receipt.sequence);
    assert.deepEqual(await receiver.reconcileOpaqueClaim(receiverNamespace, {
      brokerEpoch: offer.brokerEpoch,
      endpointEpoch: offer.endpointEpoch,
      messageId: offer.messageId,
      reservationId: offer.reservationId,
    }), { state: "claimed" });

    assert.equal(JSON.stringify(ordinaryMessages).includes(sentinel), false);
    assert.equal(JSON.stringify(genericBrokerMessages).includes(sentinel), false);
    assert.equal((await sender.listSessions()).some((session) => session.id === "wire-receiver"), true);
    assert.equal((await sender.send("wire-receiver", { text: "ordinary-after-opaque" })).delivered, true);
  });
});
