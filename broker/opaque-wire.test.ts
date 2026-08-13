import assert from "node:assert/strict";
import { once } from "node:events";
import { spawn, type ChildProcess } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { IntercomClient } from "./client.ts";
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

function nextClaimedReceipt(client: IntercomClient): Promise<Extract<OpaqueDispatchBrokerFrame, { type: "opaque_dispatch_v1_receipt" }>> {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      stop();
      reject(new Error("receipt timeout"));
    }, 5_000);
    const stop = client.onOpaqueDispatch((frame) => {
      if (frame.type !== "opaque_dispatch_v1_receipt" || frame.receipt.status !== "claimed") return;
      clearTimeout(timeout);
      stop();
      resolve(frame);
    });
  });
}

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

    const claimedReceipt = nextClaimedReceipt(sender);
    assert.deepEqual(await receiver.claimOpaqueDispatch(receiverNamespace, offer.messageId, offer.reservationId), { claimed: true });
    const receipt = await claimedReceipt;
    sender.ackOpaqueReceipt(senderNamespace, receipt.receipt.messageId, receipt.receipt.sequence);
    assert.deepEqual(await receiver.reconcileOpaqueClaim(receiverNamespace, {
      brokerEpoch: offer.brokerEpoch,
      messageId: offer.messageId,
      reservationId: offer.reservationId,
    }), { state: "claimed" });

    assert.equal(JSON.stringify(ordinaryMessages).includes(sentinel), false);
    assert.equal(JSON.stringify(genericBrokerMessages).includes(sentinel), false);
    assert.equal((await sender.listSessions()).some((session) => session.id === "wire-receiver"), true);
    assert.equal((await sender.send("wire-receiver", { text: "ordinary-after-opaque" })).delivered, true);
  });
});
