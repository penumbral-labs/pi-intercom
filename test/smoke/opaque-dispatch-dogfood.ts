import assert from "node:assert/strict";
import { closeSync, existsSync, fsyncSync, mkdtempSync, openSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { IntercomClient } from "../../broker/client.ts";
import { spawnBrokerIfNeeded } from "../../broker/spawn.ts";
import type { OpaqueDispatchBrokerFrame, SessionRegistration } from "../../types.ts";

const runtimeDir = process.env.PI_CODING_AGENT_DIR?.trim() || mkdtempSync(join(tmpdir(), "pi-intercom-opaque-dogfood-"));
const ownsRuntimeDir = !process.env.PI_CODING_AGENT_DIR?.trim();
process.env.PI_CODING_AGENT_DIR = runtimeDir;
const durablePath = join(runtimeDir, "consumer-record.json");
const sentinel = "opaque-dogfood-privacy-sentinel";
const senderNamespace = "dogfood/sender";
const receiverNamespace = "dogfood/receiver";
const sender = new IntercomClient();
const receiver = new IntercomClient();

function registration(name: string, namespace: string, role: "send" | "receive"): SessionRegistration {
  return {
    name,
    cwd: process.cwd(),
    model: "dogfood",
    pid: process.pid,
    startedAt: Date.now(),
    lastActivity: Date.now(),
    extensions: [{ namespace, ownerEligible: false, opaqueDispatch: { version: 1, roles: [role] } }],
  };
}

function nextOffer(): Promise<Extract<OpaqueDispatchBrokerFrame, { type: "opaque_dispatch_v1_offer" }>> {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      stop();
      reject(new Error("dogfood offer timeout"));
    }, 5_000);
    const stop = receiver.onOpaqueDispatch((frame) => {
      if (frame.type !== "opaque_dispatch_v1_offer") return;
      clearTimeout(timeout);
      stop();
      resolve(frame);
    });
  });
}

try {
  await spawnBrokerIfNeeded(process.execPath, [join(process.cwd(), "node_modules", "tsx", "dist", "cli.mjs")]);
  await receiver.connect(registration("dogfood-receiver", receiverNamespace, "receive"), "dogfood-receiver");
  await sender.connect(registration("dogfood-sender", senderNamespace, "send"), "dogfood-sender");

  const ordinaryMessages: unknown[] = [];
  const genericBrokerMessages: unknown[] = [];
  receiver.on("message", (...args) => ordinaryMessages.push(args));
  receiver.onBrokerMessage((message) => genericBrokerMessages.push(message));

  const offered = nextOffer();
  const acceptedPromise = sender.sendOpaqueDispatch(senderNamespace, {
    requestId: "dogfood-request",
    toSessionId: "dogfood-receiver",
    recipientNamespace: receiverNamespace,
    payload: { sentinel, action: "persist-before-claim" },
  });
  const offer = await offered;
  receiver.sendOpaqueReservationResult(offer.messageId, offer.reservationId, "reserved");
  const accepted = await acceptedPromise;
  assert.equal(accepted.accepted, true);

  writeFileSync(durablePath, JSON.stringify({ brokerEpoch: offer.brokerEpoch, messageId: offer.messageId, reservationId: offer.reservationId, payload: offer.payload }));
  const file = openSync(durablePath, "r");
  fsyncSync(file);
  closeSync(file);

  assert.deepEqual(await receiver.claimOpaqueDispatch(receiverNamespace, offer.messageId, offer.reservationId), { claimed: true });
  assert.deepEqual(await receiver.reconcileOpaqueClaim(receiverNamespace, {
    brokerEpoch: offer.brokerEpoch,
    messageId: offer.messageId,
    reservationId: offer.reservationId,
  }), { state: "claimed" });
  assert.match(readFileSync(durablePath, "utf8"), new RegExp(sentinel));
  assert.equal(JSON.stringify(ordinaryMessages).includes(sentinel), false);
  assert.equal(JSON.stringify(genericBrokerMessages).includes(sentinel), false);
  assert.equal((await sender.send("dogfood-receiver", { text: "ordinary-dogfood-after-opaque" })).delivered, true);
  console.log(JSON.stringify({ ok: true, brokerEpoch: offer.brokerEpoch, messageId: offer.messageId, durablePath, privacySentinel: "absent-from-ordinary-and-generic-paths" }));
} finally {
  await sender.disconnect().catch(() => undefined);
  await receiver.disconnect().catch(() => undefined);
  const pidPath = join(runtimeDir, "intercom", "broker.pid");
  if (existsSync(pidPath)) {
    const pid = Number.parseInt(readFileSync(pidPath, "utf8"), 10);
    if (Number.isFinite(pid)) {
      try { process.kill(pid, "SIGTERM"); } catch { /* broker already exited */ }
    }
  }
  rmSync(durablePath, { force: true });
  if (ownsRuntimeDir) rmSync(runtimeDir, { recursive: true, force: true });
}
