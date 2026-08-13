import assert from "node:assert/strict";
import { closeSync, fsyncSync, mkdtempSync, openSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ExtensionCapability, OpaqueDispatchBrokerFrame, SessionInfo } from "../../types.ts";
import { OpaqueDispatchManager, type OpaqueEndpoint } from "../../broker/opaque-dispatch.ts";

const runtimeDir = mkdtempSync(join(tmpdir(), "pi-intercom-opaque-dogfood-"));
const durablePath = join(runtimeDir, "consumer-record.json");
const senderFrames: OpaqueDispatchBrokerFrame[] = [];
const receiverFrames: OpaqueDispatchBrokerFrame[] = [];
const brokerEpoch = "33333333-3333-4333-8333-333333333333";
const senderCapabilities: ExtensionCapability[] = [{ namespace: "dogfood/sender", ownerEligible: false, opaqueDispatch: { version: 1, roles: ["send"] } }];
const receiverCapabilities: ExtensionCapability[] = [{ namespace: "dogfood/receiver", ownerEligible: false, opaqueDispatch: { version: 1, roles: ["receive"] } }];
const info = (id: string): SessionInfo => ({ id, cwd: runtimeDir, model: "dogfood", pid: process.pid, startedAt: Date.now(), lastActivity: Date.now(), trustedLocal: true });
const endpoints = new Map<string, OpaqueEndpoint>([
  ["dogfood-sender", { sessionId: "dogfood-sender", info: info("dogfood-sender"), extensions: senderCapabilities, connected: true, write: (frame) => senderFrames.push(frame) }],
  ["dogfood-receiver", { sessionId: "dogfood-receiver", info: info("dogfood-receiver"), extensions: receiverCapabilities, connected: true, write: (frame) => receiverFrames.push(frame) }],
]);
const manager = new OpaqueDispatchManager({ brokerEpoch, endpoint: (id) => endpoints.get(id), owner: () => undefined });

try {
  manager.handle(endpoints.get("dogfood-sender")!, {
    type: "opaque_dispatch_v1_send",
    operationId: "dogfood-send",
    requestId: "dogfood-request",
    senderNamespace: "dogfood/sender",
    toSessionId: "dogfood-receiver",
    recipientNamespace: "dogfood/receiver",
    payload: { sentinel: "opaque-dogfood-sentinel", action: "persist-before-claim" },
  });
  const offer = receiverFrames.find((frame): frame is Extract<OpaqueDispatchBrokerFrame, { type: "opaque_dispatch_v1_offer" }> => frame.type === "opaque_dispatch_v1_offer");
  assert.ok(offer);
  manager.handle(endpoints.get("dogfood-receiver")!, { type: "opaque_dispatch_v1_reservation_result", messageId: offer.messageId, reservationId: offer.reservationId, decision: "reserved" });

  writeFileSync(durablePath, JSON.stringify({ brokerEpoch, messageId: offer.messageId, reservationId: offer.reservationId, payload: offer.payload }));
  const file = openSync(durablePath, "r");
  fsyncSync(file);
  closeSync(file);

  manager.handle(endpoints.get("dogfood-receiver")!, { type: "opaque_dispatch_v1_claim", operationId: "dogfood-claim", messageId: offer.messageId, reservationId: offer.reservationId });
  manager.handle(endpoints.get("dogfood-receiver")!, { type: "opaque_dispatch_v1_claim_status", operationId: "dogfood-reconcile", recipientNamespace: "dogfood/receiver", brokerEpoch, messageId: offer.messageId, reservationId: offer.reservationId });
  assert.equal(receiverFrames.some((frame) => frame.type === "opaque_dispatch_v1_claim_result" && frame.claimed), true);
  assert.equal(receiverFrames.some((frame) => frame.type === "opaque_dispatch_v1_claim_status_result" && frame.result.state === "claimed"), true);
  assert.equal(senderFrames.some((frame) => frame.type === "opaque_dispatch_v1_receipt" && frame.receipt.status === "claimed"), true);
  assert.match(readFileSync(durablePath, "utf8"), /opaque-dogfood-sentinel/);
  console.log(JSON.stringify({ ok: true, brokerEpoch, messageId: offer.messageId, durablePath }));
} finally {
  manager.shutdown();
  rmSync(runtimeDir, { recursive: true, force: true });
}
