import assert from "node:assert/strict";
import test from "node:test";
import type { ExtensionCapability, OpaqueDispatchBrokerFrame, OpaqueDispatchClientFrame, SessionInfo } from "../types.ts";
import { canonicalizeOpaquePayload, OpaqueDispatchManager, type OpaqueEndpoint } from "./opaque-dispatch.ts";

const senderExtensions: ExtensionCapability[] = [{ namespace: "sender/v1", ownerEligible: false, opaqueDispatch: { version: 1, roles: ["send"] } }];
const receiverExtensions: ExtensionCapability[] = [{ namespace: "receiver/v1", ownerEligible: false, opaqueDispatch: { version: 1, roles: ["receive"] } }];

function info(id: string): SessionInfo {
  return { id, cwd: "/test", model: "test", pid: 1, startedAt: 1, lastActivity: 1, trustedLocal: true };
}

function harness(receiverConnected = true) {
  const senderFrames: OpaqueDispatchBrokerFrame[] = [];
  const receiverFrames: OpaqueDispatchBrokerFrame[] = [];
  const endpoints = new Map<string, OpaqueEndpoint>([
    ["sender", { sessionId: "sender", info: info("sender"), extensions: senderExtensions, connected: true, write: (frame) => senderFrames.push(frame) }],
    ["receiver", { sessionId: "receiver", info: info("receiver"), extensions: receiverExtensions, connected: receiverConnected, ...(receiverConnected ? { write: (frame: OpaqueDispatchBrokerFrame) => receiverFrames.push(frame) } : {}) }],
  ]);
  const manager = new OpaqueDispatchManager({ brokerEpoch: "33333333-3333-4333-8333-333333333333", endpoint: (id) => endpoints.get(id), owner: () => undefined });
  const send = (requestId = "request", operationId = "send-op", payload: unknown = { z: 1, a: true }) => manager.handle(endpoints.get("sender")!, {
    type: "opaque_dispatch_v1_send", operationId, requestId, senderNamespace: "sender/v1", toSessionId: "receiver", recipientNamespace: "receiver/v1", payload,
  });
  return { manager, endpoints, senderFrames, receiverFrames, send };
}

function offer(frames: OpaqueDispatchBrokerFrame[]) {
  return frames.find((frame): frame is Extract<OpaqueDispatchBrokerFrame, { type: "opaque_dispatch_v1_offer" }> => frame.type === "opaque_dispatch_v1_offer")!;
}

test("canonical payload grammar sorts keys and rejects non-JSON values", () => {
  const result = canonicalizeOpaquePayload({ z: 1, a: [true, null, -0] });
  assert.equal(result.ok, true);
  if (result.ok) assert.equal(result.json, '{"a":[true,null,0],"z":1}');
  assert.deepEqual(canonicalizeOpaquePayload(Number.NaN), { ok: false, code: "invalid_request" });
  assert.deepEqual(canonicalizeOpaquePayload({ value: undefined }), { ok: false, code: "invalid_request" });
  const sparse = Array(1);
  assert.deepEqual(canonicalizeOpaquePayload(sparse), { ok: false, code: "invalid_request" });
  const cyclic: { self?: unknown } = {};
  cyclic.self = cyclic;
  assert.deepEqual(canonicalizeOpaquePayload(cyclic), { ok: false, code: "invalid_request" });
});

test("live dispatch offers once, reserves, then claims with ordered receipts", () => {
  const { manager, endpoints, senderFrames, receiverFrames, send } = harness();
  send();
  send("request", "replay-op");
  assert.equal(receiverFrames.filter((frame) => frame.type === "opaque_dispatch_v1_offer").length, 1);
  const offered = offer(receiverFrames);
  manager.handle(endpoints.get("receiver")!, { type: "opaque_dispatch_v1_reservation_result", reservationId: offered.reservationId, messageId: offered.messageId, decision: "reserved" });
  assert.deepEqual(senderFrames.filter((frame) => frame.type === "opaque_dispatch_v1_ack").map((frame) => frame.operationId), ["send-op", "replay-op"]);
  manager.handle(endpoints.get("receiver")!, { type: "opaque_dispatch_v1_claim", operationId: "claim-op", reservationId: offered.reservationId, messageId: offered.messageId });
  assert.equal(receiverFrames.at(-1)?.type, "opaque_dispatch_v1_claim_result");
  assert.deepEqual(senderFrames.filter((frame) => frame.type === "opaque_dispatch_v1_receipt").map((frame) => frame.receipt.status), ["reserved", "claimed"]);
  manager.shutdown();
});

test("offline exact target queues then redelivers same message id after reconnect", () => {
  const { manager, endpoints, senderFrames, receiverFrames, send } = harness(false);
  send();
  const ack = senderFrames.find((frame): frame is Extract<OpaqueDispatchBrokerFrame, { type: "opaque_dispatch_v1_ack" }> => frame.type === "opaque_dispatch_v1_ack")!;
  assert.equal(ack.deliveryState, "mailbox_queued");
  assert.equal(senderFrames.find((frame) => frame.type === "opaque_dispatch_v1_receipt")?.receipt.status, "queued");
  const receiver = endpoints.get("receiver")!;
  receiver.connected = true;
  receiver.write = (frame) => receiverFrames.push(frame);
  manager.endpointAvailable("receiver");
  assert.equal(offer(receiverFrames).messageId, ack.messageId);
  assert.equal(offer(receiverFrames).attempt, 1);
  manager.shutdown();
});

test("cancel ends reservation before terminal receipt", () => {
  const { manager, endpoints, senderFrames, receiverFrames, send } = harness();
  send();
  const offered = offer(receiverFrames);
  manager.handle(endpoints.get("receiver")!, { type: "opaque_dispatch_v1_reservation_result", reservationId: offered.reservationId, messageId: offered.messageId, decision: "reserved" });
  const before = receiverFrames.length;
  manager.handle(endpoints.get("sender")!, { type: "opaque_dispatch_v1_cancel", operationId: "cancel-op", senderNamespace: "sender/v1", messageId: offered.messageId });
  assert.equal(receiverFrames[before]?.type, "opaque_dispatch_v1_reservation_ended");
  const terminal = senderFrames.find((frame) => frame.type === "opaque_dispatch_v1_receipt" && frame.receipt.status === "cancelled");
  assert.ok(terminal);
  assert.equal(senderFrames.at(-1)?.type, "opaque_dispatch_v1_cancel_result");
  manager.shutdown();
});

test("foreign reservation mutation is ignored and request conflicts are typed", () => {
  const { manager, endpoints, senderFrames, receiverFrames, send } = harness();
  send();
  const offered = offer(receiverFrames);
  const foreign: OpaqueEndpoint = { sessionId: "foreign", info: info("foreign"), extensions: receiverExtensions, connected: true, write: () => {} };
  manager.handle(foreign, { type: "opaque_dispatch_v1_reservation_result", reservationId: offered.reservationId, messageId: offered.messageId, decision: "reserved" });
  assert.equal(senderFrames.some((frame) => frame.type === "opaque_dispatch_v1_ack"), false);
  send("request", "conflict-op", { changed: true });
  const rejection = senderFrames.at(-1);
  assert.equal(rejection?.type, "opaque_dispatch_v1_rejected");
  if (rejection?.type === "opaque_dispatch_v1_rejected") assert.equal(rejection.code, "request_conflict");
  manager.shutdown();
});
