import assert from "node:assert/strict";
import test from "node:test";
import type { ExtensionCapability, OpaqueDispatchBrokerFrame, OpaqueDispatchClientFrame, SessionInfo } from "../types.ts";
import {
  canonicalizeOpaquePayload,
  MAX_OPAQUE_PRINCIPAL_RECORDS,
  MAX_OPAQUE_PRINCIPAL_TOMBSTONES,
  OpaqueDispatchManager,
  type OpaqueEndpoint,
} from "./opaque-dispatch.ts";

const senderExtensions: ExtensionCapability[] = [{ namespace: "sender/v1", ownerEligible: false, opaqueDispatch: { version: 1, roles: ["send"] } }];
const receiverExtensions: ExtensionCapability[] = [{ namespace: "receiver/v1", ownerEligible: false, opaqueDispatch: { version: 1, roles: ["receive"] } }];

function info(id: string): SessionInfo {
  return { id, cwd: "/test", model: "test", pid: 1, startedAt: 1, lastActivity: 1, trustedLocal: true };
}

function harness(receiverConnected = true, timeouts: { activeTtlMs?: number; tombstoneTtlMs?: number; reservationTimeoutMs?: number; claimTimeoutMs?: number } = {}) {
  const senderFrames: OpaqueDispatchBrokerFrame[] = [];
  const receiverFrames: OpaqueDispatchBrokerFrame[] = [];
  const endpoints = new Map<string, OpaqueEndpoint>([
    ["sender", { sessionId: "sender", info: info("sender"), extensions: senderExtensions, connected: true, write: (frame) => senderFrames.push(frame) }],
    ["receiver", { sessionId: "receiver", info: info("receiver"), extensions: receiverExtensions, connected: receiverConnected, ...(receiverConnected ? { write: (frame: OpaqueDispatchBrokerFrame) => receiverFrames.push(frame) } : {}) }],
  ]);
  const manager = new OpaqueDispatchManager({ brokerEpoch: "33333333-3333-4333-8333-333333333333", endpoint: (id) => endpoints.get(id), owner: () => undefined, ...timeouts });
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
  manager.handle(endpoints.get("receiver")!, {
    type: "opaque_dispatch_v1_claim_status",
    operationId: "reconcile-op",
    recipientNamespace: "receiver/v1",
    brokerEpoch: "33333333-3333-4333-8333-333333333333",
    reservationId: offered.reservationId,
    messageId: offered.messageId,
  });
  assert.deepEqual(receiverFrames.at(-1), {
    type: "opaque_dispatch_v1_claim_status_result",
    operationId: "reconcile-op",
    brokerEpoch: "33333333-3333-4333-8333-333333333333",
    reservationId: offered.reservationId,
    messageId: offered.messageId,
    result: { state: "claimed" },
  });
  assert.deepEqual(senderFrames.filter((frame) => frame.type === "opaque_dispatch_v1_receipt").map((frame) => frame.receipt.status), ["reserved", "claimed"]);
  manager.shutdown();
});

test("synchronous offer write failure terminalizes instead of stranding capacity", () => {
  const { manager, endpoints, senderFrames, send } = harness();
  endpoints.get("receiver")!.write = () => { throw new Error("sync write failed"); };
  send();
  assert.equal(manager.activeCount, 0);
  const terminal = senderFrames.find((frame): frame is Extract<OpaqueDispatchBrokerFrame, { type: "opaque_dispatch_v1_receipt" }> => frame.type === "opaque_dispatch_v1_receipt" && frame.receipt.status === "failed_closed");
  assert.equal(terminal?.receipt.reason, "receiver_disconnected");
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

test("reserved supersede ends the old reservation before offering replacement", () => {
  const { manager, endpoints, senderFrames, receiverFrames, send } = harness();
  send("original", "original-op");
  const original = offer(receiverFrames);
  manager.handle(endpoints.get("receiver")!, { type: "opaque_dispatch_v1_reservation_result", reservationId: original.reservationId, messageId: original.messageId, decision: "reserved" });
  manager.handle(endpoints.get("sender")!, {
    type: "opaque_dispatch_v1_send",
    operationId: "replacement-op",
    requestId: "replacement",
    senderNamespace: "sender/v1",
    toSessionId: "receiver",
    recipientNamespace: "receiver/v1",
    payload: { replacement: true },
    supersedesMessageId: original.messageId,
  });
  const originalEndedIndex = receiverFrames.findIndex((frame) => frame.type === "opaque_dispatch_v1_reservation_ended" && frame.messageId === original.messageId);
  const replacementOfferIndex = receiverFrames.findIndex((frame) => frame.type === "opaque_dispatch_v1_offer" && frame.messageId !== original.messageId);
  assert.ok(originalEndedIndex >= 0 && replacementOfferIndex > originalEndedIndex);
  assert.equal(senderFrames.some((frame) => frame.type === "opaque_dispatch_v1_receipt" && frame.receipt.status === "superseded"), true);
  manager.shutdown();
});

test("claim-first supersede race rejects the replacement", () => {
  const { manager, endpoints, senderFrames, receiverFrames, send } = harness();
  send("original", "original-op");
  const original = offer(receiverFrames);
  manager.handle(endpoints.get("receiver")!, { type: "opaque_dispatch_v1_reservation_result", reservationId: original.reservationId, messageId: original.messageId, decision: "reserved" });
  manager.handle(endpoints.get("receiver")!, { type: "opaque_dispatch_v1_claim", operationId: "claim-op", reservationId: original.reservationId, messageId: original.messageId });
  manager.handle(endpoints.get("sender")!, {
    type: "opaque_dispatch_v1_send",
    operationId: "replacement-op",
    requestId: "replacement",
    senderNamespace: "sender/v1",
    toSessionId: "receiver",
    recipientNamespace: "receiver/v1",
    payload: { replacement: true },
    supersedesMessageId: original.messageId,
  });
  const rejection = senderFrames.find((frame) => frame.type === "opaque_dispatch_v1_rejected" && frame.operationId === "replacement-op");
  assert.equal(rejection?.type === "opaque_dispatch_v1_rejected" ? rejection.code : undefined, "already_claimed");
  manager.shutdown();
});

test("receiver reconnect exhausts eight delivery attempts", () => {
  const { manager, endpoints, senderFrames, receiverFrames, send } = harness();
  send();
  for (let attempt = 1; attempt <= 8; attempt += 1) {
    const offered = receiverFrames.filter((frame): frame is Extract<OpaqueDispatchBrokerFrame, { type: "opaque_dispatch_v1_offer" }> => frame.type === "opaque_dispatch_v1_offer").at(-1)!;
    assert.equal(offered.attempt, attempt);
    manager.endpointDisconnected("receiver");
    if (attempt < 8) manager.endpointAvailable("receiver");
  }
  const terminal = senderFrames.find((frame): frame is Extract<OpaqueDispatchBrokerFrame, { type: "opaque_dispatch_v1_receipt" }> => frame.type === "opaque_dispatch_v1_receipt" && frame.receipt.status === "failed_closed");
  assert.equal(terminal?.receipt.reason, "attempt_limit");
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

test("repeated accepted claim is idempotent and foreign reconcile reveals no history", () => {
  const { manager, endpoints, receiverFrames, send } = harness();
  send();
  const offered = offer(receiverFrames);
  manager.handle(endpoints.get("receiver")!, { type: "opaque_dispatch_v1_reservation_result", reservationId: offered.reservationId, messageId: offered.messageId, decision: "reserved" });
  manager.handle(endpoints.get("receiver")!, { type: "opaque_dispatch_v1_claim", operationId: "claim-one", reservationId: offered.reservationId, messageId: offered.messageId });
  manager.handle(endpoints.get("receiver")!, { type: "opaque_dispatch_v1_claim", operationId: "claim-two", reservationId: offered.reservationId, messageId: offered.messageId });
  assert.equal(receiverFrames.filter((frame) => frame.type === "opaque_dispatch_v1_claim_result" && frame.claimed).length, 2);

  const foreignFrames: OpaqueDispatchBrokerFrame[] = [];
  const foreign: OpaqueEndpoint = {
    sessionId: "foreign",
    info: info("foreign"),
    extensions: [{ namespace: "foreign/v1", ownerEligible: false, opaqueDispatch: { version: 1, roles: ["receive"] } }],
    connected: true,
    write: (frame) => foreignFrames.push(frame),
  };
  manager.handle(foreign, {
    type: "opaque_dispatch_v1_claim_status",
    operationId: "foreign-status",
    recipientNamespace: "foreign/v1",
    brokerEpoch: "33333333-3333-4333-8333-333333333333",
    reservationId: offered.reservationId,
    messageId: offered.messageId,
  });
  assert.deepEqual(foreignFrames.at(-1), {
    type: "opaque_dispatch_v1_claim_status_result",
    operationId: "foreign-status",
    brokerEpoch: "33333333-3333-4333-8333-333333333333",
    reservationId: offered.reservationId,
    messageId: offered.messageId,
    result: { state: "indeterminate", code: "claim_history_unavailable" },
  });
  manager.shutdown();
});

test("active expiry emits expired without a reservation-timeout reason", async () => {
  const { manager, senderFrames, receiverFrames, send } = harness(true, { activeTtlMs: 10, reservationTimeoutMs: 1_000 });
  send();
  const offered = offer(receiverFrames);
  await new Promise((resolve) => setTimeout(resolve, 25));
  const ended = receiverFrames.find((frame) => frame.type === "opaque_dispatch_v1_reservation_ended" && frame.messageId === offered.messageId);
  assert.deepEqual(ended, { type: "opaque_dispatch_v1_reservation_ended", messageId: offered.messageId, reservationId: offered.reservationId, outcome: "expired" });
  const expired = senderFrames.find((frame): frame is Extract<OpaqueDispatchBrokerFrame, { type: "opaque_dispatch_v1_receipt" }> => frame.type === "opaque_dispatch_v1_receipt" && frame.receipt.status === "expired");
  assert.ok(expired);
  assert.equal(expired.receipt.reason, undefined);
  manager.shutdown();
});

test("global capacity evicts an old queued record without a stale per-principal count", () => {
  const senderFrames: OpaqueDispatchBrokerFrame[] = [];
  const endpoints = new Map<string, OpaqueEndpoint>();
  for (let index = 0; index < 9; index += 1) {
    const id = `sender-${index}`;
    endpoints.set(id, {
      sessionId: id,
      info: info(id),
      extensions: senderExtensions,
      connected: true,
      write: (frame) => senderFrames.push(frame),
    });
  }
  for (let index = 0; index < 8; index += 1) {
    const id = `receiver-${index}`;
    endpoints.set(id, { sessionId: id, info: info(id), extensions: receiverExtensions, connected: false });
  }
  const manager = new OpaqueDispatchManager({ brokerEpoch: "33333333-3333-4333-8333-333333333333", endpoint: (id) => endpoints.get(id), owner: () => undefined });
  for (let senderIndex = 0; senderIndex < 8; senderIndex += 1) {
    const origin = endpoints.get(`sender-${senderIndex}`)!;
    for (let recordIndex = 0; recordIndex < 32; recordIndex += 1) {
      manager.handle(origin, { type: "opaque_dispatch_v1_send", operationId: `op-${senderIndex}-${recordIndex}`, requestId: `request-${senderIndex}-${recordIndex}`, senderNamespace: "sender/v1", toSessionId: `receiver-${senderIndex}`, recipientNamespace: "receiver/v1", payload: null });
    }
  }
  assert.equal(manager.activeCount, 256);
  const origin = endpoints.get("sender-0")!;
  manager.handle(origin, { type: "opaque_dispatch_v1_send", operationId: "boundary-op", requestId: "boundary-request", senderNamespace: "sender/v1", toSessionId: "receiver-0", recipientNamespace: "receiver/v1", payload: null });
  assert.equal(manager.activeCount, 256);
  assert.equal(senderFrames.some((frame) => frame.type === "opaque_dispatch_v1_ack" && frame.operationId === "boundary-op"), true);
  manager.shutdown();
});

test("sender principal capacity is exactly 32 active records", () => {
  const { manager, senderFrames, send } = harness(false);
  for (let index = 0; index < MAX_OPAQUE_PRINCIPAL_RECORDS + 1; index += 1) send(`request-${index}`, `op-${index}`);
  assert.equal(manager.activeCount, MAX_OPAQUE_PRINCIPAL_RECORDS);
  const last = senderFrames.at(-1);
  assert.equal(last?.type, "opaque_dispatch_v1_rejected");
  if (last?.type === "opaque_dispatch_v1_rejected") assert.equal(last.code, "limit_exceeded");
  manager.shutdown();
});

test("claimed tombstones are bounded per principal", () => {
  const { manager, endpoints, receiverFrames, send } = harness();
  for (let index = 0; index < MAX_OPAQUE_PRINCIPAL_TOMBSTONES + 1; index += 1) {
    send(`request-${index}`, `send-${index}`);
    const offered = receiverFrames.filter((frame): frame is Extract<OpaqueDispatchBrokerFrame, { type: "opaque_dispatch_v1_offer" }> => frame.type === "opaque_dispatch_v1_offer").at(-1)!;
    manager.handle(endpoints.get("receiver")!, { type: "opaque_dispatch_v1_reservation_result", reservationId: offered.reservationId, messageId: offered.messageId, decision: "reserved" });
    manager.handle(endpoints.get("receiver")!, { type: "opaque_dispatch_v1_claim", operationId: `claim-${index}`, reservationId: offered.reservationId, messageId: offered.messageId });
  }
  assert.equal(manager.tombstoneCount, MAX_OPAQUE_PRINCIPAL_TOMBSTONES);
  manager.shutdown();
});
