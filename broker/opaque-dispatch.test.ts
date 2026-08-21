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
  return { id, endpointEpoch: `${id}-epoch`, cwd: "/test", model: "test", pid: 1, startedAt: 1, lastActivity: 1, trustedLocal: true };
}

function harness(receiverConnected = true, timeouts: { activeTtlMs?: number; tombstoneTtlMs?: number; reservationTimeoutMs?: number; claimTimeoutMs?: number } = {}) {
  const senderFrames: OpaqueDispatchBrokerFrame[] = [];
  const receiverFrames: OpaqueDispatchBrokerFrame[] = [];
  const endpoints = new Map<string, OpaqueEndpoint>([
    ["sender", { sessionId: "sender", endpointEpoch: "sender-epoch", info: info("sender"), extensions: senderExtensions, connected: true, write: (frame) => senderFrames.push(frame) }],
    ["receiver", { sessionId: "receiver", endpointEpoch: "receiver-epoch", info: info("receiver"), extensions: receiverExtensions, connected: receiverConnected, ...(receiverConnected ? { write: (frame: OpaqueDispatchBrokerFrame) => receiverFrames.push(frame) } : {}) }],
  ]);
  const manager = new OpaqueDispatchManager({ brokerEpoch: "33333333-3333-4333-8333-333333333333", endpoint: (id) => endpoints.get(id), owner: () => undefined, ...timeouts });
  const send = (requestId = "request", operationId = "send-op", payload: unknown = { z: 1, a: true }) => manager.handle(endpoints.get("sender")!, {
    type: "opaque_dispatch_v1_send", operationId, requestId, senderNamespace: "sender/v1", toSessionId: "receiver", targetEpoch: "receiver-epoch", recipientNamespace: "receiver/v1", payload,
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
  assert.deepEqual(canonicalizeOpaquePayload("x".repeat(64 * 1024)), { ok: false, code: "payload_too_large" });
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
  manager.handle(endpoints.get("receiver")!, { type: "opaque_dispatch_v1_reservation_result", endpointEpoch: "receiver-epoch", reservationId: offered.reservationId, messageId: offered.messageId, decision: "reserved" });
  assert.deepEqual(senderFrames.filter((frame) => frame.type === "opaque_dispatch_v1_ack").map((frame) => frame.operationId), ["send-op", "replay-op"]);
  manager.handle(endpoints.get("receiver")!, { type: "opaque_dispatch_v1_claim", endpointEpoch: "receiver-epoch", operationId: "claim-op", reservationId: offered.reservationId, messageId: offered.messageId });
  assert.equal(receiverFrames.at(-1)?.type, "opaque_dispatch_v1_claim_result");
  manager.handle(endpoints.get("receiver")!, {
    type: "opaque_dispatch_v1_claim_status",
    endpointEpoch: "receiver-epoch",
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

test("offline custody reports queued before failing closed on reconnect", () => {
  const { manager, endpoints, senderFrames, receiverFrames, send } = harness(false);
  send();
  const ack = senderFrames.find((frame): frame is Extract<OpaqueDispatchBrokerFrame, { type: "opaque_dispatch_v1_ack" }> => frame.type === "opaque_dispatch_v1_ack")!;
  assert.equal(ack.deliveryState, "mailbox_queued");
  assert.equal(senderFrames.find((frame) => frame.type === "opaque_dispatch_v1_receipt")?.receipt.status, "queued");
  endpoints.set("receiver", {
    sessionId: "receiver", endpointEpoch: "receiver-epoch-2", info: { ...info("receiver"), endpointEpoch: "receiver-epoch-2" },
    extensions: receiverExtensions, connected: true, write: (frame) => receiverFrames.push(frame),
  });
  manager.endpointAvailable("receiver");
  assert.equal(receiverFrames.some((frame) => frame.type === "opaque_dispatch_v1_offer"), false);
  const receipts = senderFrames.filter((frame): frame is Extract<OpaqueDispatchBrokerFrame, { type: "opaque_dispatch_v1_receipt" }> => frame.type === "opaque_dispatch_v1_receipt");
  assert.deepEqual(receipts.map((frame) => [frame.receipt.status, frame.receipt.reason]), [
    ["queued", undefined],
    ["failed_closed", "endpoint_epoch_changed"],
  ]);
  manager.shutdown();
});

test("terminal endpoint rotation requires a new request id", () => {
  const { manager, endpoints, senderFrames, receiverFrames, send } = harness(false);
  send();
  const originalAck = senderFrames.find((frame): frame is Extract<OpaqueDispatchBrokerFrame, { type: "opaque_dispatch_v1_ack" }> => frame.type === "opaque_dispatch_v1_ack")!;
  endpoints.set("receiver", {
    sessionId: "receiver", endpointEpoch: "receiver-epoch-2", info: { ...info("receiver"), endpointEpoch: "receiver-epoch-2" },
    extensions: receiverExtensions, connected: true, write: (frame) => receiverFrames.push(frame),
  });
  manager.endpointAvailable("receiver");
  manager.handle(endpoints.get("sender")!, {
    type: "opaque_dispatch_v1_send", operationId: "same-request-op", requestId: "request", senderNamespace: "sender/v1",
    toSessionId: "receiver", targetEpoch: "receiver-epoch-2", recipientNamespace: "receiver/v1", payload: { z: 1, a: true },
  });
  const replay = senderFrames.find((frame) => frame.type === "opaque_dispatch_v1_rejected" && frame.operationId === "same-request-op");
  assert.equal(replay?.type === "opaque_dispatch_v1_rejected" ? replay.code : undefined, "endpoint_epoch_changed");
  assert.equal(replay?.type === "opaque_dispatch_v1_rejected" ? replay.messageId : undefined, originalAck.messageId);

  manager.handle(endpoints.get("sender")!, {
    type: "opaque_dispatch_v1_send", operationId: "new-request-op", requestId: "request-after-rotation", senderNamespace: "sender/v1",
    toSessionId: "receiver", targetEpoch: "receiver-epoch-2", recipientNamespace: "receiver/v1", payload: { z: 1, a: true },
  });
  assert.equal(offer(receiverFrames).requestId, "request-after-rotation");
  manager.shutdown();
});

test("offered and reserved custody fail closed across in-place endpoint replacement", () => {
  for (const state of ["offered", "reserved"] as const) {
    const { manager, endpoints, senderFrames, receiverFrames, send } = harness();
    send(`${state}-request`, `${state}-operation`);
    const firstOffer = offer(receiverFrames);
    assert.equal(firstOffer.endpointEpoch, "receiver-epoch");
    if (state === "reserved") manager.handle(endpoints.get("receiver")!, {
      type: "opaque_dispatch_v1_reservation_result", endpointEpoch: "receiver-epoch",
      reservationId: firstOffer.reservationId, messageId: firstOffer.messageId, decision: "reserved",
    });
    manager.endpointDisconnected("receiver");
    endpoints.set("receiver", {
      sessionId: "receiver", endpointEpoch: "receiver-epoch-2", info: { ...info("receiver"), endpointEpoch: "receiver-epoch-2" },
      extensions: receiverExtensions, connected: true, write: (frame) => receiverFrames.push(frame),
    });
    manager.endpointAvailable("receiver");
    assert.equal(receiverFrames.filter((frame) => frame.type === "opaque_dispatch_v1_offer").length, 1);
    assert.equal(senderFrames.some((frame) => frame.type === "opaque_dispatch_v1_receipt"
      && frame.receipt.messageId === firstOffer.messageId && frame.receipt.reason === "endpoint_epoch_changed"), true);
    manager.shutdown();
  }
});

test("rotated endpoint cannot reserve, claim, or reconcile an old epoch offer", () => {
  const { manager, endpoints, senderFrames, receiverFrames, send } = harness();
  send();
  const offered = offer(receiverFrames);
  const replacementFrames: OpaqueDispatchBrokerFrame[] = [];
  const replacement: OpaqueEndpoint = {
    sessionId: "receiver", endpointEpoch: "receiver-epoch-2", info: { ...info("receiver"), endpointEpoch: "receiver-epoch-2" },
    extensions: receiverExtensions, connected: true, write: (frame) => replacementFrames.push(frame),
  };
  endpoints.set("receiver", replacement);
  manager.handle(replacement, {
    type: "opaque_dispatch_v1_reservation_result", endpointEpoch: "receiver-epoch-2",
    reservationId: offered.reservationId, messageId: offered.messageId, decision: "reserved",
  });
  assert.equal(senderFrames.some((frame) => frame.type === "opaque_dispatch_v1_ack"), false);
  manager.handle(replacement, {
    type: "opaque_dispatch_v1_claim", operationId: "replacement-claim", endpointEpoch: "receiver-epoch-2",
    reservationId: offered.reservationId, messageId: offered.messageId,
  });
  const claimResult = replacementFrames.at(-1);
  assert.equal(claimResult?.type === "opaque_dispatch_v1_claim_result" ? claimResult.claimed : undefined, false);
  manager.handle(replacement, {
    type: "opaque_dispatch_v1_claim_status", operationId: "replacement-status", recipientNamespace: "receiver/v1",
    brokerEpoch: "33333333-3333-4333-8333-333333333333", endpointEpoch: "receiver-epoch-2",
    reservationId: offered.reservationId, messageId: offered.messageId,
  });
  const statusResult = replacementFrames.at(-1);
  assert.deepEqual(statusResult?.type === "opaque_dispatch_v1_claim_status_result" ? statusResult.result : undefined,
    { state: "indeterminate", code: "claim_history_unavailable" });
  manager.shutdown();
});

test("stale endpoint epochs reject opaque dispatch before payload custody", () => {
  const { manager, endpoints, senderFrames, receiverFrames } = harness();
  manager.handle(endpoints.get("sender")!, {
    type: "opaque_dispatch_v1_send",
    operationId: "stale-operation",
    requestId: "stale-request",
    senderNamespace: "sender/v1",
    toSessionId: "receiver",
    targetEpoch: "superseded-epoch",
    recipientNamespace: "receiver/v1",
    payload: { secret: "must-not-be-offered" },
  });
  const rejection = senderFrames.find((frame) => frame.type === "opaque_dispatch_v1_rejected");
  assert.equal(rejection?.type === "opaque_dispatch_v1_rejected" ? rejection.code : undefined, "target_rebound");
  assert.equal(receiverFrames.length, 0);
  assert.equal(manager.activeCount, 0);
  manager.shutdown();
});

test("reserved supersede ends the old reservation before offering replacement", () => {
  const { manager, endpoints, senderFrames, receiverFrames, send } = harness();
  send("original", "original-op");
  const original = offer(receiverFrames);
  manager.handle(endpoints.get("receiver")!, { type: "opaque_dispatch_v1_reservation_result", endpointEpoch: "receiver-epoch", reservationId: original.reservationId, messageId: original.messageId, decision: "reserved" });
  manager.handle(endpoints.get("sender")!, {
    type: "opaque_dispatch_v1_send",
    operationId: "replacement-op",
    requestId: "replacement",
    senderNamespace: "sender/v1",
    toSessionId: "receiver",
    targetEpoch: "receiver-epoch",
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

test("self-supersede remains net-zero at principal capacity", () => {
  const { manager, endpoints, senderFrames, receiverFrames, send } = harness();
  for (let index = 0; index < MAX_OPAQUE_PRINCIPAL_RECORDS; index += 1) send(`request-${index}`, `operation-${index}`);
  const prior = offer(receiverFrames);
  manager.handle(endpoints.get("sender")!, {
    type: "opaque_dispatch_v1_send", operationId: "replacement-op", requestId: "replacement", senderNamespace: "sender/v1",
    toSessionId: "receiver", targetEpoch: "receiver-epoch", recipientNamespace: "receiver/v1", payload: { replacement: true },
    supersedesMessageId: prior.messageId,
  });
  assert.equal(manager.activeCount, MAX_OPAQUE_PRINCIPAL_RECORDS);
  assert.equal(senderFrames.some((frame) => frame.type === "opaque_dispatch_v1_rejected"
    && frame.operationId === "replacement-op" && frame.code === "limit_exceeded"), false);
  assert.equal(senderFrames.some((frame) => frame.type === "opaque_dispatch_v1_receipt"
    && frame.receipt.messageId === prior.messageId && frame.receipt.status === "superseded"), true);
  manager.shutdown();
});

test("claim-first supersede race rejects the replacement", () => {
  const { manager, endpoints, senderFrames, receiverFrames, send } = harness();
  send("original", "original-op");
  const original = offer(receiverFrames);
  manager.handle(endpoints.get("receiver")!, { type: "opaque_dispatch_v1_reservation_result", endpointEpoch: "receiver-epoch", reservationId: original.reservationId, messageId: original.messageId, decision: "reserved" });
  manager.handle(endpoints.get("receiver")!, { type: "opaque_dispatch_v1_claim", endpointEpoch: "receiver-epoch", operationId: "claim-op", reservationId: original.reservationId, messageId: original.messageId });
  manager.handle(endpoints.get("sender")!, {
    type: "opaque_dispatch_v1_send",
    operationId: "replacement-op",
    requestId: "replacement",
    senderNamespace: "sender/v1",
    toSessionId: "receiver",
    targetEpoch: "receiver-epoch",
    recipientNamespace: "receiver/v1",
    payload: { replacement: true },
    supersedesMessageId: original.messageId,
  });
  const rejection = senderFrames.find((frame) => frame.type === "opaque_dispatch_v1_rejected" && frame.operationId === "replacement-op");
  assert.equal(rejection?.type === "opaque_dispatch_v1_rejected" ? rejection.code : undefined, "already_claimed");
  manager.shutdown();
});

test("offered-window disconnect accepts the send as mailbox queued", () => {
  const { manager, senderFrames, send } = harness();
  send();
  manager.endpointDisconnected("receiver");
  const ack = senderFrames.find((frame): frame is Extract<OpaqueDispatchBrokerFrame, { type: "opaque_dispatch_v1_ack" }> => frame.type === "opaque_dispatch_v1_ack");
  assert.equal(ack?.deliveryState, "mailbox_queued");
  assert.equal(senderFrames.some((frame) => frame.type === "opaque_dispatch_v1_rejected"), false);
  manager.shutdown();
});

test("a disconnected offer is never attempted on its replacement endpoint", () => {
  const { manager, endpoints, senderFrames, receiverFrames, send } = harness();
  send();
  assert.equal(offer(receiverFrames).attempt, 1);
  manager.endpointDisconnected("receiver");
  endpoints.set("receiver", {
    sessionId: "receiver", endpointEpoch: "receiver-epoch-2", info: { ...info("receiver"), endpointEpoch: "receiver-epoch-2" },
    extensions: receiverExtensions, connected: true, write: (frame) => receiverFrames.push(frame),
  });
  manager.endpointAvailable("receiver");
  assert.equal(receiverFrames.filter((frame) => frame.type === "opaque_dispatch_v1_offer").length, 1);
  const terminal = senderFrames.find((frame): frame is Extract<OpaqueDispatchBrokerFrame, { type: "opaque_dispatch_v1_receipt" }> => frame.type === "opaque_dispatch_v1_receipt" && frame.receipt.status === "failed_closed");
  assert.equal(terminal?.receipt.reason, "endpoint_epoch_changed");
  manager.shutdown();
});

test("cancel ends reservation before terminal receipt", () => {
  const { manager, endpoints, senderFrames, receiverFrames, send } = harness();
  send();
  const offered = offer(receiverFrames);
  manager.handle(endpoints.get("receiver")!, { type: "opaque_dispatch_v1_reservation_result", endpointEpoch: "receiver-epoch", reservationId: offered.reservationId, messageId: offered.messageId, decision: "reserved" });
  const before = receiverFrames.length;
  manager.handle(endpoints.get("sender")!, { type: "opaque_dispatch_v1_cancel", operationId: "cancel-op", senderNamespace: "sender/v1", messageId: offered.messageId });
  assert.equal(receiverFrames[before]?.type, "opaque_dispatch_v1_reservation_ended");
  const terminal = senderFrames.find((frame) => frame.type === "opaque_dispatch_v1_receipt" && frame.receipt.status === "cancelled");
  assert.ok(terminal);
  assert.equal(senderFrames.at(-1)?.type, "opaque_dispatch_v1_cancel_result");
  manager.shutdown();
});

test("duplicate positive reservation result is idempotent", () => {
  const { manager, endpoints, senderFrames, receiverFrames, send } = harness();
  send();
  const offered = offer(receiverFrames);
  const result = { type: "opaque_dispatch_v1_reservation_result" as const, endpointEpoch: "receiver-epoch", reservationId: offered.reservationId, messageId: offered.messageId, decision: "reserved" as const };
  manager.handle(endpoints.get("receiver")!, result);
  manager.handle(endpoints.get("receiver")!, result);
  assert.equal(senderFrames.filter((frame) => frame.type === "opaque_dispatch_v1_ack").length, 1);
  assert.equal(senderFrames.filter((frame) => frame.type === "opaque_dispatch_v1_receipt" && frame.receipt.status === "reserved").length, 1);
  manager.shutdown();
});

test("receiver reservation reasons are restricted to consumer failures", () => {
  const { manager, endpoints, senderFrames, receiverFrames, send } = harness();
  send();
  const offered = offer(receiverFrames);
  manager.handle(endpoints.get("receiver")!, {
    type: "opaque_dispatch_v1_reservation_result",
    endpointEpoch: "receiver-epoch",
    reservationId: offered.reservationId,
    messageId: offered.messageId,
    decision: "refused",
    reason: "broker_epoch_changed",
  });
  const rejection = senderFrames.find((frame): frame is Extract<OpaqueDispatchBrokerFrame, { type: "opaque_dispatch_v1_rejected" }> => frame.type === "opaque_dispatch_v1_rejected");
  assert.equal(rejection?.code, "consumer_refused");
  const receipt = senderFrames.find((frame): frame is Extract<OpaqueDispatchBrokerFrame, { type: "opaque_dispatch_v1_receipt" }> => frame.type === "opaque_dispatch_v1_receipt");
  assert.equal(receipt?.receipt.reason, "consumer_refused");
  manager.shutdown();
});

test("foreign reservation mutation is ignored and request conflicts are typed", () => {
  const { manager, endpoints, senderFrames, receiverFrames, send } = harness();
  send();
  const offered = offer(receiverFrames);
  const foreign: OpaqueEndpoint = { sessionId: "foreign", endpointEpoch: "foreign-epoch", info: info("foreign"), extensions: receiverExtensions, connected: true, write: () => {} };
  manager.handle(foreign, { type: "opaque_dispatch_v1_reservation_result", endpointEpoch: "receiver-epoch", reservationId: offered.reservationId, messageId: offered.messageId, decision: "reserved" });
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
  manager.handle(endpoints.get("receiver")!, { type: "opaque_dispatch_v1_reservation_result", endpointEpoch: "receiver-epoch", reservationId: offered.reservationId, messageId: offered.messageId, decision: "reserved" });
  manager.handle(endpoints.get("receiver")!, { type: "opaque_dispatch_v1_claim", endpointEpoch: "receiver-epoch", operationId: "claim-one", reservationId: offered.reservationId, messageId: offered.messageId });
  manager.handle(endpoints.get("receiver")!, { type: "opaque_dispatch_v1_claim", endpointEpoch: "receiver-epoch", operationId: "claim-two", reservationId: offered.reservationId, messageId: offered.messageId });
  assert.equal(receiverFrames.filter((frame) => frame.type === "opaque_dispatch_v1_claim_result" && frame.claimed).length, 2);

  const foreignFrames: OpaqueDispatchBrokerFrame[] = [];
  const foreign: OpaqueEndpoint = {
    sessionId: "foreign",
    endpointEpoch: "foreign-epoch",
    info: info("foreign"),
    extensions: [{ namespace: "foreign/v1", ownerEligible: false, opaqueDispatch: { version: 1, roles: ["receive"] } }],
    connected: true,
    write: (frame) => foreignFrames.push(frame),
  };
  manager.handle(foreign, {
    type: "opaque_dispatch_v1_claim_status",
    endpointEpoch: "receiver-epoch",
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

test("claim status reports a changed broker epoch before consulting local history", () => {
  const { manager, endpoints, receiverFrames } = harness();
  manager.handle(endpoints.get("receiver")!, {
    type: "opaque_dispatch_v1_claim_status",
    endpointEpoch: "receiver-epoch",
    operationId: "changed-broker-status",
    recipientNamespace: "receiver/v1",
    brokerEpoch: "44444444-4444-4444-8444-444444444444",
    reservationId: "22222222-2222-4222-8222-222222222222",
    messageId: "11111111-1111-4111-8111-111111111111",
  });
  const result = receiverFrames.at(-1);
  assert.deepEqual(result?.type === "opaque_dispatch_v1_claim_status_result" ? result.result : undefined,
    { state: "indeterminate", code: "broker_epoch_changed" });
  manager.shutdown();
});

test("receipt replay includes only unacknowledged sequences", () => {
  const { manager, endpoints, senderFrames, receiverFrames, send } = harness();
  send();
  const offered = offer(receiverFrames);
  manager.handle(endpoints.get("receiver")!, {
    type: "opaque_dispatch_v1_reservation_result", endpointEpoch: "receiver-epoch",
    reservationId: offered.reservationId, messageId: offered.messageId, decision: "reserved",
  });
  manager.handle(endpoints.get("receiver")!, {
    type: "opaque_dispatch_v1_claim", endpointEpoch: "receiver-epoch", operationId: "claim-op",
    reservationId: offered.reservationId, messageId: offered.messageId,
  });
  manager.handle(endpoints.get("sender")!, {
    type: "opaque_dispatch_v1_receipt_ack", senderNamespace: "sender/v1", messageId: offered.messageId, sequence: 1,
  });
  senderFrames.length = 0;
  manager.endpointAvailable("sender");
  const replayed = senderFrames.filter((frame): frame is Extract<OpaqueDispatchBrokerFrame, { type: "opaque_dispatch_v1_receipt" }> => frame.type === "opaque_dispatch_v1_receipt");
  assert.deepEqual(replayed.map((frame) => frame.receipt.sequence), [2]);
  manager.handle(endpoints.get("sender")!, {
    type: "opaque_dispatch_v1_receipt_ack", senderNamespace: "sender/v1", messageId: offered.messageId, sequence: 2,
  });
  senderFrames.length = 0;
  manager.endpointAvailable("sender");
  assert.equal(senderFrames.some((frame) => frame.type === "opaque_dispatch_v1_receipt"), false);
  manager.shutdown();
});

test("reservation and claim deadlines fail closed with typed reasons", async () => {
  const reservationHarness = harness(true, { reservationTimeoutMs: 10, claimTimeoutMs: 1_000 });
  reservationHarness.send();
  await new Promise((resolve) => setTimeout(resolve, 25));
  const reservationTimeout = reservationHarness.senderFrames.find((frame): frame is Extract<OpaqueDispatchBrokerFrame, { type: "opaque_dispatch_v1_receipt" }> => frame.type === "opaque_dispatch_v1_receipt" && frame.receipt.status === "failed_closed");
  assert.equal(reservationTimeout?.receipt.reason, "reservation_timeout");
  reservationHarness.manager.shutdown();

  const claimHarness = harness(true, { reservationTimeoutMs: 1_000, claimTimeoutMs: 10 });
  claimHarness.send();
  const offered = offer(claimHarness.receiverFrames);
  claimHarness.manager.handle(claimHarness.endpoints.get("receiver")!, {
    type: "opaque_dispatch_v1_reservation_result", endpointEpoch: "receiver-epoch", reservationId: offered.reservationId, messageId: offered.messageId, decision: "reserved",
  });
  await new Promise((resolve) => setTimeout(resolve, 25));
  const claimTimeout = claimHarness.senderFrames.find((frame): frame is Extract<OpaqueDispatchBrokerFrame, { type: "opaque_dispatch_v1_receipt" }> => frame.type === "opaque_dispatch_v1_receipt" && frame.receipt.status === "failed_closed");
  assert.equal(claimTimeout?.receipt.reason, "claim_timeout");
  claimHarness.manager.shutdown();
});

test("capability invalidation and reservation rate limits fail closed", async () => {
  const capabilityHarness = harness();
  capabilityHarness.send();
  const capabilityOffer = offer(capabilityHarness.receiverFrames);
  capabilityHarness.manager.handle(capabilityHarness.endpoints.get("receiver")!, {
    type: "opaque_dispatch_v1_reservation_result", endpointEpoch: "receiver-epoch", reservationId: capabilityOffer.reservationId, messageId: capabilityOffer.messageId, decision: "reserved",
  });
  capabilityHarness.endpoints.get("receiver")!.extensions = [];
  capabilityHarness.manager.capabilityChanged("receiver");
  await new Promise((resolve) => setImmediate(resolve));
  const invalidated = capabilityHarness.senderFrames.find((frame): frame is Extract<OpaqueDispatchBrokerFrame, { type: "opaque_dispatch_v1_receipt" }> => frame.type === "opaque_dispatch_v1_receipt" && frame.receipt.status === "failed_closed");
  assert.equal(invalidated?.receipt.reason, "capability_invalidated");
  capabilityHarness.manager.shutdown();

  const rateHarness = harness();
  rateHarness.send();
  const rateOffer = offer(rateHarness.receiverFrames);
  rateHarness.manager.rateLimited(rateHarness.endpoints.get("receiver")!, {
    type: "opaque_dispatch_v1_reservation_result", endpointEpoch: "receiver-epoch", reservationId: rateOffer.reservationId, messageId: rateOffer.messageId, decision: "reserved",
  });
  const rateLimited = rateHarness.senderFrames.find((frame): frame is Extract<OpaqueDispatchBrokerFrame, { type: "opaque_dispatch_v1_receipt" }> => frame.type === "opaque_dispatch_v1_receipt" && frame.receipt.status === "failed_closed");
  assert.equal(rateLimited?.receipt.reason, "rate_limited");
  rateHarness.manager.shutdown();
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
      endpointEpoch: `${id}-epoch`,
      info: info(id),
      extensions: senderExtensions,
      connected: true,
      write: (frame) => senderFrames.push(frame),
    });
  }
  for (let index = 0; index < 9; index += 1) {
    const id = `receiver-${index}`;
    endpoints.set(id, { sessionId: id, endpointEpoch: `${id}-epoch`, info: info(id), extensions: receiverExtensions, connected: false });
  }
  const manager = new OpaqueDispatchManager({ brokerEpoch: "33333333-3333-4333-8333-333333333333", endpoint: (id) => endpoints.get(id), owner: () => undefined });
  for (let senderIndex = 0; senderIndex < 8; senderIndex += 1) {
    const origin = endpoints.get(`sender-${senderIndex}`)!;
    for (let recordIndex = 0; recordIndex < 32; recordIndex += 1) {
      manager.handle(origin, { type: "opaque_dispatch_v1_send", operationId: `op-${senderIndex}-${recordIndex}`, requestId: `request-${senderIndex}-${recordIndex}`, senderNamespace: "sender/v1", toSessionId: `receiver-${senderIndex}`, targetEpoch: `receiver-${senderIndex}-epoch`, recipientNamespace: "receiver/v1", payload: null });
    }
  }
  assert.equal(manager.activeCount, 256);
  const origin = endpoints.get("sender-8")!;
  manager.handle(origin, { type: "opaque_dispatch_v1_send", operationId: "boundary-op", requestId: "boundary-request", senderNamespace: "sender/v1", toSessionId: "receiver-8", targetEpoch: "receiver-8-epoch", recipientNamespace: "receiver/v1", payload: null });
  assert.equal(manager.activeCount, 256);
  assert.equal(senderFrames.some((frame) => frame.type === "opaque_dispatch_v1_ack" && frame.operationId === "boundary-op"), true);
  manager.shutdown();
});

test("capped principal cannot evict another principal at global capacity", () => {
  const senderFrames = new Map<string, OpaqueDispatchBrokerFrame[]>();
  const endpoints = new Map<string, OpaqueEndpoint>();
  for (let index = 0; index < 9; index += 1) {
    const id = `sender-${index}`;
    const frames: OpaqueDispatchBrokerFrame[] = [];
    senderFrames.set(id, frames);
    endpoints.set(id, { sessionId: id, endpointEpoch: `${id}-epoch`, info: info(id), extensions: senderExtensions, connected: true, write: (frame) => frames.push(frame) });
    endpoints.set(`receiver-${index}`, { sessionId: `receiver-${index}`, endpointEpoch: `receiver-${index}-epoch`, info: info(`receiver-${index}`), extensions: receiverExtensions, connected: false });
  }
  const manager = new OpaqueDispatchManager({ brokerEpoch: "33333333-3333-4333-8333-333333333333", endpoint: (id) => endpoints.get(id), owner: () => undefined });
  for (let senderIndex = 0; senderIndex < 8; senderIndex += 1) {
    for (let recordIndex = 0; recordIndex < 32; recordIndex += 1) {
      manager.handle(endpoints.get(`sender-${senderIndex}`)!, {
        type: "opaque_dispatch_v1_send", operationId: `op-${senderIndex}-${recordIndex}`, requestId: `request-${senderIndex}-${recordIndex}`,
        senderNamespace: "sender/v1", toSessionId: `receiver-${senderIndex}`, targetEpoch: `receiver-${senderIndex}-epoch`, recipientNamespace: "receiver/v1", payload: null,
      });
    }
  }
  const foreignBefore = senderFrames.get("sender-1")!.length;
  manager.handle(endpoints.get("sender-0")!, {
    type: "opaque_dispatch_v1_send", operationId: "capped-op", requestId: "capped-request", senderNamespace: "sender/v1",
    toSessionId: "receiver-8", targetEpoch: "receiver-8-epoch", recipientNamespace: "receiver/v1", payload: null,
  });
  assert.equal(manager.activeCount, 256);
  const rejection = senderFrames.get("sender-0")!.at(-1);
  assert.equal(rejection?.type === "opaque_dispatch_v1_rejected" ? rejection.code : undefined, "limit_exceeded");
  assert.equal(senderFrames.get("sender-1")!.length, foreignBefore);
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
    manager.handle(endpoints.get("receiver")!, { type: "opaque_dispatch_v1_reservation_result", endpointEpoch: "receiver-epoch", reservationId: offered.reservationId, messageId: offered.messageId, decision: "reserved" });
    manager.handle(endpoints.get("receiver")!, { type: "opaque_dispatch_v1_claim", endpointEpoch: "receiver-epoch", operationId: `claim-${index}`, reservationId: offered.reservationId, messageId: offered.messageId });
  }
  assert.equal(manager.tombstoneCount, MAX_OPAQUE_PRINCIPAL_TOMBSTONES);
  manager.shutdown();
});
