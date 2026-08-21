import assert from "node:assert/strict";
import test from "node:test";
import {
  isExtensionCapability,
  isExtensionStateSnapshot,
  isOpaqueDispatchBrokerFrame,
  isOpaqueDispatchClientFrame,
} from "./protocol.ts";

const messageId = "11111111-1111-4111-8111-111111111111";
const reservationId = "22222222-2222-4222-8222-222222222222";
const brokerEpoch = "33333333-3333-4333-8333-333333333333";

test("opaque capabilities enforce unique roles and receive advertisement shape", () => {
  assert.equal(isExtensionCapability({
    namespace: "sample/v1",
    ownerEligible: false,
    opaqueDispatch: { version: 1, roles: ["send", "receive"] },
  }), true);
  assert.equal(isExtensionCapability({
    namespace: "sample/v1",
    ownerEligible: false,
    opaqueDispatch: { version: 1, roles: ["send", "send"] },
  }), false);
  assert.equal(isExtensionCapability({
    namespace: "sample/v1",
    ownerEligible: false,
    opaqueDispatch: { version: 1, roles: [] },
  }), false);
});

test("extension state snapshots are discriminated and exact", () => {
  assert.equal(isExtensionStateSnapshot({ namespace: "sample/v1", revision: 0, present: false }), true);
  assert.equal(isExtensionStateSnapshot({ namespace: "sample/v1", revision: 1, present: true, payload: null }), true);
  assert.equal(isExtensionStateSnapshot({ namespace: "sample/v1", revision: 0, present: false, payload: null }), false);
  assert.equal(isExtensionStateSnapshot({ namespace: "sample/v1", revision: 0, present: true, payload: null }), false);
});

test("every opaque client frame validates exact fields and bounds", () => {
  const frames = [
    { type: "opaque_dispatch_v1_send", operationId: "op", requestId: "request", senderNamespace: "sender/v1", toSessionId: "target", targetEpoch: "target-epoch", recipientNamespace: "receiver/v1", payload: { hidden: true } },
    { type: "opaque_dispatch_v1_cancel", operationId: "op", senderNamespace: "sender/v1", messageId },
    { type: "opaque_dispatch_v1_reservation_result", endpointEpoch: "target-epoch", reservationId, messageId, decision: "reserved" },
    { type: "opaque_dispatch_v1_claim", operationId: "op", endpointEpoch: "target-epoch", reservationId, messageId },
    { type: "opaque_dispatch_v1_fail", operationId: "op", endpointEpoch: "target-epoch", reservationId, messageId, reason: "consumer_failed" },
    { type: "opaque_dispatch_v1_claim_status", operationId: "op", recipientNamespace: "receiver/v1", brokerEpoch, endpointEpoch: "target-epoch", reservationId, messageId },
    { type: "opaque_dispatch_v1_peer_capability_get", operationId: "op", toSessionId: "target", recipientNamespace: "receiver/v1" },
    { type: "opaque_dispatch_v1_receipt_ack", senderNamespace: "sender/v1", messageId, sequence: 1 },
  ];
  for (const frame of frames) assert.equal(isOpaqueDispatchClientFrame(frame), true, frame.type);
  assert.equal(isOpaqueDispatchClientFrame({ ...frames[0], unexpected: true }), false);
  const { targetEpoch: _targetEpoch, ...sendWithoutEpoch } = frames[0]!;
  assert.equal(isOpaqueDispatchClientFrame(sendWithoutEpoch), false);
  assert.equal(isOpaqueDispatchClientFrame({ ...frames[0], operationId: "x".repeat(129) }), false);
  assert.equal(isOpaqueDispatchClientFrame({ ...frames[2], decision: "refused", reason: "consumer_refused" }), true);
  assert.equal(isOpaqueDispatchClientFrame({ ...frames[2], decision: "failed_closed", reason: "consumer_unloaded" }), true);
  assert.equal(isOpaqueDispatchClientFrame({ ...frames[2], decision: "reserved", reason: "consumer_failed" }), false);
  assert.equal(isOpaqueDispatchClientFrame({ ...frames[2], decision: "refused", reason: "broker_epoch_changed" }), false);
});

test("every opaque broker frame validates exact fields and bounds", () => {
  const frames = [
    { type: "opaque_dispatch_v1_ack", operationId: "op", requestId: "request", messageId, brokerEpoch, deliveryState: "live" },
    { type: "opaque_dispatch_v1_rejected", operationId: "op", code: "unsupported_target" },
    { type: "opaque_dispatch_v1_offer", reservationId, requestId: "request", messageId, attempt: 1, brokerEpoch, endpointEpoch: "target-epoch", toSessionId: "target", recipientNamespace: "receiver/v1", sender: { sessionId: "origin", namespace: "sender/v1", trustedLocal: true }, payload: null, reserveBy: 1 },
    { type: "opaque_dispatch_v1_reservation_ended", messageId, reservationId, outcome: "cancelled" },
    ...(["queued", "reserved", "claimed", "refused", "expired", "cancelled", "superseded", "failed_closed"] as const).map((status, index) => ({
      type: "opaque_dispatch_v1_receipt",
      senderNamespace: "sender/v1",
      receipt: { requestId: "request", messageId, status, at: 1, attempt: 1, sequence: index + 1 },
    })),
    { type: "opaque_dispatch_v1_claim_result", operationId: "op", reservationId, messageId, claimed: true },
    { type: "opaque_dispatch_v1_fail_result", operationId: "op", reservationId, messageId, failedClosed: true },
    { type: "opaque_dispatch_v1_claim_status_result", operationId: "op", brokerEpoch, reservationId, messageId, result: { state: "indeterminate", code: "broker_epoch_changed" } },
    { type: "opaque_dispatch_v1_cancel_result", operationId: "op", messageId, cancelled: false, code: "already_claimed" },
    { type: "opaque_dispatch_v1_peer_capability_result", operationId: "op", toSessionId: "target", recipientNamespace: "receiver/v1", state: "present", version: 1, endpointEpoch: "target-epoch" },
  ];
  for (const frame of frames) assert.equal(isOpaqueDispatchBrokerFrame(frame), true, frame.type);
  assert.equal(isOpaqueDispatchBrokerFrame({ ...frames[2], attempt: 9 }), false);
  const receipt = frames.find((frame) => frame.type === "opaque_dispatch_v1_receipt") as { receipt: object };
  assert.equal(isOpaqueDispatchBrokerFrame({ ...receipt, receipt: { ...receipt.receipt, sequence: 21 } }), false);
});
