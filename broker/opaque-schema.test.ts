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
    { type: "opaque_dispatch_v1_send", operationId: "op", requestId: "request", senderNamespace: "sender/v1", toSessionId: "target", recipientNamespace: "receiver/v1", payload: { hidden: true } },
    { type: "opaque_dispatch_v1_cancel", operationId: "op", senderNamespace: "sender/v1", messageId },
    { type: "opaque_dispatch_v1_reservation_result", reservationId, messageId, decision: "reserved" },
    { type: "opaque_dispatch_v1_claim", operationId: "op", reservationId, messageId },
    { type: "opaque_dispatch_v1_fail", operationId: "op", reservationId, messageId, reason: "consumer_failed" },
    { type: "opaque_dispatch_v1_claim_status", operationId: "op", recipientNamespace: "receiver/v1", brokerEpoch, reservationId, messageId },
    { type: "opaque_dispatch_v1_peer_capability_get", operationId: "op", toSessionId: "target", recipientNamespace: "receiver/v1" },
    { type: "opaque_dispatch_v1_receipt_ack", senderNamespace: "sender/v1", messageId, sequence: 1 },
  ];
  for (const frame of frames) assert.equal(isOpaqueDispatchClientFrame(frame), true, frame.type);
  assert.equal(isOpaqueDispatchClientFrame({ ...frames[0], unexpected: true }), false);
  assert.equal(isOpaqueDispatchClientFrame({ ...frames[0], operationId: "x".repeat(129) }), false);
});

test("every opaque broker frame validates exact fields and bounds", () => {
  const frames = [
    { type: "opaque_dispatch_v1_ack", operationId: "op", requestId: "request", messageId, brokerEpoch, deliveryState: "live" },
    { type: "opaque_dispatch_v1_rejected", operationId: "op", code: "unsupported_target" },
    { type: "opaque_dispatch_v1_offer", reservationId, requestId: "request", messageId, attempt: 1, brokerEpoch, toSessionId: "target", recipientNamespace: "receiver/v1", sender: { sessionId: "origin", namespace: "sender/v1", trustedLocal: true }, payload: null, reserveBy: 1 },
    { type: "opaque_dispatch_v1_reservation_ended", messageId, reservationId, outcome: "cancelled" },
    { type: "opaque_dispatch_v1_receipt", senderNamespace: "sender/v1", receipt: { requestId: "request", messageId, status: "queued", at: 1, attempt: 1, sequence: 1 } },
    { type: "opaque_dispatch_v1_claim_result", operationId: "op", reservationId, messageId, claimed: true },
    { type: "opaque_dispatch_v1_fail_result", operationId: "op", reservationId, messageId, failedClosed: true },
    { type: "opaque_dispatch_v1_claim_status_result", operationId: "op", brokerEpoch, reservationId, messageId, result: { state: "indeterminate", code: "broker_epoch_changed" } },
    { type: "opaque_dispatch_v1_cancel_result", operationId: "op", messageId, cancelled: false, code: "already_claimed" },
    { type: "opaque_dispatch_v1_peer_capability_result", operationId: "op", toSessionId: "target", recipientNamespace: "receiver/v1", state: "present", version: 1 },
  ];
  for (const frame of frames) assert.equal(isOpaqueDispatchBrokerFrame(frame), true, frame.type);
  assert.equal(isOpaqueDispatchBrokerFrame({ ...frames[2], attempt: 9 }), false);
  assert.equal(isOpaqueDispatchBrokerFrame({ ...frames[4], receipt: { ...(frames[4] as { receipt: object }).receipt, sequence: 21 } }), false);
});
