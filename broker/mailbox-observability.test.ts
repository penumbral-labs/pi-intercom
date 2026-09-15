import test from "node:test";
import assert from "node:assert/strict";
import { isMessageControl, isMessageReceipt, isSessionInfo } from "./protocol.ts";
import { BROKER_SESSION_ID, CORRELATED_OPERATIONS_FEATURE } from "../types.ts";

test("ordinary correlation feature and reserved broker identity are stable", () => {
  assert.equal(CORRELATED_OPERATIONS_FEATURE, "correlated-operations-v1");
  assert.equal(BROKER_SESSION_ID, "__pi_intercom_broker__");
  assert.equal(isSessionInfo({
    id: BROKER_SESSION_ID,
    name: "pi-intercom-broker",
    cwd: "",
    model: "broker",
    pid: 1,
    startedAt: 1,
    lastActivity: 1,
    status: "broker",
    trustedLocal: true,
  }), true);
});

test("message receipt validation accepts typed terminal mailbox failures", () => {
  assert.equal(isMessageReceipt({
    messageId: "message-1",
    status: "failed",
    timestamp: 1,
    code: "E_DELIVERY_TOO_LARGE",
    detail: "Mailbox message is too large after broker metadata was added",
  }), true);
});

test("message controls reject the retired detail field", () => {
  assert.equal(isMessageControl({
    messageId: "message-1",
    action: "cancel",
    timestamp: 1,
  }), true);
  assert.equal(isMessageControl({
    messageId: "message-1",
    action: "cancel",
    timestamp: 1,
    detail: "dead field",
  }), false);
});
