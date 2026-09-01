import test from "node:test";
import assert from "node:assert/strict";
import { IntercomClient, MAX_POISONED_LEGACY_MESSAGE_IDS } from "./client.ts";

test("validated session lifecycle messages reach broker-message subscribers", () => {
  const client = new IntercomClient();
  (client as any)._sessionId = "session-1";
  const received: unknown[] = [];
  client.onBrokerMessage((message) => received.push(message));
  const session = {
    id: "session-2",
    cwd: "/test",
    model: "test",
    pid: 2,
    startedAt: 1,
    lastActivity: 1,
  };

  (client as any).handleBrokerMessage({ type: "session_joined", session });
  (client as any).handleBrokerMessage({ type: "presence_update", session });
  (client as any).handleBrokerMessage({ type: "session_left", sessionId: "session-2" });

  assert.deepEqual(received, [
    { type: "session_joined", session },
    { type: "presence_update", session },
    { type: "session_left", sessionId: "session-2" },
  ]);
});

test("registered feature negotiation rejects non-string feature entries", () => {
  const client = new IntercomClient();
  assert.throws(
    () => (client as any).handleBrokerMessage({ type: "registered", sessionId: "session-1", features: ["valid", 123] }),
    /Invalid registered features/,
  );
});

test("malformed extension broker messages are rejected", () => {
  const client = new IntercomClient();
  (client as any)._sessionId = "session-1";

  assert.throws(
    () => (client as any).handleBrokerMessage({ type: "extension_owner", namespace: "test/v1", ownerId: "owner" }),
    /Invalid extension_owner/,
  );
  assert.throws(
    () => (client as any).handleBrokerMessage({ type: "extension_owner", namespace: "test/v1", ownerEpoch: "epoch" }),
    /Invalid extension_owner/,
  );
  assert.throws(
    () => (client as any).handleBrokerMessage({ type: "extension_message", namespace: "test/v1" }),
    /Invalid extension_message/,
  );
  assert.throws(
    () => (client as any).handleBrokerMessage({ type: "extension_state", namespace: "test/v1", revision: -1 }),
    /Invalid extension_state/,
  );
  assert.throws(
    () => (client as any).handleBrokerMessage({ type: "extension_state_result", namespace: "test/v1", committed: "yes", revision: 1 }),
    /Invalid extension_state_result/,
  );
  assert.doesNotThrow(() => (client as any).handleBrokerMessage({
    type: "extension_message",
    namespace: "test/v1",
    fromSessionId: "session-2",
    payload: { peerOnly: true },
  }));
});

test("atomic supersede messages emit control before replacement", () => {
  const client = new IntercomClient();
  (client as any)._sessionId = "session-1";
  const events: string[] = [];
  client.onMessageControl(() => events.push("control"));
  client.on("message", () => events.push("message"));
  const from = {
    id: "session-2", cwd: "/test", model: "test", pid: 2, startedAt: 1, lastActivity: 1,
  };
  const message = { id: "replacement", timestamp: 2, supersedes: "original", content: { text: "new" } };

  (client as any).handleBrokerMessage({
    type: "message",
    from,
    control: { action: "supersede", messageId: "original", supersededBy: "replacement", timestamp: 2 },
    message,
  });

  assert.deepEqual(events, ["control", "message"]);
  assert.throws(
    () => (client as any).handleBrokerMessage({
      type: "message",
      from,
      control: { action: "supersede", messageId: "different-original", supersededBy: "replacement", timestamp: 2 },
      message,
    }),
    /Invalid message event/,
  );
});

test("correlated operation results settle only their exact waiter", () => {
  const client = new IntercomClient();
  (client as any)._sessionId = "session-1";
  const resolved: string[] = [];
  (client as any).pendingOperations.set("send-operation", {
    messageId: "shared-message",
    resolve: () => resolved.push("send"),
    reject: () => undefined,
  });
  (client as any).pendingOperations.set("cancel-operation", {
    messageId: "shared-message",
    resolve: () => resolved.push("cancel"),
    reject: () => undefined,
  });

  (client as any).handleBrokerMessage({
    type: "delivered",
    messageId: "shared-message",
    operationId: "cancel-operation",
  });

  assert.deepEqual(resolved, ["cancel"]);
  assert.equal((client as any).pendingOperations.has("send-operation"), true);
});

test("late legacy results consume poison instead of settling a later operation", () => {
  const client = new IntercomClient();
  (client as any)._sessionId = "session-1";
  const resolved: string[] = [];
  (client as any).poisonedLegacyMessageIds.add("shared-message");
  (client as any).pendingOperations.set("shared-message", {
    messageId: "shared-message",
    resolve: () => resolved.push("later"),
    reject: () => undefined,
  });
  (client as any).legacyOperations.set("shared-message", "shared-message");

  (client as any).handleBrokerMessage({ type: "delivered", messageId: "shared-message" });

  assert.deepEqual(resolved, []);
  assert.equal((client as any).poisonedLegacyMessageIds.has("shared-message"), false);
  assert.equal((client as any).pendingOperations.has("shared-message"), true);
});

test("legacy timeout poison is bounded to the newest message IDs", () => {
  const client = new IntercomClient();

  for (let index = 0; index <= MAX_POISONED_LEGACY_MESSAGE_IDS; index += 1) {
    (client as any).poisonLegacyMessageId(`message-${index}`);
  }

  assert.equal((client as any).poisonedLegacyMessageIds.size, MAX_POISONED_LEGACY_MESSAGE_IDS);
  assert.equal((client as any).poisonedLegacyMessageIds.has("message-0"), false);
  assert.equal((client as any).poisonedLegacyMessageIds.has(`message-${MAX_POISONED_LEGACY_MESSAGE_IDS}`), true);
});

test("cancelAsk ignores synchronous socket write failures", () => {
  const client = new IntercomClient();
  (client as any)._sessionId = "session-1";
  (client as any).socket = {
    destroyed: false,
    writableEnded: false,
    writable: true,
    write() {
      throw new Error("write failed");
    },
  };

  assert.doesNotThrow(() => client.cancelAsk("ask-1"));
});
