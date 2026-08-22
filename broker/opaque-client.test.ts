import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import test from "node:test";
import { IntercomClient } from "./client.ts";
import { OPAQUE_DISPATCH_FEATURE } from "../types.ts";

function internals(client: IntercomClient) {
  return client as unknown as {
    _features: Set<string>;
    _endpointEpoch: string;
    socket: { destroyed: boolean; writableEnded: boolean; writable: boolean; write(data: Uint8Array): boolean };
    _sessionId: string;
    handleBrokerMessage(message: unknown): void;
  };
}

function decode(data: Uint8Array): Record<string, unknown> {
  const buffer = Buffer.from(data);
  return JSON.parse(buffer.subarray(4, 4 + buffer.readUInt32BE(0)).toString("utf8")) as Record<string, unknown>;
}

function installOpaqueSocket(client: IntercomClient, operationIds: string[]) {
  const raw = internals(client);
  raw._sessionId = "sender";
  raw._features = new Set([OPAQUE_DISPATCH_FEATURE]);
  raw.socket = {
    destroyed: false,
    writableEnded: false,
    writable: true,
    write: (data) => {
      const frame = decode(data);
      if (frame.type === "opaque_dispatch_v1_peer_capability_get") {
        queueMicrotask(() => raw.handleBrokerMessage({
          type: "opaque_dispatch_v1_peer_capability_result",
          operationId: frame.operationId,
          toSessionId: frame.toSessionId,
          recipientNamespace: frame.recipientNamespace,
          state: "present",
          version: 1,
          endpointEpoch: "receiver-epoch",
        }));
      } else if (typeof frame.operationId === "string") operationIds.push(frame.operationId);
      return true;
    },
  };
  return raw;
}

test("opaque client returns a typed error before write when broker capability is absent", async () => {
  const client = new IntercomClient();
  let writes = 0;
  const raw = internals(client);
  raw._sessionId = "sender";
  raw.socket = { destroyed: false, writableEnded: false, writable: true, write: () => { writes += 1; return true; } };
  assert.deepEqual(await client.sendOpaqueDispatch("sender/v1", {
    requestId: "request", toSessionId: "receiver", recipientNamespace: "receiver/v1", payload: null,
  }), { accepted: false, requestId: "request", code: "unsupported_broker" });
  assert.equal(writes, 0);
});

test("opaque pending operations enforce a 32-per-namespace cap", async () => {
  const client = new IntercomClient();
  const operationIds: string[] = [];
  const raw = installOpaqueSocket(client, operationIds);
  const pending = Array.from({ length: 32 }, (_, index) => client.sendOpaqueDispatch("sender/v1", {
    requestId: `request-${index}`, toSessionId: "receiver", recipientNamespace: "receiver/v1", payload: null,
  }));
  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(await client.sendOpaqueDispatch("sender/v1", {
    requestId: "request-over-limit", toSessionId: "receiver", recipientNamespace: "receiver/v1", payload: null,
  }), { accepted: false, requestId: "request-over-limit", code: "limit_exceeded" });
  operationIds.forEach((operationId, index) => raw.handleBrokerMessage({
    type: "opaque_dispatch_v1_rejected", operationId, requestId: `request-${index}`, code: "unsupported_target",
  }));
  await Promise.all(pending);
});

test("opaque pending operations enforce the 256 global cap", async () => {
  const client = new IntercomClient();
  const operationIds: string[] = [];
  const raw = installOpaqueSocket(client, operationIds);
  const pending: Array<ReturnType<IntercomClient["sendOpaqueDispatch"]>> = [];
  for (let namespaceIndex = 0; namespaceIndex < 8; namespaceIndex += 1) {
    for (let operationIndex = 0; operationIndex < 32; operationIndex += 1) {
      pending.push(client.sendOpaqueDispatch(`sender/${namespaceIndex}`, {
        requestId: `request-${namespaceIndex}-${operationIndex}`,
        toSessionId: "receiver", recipientNamespace: `receiver/${namespaceIndex}`, payload: null,
      }));
    }
  }
  while (operationIds.length < 256) await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(await client.sendOpaqueDispatch("sender/overflow", {
    requestId: "request-over-global-limit", toSessionId: "receiver", recipientNamespace: "receiver/v1", payload: null,
  }), { accepted: false, requestId: "request-over-global-limit", code: "limit_exceeded" });
  operationIds.forEach((operationId, index) => raw.handleBrokerMessage({
    type: "opaque_dispatch_v1_rejected", operationId, requestId: `settled-${index}`, code: "unsupported_target",
  }));
  await Promise.all(pending);
});

test("peer capability rejection settles immediately with its typed reason", async () => {
  const client = new IntercomClient();
  const raw = internals(client);
  raw._sessionId = "sender";
  raw._features = new Set([OPAQUE_DISPATCH_FEATURE]);
  raw.socket = {
    destroyed: false, writableEnded: false, writable: true,
    write: (data) => {
      const frame = decode(data);
      queueMicrotask(() => raw.handleBrokerMessage({
        type: "opaque_dispatch_v1_rejected", operationId: frame.operationId, code: "rate_limited",
      }));
      return true;
    },
  };

  await assert.rejects(
    client.peerCapability("receiver", "receiver/v1", { timeoutMs: 60_000 }),
    /rate_limited/,
  );
});

test("opaque send re-resolves one target rebound with the same request ID", async () => {
  const client = new IntercomClient();
  const raw = internals(client);
  raw._sessionId = "sender";
  raw._features = new Set([OPAQUE_DISPATCH_FEATURE]);
  const sends: Record<string, unknown>[] = [];
  let queryCount = 0;
  raw.socket = {
    destroyed: false, writableEnded: false, writable: true,
    write: (data) => {
      const frame = decode(data);
      if (frame.type === "opaque_dispatch_v1_peer_capability_get") {
        queryCount += 1;
        queueMicrotask(() => raw.handleBrokerMessage({
          type: "opaque_dispatch_v1_peer_capability_result", operationId: frame.operationId,
          toSessionId: frame.toSessionId, recipientNamespace: frame.recipientNamespace,
          state: "present", version: 1, endpointEpoch: `receiver-epoch-${queryCount}`,
        }));
      } else if (frame.type === "opaque_dispatch_v1_send") {
        sends.push(frame);
        queueMicrotask(() => raw.handleBrokerMessage(sends.length === 1 ? {
          type: "opaque_dispatch_v1_rejected", operationId: frame.operationId,
          requestId: frame.requestId, code: "target_rebound",
        } : {
          type: "opaque_dispatch_v1_ack", operationId: frame.operationId, requestId: frame.requestId,
          messageId: "11111111-1111-4111-8111-111111111111", brokerEpoch: "33333333-3333-4333-8333-333333333333", deliveryState: "live",
        }));
      }
      return true;
    },
  };
  const result = await client.sendOpaqueDispatch("sender/v1", {
    requestId: "stable-request", toSessionId: "receiver", recipientNamespace: "receiver/v1", payload: { private: true },
  });
  assert.equal(result.accepted, true);
  assert.deepEqual(sends.map((frame) => [frame.requestId, frame.targetEpoch]), [
    ["stable-request", "receiver-epoch-1"], ["stable-request", "receiver-epoch-2"],
  ]);
  assert.equal(queryCount, 2);
});

test("opaque cancel preserves a broker rejection code", async () => {
  const client = new IntercomClient();
  const operationIds: string[] = [];
  const raw = installOpaqueSocket(client, operationIds);
  const pending = client.cancelOpaqueDispatch("sender/v1", "11111111-1111-4111-8111-111111111111");
  assert.equal(operationIds.length, 1);
  raw.handleBrokerMessage({
    type: "opaque_dispatch_v1_rejected",
    operationId: operationIds[0],
    messageId: "11111111-1111-4111-8111-111111111111",
    code: "rate_limited",
  });
  assert.deepEqual(await pending, { cancelled: false, code: "rate_limited" });
});

test("opaque claim result settles only the correlated operation", async () => {
  const client = new IntercomClient();
  const raw = internals(client);
  raw._sessionId = "receiver";
  raw._endpointEpoch = "receiver-epoch";
  raw._features = new Set([OPAQUE_DISPATCH_FEATURE]);
  const socket = new EventEmitter() as EventEmitter & { destroyed: boolean; writableEnded: boolean; writable: boolean; write(data: Uint8Array): boolean };
  socket.destroyed = false;
  socket.writableEnded = false;
  socket.writable = true;
  let operationId = "";
  socket.write = (data) => {
    operationId = decode(data).operationId as string;
    return true;
  };
  raw.socket = socket;
  const pending = client.claimOpaqueDispatch("receiver/v1", "11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222");
  raw.handleBrokerMessage({
    type: "opaque_dispatch_v1_claim_result", operationId,
    messageId: "11111111-1111-4111-8111-111111111111",
    reservationId: "22222222-2222-4222-8222-222222222222", claimed: true,
  });
  assert.deepEqual(await pending, { claimed: true });
});
