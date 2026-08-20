import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import test from "node:test";
import { IntercomClient } from "./client.ts";
import { OPAQUE_DISPATCH_FEATURE } from "../types.ts";

function internals(client: IntercomClient) {
  return client as unknown as {
    _features: Set<string>;
    socket: { destroyed: boolean; writableEnded: boolean; writable: boolean; write(data: Uint8Array): boolean };
    _sessionId: string;
    handleBrokerMessage(message: unknown): void;
  };
}

test("opaque client refuses before write when broker capability is absent", async () => {
  const client = new IntercomClient();
  let writes = 0;
  const raw = internals(client);
  raw._sessionId = "sender";
  raw.socket = { destroyed: false, writableEnded: false, writable: true, write: () => { writes += 1; return true; } };
  await assert.rejects(client.sendOpaqueDispatch("sender/v1", { requestId: "request", toSessionId: "receiver", recipientNamespace: "receiver/v1", payload: null }), /unsupported_broker/);
  assert.equal(writes, 0);
});

test("opaque pending operations enforce a 32-per-namespace cap", async () => {
  const client = new IntercomClient();
  const raw = internals(client);
  raw._sessionId = "sender";
  raw._features = new Set([OPAQUE_DISPATCH_FEATURE]);
  const operationIds: string[] = [];
  raw.socket = {
    destroyed: false,
    writableEnded: false,
    writable: true,
    write: (data) => {
      const buffer = Buffer.from(data);
      const frame = JSON.parse(buffer.subarray(4, 4 + buffer.readUInt32BE(0)).toString("utf8")) as { operationId: string };
      operationIds.push(frame.operationId);
      return true;
    },
  };
  const pending = Array.from({ length: 32 }, (_, index) => client.sendOpaqueDispatch("sender/v1", {
    requestId: `request-${index}`,
    toSessionId: "receiver",
    recipientNamespace: "receiver/v1",
    payload: null,
  }));
  await assert.rejects(client.sendOpaqueDispatch("sender/v1", {
    requestId: "request-over-limit",
    toSessionId: "receiver",
    recipientNamespace: "receiver/v1",
    payload: null,
  }), /limit_exceeded/);
  operationIds.forEach((operationId, index) => raw.handleBrokerMessage({
    type: "opaque_dispatch_v1_rejected",
    operationId,
    requestId: `request-${index}`,
    code: "unsupported_target",
  }));
  await Promise.all(pending);
});

test("opaque pending operations enforce the 256 global cap", async () => {
  const client = new IntercomClient();
  const raw = internals(client);
  raw._sessionId = "sender";
  raw._features = new Set([OPAQUE_DISPATCH_FEATURE]);
  const operationIds: string[] = [];
  raw.socket = {
    destroyed: false,
    writableEnded: false,
    writable: true,
    write: (data) => {
      const buffer = Buffer.from(data);
      const frame = JSON.parse(buffer.subarray(4, 4 + buffer.readUInt32BE(0)).toString("utf8")) as { operationId: string };
      operationIds.push(frame.operationId);
      return true;
    },
  };
  const pending: Array<ReturnType<IntercomClient["sendOpaqueDispatch"]>> = [];
  for (let namespaceIndex = 0; namespaceIndex < 8; namespaceIndex += 1) {
    for (let operationIndex = 0; operationIndex < 32; operationIndex += 1) {
      pending.push(client.sendOpaqueDispatch(`sender/${namespaceIndex}`, {
        requestId: `request-${namespaceIndex}-${operationIndex}`,
        toSessionId: "receiver",
        recipientNamespace: "receiver/v1",
        payload: null,
      }));
    }
  }
  await assert.rejects(client.sendOpaqueDispatch("sender/overflow", {
    requestId: "request-over-global-limit",
    toSessionId: "receiver",
    recipientNamespace: "receiver/v1",
    payload: null,
  }), /limit_exceeded/);
  operationIds.forEach((operationId, index) => raw.handleBrokerMessage({
    type: "opaque_dispatch_v1_rejected",
    operationId,
    requestId: `settled-${index}`,
    code: "unsupported_target",
  }));
  await Promise.all(pending);
});

test("opaque claim result settles only the correlated operation", async () => {
  const client = new IntercomClient();
  const raw = internals(client);
  raw._sessionId = "receiver";
  raw._features = new Set([OPAQUE_DISPATCH_FEATURE]);
  const socket = new EventEmitter() as EventEmitter & { destroyed: boolean; writableEnded: boolean; writable: boolean; write(data: Uint8Array): boolean };
  socket.destroyed = false;
  socket.writableEnded = false;
  socket.writable = true;
  let operationId = "";
  socket.write = (data) => {
    const length = Buffer.from(data).readUInt32BE(0);
    const frame = JSON.parse(Buffer.from(data).subarray(4, 4 + length).toString("utf8")) as { operationId: string };
    operationId = frame.operationId;
    return true;
  };
  raw.socket = socket;
  const pending = client.claimOpaqueDispatch("receiver/v1", "11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222");
  raw.handleBrokerMessage({
    type: "opaque_dispatch_v1_claim_result",
    operationId,
    messageId: "11111111-1111-4111-8111-111111111111",
    reservationId: "22222222-2222-4222-8222-222222222222",
    claimed: true,
  });
  assert.deepEqual(await pending, { claimed: true });
});
