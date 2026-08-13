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
  const pending = client.claimOpaqueDispatch("11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222");
  raw.handleBrokerMessage({
    type: "opaque_dispatch_v1_claim_result",
    operationId,
    messageId: "11111111-1111-4111-8111-111111111111",
    reservationId: "22222222-2222-4222-8222-222222222222",
    claimed: true,
  });
  assert.deepEqual(await pending, { claimed: true });
});
