import test from "node:test";
import assert from "node:assert/strict";
import { MAX_FRAME_BYTES, createMessageReader, writeMessage } from "./framing.ts";

function framePayload(payload: Buffer): Buffer {
  const header = Buffer.alloc(4);
  header.writeUInt32BE(payload.length, 0);
  return Buffer.concat([header, payload]);
}

test("createMessageReader handles normal fragmented frames", () => {
  const messages: unknown[] = [];
  const errors: Error[] = [];
  const reader = createMessageReader(
    (message) => messages.push(message),
    (error) => errors.push(error),
    64,
  );
  const frameA = framePayload(Buffer.from(JSON.stringify({ type: "one" }), "utf-8"));
  const frameB = framePayload(Buffer.from(JSON.stringify({ type: "two" }), "utf-8"));
  const combined = Buffer.concat([frameA, frameB]);

  reader(combined.subarray(0, 2));
  reader(combined.subarray(2, 7));
  reader(combined.subarray(7));

  assert.deepEqual(messages, [{ type: "one" }, { type: "two" }]);
  assert.deepEqual(errors, []);
});

test("createMessageReader rejects an oversized declared frame", () => {
  const messages: unknown[] = [];
  const errors: Error[] = [];
  const reader = createMessageReader(
    (message) => messages.push(message),
    (error) => errors.push(error),
    8,
  );
  const oversizedFrame = framePayload(Buffer.from(JSON.stringify({ text: "too large" }), "utf-8"));

  reader(oversizedFrame);

  assert.deepEqual(messages, []);
  assert.equal(errors.length, 1);
  assert.match(errors[0].message, /Intercom frame length \d+ exceeds maximum 8 bytes/);
});

test("createMessageReader rejects an oversized frame before retaining same-chunk payload bytes", () => {
  const messages: unknown[] = [];
  const errors: Error[] = [];
  const reader = createMessageReader(
    (message) => messages.push(message),
    (error) => errors.push(error),
    8,
  );
  const header = Buffer.alloc(4);
  header.writeUInt32BE(9, 0);

  reader(Buffer.concat([header, Buffer.alloc(1024 * 1024)]));

  assert.deepEqual(messages, []);
  assert.equal(errors.length, 1);
  assert.equal(errors[0].message, "Intercom frame length 9 exceeds maximum 8 bytes");
});

test("createMessageReader rejects a partial oversized frame before buffering the payload", () => {
  const messages: unknown[] = [];
  const errors: Error[] = [];
  const reader = createMessageReader(
    (message) => messages.push(message),
    (error) => errors.push(error),
    8,
  );
  const header = Buffer.alloc(4);
  header.writeUInt32BE(9, 0);

  reader(header);

  assert.deepEqual(messages, []);
  assert.equal(errors.length, 1);
  assert.equal(errors[0].message, "Intercom frame length 9 exceeds maximum 8 bytes");
});

test("writeMessage emits frames accepted by createMessageReader", () => {
  const chunks: Buffer[] = [];
  const socket = { write: (chunk: Buffer) => chunks.push(chunk) };
  const messages: unknown[] = [];
  const reader = createMessageReader((message) => messages.push(message), assert.fail, 64);

  writeMessage(socket as never, { ok: true });
  reader(Buffer.concat(chunks));

  assert.deepEqual(messages, [{ ok: true }]);
});

test("writeMessage rejects oversized frames without writing bytes", () => {
  const chunks: Buffer[] = [];
  const socket = { write: (chunk: Buffer) => chunks.push(chunk) };
  const tooLarge = "x".repeat(MAX_FRAME_BYTES + 1 - Buffer.byteLength('{"text":""}', "utf-8"));

  assert.throws(
    () => writeMessage(socket as never, { text: tooLarge }),
    new RegExp(`Intercom frame length ${MAX_FRAME_BYTES + 1} exceeds maximum ${MAX_FRAME_BYTES} bytes`),
  );
  assert.equal(chunks.length, 0);
});

test("writeMessage allows frames exactly at the maximum", () => {
  const chunks: Buffer[] = [];
  const socket = { write: (chunk: Buffer) => chunks.push(chunk) };
  const exact = "x".repeat(MAX_FRAME_BYTES - Buffer.byteLength('{"text":""}', "utf-8"));

  writeMessage(socket as never, { text: exact });

  assert.equal(chunks.length, 1);
  assert.equal(chunks[0].readUInt32BE(0), MAX_FRAME_BYTES);
  assert.equal(chunks[0].length, 4 + MAX_FRAME_BYTES);
});

test("createMessageReader handles one-byte fragmentation across multiple max-sized frames", () => {
  const messages: unknown[] = [];
  const errors: Error[] = [];
  const frameA = framePayload(Buffer.from(JSON.stringify({ text: "a".repeat(32) }), "utf-8"));
  const frameB = framePayload(Buffer.from(JSON.stringify({ text: "b".repeat(32) }), "utf-8"));
  const combined = Buffer.concat([frameA, frameB]);
  const reader = createMessageReader(
    (message) => messages.push(message),
    (error) => errors.push(error),
    Math.max(frameA.length, frameB.length) - 4,
  );

  for (let i = 0; i < combined.length; i += 1) {
    reader(combined.subarray(i, i + 1));
  }

  assert.deepEqual(messages, [{ text: "a".repeat(32) }, { text: "b".repeat(32) }]);
  assert.deepEqual(errors, []);
});
