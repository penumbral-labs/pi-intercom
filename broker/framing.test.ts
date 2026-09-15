import test from "node:test";
import assert from "node:assert/strict";
import {
  createMessageReader,
  writeMessage,
  writeMessages,
  validateFrameBytes,
  MAX_FRAME_BYTES,
  IntercomFrameTooLargeError,
} from "./framing.ts";

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

test("createMessageReader reassembles a fragmented frame after a header-boundary fast-path frame", () => {
  const messages: unknown[] = [];
  const errors: Error[] = [];
  const reader = createMessageReader(
    (message) => messages.push(message),
    (error) => errors.push(error),
    64,
  );
  const frameA = framePayload(Buffer.from(JSON.stringify({ a: 1 }), "utf-8"));
  const frameB = framePayload(Buffer.from(JSON.stringify({ bb: "1234567890" }), "utf-8"));

  reader(frameA.subarray(0, 4)); // chunk ends exactly at the end of frame A's header
  reader(frameA.subarray(4)); // frame A's payload arrives whole (zero-copy fast path)
  reader(frameB.subarray(0, 6)); // frame B header + partial payload (buffered path)
  reader(frameB.subarray(6));

  assert.deepEqual(messages, [{ a: 1 }, { bb: "1234567890" }]);
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

test("writeMessage refuses an oversized frame before writing any bytes", () => {
  const chunks: Buffer[] = [];
  const socket = { write: (chunk: Buffer) => { chunks.push(chunk); return true; } };
  const envelope = Buffer.byteLength(JSON.stringify({ text: "" }), "utf-8");
  const tooLarge = "x".repeat(MAX_FRAME_BYTES - envelope + 1);

  assert.throws(
    () => writeMessage(socket as never, { text: tooLarge }),
    (error: unknown) => error instanceof IntercomFrameTooLargeError && error.length === MAX_FRAME_BYTES + 1,
  );
  assert.deepEqual(chunks, [], "no partial frame may reach the socket");
});

test("writeMessage still writes a frame exactly at the cap", () => {
  const chunks: Buffer[] = [];
  const socket = { write: (chunk: Buffer) => { chunks.push(chunk); return true; } };
  const envelope = Buffer.byteLength(JSON.stringify({ text: "" }), "utf-8");
  const exact = "x".repeat(MAX_FRAME_BYTES - envelope);

  writeMessage(socket as never, { text: exact });

  assert.equal(chunks.length, 1);
  assert.equal(chunks[0]!.readUInt32BE(0), MAX_FRAME_BYTES);
  assert.equal(chunks[0]!.length, 4 + MAX_FRAME_BYTES);
});

// A socket stand-in that records every write, so "zero bytes written" is directly observable.
function recordingSocket() {
  const writes: Buffer[] = [];
  return {
    writes,
    socket: {
      write(chunk: Buffer) {
        writes.push(Buffer.from(chunk));
        return true;
      },
    } as unknown as import("net").Socket,
  };
}

function frameBytes(payloadLength: number, declaredLength = payloadLength): Buffer {
  const buf = Buffer.alloc(4 + payloadLength, 0x61);
  buf.writeUInt32BE(declaredLength, 0);
  return buf;
}

test("validateFrameBytes rejects a declared length above the cap", () => {
  assert.throws(
    () => validateFrameBytes(frameBytes(16, MAX_FRAME_BYTES + 1)),
    (error: unknown) => error instanceof IntercomFrameTooLargeError && error.length === MAX_FRAME_BYTES + 1,
  );
});

test("validateFrameBytes rejects a forged small header over a large body", () => {
  // Header claims 8 bytes; the buffer actually carries 4096. A writer trusting the header would
  // emit the whole body, and the peer would read the surplus as the start of the next frame.
  assert.throws(() => validateFrameBytes(frameBytes(4096, 8)), /declares 8 bytes but buffer carries 4096/);
});

test("validateFrameBytes rejects a truncated body", () => {
  assert.throws(() => validateFrameBytes(frameBytes(16, 64)), /declares 64 bytes but buffer carries 16/);
});

test("validateFrameBytes rejects a buffer too short to hold a header", () => {
  assert.throws(() => validateFrameBytes(Buffer.alloc(3)), /truncated/);
});

test("validateFrameBytes accepts a frame exactly at the cap", () => {
  assert.doesNotThrow(() => validateFrameBytes(frameBytes(MAX_FRAME_BYTES)));
});

test("an oversize message writes zero bytes", () => {
  const { writes, socket } = recordingSocket();
  assert.throws(
    () => writeMessage(socket, { padding: "x".repeat(MAX_FRAME_BYTES) }),
    IntercomFrameTooLargeError,
  );
  assert.equal(writes.length, 0, "no write may occur when encoding fails");
});

test("writeMessages emits ordinary messages in order as one unit", () => {
  const { writes, socket } = recordingSocket();
  writeMessages(socket, { seq: 1 }, { seq: 2 });
  assert.equal(writes.length, 2);
  const decode = (buf: Buffer) => JSON.parse(buf.subarray(4).toString("utf-8"));
  assert.deepEqual(decode(writes[0]!), { seq: 1 });
  assert.deepEqual(decode(writes[1]!), { seq: 2 });
});

test("framing exports no raw or structural frame capability", async () => {
  const framing = await import("./framing.ts");
  for (const name of ["writeFrame", "writeEncodedFrames", "encodeFrame", "EncodedFrame"]) {
    assert.equal(name in framing, false, `${name} must not be exported`);
  }
  const exported = Object.keys(framing).filter((key) => /^write/.test(key)).sort();
  assert.deepEqual(exported, ["writeMessage", "writeMessages"]);
});

test("plain objects cannot forge frames through the ordinary-message sink", () => {
  const { writes, socket } = recordingSocket();
  const forgedBytes = frameBytes(16, MAX_FRAME_BYTES + 1);
  writeMessages(socket, { toValidatedBytes: () => forgedBytes });

  assert.equal(writes.length, 1);
  assert.notDeepEqual(writes[0], forgedBytes);
  assert.deepEqual(JSON.parse(writes[0]!.subarray(4).toString("utf-8")), {});
});

test("runtime constructors cannot mint writable frames", async () => {
  const framing = await import("./framing.ts");
  assert.equal((framing as Record<string, unknown>).EncodedFrame, undefined);
  assert.equal((framing as Record<string, unknown>).encodeFrame, undefined);
});

test("subclasses cannot override frame bytes through the ordinary-message sink", () => {
  const { writes, socket } = recordingSocket();
  const forgedBytes = frameBytes(16, MAX_FRAME_BYTES + 1);
  class ForgedFrame {
    toValidatedBytes(): Buffer {
      return forgedBytes;
    }
  }

  writeMessages(socket, new ForgedFrame());

  assert.equal(writes.length, 1);
  assert.notDeepEqual(writes[0], forgedBytes);
  assert.deepEqual(JSON.parse(writes[0]!.subarray(4).toString("utf-8")), {});
});

test("an oversized second message causes zero writes", () => {
  const { writes, socket } = recordingSocket();
  assert.throws(
    () => writeMessages(socket, { seq: 1 }, { padding: "x".repeat(MAX_FRAME_BYTES) }),
    IntercomFrameTooLargeError,
  );
  assert.equal(writes.length, 0, "a valid leading message must not be written when a later message is oversized");
});

test("a malformed second message causes zero writes", () => {
  const { writes, socket } = recordingSocket();
  const circular: { self?: unknown } = {};
  circular.self = circular;

  assert.throws(() => writeMessages(socket, { seq: 1 }, circular), TypeError);
  assert.equal(writes.length, 0, "a valid leading message must not be written when a later message cannot encode");
});
