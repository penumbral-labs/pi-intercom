import type { Socket } from "net";

export const MAX_FRAME_BYTES = 1024 * 1024;

/**
 * Thrown by writeMessage when an encoded frame would exceed the wire cap.
 *
 * Distinguishable so callers can contain an oversize frame at the specific write site
 * instead of letting it reach the peer, whose reader would reject the length prefix and
 * tear down an otherwise healthy connection.
 */
export class IntercomFrameTooLargeError extends Error {
  constructor(readonly length: number, readonly maxFrameBytes: number = MAX_FRAME_BYTES) {
    super(`Intercom frame length ${length} exceeds maximum ${maxFrameBytes} bytes`);
    this.name = "IntercomFrameTooLargeError";
  }
}

// Validate that a buffer really is one whole legal frame: a 4-byte header, a declared length
// within the cap, and a body of exactly that length with nothing trailing. Pure, so it can be
// tested directly and reused without granting any ability to write.
//
// Exported as a predicate rather than as part of a writer on purpose. A validator cannot be
// used to put bytes on a socket, so exposing it opens no bypass of the frame cap.
export function validateFrameBytes(bytes: Buffer): void {
  if (bytes.length < 4) {
    throw new Error(`Intercom frame is truncated: ${bytes.length} bytes cannot contain a length header`);
  }
  const declaredLength = bytes.readUInt32BE(0);
  if (declaredLength > MAX_FRAME_BYTES) {
    throw new IntercomFrameTooLargeError(declaredLength);
  }
  if (bytes.length !== 4 + declaredLength) {
    throw new Error(
      `Intercom frame header declares ${declaredLength} bytes but buffer carries ${bytes.length - 4}`,
    );
  }
}

// One whole validated frame, ready to write.
//
// The bytes are private and the constructor is private, so encodeFrame is the only way to obtain
// one. That is the point: the writer accepts this type and nothing else, which makes an oversize
// or forged buffer unrepresentable at the module boundary instead of merely rejected inside it.
export class EncodedFrame {
  private constructor(private readonly bytes: Buffer) {}

  // Mint a frame, validating before the value exists at all. The buffer is retained by reference
  // rather than copied, so encodeFrame does not pay a second allocation for a buffer it just made
  // and exclusively owns. A caller that keeps its own reference can still mutate it afterwards,
  // which is why toValidatedBytes re-checks at write time.
  static from(bytes: Buffer): EncodedFrame {
    validateFrameBytes(bytes);
    return new EncodedFrame(bytes);
  }

  get byteLength(): number {
    return this.bytes.length;
  }

  // Re-checked at write time so even a frame built inside this module cannot reach a socket
  // malformed. Returns the bytes only after passing.
  toValidatedBytes(): Buffer {
    validateFrameBytes(this.bytes);
    return this.bytes;
  }
}

// Encode a message into a length-prefixed frame without touching a socket.
// Format: 4-byte big-endian length + JSON payload
//
// Throws IntercomFrameTooLargeError when the payload exceeds MAX_FRAME_BYTES. Separated from the
// write so a caller emitting several frames as one logical unit can validate all of them first
// and write only if every one is legal — otherwise a mid-sequence throw leaves the peer holding a
// partial sequence it cannot undo.
export function encodeFrame(msg: unknown): EncodedFrame {
  const json = JSON.stringify(msg);
  const payloadLength = Buffer.byteLength(json, "utf-8");
  if (payloadLength > MAX_FRAME_BYTES) {
    throw new IntercomFrameTooLargeError(payloadLength);
  }
  const frame = Buffer.allocUnsafe(4 + payloadLength);
  frame.writeUInt32BE(payloadLength, 0);
  frame.write(json, 4, payloadLength, "utf-8");
  return EncodedFrame.from(frame);
}

// Write pre-encoded frames in order, as one unit.
//
// Every frame is validated before the first byte is written, so a malformed frame anywhere in the
// sequence means zero writes rather than a peer left holding a partial sequence.
export function writeEncodedFrames(socket: Socket, ...frames: EncodedFrame[]): void {
  const validated = frames.map((frame) => frame.toValidatedBytes());
  for (const bytes of validated) {
    socket.write(bytes);
  }
}

// Write a length-prefixed message to a socket.
//
// Throws IntercomFrameTooLargeError before writing any bytes when the payload exceeds
// MAX_FRAME_BYTES, so a partial frame is never emitted.
export function writeMessage(socket: Socket, msg: unknown): void {
  writeEncodedFrames(socket, encodeFrame(msg));
}

/**
 * Create a message reader that handles partial reads.
 * Calls onMessage for each complete message received.
 * Protocol or handler errors are reported to onError so the caller can close the socket.
 */
export function createMessageReader(
  onMessage: (msg: unknown) => void,
  onError: (error: Error) => void,
  maxFrameBytes = MAX_FRAME_BYTES,
) {
  const header = Buffer.allocUnsafe(4);
  let headerBytes = 0;
  let payload: Buffer | null = null;
  let payloadBytes = 0;
  let payloadLength = 0;

  function reportMessage(framePayload: Buffer): boolean {
    let msg: unknown;
    try {
      msg = JSON.parse(framePayload.toString("utf-8"));
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      onError(new Error(`Failed to parse intercom message: ${message}`, { cause: error }));
      return false;
    }

    try {
      onMessage(msg);
      return true;
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      onError(new Error(`Failed to handle intercom message: ${message}`, { cause: error }));
      return false;
    }
  }

  return (data: Buffer) => {
    let offset = 0;

    while (offset < data.length) {
      if (headerBytes < 4) {
        const bytes = Math.min(4 - headerBytes, data.length - offset);
        data.copy(header, headerBytes, offset, offset + bytes);
        headerBytes += bytes;
        offset += bytes;
        if (headerBytes < 4) {
          return;
        }

        payloadLength = header.readUInt32BE(0);
        if (payloadLength > maxFrameBytes) {
          headerBytes = 0;
          onError(new Error(`Intercom frame length ${payloadLength} exceeds maximum ${maxFrameBytes} bytes`));
          return;
        }
      }

      // Fast path: the whole payload is already in this chunk, so parse it
      // in place without copying into a reassembly buffer.
      if (payloadBytes === 0 && data.length - offset >= payloadLength) {
        const framePayload = data.subarray(offset, offset + payloadLength);
        offset += payloadLength;
        headerBytes = 0;
        payload = null;
        payloadLength = 0;
        if (!reportMessage(framePayload)) {
          return;
        }
        continue;
      }

      // A buffer allocated for a previous frame (e.g. when a chunk ended
      // exactly on a header boundary) must not be reused for a frame of a
      // different size, so the reassembly buffer is size-checked, not just
      // presence-checked.
      if (payload === null || payload.length !== payloadLength) {
        payload = Buffer.allocUnsafe(payloadLength);
      }
      const bytes = Math.min(payloadLength - payloadBytes, data.length - offset);
      data.copy(payload, payloadBytes, offset, offset + bytes);
      payloadBytes += bytes;
      offset += bytes;

      if (payloadBytes < payloadLength) {
        return;
      }

      const framePayload = payload;
      headerBytes = 0;
      payload = null;
      payloadBytes = 0;
      payloadLength = 0;
      if (!reportMessage(framePayload)) {
        return;
      }
    }
  };
}
