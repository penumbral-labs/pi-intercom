import type { Socket } from "net";

export const MAX_FRAME_BYTES = 1024 * 1024;

/**
 * Write a length-prefixed message to a socket.
 * Format: 4-byte big-endian length + JSON payload
 */
export function writeMessage(socket: Socket, msg: unknown): void {
  const json = JSON.stringify(msg);
  const payload = Buffer.from(json, "utf-8");
  if (payload.length > MAX_FRAME_BYTES) {
    throw new Error(`Intercom frame length ${payload.length} exceeds maximum ${MAX_FRAME_BYTES} bytes`);
  }
  const header = Buffer.alloc(4);
  header.writeUInt32BE(payload.length, 0);
  socket.write(Buffer.concat([header, payload]));
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
  const header = Buffer.alloc(4);
  let headerOffset = 0;
  let payload: Buffer | null = null;
  let payloadOffset = 0;

  function resetFrame(): void {
    headerOffset = 0;
    payload = null;
    payloadOffset = 0;
  }

  function reportMessage(payloadBuffer: Buffer): boolean {
    let msg: unknown;
    try {
      msg = JSON.parse(payloadBuffer.toString("utf-8"));
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
      if (headerOffset < 4) {
        const headerBytes = Math.min(4 - headerOffset, data.length - offset);
        data.copy(header, headerOffset, offset, offset + headerBytes);
        headerOffset += headerBytes;
        offset += headerBytes;
        if (headerOffset < 4) {
          return;
        }

        const length = header.readUInt32BE(0);
        if (length > maxFrameBytes) {
          resetFrame();
          onError(new Error(`Intercom frame length ${length} exceeds maximum ${maxFrameBytes} bytes`));
          return;
        }
        payload = Buffer.alloc(length);
        payloadOffset = 0;
      }

      const activePayload = payload;
      if (!activePayload) {
        throw new Error("Intercom frame reader reached payload copy without an active payload");
      }

      const payloadBytes = Math.min(activePayload.length - payloadOffset, data.length - offset);
      if (payloadBytes > 0) {
        data.copy(activePayload, payloadOffset, offset, offset + payloadBytes);
        payloadOffset += payloadBytes;
        offset += payloadBytes;
      }

      if (payloadOffset < activePayload.length) {
        return;
      }

      resetFrame();
      if (!reportMessage(activePayload)) {
        return;
      }
    }
  };
}
