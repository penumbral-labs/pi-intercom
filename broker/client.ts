import { EventEmitter } from "events";
import net from "net";
import { randomUUID } from "crypto";
import { writeMessage, createMessageReader } from "./framing.ts";
import { getBrokerConnectTarget, type BrokerConnectTarget } from "./paths.ts";
import {
  isExtensionStateSnapshot,
  isMessage,
  isMessageControl,
  isMessageReceipt,
  isOpaqueDispatchBrokerFrame,
  isOpaqueDispatchReason,
  isSessionInfo,
} from "./protocol.ts";
import {
  ATOMIC_SUPERSEDE_FEATURE,
  CORRELATED_OPERATIONS_FEATURE,
  EXACT_SEND_FEATURE,
  EXTENSION_BUS_FEATURE,
  EXTENSION_STATE_REFRESH_FEATURE,
  OPAQUE_DISPATCH_FEATURE,
} from "../types.ts";
import type {
  Attachment,
  BrokerMessage,
  ClientMessage,
  Message,
  MessageControl,
  MessageReceipt,
  ExtensionStateSnapshot,
  OpaqueDispatchBrokerFrame,
  OpaqueDispatchClientFrame,
  OpaqueDispatchReason,
  DeliveryDetails,
  SessionInfo,
  SessionRegistration,
} from "../types.ts";

interface SendOptions {
  text: string;
  attachments?: Attachment[];
  replyTo?: string;
  expectsReply?: boolean;
  messageId?: string;
  supersedes?: string;
  retryOf?: string;
}

export interface SendResult extends DeliveryDetails {
  id: string;
  delivered: boolean;
  reason?: string;
}

interface PendingRequest<T> {
  namespace?: string;
  resolve: (result: T) => void;
  reject: (error: Error) => void;
}

interface PendingOperation {
  messageId: string;
  resolve: (result: SendResult) => void;
  reject: (error: Error) => void;
}

const MAX_PENDING_OPERATIONS = 256;
const MAX_PENDING_OPERATIONS_PER_NAMESPACE = 32;
export const MAX_POISONED_LEGACY_MESSAGE_IDS = 256;

export class IntercomListSessionsError extends Error {
  constructor(readonly code: "response_too_large", message: string) {
    super(message);
    this.name = "IntercomListSessionsError";
  }
}

function namespacePendingCount<T>(pending: Map<string, PendingRequest<T>>, namespace: string): number {
  let count = 0;
  for (const request of pending.values()) if (request.namespace === namespace) count += 1;
  return count;
}

function toError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

/**
 * Liveness heartbeat interval. A half-open socket (peer killed with SIGKILL or
 * crashed without sending a FIN) stays "writable" indefinitely, so passive
 * close-event detection never fires and the client silently drops out of the
 * roster. The heartbeat actively round-trips a lightweight request and tears
 * down the socket if the broker does not respond within the timeout, letting
 * the existing onClose -> "disconnected" path drive reconnection.
 */
function getLivenessIntervalMs(): number {
  const raw = Number.parseInt(process.env.PI_INTERCOM_LIVENESS_INTERVAL_MS ?? "", 10);
  return Number.isFinite(raw) && raw > 0 ? raw : 30_000;
}

function getLivenessTimeoutMs(): number {
  const raw = Number.parseInt(process.env.PI_INTERCOM_LIVENESS_TIMEOUT_MS ?? "", 10);
  return Number.isFinite(raw) && raw > 0 ? Math.min(raw, getLivenessIntervalMs()) : 5_000;
}

function connectToBrokerTarget(target: BrokerConnectTarget): net.Socket {
  return typeof target === "string"
    ? net.connect(target)
    : net.connect({ host: target.host, port: target.port });
}

export class IntercomClient extends EventEmitter {
  private socket: net.Socket | null = null;
  private _sessionId: string | null = null;
  private _features = new Set<string>();
  private _brokerEpoch: string | null = null;
  private _endpointEpoch: string | null = null;
  private pendingOperations = new Map<string, PendingOperation>();
  private legacyOperations = new Map<string, string>();
  private poisonedLegacyMessageIds = new Set<string>();
  private pendingLists = new Map<string, PendingRequest<SessionInfo[]>>();
  private pendingStateRefreshes = new Map<string, PendingRequest<ExtensionStateSnapshot>>();
  private pendingPeerQueries = new Map<string, PendingRequest<Extract<OpaqueDispatchBrokerFrame, { type: "opaque_dispatch_v1_peer_capability_result" }>>>();
  private pendingOpaque = new Map<string, PendingRequest<OpaqueDispatchBrokerFrame>>();
  private nextSenderSequence = 1;
  private disconnecting = false;
  private disconnectError: Error | null = null;
  private livenessTimer: NodeJS.Timeout | null = null;
  private livenessInFlight = false;

  private failPending(error: Error): void {
    for (const pending of this.pendingOperations.values()) {
      pending.reject(error);
    }
    this.pendingOperations.clear();
    this.legacyOperations.clear();
    this.poisonedLegacyMessageIds.clear();
    for (const pending of this.pendingLists.values()) {
      pending.reject(error);
    }
    this.pendingLists.clear();
    for (const pending of this.pendingStateRefreshes.values()) pending.reject(error);
    this.pendingStateRefreshes.clear();
    for (const pending of this.pendingPeerQueries.values()) pending.reject(error);
    this.pendingPeerQueries.clear();
    for (const pending of this.pendingOpaque.values()) pending.reject(error);
    this.pendingOpaque.clear();
  }

  get sessionId(): string | null {
    return this._sessionId;
  }

  get brokerEpoch(): string | null {
    return this._brokerEpoch;
  }

  get endpointEpoch(): string | null {
    return this._endpointEpoch;
  }

  supportsFeature(feature: string): boolean {
    return this._features.has(feature);
  }

  isConnected(): boolean {
    const socket = this.socket;
    return Boolean(socket && this._sessionId && !this.disconnecting && !socket.destroyed && !socket.writableEnded && socket.writable);
  }

  /**
   * Start the liveness heartbeat. Must be called once the connection is
   * registered. The heartbeat periodically round-trips a lightweight list
   * request and tears down the socket if the broker does not respond within
   * the liveness timeout, so a half-open connection is detected within a
   * bounded window instead of silently lingering forever.
   */
  private startLivenessHeartbeat(): void {
    this.stopLivenessHeartbeat();
    this.livenessTimer = setInterval(() => {
      this.runLivenessProbe();
    }, getLivenessIntervalMs());
    this.livenessTimer.unref?.();
  }

  private stopLivenessHeartbeat(): void {
    if (this.livenessTimer) {
      clearInterval(this.livenessTimer);
      this.livenessTimer = null;
    }
    this.livenessInFlight = false;
  }

  private async runLivenessProbe(): Promise<void> {
    if (this.livenessInFlight || !this.isConnected()) {
      return;
    }
    this.livenessInFlight = true;
    try {
      await this.listSessions({ timeoutMs: getLivenessTimeoutMs() });
    } catch (error) {
      // A correlated application refusal proves the broker is responsive; only transport-style
      // failures should tear down the socket and drive reconnection.
      if (error instanceof IntercomListSessionsError) return;
      const socket = this.socket;
      if (socket && !socket.destroyed) {
        this.disconnectError = toError(error);
        socket.destroy();
      }
    } finally {
      this.livenessInFlight = false;
    }
  }

  private requireActiveSocket(): net.Socket {
    if (this.disconnecting) {
      throw new Error("Client disconnecting");
    }

    const socket = this.socket;
    if (!socket || !this._sessionId) {
      throw new Error("Not connected");
    }

    if (socket.destroyed || socket.writableEnded || !socket.writable) {
      throw new Error("Client disconnected");
    }

    return socket;
  }

  connect(session: SessionRegistration, sessionId?: string): Promise<void> {
    if (this.socket) {
      return Promise.reject(new Error("Already connected"));
    }

    return new Promise((resolve, reject) => {
      let socket: net.Socket;
      let target: BrokerConnectTarget;
      try {
        target = getBrokerConnectTarget();
        socket = connectToBrokerTarget(target);
      } catch (error) {
        reject(toError(error));
        return;
      }
      this.socket = socket;
      this.disconnectError = null;
      let settled = false;
      const timeout = setTimeout(() => {
        if (!this._sessionId) {
          cleanupConnectionAttempt();
          cleanupSocketListeners();
          if (this.socket === socket) {
            this.socket = null;
          }
          socket.destroy();
          reject(new Error("Connection timeout"));
        }
      }, 10000);
      
      let connectionEstablished = false;
      
      const onRegistered = () => {
        settled = true;
        connectionEstablished = true;
        cleanupConnectionAttempt();
        this.startLivenessHeartbeat();
        resolve();
      };
      
      const onError = (err: Error) => {
        settled = true;
        cleanupConnectionAttempt();
        cleanupSocketListeners();
        if (this.socket === socket) {
          this.socket = null;
        }
        socket.destroy();
        reject(err);
      };
      
      const onClose = () => {
        const wasConnecting = !settled && !this._sessionId;
        const wasDisconnecting = this.disconnecting;
        const disconnectError = this.disconnectError ?? new Error("Client disconnected");
        this.disconnecting = false;
        this.stopLivenessHeartbeat();
        cleanupConnectionAttempt();
        cleanupSocketListeners();
        this.failPending(disconnectError);
        if (this.socket === socket) {
          this.socket = null;
        }
        this._sessionId = null;
        this._features.clear();
        this._brokerEpoch = null;
        this._endpointEpoch = null;
        this.disconnectError = null;
        if (connectionEstablished && !wasDisconnecting) {
          this.emit("disconnected", disconnectError);
        }
        if (wasConnecting) {
          reject(new Error("Connection closed before registration"));
        }
      };

      const onSocketError = (err: Error) => {
        if (connectionEstablished) {
          this.disconnectError = err;
          this.emit("error", err);
          // A socket error after registration means the connection is dead.
          // Destroy the socket so onClose fires and emits "disconnected",
          // driving the extension's reconnect path. Without this, a half-open
          // socket can linger with isConnected() returning true.
          if (!socket.destroyed) {
            socket.destroy();
          }
        }
      };

      const onReaderError = (error: Error) => {
        const protocolError = new Error(`Intercom protocol error: ${error.message}`, { cause: error });
        if (!connectionEstablished) {
          onError(protocolError);
          return;
        }
        this.disconnectError = protocolError;
        this.emit("error", protocolError);
        socket.destroy();
      };

      const reader = createMessageReader((msg) => {
        this.handleBrokerMessage(msg);
      }, onReaderError);
      
      const cleanupConnectionAttempt = () => {
        this.off("_registered", onRegistered);
        socket.off("error", onError);
        clearTimeout(timeout);
      };

      const cleanupSocketListeners = () => {
        socket.off("data", reader);
        socket.off("error", onSocketError);
        socket.off("close", onClose);
      };
      
      socket.on("data", reader);
      socket.on("error", onError);
      socket.on("close", onClose);
      
      socket.on("error", onSocketError);
      this.once("_registered", onRegistered);
      
      try {
        writeMessage(socket, {
          type: "register",
          session,
          ...(sessionId ? { sessionId } : {}),
          features: [ATOMIC_SUPERSEDE_FEATURE],
          ...(typeof target === "string" ? {} : { stateId: target.stateId }),
        });
      } catch (error) {
        cleanupConnectionAttempt();
        cleanupSocketListeners();
        if (this.socket === socket) {
          this.socket = null;
        }
        socket.destroy();
        reject(toError(error));
      }
    });
  }

  private handleBrokerMessage(msg: unknown): void {
    if (typeof msg !== "object" || msg === null || !("type" in msg) || typeof msg.type !== "string") {
      throw new Error("Invalid broker message");
    }

    const brokerMessage = msg as { type: string } & Record<string, unknown>;

    if (this._sessionId === null && brokerMessage.type !== "registered" && brokerMessage.type !== "error") {
      throw new Error(`Received ${brokerMessage.type} before registered`);
    }

    switch (brokerMessage.type) {
      case "registered": {
        if (typeof brokerMessage.sessionId !== "string") {
          throw new Error("Invalid registered message");
        }

        if (this._sessionId !== null) {
          throw new Error("Received duplicate registered message");
        }

        if (
          brokerMessage.features !== undefined
          && (!Array.isArray(brokerMessage.features) || !brokerMessage.features.every((feature) => typeof feature === "string"))
        ) {
          throw new Error("Invalid registered features");
        }

        if (brokerMessage.brokerEpoch !== undefined && typeof brokerMessage.brokerEpoch !== "string") {
          throw new Error("Invalid registered brokerEpoch");
        }
        if (brokerMessage.endpointEpoch !== undefined && typeof brokerMessage.endpointEpoch !== "string") {
          throw new Error("Invalid registered endpointEpoch");
        }
        const advertisedFeatures = new Set((brokerMessage.features as string[] | undefined) ?? []);
        if (advertisedFeatures.has(OPAQUE_DISPATCH_FEATURE)
          && (typeof brokerMessage.brokerEpoch !== "string" || typeof brokerMessage.endpointEpoch !== "string")) {
          throw new Error("Opaque dispatch broker omitted an epoch");
        }
        this._sessionId = brokerMessage.sessionId;
        this._features = advertisedFeatures;
        this._brokerEpoch = typeof brokerMessage.brokerEpoch === "string" ? brokerMessage.brokerEpoch : null;
        this._endpointEpoch = typeof brokerMessage.endpointEpoch === "string" ? brokerMessage.endpointEpoch : null;
        const registered: BrokerMessage = {
          type: "registered",
          sessionId: brokerMessage.sessionId,
          ...(this._features.size > 0 ? { features: [...this._features] } : {}),
          ...(this._brokerEpoch ? { brokerEpoch: this._brokerEpoch } : {}),
          ...(this._endpointEpoch ? { endpointEpoch: this._endpointEpoch } : {}),
        };
        this.emit("broker_message", registered);
        this.emit("_registered", registered);
        break;
      }

      case "sessions": {
        const { requestId, sessions } = brokerMessage;
        if (typeof requestId !== "string" || !Array.isArray(sessions) || !sessions.every(isSessionInfo)) {
          throw new Error("Invalid sessions message");
        }

        const pending = this.pendingLists.get(requestId);
        if (!pending) {
          // Late list responses can still arrive after the caller has already timed out.
          return;
        }

        this.pendingLists.delete(requestId);
        pending.resolve(sessions);
        break;
      }

      case "sessions_failed": {
        const { requestId, code, error } = brokerMessage;
        if (typeof requestId !== "string" || code !== "response_too_large" || typeof error !== "string") {
          throw new Error("Invalid sessions_failed message");
        }
        const pending = this.pendingLists.get(requestId);
        if (!pending) break;
        this.pendingLists.delete(requestId);
        pending.reject(new IntercomListSessionsError(code, error));
        break;
      }

      case "message": {
        const { from, message, control } = brokerMessage;
        if (!isSessionInfo(from) || !isMessage(message)
          || (control !== undefined && (!isMessageControl(control)
            || control.action !== "supersede" || control.supersededBy !== message.id))) {
          throw new Error("Invalid message event");
        }
        if (control) {
          // Dispatch both logical events synchronously from one wire frame. The sender is not
          // acknowledged until this transaction has reached the receiver socket as a whole.
          this.emit("broker_message", { type: "message_control", from, control } satisfies BrokerMessage);
          this.emit("message_control", from, control);
        }
        this.emit("message", from, message);
        break;
      }

      case "delivered": {
        const { messageId, operationId, delivery, retryable, outcomeKnown } = brokerMessage;
        if (typeof messageId !== "string" || (operationId !== undefined && typeof operationId !== "string")
          || (delivery !== undefined && delivery !== "socket_delivered" && delivery !== "queued")
          || (retryable !== undefined && typeof retryable !== "boolean")
          || (outcomeKnown !== undefined && typeof outcomeKnown !== "boolean")) {
          throw new Error("Invalid delivered message");
        }
        this.settleOperation(messageId, operationId, {
          id: messageId,
          delivered: true,
          delivery: (delivery as "socket_delivered" | "queued" | undefined) ?? "socket_delivered",
          retryable: (retryable as boolean | undefined) ?? false,
          outcomeKnown: (outcomeKnown as boolean | undefined) ?? true,
          ...(typeof brokerMessage.code === "string" ? { code: brokerMessage.code } : {}),
        });
        break;
      }

      case "delivery_failed": {
        const { messageId, operationId, reason, delivery, retryable, outcomeKnown } = brokerMessage;
        if (
          typeof messageId !== "string"
          || (operationId !== undefined && typeof operationId !== "string")
          || typeof reason !== "string"
          || (delivery !== undefined && delivery !== "failed" && delivery !== "unknown")
          || (retryable !== undefined && typeof retryable !== "boolean")
          || (outcomeKnown !== undefined && typeof outcomeKnown !== "boolean")
        ) {
          throw new Error("Invalid delivery_failed message");
        }
        this.settleOperation(messageId, operationId, {
          id: messageId,
          delivered: false,
          reason,
          delivery: (delivery as "failed" | "unknown" | undefined) ?? "failed",
          retryable: (retryable as boolean | undefined) ?? false,
          outcomeKnown: (outcomeKnown as boolean | undefined) ?? true,
          ...(typeof brokerMessage.code === "string" ? { code: brokerMessage.code } : {}),
        });
        break;
      }

      case "message_receipt": {
        if (!isSessionInfo(brokerMessage.from) || !isMessageReceipt(brokerMessage.receipt)) {
          throw new Error("Invalid message_receipt event");
        }
        this.emit("broker_message", brokerMessage as BrokerMessage);
        this.emit("message_receipt", brokerMessage.from, brokerMessage.receipt);
        break;
      }

      case "message_control": {
        if (!isSessionInfo(brokerMessage.from) || !isMessageControl(brokerMessage.control)) {
          throw new Error("Invalid message_control event");
        }
        this.emit("broker_message", brokerMessage as BrokerMessage);
        this.emit("message_control", brokerMessage.from, brokerMessage.control);
        break;
      }

      case "session_joined": {
        if (!isSessionInfo(brokerMessage.session)) {
          throw new Error("Invalid session_joined message");
        }

        const message: BrokerMessage = { type: "session_joined", session: brokerMessage.session };
        this.emit("broker_message", message);
        this.emit("session_joined", brokerMessage.session);
        break;
      }

      case "session_left": {
        if (typeof brokerMessage.sessionId !== "string") {
          throw new Error("Invalid session_left message");
        }

        const message: BrokerMessage = { type: "session_left", sessionId: brokerMessage.sessionId };
        this.emit("broker_message", message);
        this.emit("session_left", brokerMessage.sessionId);
        break;
      }

      case "presence_update": {
        if (!isSessionInfo(brokerMessage.session)) {
          throw new Error("Invalid presence_update message");
        }

        const message: BrokerMessage = { type: "presence_update", session: brokerMessage.session };
        this.emit("broker_message", message);
        this.emit("presence_update", brokerMessage.session);
        break;
      }

      case "error": {
        if (typeof brokerMessage.error !== "string") {
          throw new Error("Invalid error message");
        }

        if (this._sessionId === null) {
          throw new Error(brokerMessage.error);
        }
        this.emit("error", new Error(brokerMessage.error));
        break;
      }

      case "extension_owner": {
        const hasOwnerId = typeof brokerMessage.ownerId === "string";
        const hasOwnerEpoch = typeof brokerMessage.ownerEpoch === "string";
        if (
          typeof brokerMessage.namespace !== "string"
          || hasOwnerId !== hasOwnerEpoch
          || (brokerMessage.ownerId !== undefined && !hasOwnerId)
          || (brokerMessage.ownerEpoch !== undefined && !hasOwnerEpoch)
        ) {
          throw new Error("Invalid extension_owner message");
        }
        this.emit("broker_message", brokerMessage as BrokerMessage);
        this.emit("extension_owner", brokerMessage);
        break;
      }

      case "extension_message": {
        const hasOwnerId = typeof brokerMessage.ownerId === "string";
        const hasOwnerEpoch = typeof brokerMessage.ownerEpoch === "string";
        if (
          typeof brokerMessage.namespace !== "string"
          || typeof brokerMessage.fromSessionId !== "string"
          || hasOwnerId !== hasOwnerEpoch
          || (brokerMessage.ownerId !== undefined && !hasOwnerId)
          || (brokerMessage.ownerEpoch !== undefined && !hasOwnerEpoch)
        ) {
          throw new Error("Invalid extension_message");
        }
        this.emit("broker_message", brokerMessage as BrokerMessage);
        this.emit("extension_message", brokerMessage);
        break;
      }

      case "extension_state": {
        if (
          typeof brokerMessage.namespace !== "string"
          || !Number.isSafeInteger(brokerMessage.revision)
          || Number(brokerMessage.revision) < 0
        ) {
          throw new Error("Invalid extension_state");
        }
        this.emit("broker_message", brokerMessage as BrokerMessage);
        this.emit("extension_state", brokerMessage);
        break;
      }

      case "extension_state_snapshot": {
        if (typeof brokerMessage.requestId !== "string" || !isExtensionStateSnapshot(brokerMessage.snapshot)) {
          throw new Error("Invalid extension_state_snapshot");
        }
        const pending = this.pendingStateRefreshes.get(brokerMessage.requestId);
        if (!pending) break;
        this.pendingStateRefreshes.delete(brokerMessage.requestId);
        pending.resolve(brokerMessage.snapshot);
        break;
      }

      case "opaque_dispatch_v1_peer_capability_result": {
        if (!isOpaqueDispatchBrokerFrame(brokerMessage) || brokerMessage.type !== "opaque_dispatch_v1_peer_capability_result") {
          throw new Error("Invalid opaque peer capability result");
        }
        const pending = this.pendingPeerQueries.get(brokerMessage.operationId);
        if (!pending) break;
        this.pendingPeerQueries.delete(brokerMessage.operationId);
        pending.resolve(brokerMessage);
        break;
      }

      case "opaque_dispatch_v1_ack":
      case "opaque_dispatch_v1_rejected":
      case "opaque_dispatch_v1_claim_result":
      case "opaque_dispatch_v1_fail_result":
      case "opaque_dispatch_v1_claim_status_result":
      case "opaque_dispatch_v1_cancel_result": {
        if (!isOpaqueDispatchBrokerFrame(brokerMessage) || !("operationId" in brokerMessage)) {
          throw new Error("Invalid opaque dispatch result");
        }
        if (brokerMessage.type === "opaque_dispatch_v1_rejected") {
          const pendingQuery = this.pendingPeerQueries.get(brokerMessage.operationId);
          if (pendingQuery) {
            this.pendingPeerQueries.delete(brokerMessage.operationId);
            pendingQuery.reject(new Error(brokerMessage.code));
            break;
          }
        }
        const pending = this.pendingOpaque.get(brokerMessage.operationId);
        if (!pending) break;
        this.pendingOpaque.delete(brokerMessage.operationId);
        pending.resolve(brokerMessage);
        break;
      }

      case "opaque_dispatch_v1_offer":
      case "opaque_dispatch_v1_reservation_ended":
      case "opaque_dispatch_v1_receipt": {
        if (!isOpaqueDispatchBrokerFrame(brokerMessage)) throw new Error("Invalid opaque dispatch event");
        this.emit("opaque_dispatch", brokerMessage);
        break;
      }

      case "extension_state_result": {
        if (
          typeof brokerMessage.namespace !== "string"
          || typeof brokerMessage.committed !== "boolean"
          || !Number.isSafeInteger(brokerMessage.revision)
          || Number(brokerMessage.revision) < 0
          || (brokerMessage.reason !== undefined && typeof brokerMessage.reason !== "string")
        ) {
          throw new Error("Invalid extension_state_result");
        }
        this.emit("broker_message", brokerMessage as BrokerMessage);
        this.emit("extension_state_result", brokerMessage);
        break;
      }

      default:
        throw new Error(`Unknown broker message type: ${brokerMessage.type}`);
    }
  }

  private settleOperation(messageId: string, operationId: unknown, result: SendResult): void {
    if (typeof operationId === "string") {
      const pending = this.pendingOperations.get(operationId);
      if (!pending || pending.messageId !== messageId) {
        return;
      }
      this.pendingOperations.delete(operationId);
      pending.resolve(result);
      return;
    }

    if (this.poisonedLegacyMessageIds.delete(messageId)) {
      return;
    }
    const operationKey = this.legacyOperations.get(messageId);
    if (!operationKey) {
      return;
    }
    const pending = this.pendingOperations.get(operationKey);
    this.legacyOperations.delete(messageId);
    this.pendingOperations.delete(operationKey);
    pending?.resolve(result);
  }

  private poisonLegacyMessageId(messageId: string): void {
    this.poisonedLegacyMessageIds.delete(messageId);
    this.poisonedLegacyMessageIds.add(messageId);
    while (this.poisonedLegacyMessageIds.size > MAX_POISONED_LEGACY_MESSAGE_IDS) {
      const oldest = this.poisonedLegacyMessageIds.values().next().value;
      if (typeof oldest !== "string") break;
      this.poisonedLegacyMessageIds.delete(oldest);
    }
  }

  private runMessageOperation(
    messageId: string,
    timeoutLabel: "Send" | "Cancel",
    write: (operationId: string | undefined) => void,
  ): Promise<SendResult> {
    if (this.pendingOperations.size >= MAX_PENDING_OPERATIONS) {
      return Promise.reject(new Error("Too many pending intercom operations"));
    }

    const correlated = this.supportsFeature(CORRELATED_OPERATIONS_FEATURE);
    if (!correlated && (this.legacyOperations.has(messageId) || this.poisonedLegacyMessageIds.has(messageId))) {
      return Promise.reject(new Error("uncorrelated_operation_pending"));
    }

    const operationId = correlated ? randomUUID() : undefined;
    const operationKey = operationId ?? messageId;
    return new Promise((resolve, reject) => {
      const wrappedResolve = (result: SendResult) => {
        clearTimeout(timeout);
        resolve(result);
      };
      const wrappedReject = (error: Error) => {
        clearTimeout(timeout);
        reject(error);
      };
      const timeout = setTimeout(() => {
        if (!this.pendingOperations.delete(operationKey)) {
          return;
        }
        if (!correlated) {
          this.legacyOperations.delete(messageId);
          this.poisonLegacyMessageId(messageId);
        }
        wrappedReject(new Error(`${timeoutLabel} timeout`));
      }, 10000);

      this.pendingOperations.set(operationKey, { messageId, resolve: wrappedResolve, reject: wrappedReject });
      if (!correlated) {
        this.legacyOperations.set(messageId, operationKey);
      }
      try {
        write(operationId);
      } catch (error) {
        clearTimeout(timeout);
        this.pendingOperations.delete(operationKey);
        this.legacyOperations.delete(messageId);
        reject(toError(error));
      }
    });
  }

  async disconnect(): Promise<void> {
    const socket = this.socket;
    if (!socket) {
      return;
    }

    this.disconnecting = true;
    this.disconnectError = null;
    this.stopLivenessHeartbeat();
    this.failPending(new Error("Client disconnected"));

    await new Promise<void>((resolve) => {
      let settled = false;
      const finish = () => {
        if (settled) {
          return;
        }
        settled = true;
        clearTimeout(timeout);
        socket.off("close", onClose);
        socket.off("error", onError);
        resolve();
      };
      const onClose = () => finish();
      const onError = () => {
        socket.destroy();
      };
      const timeout = setTimeout(() => {
        socket.destroy();
      }, 2000);

      socket.once("close", onClose);
      socket.once("error", onError);

      try {
        writeMessage(socket, { type: "unregister" });
        socket.end();
      } catch {
        // Disconnect should still finish even if the unregister write fails.
        socket.destroy();
      }
    });
  }

  updateExtensionCapabilities(extensions: SessionRegistration["extensions"]): void {
    if (!this.supportsFeature(EXTENSION_BUS_FEATURE)) return;
    const socket = this.requireActiveSocket();
    writeMessage(socket, { type: "extension_capabilities_update", extensions: extensions ?? [] });
  }

  listSessions(options: { timeoutMs?: number } = {}): Promise<SessionInfo[]> {
    let socket: net.Socket;
    try {
      socket = this.requireActiveSocket();
    } catch (error) {
      return Promise.reject(toError(error));
    }
    
    return new Promise((resolve, reject) => {
      const requestId = randomUUID();
      const wrappedResolve = (sessions: SessionInfo[]) => {
        clearTimeout(timeout);
        resolve(sessions);
      };
      const wrappedReject = (error: Error) => {
        clearTimeout(timeout);
        reject(error);
      };
      const timeout = setTimeout(() => {
        if (this.pendingLists.has(requestId)) {
          this.pendingLists.delete(requestId);
          wrappedReject(new Error("List sessions timeout"));
        }
      }, options.timeoutMs ?? 5000);
      this.pendingLists.set(requestId, { resolve: wrappedResolve, reject: wrappedReject });
      try {
        writeMessage(socket, { type: "list", requestId });
      } catch (error) {
        clearTimeout(timeout);
        this.pendingLists.delete(requestId);
        reject(toError(error));
      }
    });
  }

  async send(to: string, options: SendOptions): Promise<SendResult> {
    this.requireActiveSocket();
    const messageId = options.messageId ?? randomUUID();
    const message: Message = {
      id: messageId,
      timestamp: Date.now(),
      senderSequence: this.nextSenderSequence++,
      supersedes: options.supersedes,
      retryOf: options.retryOf,
      replyTo: options.replyTo,
      expectsReply: options.expectsReply,
      content: {
        text: options.text,
        attachments: options.attachments,
      },
    };

    const sendOnce = (target?: { id: string; epoch: string }): Promise<SendResult> => this.runMessageOperation(
      messageId,
      "Send",
      (operationId) => {
        writeMessage(this.requireActiveSocket(), {
          type: "send",
          to,
          message,
          ...(operationId ? { operationId } : {}),
          ...(target ? { targetId: target.id, targetEpoch: target.epoch } : {}),
        });
      },
    );

    if (!this.supportsFeature(EXACT_SEND_FEATURE) || options.replyTo) return sendOnce();

    const resolveTarget = async (): Promise<{ id: string; epoch: string } | null> => {
      const sessions = await this.listSessions();
      const byId = sessions.find((session) => session.id === to);
      const byName = byId ? [] : sessions.filter((session) => session.name?.toLowerCase() === to.toLowerCase());
      const byPrefix = byId || byName.length > 0 ? [] : sessions.filter((session) => session.id.startsWith(to));
      const matches = byId ? [byId] : byName.length > 0 ? byName : byPrefix;
      const target = matches.length === 1 ? matches[0]! : null;
      return target?.endpointEpoch ? { id: target.id, epoch: target.endpointEpoch } : null;
    };

    const target = await resolveTarget();
    if (!target) return sendOnce();
    const result = await sendOnce(target);
    if (result.code !== "E_TARGET_REBOUND") return result;
    const reboundTarget = await resolveTarget();
    return reboundTarget ? sendOnce(reboundTarget) : result;
  }

  cancelMessage(messageId: string): Promise<SendResult> {
    let socket: net.Socket;
    try {
      socket = this.requireActiveSocket();
    } catch (error) {
      return Promise.reject(toError(error));
    }

    return this.runMessageOperation(messageId, "Cancel", (operationId) => {
      writeMessage(socket, { type: "cancel_message", messageId, ...(operationId ? { operationId } : {}) });
    });
  }

  sendMessageReceipt(receipt: MessageReceipt): void {
    if (this.disconnecting) {
      return;
    }

    const socket = this.socket;
    if (!socket || !this._sessionId || socket.destroyed || socket.writableEnded || !socket.writable) {
      return;
    }

    writeMessage(socket, { type: "message_receipt", receipt });
  }

  cancelAsk(messageId: string): void {
    if (this.disconnecting) {
      return;
    }

    const socket = this.socket;
    if (!socket || !this._sessionId || socket.destroyed || socket.writableEnded || !socket.writable) {
      return;
    }

    try {
      writeMessage(socket, { type: "cancel_ask", messageId });
    } catch {
      // Cancellation is best-effort; local waiter cleanup must still proceed.
    }
  }

  updatePresence(updates: { name?: string; runtimeFallbackAlias?: boolean; status?: string; model?: string; contextPct?: number | null; contextTokens?: number | null; contextWindow?: number | null }): void {
    if (this.disconnecting) {
      return;
    }

    const socket = this.socket;
    if (!socket || !this._sessionId || socket.destroyed || socket.writableEnded || !socket.writable) {
      return;
    }

    writeMessage(socket, { type: "presence", ...updates });
  }

  refreshExtensionState(namespace: string, options: { timeoutMs?: number } = {}): Promise<ExtensionStateSnapshot> {
    if (!this.supportsFeature(EXTENSION_STATE_REFRESH_FEATURE)) return Promise.reject(new Error("unsupported_broker"));
    let socket: net.Socket;
    try { socket = this.requireActiveSocket(); } catch (error) { return Promise.reject(toError(error)); }
    if (this.pendingStateRefreshes.size >= MAX_PENDING_OPERATIONS
      || namespacePendingCount(this.pendingStateRefreshes, namespace) >= MAX_PENDING_OPERATIONS_PER_NAMESPACE) return Promise.reject(new Error("limit_exceeded"));
    return new Promise((resolve, reject) => {
      const requestId = randomUUID();
      const timeout = setTimeout(() => {
        if (this.pendingStateRefreshes.delete(requestId)) reject(new Error("connection_lost"));
      }, options.timeoutMs ?? 5000);
      const settle = (value: ExtensionStateSnapshot) => { clearTimeout(timeout); resolve(value); };
      const fail = (error: Error) => { clearTimeout(timeout); reject(error); };
      this.pendingStateRefreshes.set(requestId, { namespace, resolve: settle, reject: fail });
      try { writeMessage(socket, { type: "extension_state_get", requestId, namespace }); }
      catch (error) { this.pendingStateRefreshes.delete(requestId); fail(toError(error)); }
    });
  }

  peerCapability(toSessionId: string, recipientNamespace: string, options: { timeoutMs?: number } = {}): Promise<
    { state: "present"; version: 1; endpointEpoch: string }
    | { state: "absent"; endpointEpoch: string }
    | { state: "unknown" }
  > {
    if (!this.supportsFeature(OPAQUE_DISPATCH_FEATURE)) return Promise.reject(new Error("unsupported_broker"));
    let socket: net.Socket;
    try { socket = this.requireActiveSocket(); } catch (error) { return Promise.reject(toError(error)); }
    if (this.pendingPeerQueries.size >= MAX_PENDING_OPERATIONS
      || namespacePendingCount(this.pendingPeerQueries, recipientNamespace) >= MAX_PENDING_OPERATIONS_PER_NAMESPACE) return Promise.reject(new Error("limit_exceeded"));
    return new Promise((resolve, reject) => {
      const operationId = randomUUID();
      const timeout = setTimeout(() => {
        if (this.pendingPeerQueries.delete(operationId)) reject(new Error("connection_lost"));
      }, options.timeoutMs ?? 5000);
      const settle = (result: Extract<OpaqueDispatchBrokerFrame, { type: "opaque_dispatch_v1_peer_capability_result" }>) => {
        clearTimeout(timeout);
        if (result.state === "present") resolve({ state: "present", version: 1, endpointEpoch: result.endpointEpoch });
        else if (result.state === "absent") resolve({ state: "absent", endpointEpoch: result.endpointEpoch });
        else resolve({ state: "unknown" });
      };
      const fail = (error: Error) => { clearTimeout(timeout); reject(error); };
      this.pendingPeerQueries.set(operationId, { namespace: recipientNamespace, resolve: settle, reject: fail });
      try { writeMessage(socket, { type: "opaque_dispatch_v1_peer_capability_get", operationId, toSessionId, recipientNamespace }); }
      catch (error) { this.pendingPeerQueries.delete(operationId); fail(toError(error)); }
    });
  }

  private writeOpaque(frame: OpaqueDispatchClientFrame): void {
    if (!this.supportsFeature(OPAQUE_DISPATCH_FEATURE)) throw new Error("unsupported_broker");
    writeMessage(this.requireActiveSocket(), frame);
  }

  private runOpaqueOperation(
    namespace: string,
    build: (operationId: string) => OpaqueDispatchClientFrame,
    options: { timeoutMs?: number } = {},
  ): Promise<OpaqueDispatchBrokerFrame> {
    if (!this.supportsFeature(OPAQUE_DISPATCH_FEATURE)) return Promise.reject(new Error("unsupported_broker"));
    if (this.pendingOpaque.size >= MAX_PENDING_OPERATIONS
      || namespacePendingCount(this.pendingOpaque, namespace) >= MAX_PENDING_OPERATIONS_PER_NAMESPACE) return Promise.reject(new Error("limit_exceeded"));
    const operationId = randomUUID();
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        if (this.pendingOpaque.delete(operationId)) reject(new Error("connection_lost"));
      }, options.timeoutMs ?? 10_000);
      const settle = (result: OpaqueDispatchBrokerFrame) => { clearTimeout(timeout); resolve(result); };
      const fail = (error: Error) => { clearTimeout(timeout); reject(error); };
      this.pendingOpaque.set(operationId, { namespace, resolve: settle, reject: fail });
      try { this.writeOpaque(build(operationId)); }
      catch (error) { this.pendingOpaque.delete(operationId); fail(toError(error)); }
    });
  }

  async sendOpaqueDispatch(senderNamespace: string, input: { requestId: string; toSessionId: string; recipientNamespace: string; payload: unknown; supersedesMessageId?: string }) {
    const rejected = (code: OpaqueDispatchReason) => ({ accepted: false as const, requestId: input.requestId, code });
    if (!this.supportsFeature(OPAQUE_DISPATCH_FEATURE)) return rejected("unsupported_broker");
    const errorReason = (error: unknown): OpaqueDispatchReason => {
      const message = toError(error).message;
      return isOpaqueDispatchReason(message) ? message : "connection_lost";
    };
    const resolveEpoch = async (): Promise<{ endpointEpoch?: string; code?: OpaqueDispatchReason }> => {
      try {
        const capability = await this.peerCapability(input.toSessionId, input.recipientNamespace);
        return capability.state === "unknown" ? { code: "unknown_exact_target" } : { endpointEpoch: capability.endpointEpoch };
      } catch (error) {
        return { code: errorReason(error) };
      }
    };
    const sendOnce = async (targetEpoch: string) => {
      try {
        const result = await this.runOpaqueOperation(senderNamespace, (operationId) => ({
          type: "opaque_dispatch_v1_send",
          operationId,
          senderNamespace,
          ...input,
          targetEpoch,
        }));
        if (result.type === "opaque_dispatch_v1_ack") return { accepted: true as const, requestId: result.requestId, messageId: result.messageId, brokerEpoch: result.brokerEpoch, deliveryState: result.deliveryState };
        if (result.type === "opaque_dispatch_v1_rejected") return { accepted: false as const, requestId: result.requestId ?? input.requestId, ...(result.messageId ? { messageId: result.messageId } : {}), code: result.code, ...(result.terminal ? { terminal: result.terminal } : {}) };
        return rejected("invalid_frame");
      } catch (error) {
        return rejected(errorReason(error));
      }
    };
    const target = await resolveEpoch();
    if (!target.endpointEpoch) return rejected(target.code ?? "unknown_exact_target");
    const result = await sendOnce(target.endpointEpoch);
    if (result.accepted || result.code !== "target_rebound") return result;
    const rebound = await resolveEpoch();
    return rebound.endpointEpoch ? sendOnce(rebound.endpointEpoch) : rejected(rebound.code ?? "target_rebound");
  }

  async cancelOpaqueDispatch(senderNamespace: string, messageId: string) {
    const result = await this.runOpaqueOperation(senderNamespace, (operationId) => ({ type: "opaque_dispatch_v1_cancel", operationId, senderNamespace, messageId }));
    if (result.type === "opaque_dispatch_v1_cancel_result" && result.cancelled) return { cancelled: true as const };
    return {
      cancelled: false as const,
      code: result.type === "opaque_dispatch_v1_cancel_result" || result.type === "opaque_dispatch_v1_rejected"
        ? result.code ?? "invalid_frame" as const
        : "invalid_frame" as const,
    };
  }

  sendOpaqueReservationResult(messageId: string, reservationId: string, decision: "reserved" | "refused" | "failed_closed", reason?: OpaqueDispatchReason): void {
    const endpointEpoch = this._endpointEpoch;
    if (!endpointEpoch) throw new Error("connection_lost");
    this.writeOpaque({ type: "opaque_dispatch_v1_reservation_result", endpointEpoch, messageId, reservationId, decision, ...(reason ? { reason } : {}) });
  }

  async claimOpaqueDispatch(recipientNamespace: string, messageId: string, reservationId: string) {
    const endpointEpoch = this._endpointEpoch;
    if (!endpointEpoch) return { claimed: false as const, code: "connection_lost" as const };
    const result = await this.runOpaqueOperation(recipientNamespace, (operationId) => ({ type: "opaque_dispatch_v1_claim", operationId, endpointEpoch, messageId, reservationId }));
    return result.type === "opaque_dispatch_v1_claim_result" && result.claimed
      ? { claimed: true as const }
      : { claimed: false as const, code: result.type === "opaque_dispatch_v1_claim_result" ? result.code ?? "invalid_frame" as const : "invalid_frame" as const };
  }

  async failOpaqueDispatch(recipientNamespace: string, messageId: string, reservationId: string) {
    const endpointEpoch = this._endpointEpoch;
    if (!endpointEpoch) return { failedClosed: false as const, code: "connection_lost" as const };
    const result = await this.runOpaqueOperation(recipientNamespace, (operationId) => ({ type: "opaque_dispatch_v1_fail", operationId, endpointEpoch, messageId, reservationId, reason: "consumer_failed" }));
    return result.type === "opaque_dispatch_v1_fail_result" && result.failedClosed
      ? { failedClosed: true as const }
      : { failedClosed: false as const, code: result.type === "opaque_dispatch_v1_fail_result" ? result.code ?? "invalid_frame" as const : "invalid_frame" as const };
  }

  async reconcileOpaqueClaim(recipientNamespace: string, input: { brokerEpoch: string; endpointEpoch: string; messageId: string; reservationId: string }) {
    const result = await this.runOpaqueOperation(recipientNamespace, (operationId) => ({ type: "opaque_dispatch_v1_claim_status", operationId, recipientNamespace, ...input }));
    return result.type === "opaque_dispatch_v1_claim_status_result"
      ? result.result
      : { state: "indeterminate" as const, code: "claim_history_unavailable" as const };
  }

  ackOpaqueReceipt(senderNamespace: string, messageId: string, sequence: number): void {
    this.writeOpaque({ type: "opaque_dispatch_v1_receipt_ack", senderNamespace, messageId, sequence });
  }

  onOpaqueDispatch(handler: (frame: Extract<OpaqueDispatchBrokerFrame, { type: "opaque_dispatch_v1_offer" | "opaque_dispatch_v1_reservation_ended" | "opaque_dispatch_v1_receipt" }>) => void): () => void {
    this.on("opaque_dispatch", handler);
    return () => this.off("opaque_dispatch", handler);
  }

  sendExtensionMessage(message: Extract<ClientMessage, { type: "extension_publish" | "extension_state_commit" }>): void {
    if (!this.supportsFeature(EXTENSION_BUS_FEATURE)) {
      throw new Error(`Connected broker does not support ${EXTENSION_BUS_FEATURE}`);
    }
    const socket = this.requireActiveSocket();
    writeMessage(socket, message);
  }

  onBrokerMessage(handler: (message: BrokerMessage) => void): () => void {
    this.on("broker_message", handler);
    return () => this.off("broker_message", handler);
  }

  onMessageReceipt(handler: (from: SessionInfo, receipt: MessageReceipt) => void): () => void {
    this.on("message_receipt", handler);
    return () => this.off("message_receipt", handler);
  }

  onMessageControl(handler: (from: SessionInfo, control: MessageControl) => void): () => void {
    this.on("message_control", handler);
    return () => this.off("message_control", handler);
  }
}
