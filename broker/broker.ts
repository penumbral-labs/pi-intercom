import net from "net";
import { chmodSync, mkdirSync, readdirSync, readFileSync, writeFileSync, unlinkSync } from "fs";
import { join } from "path";
import { randomUUID } from "crypto";
import {
  writeMessage,
  writeMessages,
  createMessageReader,
  IntercomFrameTooLargeError,
} from "./framing.ts";
import { ASK_REPLY_AUTHORIZATION_RETENTION_MS, AskEdges } from "./ask-edges.ts";
import {
  isBoundedId,
  isExtensionCapability,
  isMessage,
  isMessageReceipt,
  isNamespace,
  isOpaqueDispatchClientFrame,
  isSessionId,
  isSessionRegistration,
  opaqueSessionCapability,
} from "./protocol.ts";
import {
  ensureIntercomRuntimeDir,
  getBrokerListenTarget,
  getBrokerPortFilePath,
  getIntercomDirPath,
  INTERCOM_DIR_MODE,
  INTERCOM_PROTOCOL_NAME,
  INTERCOM_PROTOCOL_VERSION,
  INTERCOM_RUNTIME_FILE_MODE,
  restrictIntercomRuntimeFile,
  type BrokerConnectTarget,
} from "./paths.ts";
import { getAskTimeoutMs } from "../config.ts";
import { sameCwd } from "../cwd.ts";
import {
  BROKER_SESSION_ID,
  CORRELATED_OPERATIONS_FEATURE,
  EXACT_SEND_FEATURE,
  EXTENSION_BUS_FEATURE,
  EXTENSION_STATE_REFRESH_FEATURE,
  OPAQUE_DISPATCH_FEATURE,
} from "../types.ts";
import type {
  SessionInfo,
  Message,
  BrokerMessage,
  ExtensionCapability,
  MessageControl,
  MessageReceipt,
  MessageReceiptStatus,
} from "../types.ts";
import { ExtensionStateManager } from "./extension-state.ts";
import { assertNoLiveBroker } from "./runtime-claim.ts";
import { OpaqueDispatchManager, writeOpaqueTo, type OpaqueEndpoint } from "./opaque-dispatch.ts";
import { pruneDisconnectedSessions } from "./disconnected-sessions.ts";

const INTERCOM_DIR = getIntercomDirPath();
const LISTEN_TARGET = getBrokerListenTarget();
const PID_PATH = join(INTERCOM_DIR, "broker.pid");
const PORT_PATH = getBrokerPortFilePath(INTERCOM_DIR);
const PENDING_ASKS_DIR = join(INTERCOM_DIR, "pending-asks");
const BROKER_STATE_ID = randomUUID();
const BROKER_EPOCH = randomUUID();
const MAX_SESSIONS = 128;
const MAX_UNREGISTERED_CONNECTIONS = 32;
const REGISTRATION_TIMEOUT_MS = 1000;
const RATE_LIMIT_CAPACITY = 512;
const RATE_LIMIT_REFILL_PER_SECOND = 120;
const PRESENCE_HEARTBEAT_MS = 1000;
const MAX_EXTENSIONS_PER_SESSION = 32;
const MAX_EXTENSION_MESSAGE_BYTES = 16 * 1024;
const MAX_EXTENSION_STATE_BYTES = 64 * 1024;
const MESSAGE_RECEIPT_ROUTE_RETENTION_MS = 60 * 60 * 1000;
const DELIVERY_RECORD_RETENTION_MS = 60 * 60 * 1000;
const TEST_DELIVERY_RECORD_CLOCK_PATH = process.env.PI_INTERCOM_TEST_DELIVERY_RECORD_CLOCK_PATH;
const DISCONNECTED_SESSION_RETENTION_MS = 24 * 60 * 60 * 1000;
const MAILBOX_MESSAGE_RETENTION_MS = 24 * 60 * 60 * 1000;
const MAX_MAILBOX_MESSAGES = 256;
const MAX_RATE_LIMITED_OPAQUE_OPERATIONS = 256;
const MAX_DELIVERY_RECORDS = 4096;
const BROKER_STARTED_AT = Date.now();

function deliveryRecordNow(): number {
  if (!TEST_DELIVERY_RECORD_CLOCK_PATH) return Date.now();
  const value = Number(readFileSync(TEST_DELIVERY_RECORD_CLOCK_PATH, "utf8"));
  if (!Number.isSafeInteger(value)) throw new Error("Invalid PI_INTERCOM_TEST_DELIVERY_RECORD_CLOCK_PATH value");
  return value;
}

function serializedPayloadSize(payload: unknown): number | null {
  try {
    const json = JSON.stringify(payload);
    return json === undefined ? null : Buffer.byteLength(json, "utf8");
  } catch {
    return null;
  }
}

interface ConnectedSession {
  socket: net.Socket;
  info: SessionInfo;
  lastPresenceBroadcastAt: number;
  ownerOrder: number;
  extensions?: ExtensionCapability[];
}

interface DeliveryRecord {
  fingerprint: string;
  state: "socket_delivered" | "queued" | "failed";
  reason?: string;
  code?: string;
  retryable: boolean;
  createdAt: number;
}

interface NamespaceOwner {
  sessionId: string;
  socket: net.Socket;
  epoch: string;
}

interface ConnectionState {
  socket: net.Socket;
  tokens: number;
  lastRefillAt: number;
  opaqueTokens: number;
  opaqueLastRefillAt: number;
  rateLimitedOpaqueOperations: Set<string>;
}


interface MessageReceiptRoute {
  from: string;
  to: string;
  createdAt: number;
}

interface DisconnectedSession {
  info: SessionInfo;
  disconnectedAt: number;
}

interface MailboxMessage {
  from: SessionInfo;
  target: SessionInfo;
  message: Message;
  queuedAt: number;
}

interface PendingAskRecord {
  askId: string;
  messageId: string;
  asker: { sessionId: string; name: string | null };
  target: { sessionId: string; name: string | null };
  question: string;
  createdAt: number;
  expiresAt: number;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isPendingAskRecord(value: unknown): value is PendingAskRecord {
  if (!isRecord(value) || !isRecord(value.asker) || !isRecord(value.target)) return false;
  return typeof value.askId === "string"
    && typeof value.messageId === "string"
    && typeof value.asker.sessionId === "string"
    && (typeof value.asker.name === "string" || value.asker.name === null)
    && typeof value.target.sessionId === "string"
    && (typeof value.target.name === "string" || value.target.name === null)
    && typeof value.question === "string"
    && typeof value.createdAt === "number"
    && Number.isSafeInteger(value.createdAt)
    && typeof value.expiresAt === "number"
    && Number.isSafeInteger(value.expiresAt)
    && value.expiresAt >= value.createdAt;
}

function pendingAskRecordPath(messageId: string): string {
  return join(PENDING_ASKS_DIR, `${encodeURIComponent(messageId)}.json`);
}

function ensurePendingAskRecordDir(): void {
  mkdirSync(PENDING_ASKS_DIR, { recursive: true, mode: INTERCOM_DIR_MODE });
  if (process.platform !== "win32") chmodSync(PENDING_ASKS_DIR, INTERCOM_DIR_MODE);
}

class IntercomBroker {
  private sessions = new Map<string, ConnectedSession>();
  private askEdges = new AskEdges(MAX_SESSIONS * 4);
  private messageReceiptRoutes = new Map<string, MessageReceiptRoute>();
  private disconnectedSessions = new Map<string, DisconnectedSession>();
  private mailboxMessages: MailboxMessage[] = [];
  private deliveryRecords = new Map<string, DeliveryRecord>();
  private connections = new Set<net.Socket>();
  private unregisteredConnections = new Set<net.Socket>();
  private server: net.Server;
  private shutdownTimer: NodeJS.Timeout | null = null;
  private readonly askTimeoutMs = getAskTimeoutMs();
  private namespaceOwners = new Map<string, NamespaceOwner>();
  private nextOwnerOrder = 1;
  private extensionStateManager: ExtensionStateManager;
  private opaqueDispatch: OpaqueDispatchManager;

  constructor() {
    ensureIntercomRuntimeDir(INTERCOM_DIR);
    assertNoLiveBroker(PID_PATH);
    ensurePendingAskRecordDir();
    this.prunePendingAskRecords();
    this.extensionStateManager = new ExtensionStateManager(INTERCOM_DIR);
    this.opaqueDispatch = new OpaqueDispatchManager({
      brokerEpoch: BROKER_EPOCH,
      endpoint: (sessionId) => this.opaqueEndpoint(sessionId),
      now: Date.now,
      owner: (namespace) => {
        const owner = this.namespaceOwners.get(namespace);
        return owner ? { sessionId: owner.sessionId, epoch: owner.epoch } : undefined;
      },
    });
    if (typeof LISTEN_TARGET === "string" && process.platform !== "win32") {
      try {
        unlinkSync(LISTEN_TARGET);
      } catch {
        // A clean startup has no stale socket to remove.
      }
    }
    this.server = net.createServer(this.handleConnection.bind(this));
  }

  start(): void {
    const onListening = () => {
      if (typeof LISTEN_TARGET === "string") {
        restrictIntercomRuntimeFile(LISTEN_TARGET);
      } else {
        const address = this.server.address();
        if (!address || typeof address === "string") {
          throw new Error("Intercom TCP broker started without a TCP address");
        }
        const endpoint: BrokerConnectTarget = {
          transport: "tcp",
          host: LISTEN_TARGET.host,
          port: address.port,
          stateId: BROKER_STATE_ID,
        };
        writeFileSync(PORT_PATH, `${JSON.stringify(endpoint)}\n`, { mode: INTERCOM_RUNTIME_FILE_MODE });
        restrictIntercomRuntimeFile(PORT_PATH);
      }
      writeFileSync(PID_PATH, String(process.pid), { mode: INTERCOM_RUNTIME_FILE_MODE });
      restrictIntercomRuntimeFile(PID_PATH);
      console.log(`Intercom broker started (pid: ${process.pid})`);
    };

    if (typeof LISTEN_TARGET === "string") {
      this.server.listen(LISTEN_TARGET, onListening);
    } else {
      this.server.listen({ host: LISTEN_TARGET.host, port: LISTEN_TARGET.port }, onListening);
    }
    process.stderr.on("error", (error: NodeJS.ErrnoException) => {
      if (error.code !== "EPIPE") throw error;
    });
    process.on("SIGTERM", () => this.shutdown());
    process.on("SIGINT", () => this.shutdown());
  }

  private handleConnection(socket: net.Socket): void {
    this.connections.add(socket);
    let sessionId: string | null = null;
    let registrationTimeout: NodeJS.Timeout | null = null;
    const armRegistrationTimeout = () => {
      if (registrationTimeout) {
        clearTimeout(registrationTimeout);
      }
      this.unregisteredConnections.delete(socket);
      this.unregisteredConnections.add(socket);
      this.evictOldestUnregisteredConnections(socket);
      registrationTimeout = setTimeout(() => {
        if (!sessionId) {
          socket.destroy();
        }
      }, REGISTRATION_TIMEOUT_MS);
      registrationTimeout.unref?.();
    };
    const clearRegistrationTimeout = () => {
      if (registrationTimeout) {
        clearTimeout(registrationTimeout);
        registrationTimeout = null;
      }
      this.unregisteredConnections.delete(socket);
    };
    armRegistrationTimeout();
    const connection: ConnectionState = {
      socket,
      tokens: RATE_LIMIT_CAPACITY,
      lastRefillAt: Date.now(),
      opaqueTokens: 60,
      opaqueLastRefillAt: Date.now(),
      rateLimitedOpaqueOperations: new Set(),
    };

    const reader = createMessageReader((msg) => {
      if (isOpaqueDispatchClientFrame(msg)) {
        if (!this.consumeOpaqueToken(connection)) {
          const operationId = "operationId" in msg ? msg.operationId : undefined;
          if (operationId && connection.rateLimitedOpaqueOperations.has(operationId)) return;
          if (operationId) {
            connection.rateLimitedOpaqueOperations.add(operationId);
            while (connection.rateLimitedOpaqueOperations.size > MAX_RATE_LIMITED_OPAQUE_OPERATIONS) {
              const oldest = connection.rateLimitedOpaqueOperations.values().next().value;
              if (typeof oldest !== "string") break;
              connection.rateLimitedOpaqueOperations.delete(oldest);
            }
          }
          if (sessionId) {
            const origin = this.sessions.get(sessionId);
            if (!origin || origin.socket !== socket) throw new Error("Opaque dispatch origin not found");
            const endpoint = this.opaqueEndpoint(sessionId);
            if (endpoint) this.opaqueDispatch.rateLimited(endpoint, msg);
          }
          return;
        }
        if ("operationId" in msg) connection.rateLimitedOpaqueOperations.delete(msg.operationId);
      } else if (!this.consumeToken(connection)) {
        writeMessage(socket, { type: "error", error: "Intercom broker rate limit exceeded" });
        socket.destroy(new Error("Intercom broker rate limit exceeded"));
        return;
      }
      this.handleMessage(socket, msg, sessionId, (id) => {
        sessionId = id;
        if (id) {
          clearRegistrationTimeout();
        } else {
          armRegistrationTimeout();
        }
      });
    }, (error) => {
      socket.destroy(error);
    });

    socket.on("data", reader);

    socket.on("close", () => {
      clearRegistrationTimeout();
      this.connections.delete(socket);
      if (sessionId) this.retireSession(sessionId, socket);
    });

    socket.on("error", (error) => {
      console.error("Socket error:", error);
    });
  }

  private evictOldestUnregisteredConnections(currentSocket: net.Socket): void {
    while (this.unregisteredConnections.size > MAX_UNREGISTERED_CONNECTIONS) {
      const [oldest] = this.unregisteredConnections;
      if (!oldest) {
        return;
      }
      if (oldest === currentSocket && this.unregisteredConnections.size === 1) {
        return;
      }
      this.unregisteredConnections.delete(oldest);
      oldest.destroy();
    }
  }

  private consumeOpaqueToken(connection: ConnectionState, now = Date.now()): boolean {
    const elapsedMs = now - connection.opaqueLastRefillAt;
    if (elapsedMs > 0) {
      connection.opaqueTokens = Math.min(60, connection.opaqueTokens + elapsedMs * 30 / 1000);
      connection.opaqueLastRefillAt = now;
      if (connection.opaqueTokens >= 60) connection.rateLimitedOpaqueOperations.clear();
    }
    if (connection.opaqueTokens < 1) return false;
    connection.opaqueTokens -= 1;
    return true;
  }

  private consumeToken(connection: ConnectionState, now = Date.now()): boolean {
    const elapsedMs = now - connection.lastRefillAt;
    if (elapsedMs > 0) {
      connection.tokens = Math.min(
        RATE_LIMIT_CAPACITY,
        connection.tokens + elapsedMs * RATE_LIMIT_REFILL_PER_SECOND / 1000,
      );
      connection.lastRefillAt = now;
    }
    if (connection.tokens < 1) {
      return false;
    }
    connection.tokens -= 1;
    return true;
  }

  private scheduleShutdownCheck(): void {
    if (this.shutdownTimer) return;

    this.shutdownTimer = setTimeout(() => {
      this.shutdownTimer = null;
      if (this.sessions.size === 0) {
        console.log("No sessions connected, shutting down");
        this.shutdown();
      }
    }, 5000);
  }

  private handleMessage(
    socket: net.Socket,
    msg: unknown,
    currentId: string | null,
    setId: (id: string | null) => void,
  ): void {
    if (typeof msg !== "object" || msg === null || !("type" in msg) || typeof msg.type !== "string") {
      throw new Error("Invalid client message");
    }

    const clientMessage = msg as { type: string } & Record<string, unknown>;
    const requiresEndpointAuth = typeof LISTEN_TARGET !== "string";
    const hasEndpointAuth = clientMessage.stateId === BROKER_STATE_ID;

    if (clientMessage.type === "health") {
      if (typeof clientMessage.requestId !== "string") {
        throw new Error("Invalid health message");
      }
      if (requiresEndpointAuth && !hasEndpointAuth) {
        throw new Error("Invalid intercom TCP endpoint credentials");
      }
      writeMessage(socket, {
        type: "health_ok",
        requestId: clientMessage.requestId,
        protocol: INTERCOM_PROTOCOL_NAME,
        version: INTERCOM_PROTOCOL_VERSION,
      });
      return;
    }

    if (requiresEndpointAuth && clientMessage.type === "register" && !hasEndpointAuth) {
      throw new Error("Invalid intercom TCP endpoint credentials");
    }

    if (currentId === null && clientMessage.type !== "register") {
      throw new Error(`Received ${clientMessage.type} before register`);
    }

    switch (clientMessage.type) {
      case "register": {
        if (!isSessionRegistration(clientMessage.session)) {
          throw new Error("Invalid register message");
        }

        if (currentId) {
          throw new Error("Received duplicate register message");
        }

        let id: string = randomUUID();
        if (clientMessage.sessionId !== undefined) {
          if (!isSessionId(clientMessage.sessionId)) {
            throw new Error("Invalid register sessionId");
          }
          id = clientMessage.sessionId;
        }
        if (id === BROKER_SESSION_ID) {
          throw new Error("Reserved broker sessionId");
        }
        const session = clientMessage.session;
        const extensions = session.extensions;
        if (extensions !== undefined) {
          if (!Array.isArray(extensions) || extensions.length > MAX_EXTENSIONS_PER_SESSION) {
            throw new Error(`Invalid extensions field (maximum ${MAX_EXTENSIONS_PER_SESSION})`);
          }
          for (const extension of extensions) {
            if (!this.validateExtensionCapability(extension)) {
              throw new Error(`Invalid extension capability: ${JSON.stringify(extension)}`);
            }
          }
        }

        this.pruneDisconnectedSessions();
        this.pruneMailboxMessages();
        const previous = this.sessions.get(id);
        if (!previous && this.sessions.size >= MAX_SESSIONS) {
          writeMessage(socket, { type: "error", error: "Too many registered intercom sessions" });
          socket.destroy();
          break;
        }
        const ownerOrder = previous?.ownerOrder ?? this.nextOwnerOrder++;
        if (previous) {
          this.clearAskEdgesForSession(id);
          this.retireSession(id, previous.socket);
          previous.socket.end();
        }
        setId(id);
        const opaqueDispatch = opaqueSessionCapability(extensions);
        const info: SessionInfo = {
          id,
          endpointEpoch: randomUUID(),
          ...(session.name !== undefined ? { name: session.name } : {}),
          ...(session.runtimeFallbackAlias !== undefined ? { runtimeFallbackAlias: session.runtimeFallbackAlias } : {}),
          cwd: session.cwd,
          model: session.model,
          pid: session.pid,
          startedAt: session.startedAt,
          lastActivity: session.lastActivity,
          ...(session.status !== undefined ? { status: session.status } : {}),
          ...(session.tmuxPane !== undefined ? { tmuxPane: session.tmuxPane } : {}),
          trustedLocal: typeof LISTEN_TARGET === "string" && process.platform !== "win32",
          ...(opaqueDispatch ? { opaqueDispatch } : {}),
        };

        const connectedSession: ConnectedSession = {
          socket,
          info,
          lastPresenceBroadcastAt: Date.now(),
          ownerOrder,
          extensions,
        };
        this.sessions.set(id, connectedSession);
        this.disconnectedSessions.delete(id);

        if (this.shutdownTimer) {
          clearTimeout(this.shutdownTimer);
          this.shutdownTimer = null;
        }

        // This must be the first broker message. Older clients ignore the
        // additive features field; newer clients use it to avoid sending
        // extension operations to an older broker.
        writeMessage(socket, {
          type: "registered",
          sessionId: id,
          features: [
            EXTENSION_BUS_FEATURE,
            CORRELATED_OPERATIONS_FEATURE,
            EXACT_SEND_FEATURE,
            EXTENSION_STATE_REFRESH_FEATURE,
            OPAQUE_DISPATCH_FEATURE,
          ],
          brokerEpoch: BROKER_EPOCH,
          endpointEpoch: info.endpointEpoch,
        });
        this.broadcast({ type: "session_joined", session: info }, id);

        this.recomputeNamespaceOwners();
        this.opaqueDispatch.endpointAvailable(id);
        this.flushMailboxForSession(connectedSession);

        if (extensions) {
          for (const ext of extensions) {
            const owner = this.namespaceOwners.get(ext.namespace);
            writeMessage(socket, {
              type: "extension_owner",
              namespace: ext.namespace,
              ...(owner ? { ownerId: owner.sessionId, ownerEpoch: owner.epoch } : {}),
            });
            const state = this.extensionStateManager.loadState(ext.namespace);
            if (state) {
              writeMessage(socket, {
                type: "extension_state",
                namespace: ext.namespace,
                revision: state.revision,
                payload: state.payload,
              });
            }
          }
        }
        break;
      }

      case "unregister": {
        if (!currentId) {
          throw new Error("Received unregister before register");
        }
        this.retireSession(currentId, socket);
        setId(null);
        break;
      }

      case "extension_capabilities_update": {
        if (!currentId) {
          throw new Error("Received extension_capabilities_update before register");
        }
        const session = this.sessions.get(currentId);
        if (!session || session.socket !== socket) {
          throw new Error("Extension capability session not found");
        }
        const extensions = clientMessage.extensions;
        if (!Array.isArray(extensions) || extensions.length > MAX_EXTENSIONS_PER_SESSION) {
          throw new Error(`Invalid extensions field (maximum ${MAX_EXTENSIONS_PER_SESSION})`);
        }
        for (const extension of extensions) {
          if (!this.validateExtensionCapability(extension)) {
            throw new Error(`Invalid extension capability: ${JSON.stringify(extension)}`);
          }
        }
        session.extensions = extensions;
        const opaqueDispatch = opaqueSessionCapability(extensions);
        if (opaqueDispatch) session.info.opaqueDispatch = opaqueDispatch;
        else delete session.info.opaqueDispatch;
        this.recomputeNamespaceOwners();
        this.opaqueDispatch.capabilityChanged(currentId);
        for (const extension of extensions) {
          const owner = this.namespaceOwners.get(extension.namespace);
          writeMessage(socket, {
            type: "extension_owner",
            namespace: extension.namespace,
            ...(owner ? { ownerId: owner.sessionId, ownerEpoch: owner.epoch } : {}),
          });
          const state = this.extensionStateManager.loadState(extension.namespace);
          if (state) {
            writeMessage(socket, {
              type: "extension_state",
              namespace: extension.namespace,
              revision: state.revision,
              payload: state.payload,
            });
          }
        }
        break;
      }

      case "list": {
        if (typeof clientMessage.requestId !== "string") {
          throw new Error("Invalid list message");
        }

        const sessions = Array.from(this.sessions.values()).map(s => s.info);
        try {
          writeMessage(socket, { type: "sessions", requestId: clientMessage.requestId, sessions });
        } catch (error) {
          if (error instanceof IntercomFrameTooLargeError) {
            writeMessage(socket, {
              type: "sessions_failed",
              requestId: clientMessage.requestId,
              code: "response_too_large",
              error: "Intercom session list is too large",
            });
            break;
          }
          throw error;
        }
        break;
      }

      case "send": {
        const operationId = typeof clientMessage.operationId === "string" ? clientMessage.operationId : undefined;
        const delivered = (messageId: string, delivery: "socket_delivered" | "queued" = "socket_delivered") => ({
          type: "delivered" as const,
          messageId,
          ...(operationId ? { operationId } : {}),
          delivery,
          retryable: false,
          outcomeKnown: true,
        });
        const deliveryFailed = (messageId: string, reason: string, code = "E_DELIVERY_FAILED", retryable = false) => ({
          type: "delivery_failed" as const,
          messageId,
          ...(operationId ? { operationId } : {}),
          reason,
          delivery: "failed" as const,
          code,
          retryable,
          outcomeKnown: true,
        });
        if (clientMessage.operationId !== undefined && !operationId) {
          throw new Error("Invalid send operationId");
        }
        if (!currentId) {
          throw new Error("Received send before register");
        }
        const message = clientMessage.message;
        const messageId = isMessage(message) ? message.id : "unknown";

        if (typeof clientMessage.to !== "string" || !isMessage(message)) {
          writeMessage(socket, deliveryFailed(messageId, "Invalid message format", "E_INVALID_MESSAGE"));
          break;
        }

        const brokerReceivedAt = Date.now();
        this.pruneAskEdges();
        this.pruneMessageReceiptRoutes(brokerReceivedAt);
        const replyEdge = message.replyTo ? this.askEdges.get(message.replyTo) : undefined;
        const hasTargetId = clientMessage.targetId !== undefined;
        const hasTargetEpoch = clientMessage.targetEpoch !== undefined;
        if (
          hasTargetId !== hasTargetEpoch
          || (hasTargetId && !isSessionId(clientMessage.targetId))
          || (hasTargetEpoch && !isBoundedId(clientMessage.targetEpoch))
        ) {
          writeMessage(socket, deliveryFailed(message.id, "Exact target requires an id and endpoint epoch", "E_INVALID_TARGET"));
          break;
        }
        if (hasTargetId && hasTargetEpoch) {
          const targetId = clientMessage.targetId as string;
          const targetEpoch = clientMessage.targetEpoch as string;
          const fingerprint = this.deliveryFingerprint(message, targetId, targetEpoch);
          const exactTarget = this.sessions.get(targetId);
          if (!exactTarget || exactTarget.info.endpointEpoch !== targetEpoch) {
            if (this.replayOrRejectDelivery(socket, currentId, message.id, fingerprint, operationId)) break;
            this.recordDelivery(currentId, message.id, fingerprint, "failed", "Target endpoint changed before delivery", "E_TARGET_REBOUND", true);
            writeMessage(socket, deliveryFailed(message.id, "Target endpoint changed before delivery", "E_TARGET_REBOUND", true));
            break;
          }
          clientMessage.to = targetId;
        }

        const targets = this.findSessions(clientMessage.to as string);
        if (targets.length === 1) {
          if (message.replyTo && !replyEdge) {
            writeMessage(socket, deliveryFailed(message.id, "Reply target does not match a pending ask", "E_REPLY_TARGET"));
            break;
          }
          const fromSession = this.sessions.get(currentId);
          if (!fromSession || fromSession.socket !== socket) {
            writeMessage(socket, deliveryFailed(message.id, "Sender session not found", "E_SENDER_NOT_FOUND"));
            break;
          }
          const target = targets[0];
          let ownedSupersededAskId: string | undefined;
          if (message.supersedes) {
            const supersededRoute = this.messageReceiptRoutes.get(message.supersedes);
            if (!supersededRoute || supersededRoute.from !== currentId || supersededRoute.to !== target.info.id) {
              writeMessage(socket, deliveryFailed(message.id, "Supersede target does not match a previous message from this sender to this receiver", "E_SUPERSEDE_TARGET"));
              break;
            }
            const supersededEdge = this.askEdges.get(message.supersedes);
            if (supersededEdge?.from === currentId && supersededEdge.to === target.info.id) {
              ownedSupersededAskId = message.supersedes;
            }
          }
          const fingerprint = this.deliveryFingerprint(message, target.info.id, target.info.endpointEpoch);
          if (this.replayOrRejectDelivery(socket, currentId, message.id, fingerprint, operationId)) break;
          if (message.expectsReply && this.askEdges.has(message.id) && ownedSupersededAskId !== message.id) {
            writeMessage(socket, deliveryFailed(message.id, "Duplicate pending ask message ID"));
            break;
          }
          if (replyEdge && (replyEdge.to !== currentId || replyEdge.from !== target.info.id)) {
            writeMessage(socket, deliveryFailed(message.id, "Reply target does not match the pending ask", "E_REPLY_TARGET"));
            break;
          }
          if (message.expectsReply) {
            if (this.askEdges.hasReverse(currentId, target.info.id, message.replyTo)) {
              writeMessage(socket, deliveryFailed(message.id, "Mutual ask refused: target session is already waiting for a reply from this session.", "E_MUTUAL_ASK"));
              break;
            }
            const replacedAskIds = [
              message.replyTo,
              ownedSupersededAskId,
            ].filter((messageId): messageId is string => messageId !== undefined);
            const capacity = this.askEdges.canAdd(currentId, replacedAskIds);
            if (!capacity.ok) {
              writeMessage(socket, deliveryFailed(message.id, capacity.reason));
              break;
            }
          }
          const deliveredMessage: Message = {
            ...message,
            brokerReceivedAt,
            brokerDeliveredAt: Date.now(),
          };
          const deliveredEnvelope = {
            type: "message",
            from: fromSession.info,
            message: deliveredMessage,
          };
          try {
            if (message.supersedes) {
              const control: MessageControl = {
                action: "supersede",
                messageId: message.supersedes,
                supersededBy: message.id,
                timestamp: Date.now(),
              };
              // A supersede is two frames that mean nothing apart: the control retires the old ID
              // and the message supplies its replacement. The sink privately encodes both before
              // writing either, keeping them atomic with respect to the frame cap while preserving
              // control-before-message wire order.
              writeMessages(
                target.socket,
                {
                  type: "message_control",
                  from: fromSession.info,
                  control,
                },
                deliveredEnvelope,
              );
            } else {
              writeMessage(target.socket, deliveredEnvelope);
            }
          } catch (error) {
            if (error instanceof IntercomFrameTooLargeError) {
              writeMessage(socket, deliveryFailed(message.id, "Message is too large after broker metadata was added"));
              break;
            }
            throw error;
          }
          // Delivery precedes recording the ask edge. Broker metadata (brokerReceivedAt /
          // brokerDeliveredAt) can push a borderline message past the frame cap, and an edge
          // recorded for a message that never left would strand the asker on a reply that can
          // never arrive.
          if (message.replyTo) {
            this.askEdges.delete(message.replyTo);
            this.removePendingAskRecord(message.replyTo);
          }
          if (ownedSupersededAskId) {
            this.askEdges.delete(ownedSupersededAskId);
            this.removePendingAskRecord(ownedSupersededAskId);
          }
          if (message.expectsReply) {
            this.askEdges.add(message.id, currentId, target.info.id);
            this.writePendingAskRecord(message, fromSession.info, target.info, brokerReceivedAt);
          }
          this.messageReceiptRoutes.set(message.id, { from: currentId, to: target.info.id, createdAt: brokerReceivedAt });
          this.recordDelivery(currentId, message.id, fingerprint, "socket_delivered");
          writeMessage(socket, delivered(message.id));
          break;
        }

        if (targets.length > 1) {
          writeMessage(socket, deliveryFailed(message.id, `Multiple sessions named \"${clientMessage.to}\" are connected. Use the session ID instead.`, "E_AMBIGUOUS_TARGET"));
          break;
        }

        const disconnectedTargets = this.findDisconnectedSessions(clientMessage.to as string);
        if (disconnectedTargets.length === 1) {
          if (message.replyTo && !replyEdge) {
            writeMessage(socket, deliveryFailed(message.id, "Reply target does not match a pending ask", "E_REPLY_TARGET"));
            break;
          }
          const fromSession = this.sessions.get(currentId);
          if (!fromSession || fromSession.socket !== socket) {
            writeMessage(socket, deliveryFailed(message.id, "Sender session not found", "E_SENDER_NOT_FOUND"));
            break;
          }
          const target = disconnectedTargets[0]!.info;
          const fingerprint = this.deliveryFingerprint(message, target.id, target.endpointEpoch);
          if (this.replayOrRejectDelivery(socket, currentId, message.id, fingerprint, operationId)) break;
          if (message.supersedes) {
            writeMessage(socket, deliveryFailed(message.id, "Supersede target is not connected", "E_SUPERSEDE_TARGET"));
            break;
          }
          if (replyEdge && (replyEdge.to !== currentId || replyEdge.from !== target.id)) {
            writeMessage(socket, deliveryFailed(message.id, "Reply target does not match the pending ask", "E_REPLY_TARGET"));
            break;
          }
          if (message.expectsReply) {
            writeMessage(socket, deliveryFailed(message.id, "Target session is not currently connected; blocking asks are not queued", "E_TARGET_DISCONNECTED"));
            break;
          }
          const liveMailboxTarget = this.findUniqueLiveSessionForDisconnectedSession(target, currentId);
          if (liveMailboxTarget) {
            const deliveredMessage: Message = {
              ...message,
              brokerReceivedAt,
              brokerDeliveredAt: Date.now(),
            };
            try {
              writeMessage(liveMailboxTarget.socket, {
                type: "message",
                from: fromSession.info,
                message: deliveredMessage,
              });
            } catch (error) {
              if (error instanceof IntercomFrameTooLargeError) {
                writeMessage(socket, deliveryFailed(message.id, "Message is too large after broker metadata was added"));
                break;
              }
              throw error;
            }
            this.messageReceiptRoutes.set(message.id, { from: currentId, to: liveMailboxTarget.info.id, createdAt: brokerReceivedAt });
          } else {
            this.queueMailboxMessage(fromSession.info, target, message, brokerReceivedAt);
          }
          if (message.replyTo) {
            this.askEdges.delete(message.replyTo);
            this.removePendingAskRecord(message.replyTo);
          }
          const delivery = liveMailboxTarget ? "socket_delivered" : "queued";
          this.recordDelivery(currentId, message.id, fingerprint, delivery);
          writeMessage(socket, delivered(message.id, delivery));
          break;
        }

        if (disconnectedTargets.length > 1) {
          writeMessage(socket, deliveryFailed(message.id, `Multiple disconnected sessions named \"${clientMessage.to}\" can receive queued mail. Use the session ID instead.`, "E_AMBIGUOUS_TARGET"));
          break;
        }

        writeMessage(socket, deliveryFailed(message.id, "Session not found", "E_TARGET_NOT_FOUND"));
        break;
      }

      case "message_receipt": {
        if (!currentId) {
          throw new Error("Received message_receipt before register");
        }
        if (!isMessageReceipt(clientMessage.receipt)) {
          throw new Error("Invalid message_receipt message");
        }
        this.pruneMessageReceiptRoutes();
        const route = this.messageReceiptRoutes.get(clientMessage.receipt.messageId);
        const receiver = this.sessions.get(currentId);
        const sender = route ? this.sessions.get(route.from) : undefined;
        if (route?.to === currentId && receiver?.socket === socket && sender) {
          this.forwardMessageReceipt(sender.socket, receiver.info, clientMessage.receipt);
        }
        break;
      }

      case "cancel_message": {
        const operationId = typeof clientMessage.operationId === "string" ? clientMessage.operationId : undefined;
        const delivered = (messageId: string) => ({ type: "delivered" as const, messageId, ...(operationId ? { operationId } : {}) });
        const deliveryFailed = (messageId: string, reason: string) => ({
          type: "delivery_failed" as const,
          messageId,
          ...(operationId ? { operationId } : {}),
          reason,
        });
        if (clientMessage.operationId !== undefined && !operationId) {
          throw new Error("Invalid cancel_message operationId");
        }
        if (!currentId) {
          throw new Error("Received cancel_message before register");
        }
        if (typeof clientMessage.messageId !== "string") {
          throw new Error("Invalid cancel_message message");
        }
        this.pruneMessageReceiptRoutes();
        this.pruneMailboxMessages();
        const sender = this.sessions.get(currentId);
        const queuedIndex = this.mailboxMessages.findIndex(entry => entry.message.id === clientMessage.messageId && entry.from.id === currentId);
        if (queuedIndex >= 0 && sender?.socket === socket) {
          this.mailboxMessages.splice(queuedIndex, 1);
          this.updateDeliveryRecord(currentId, clientMessage.messageId, "failed", "Sender cancelled the queued delivery", "E_DELIVERY_CANCELLED");
          const edge = this.askEdges.get(clientMessage.messageId);
          if (edge?.from === currentId) {
            this.askEdges.delete(clientMessage.messageId);
            this.removePendingAskRecord(clientMessage.messageId);
          }
          this.emitBrokerReceipt(socket, clientMessage.messageId, "cancelled");
          writeMessage(socket, delivered(clientMessage.messageId));
          break;
        }
        const route = this.messageReceiptRoutes.get(clientMessage.messageId);
        const receiver = route ? this.sessions.get(route.to) : undefined;
        if (route?.from !== currentId || sender?.socket !== socket || !receiver) {
          writeMessage(socket, deliveryFailed(clientMessage.messageId, "Message cannot be cancelled by this session"));
          break;
        }
        try {
          writeMessage(receiver.socket, {
            type: "message_control",
            from: sender.info,
            control: {
              action: "cancel",
              messageId: clientMessage.messageId,
              timestamp: Date.now(),
            },
          });
        } catch (error) {
          if (error instanceof IntercomFrameTooLargeError) {
            writeMessage(socket, deliveryFailed(clientMessage.messageId, "Cancellation is too large after broker metadata was added"));
            break;
          }
          throw error;
        }
        const edge = this.askEdges.get(clientMessage.messageId);
        if (edge?.from === currentId) {
          this.askEdges.delete(clientMessage.messageId);
          this.removePendingAskRecord(clientMessage.messageId);
        }
        writeMessage(socket, delivered(clientMessage.messageId));
        break;
      }

      case "cancel_ask": {
        if (!currentId) {
          throw new Error("Received cancel_ask before register");
        }
        if (typeof clientMessage.messageId !== "string") {
          throw new Error("Invalid cancel_ask message");
        }
        const session = this.sessions.get(currentId);
        const edge = this.askEdges.get(clientMessage.messageId);
        if (session?.socket === socket && edge?.from === currentId) {
          this.askEdges.delete(clientMessage.messageId);
          this.removePendingAskRecord(clientMessage.messageId);
        }
        break;
      }

      case "presence": {
        if (!currentId) {
          throw new Error("Received presence before register");
        }
        const session = this.sessions.get(currentId);
        if (session?.socket === socket) {
          let changed = false;
          if (clientMessage.name !== undefined) {
            if (typeof clientMessage.name !== "string") {
              throw new Error("Invalid presence name");
            }
            if (session.info.name !== clientMessage.name) {
              session.info.name = clientMessage.name;
              changed = true;
            }
          }
          if (clientMessage.runtimeFallbackAlias !== undefined) {
            if (typeof clientMessage.runtimeFallbackAlias !== "boolean") {
              throw new Error("Invalid presence runtimeFallbackAlias");
            }
            if (session.info.runtimeFallbackAlias !== clientMessage.runtimeFallbackAlias) {
              session.info.runtimeFallbackAlias = clientMessage.runtimeFallbackAlias;
              changed = true;
            }
          }
          if (clientMessage.status !== undefined) {
            if (typeof clientMessage.status !== "string") {
              throw new Error("Invalid presence status");
            }
            if (session.info.status !== clientMessage.status) {
              session.info.status = clientMessage.status;
              changed = true;
            }
          }
          if (clientMessage.model !== undefined) {
            if (typeof clientMessage.model !== "string") {
              throw new Error("Invalid presence model");
            }
            if (session.info.model !== clientMessage.model) {
              session.info.model = clientMessage.model;
              changed = true;
            }
          }
          // Context-usage fields: a number updates, an explicit null CLEARS (the
          // value is unknown right after a compaction — delete rather than carry
          // the stale-high value forward), undefined leaves the field untouched.
          if (clientMessage.contextPct !== undefined) {
            if (clientMessage.contextPct === null) {
              if (session.info.contextPct !== undefined) { delete session.info.contextPct; changed = true; }
            } else if (typeof clientMessage.contextPct !== "number") {
              throw new Error("Invalid presence contextPct");
            } else if (session.info.contextPct !== clientMessage.contextPct) {
              session.info.contextPct = clientMessage.contextPct;
              changed = true;
            }
          }
          if (clientMessage.contextTokens !== undefined) {
            if (clientMessage.contextTokens === null) {
              if (session.info.contextTokens !== undefined) { delete session.info.contextTokens; changed = true; }
            } else if (typeof clientMessage.contextTokens !== "number") {
              throw new Error("Invalid presence contextTokens");
            } else if (session.info.contextTokens !== clientMessage.contextTokens) {
              session.info.contextTokens = clientMessage.contextTokens;
              changed = true;
            }
          }
          if (clientMessage.contextWindow !== undefined) {
            if (clientMessage.contextWindow === null) {
              if (session.info.contextWindow !== undefined) { delete session.info.contextWindow; changed = true; }
            } else if (typeof clientMessage.contextWindow !== "number") {
              throw new Error("Invalid presence contextWindow");
            } else if (session.info.contextWindow !== clientMessage.contextWindow) {
              session.info.contextWindow = clientMessage.contextWindow;
              changed = true;
            }
          }
          const now = Date.now();
          session.info.lastActivity = now;
          if (changed || now - session.lastPresenceBroadcastAt >= PRESENCE_HEARTBEAT_MS) {
            session.lastPresenceBroadcastAt = now;
            this.broadcast({ type: "presence_update", session: session.info }, currentId);
          }
        }
        break;
      }

      case "extension_state_get": {
        if (!currentId) throw new Error("Received extension_state_get before register");
        const session = this.sessions.get(currentId);
        if (!session || session.socket !== socket) throw new Error("Extension state session not found");
        if (!isBoundedId(clientMessage.requestId) || !isNamespace(clientMessage.namespace)) {
          throw new Error("Invalid extension_state_get");
        }
        if (!session.extensions?.some((extension) => extension.namespace === clientMessage.namespace)) {
          throw new Error("Extension state namespace capability required");
        }
        const state = this.extensionStateManager.loadState(clientMessage.namespace);
        writeMessage(socket, {
          type: "extension_state_snapshot",
          requestId: clientMessage.requestId,
          snapshot: state
            ? { namespace: clientMessage.namespace, revision: state.revision, present: true, payload: state.payload }
            : { namespace: clientMessage.namespace, revision: 0, present: false },
        });
        break;
      }

      case "opaque_dispatch_v1_peer_capability_get": {
        if (!currentId || !isOpaqueDispatchClientFrame(clientMessage) || clientMessage.type !== "opaque_dispatch_v1_peer_capability_get") {
          throw new Error("Invalid opaque peer capability query");
        }
        const origin = this.sessions.get(currentId);
        if (!origin || origin.socket !== socket) throw new Error("Opaque query origin not found");
        this.pruneDisconnectedSessions();
        const target = this.sessions.get(clientMessage.toSessionId)?.info
          ?? this.disconnectedSessions.get(clientMessage.toSessionId)?.info;
        const receive = target?.opaqueDispatch?.namespaces.some((entry) =>
          entry.namespace === clientMessage.recipientNamespace && entry.roles.includes("receive"));
        const result = target
          ? receive
            ? { state: "present" as const, version: 1 as const, endpointEpoch: target.endpointEpoch! }
            : { state: "absent" as const, endpointEpoch: target.endpointEpoch! }
          : { state: "unknown" as const };
        writeOpaqueTo(this.opaqueEndpoint(currentId), { anyOpaqueCapability: true }, {
          type: "opaque_dispatch_v1_peer_capability_result",
          operationId: clientMessage.operationId,
          toSessionId: clientMessage.toSessionId,
          recipientNamespace: clientMessage.recipientNamespace,
          ...result,
        });
        break;
      }

      case "opaque_dispatch_v1_send":
      case "opaque_dispatch_v1_cancel":
      case "opaque_dispatch_v1_reservation_result":
      case "opaque_dispatch_v1_claim":
      case "opaque_dispatch_v1_fail":
      case "opaque_dispatch_v1_claim_status":
      case "opaque_dispatch_v1_receipt_ack": {
        if (!currentId || !isOpaqueDispatchClientFrame(clientMessage)) throw new Error("Invalid opaque dispatch frame");
        const origin = this.sessions.get(currentId);
        if (!origin || origin.socket !== socket) throw new Error("Opaque dispatch origin not found");
        const endpoint = this.opaqueEndpoint(currentId);
        if (!endpoint || endpoint.write === undefined) throw new Error("Opaque endpoint not found");
        this.opaqueDispatch.handle(endpoint, clientMessage);
        break;
      }

      case "extension_publish": {
        this.handleExtensionPublish(socket, currentId, clientMessage);
        break;
      }

      case "extension_state_commit": {
        this.handleExtensionStateCommit(socket, currentId, clientMessage);
        break;
      }

      default:
        throw new Error(`Unknown client message type: ${clientMessage.type}`);
    }
  }

  private deliveryFingerprint(message: Message, targetId: string, _targetEpoch: string | undefined): string {
    return JSON.stringify({
      targetId,
      text: message.content.text,
      attachments: message.content.attachments,
      replyTo: message.replyTo,
      expectsReply: message.expectsReply,
      supersedes: message.supersedes,
      retryOf: message.retryOf,
    });
  }

  private deliveryRecordKey(fromSessionId: string, messageId: string): string {
    return JSON.stringify([fromSessionId, messageId]);
  }

  private replayOrRejectDelivery(
    socket: net.Socket,
    fromSessionId: string,
    messageId: string,
    fingerprint: string,
    operationId?: string,
    now = deliveryRecordNow(),
  ): boolean {
    this.pruneDeliveryRecords(now);
    const record = this.deliveryRecords.get(this.deliveryRecordKey(fromSessionId, messageId));
    if (!record) return false;
    if (record.fingerprint !== fingerprint) {
      writeMessage(socket, {
        type: "delivery_failed",
        messageId,
        ...(operationId ? { operationId } : {}),
        reason: "Message id was reused with different authored content",
        delivery: "failed",
        code: "E_MESSAGE_ID_REUSE",
        retryable: false,
        outcomeKnown: true,
      });
      return true;
    }
    if (record.code === "E_TARGET_REBOUND" && record.retryable) return false;
    if (record.state === "socket_delivered" || record.state === "queued") {
      writeMessage(socket, {
        type: "delivered",
        messageId,
        ...(operationId ? { operationId } : {}),
        delivery: record.state,
        retryable: false,
        outcomeKnown: true,
      });
    } else {
      writeMessage(socket, {
        type: "delivery_failed",
        messageId,
        ...(operationId ? { operationId } : {}),
        reason: record.reason ?? "Previous delivery failed",
        delivery: "failed",
        code: record.code ?? "E_DELIVERY_FAILED",
        retryable: record.retryable,
        outcomeKnown: true,
      });
    }
    return true;
  }

  private recordDelivery(
    fromSessionId: string,
    messageId: string,
    fingerprint: string,
    state: DeliveryRecord["state"],
    reason?: string,
    code?: string,
    retryable = false,
    now = deliveryRecordNow(),
  ): void {
    this.pruneDeliveryRecords(now);
    while (this.deliveryRecords.size >= MAX_DELIVERY_RECORDS) {
      const oldest = this.deliveryRecords.keys().next().value;
      if (oldest === undefined) break;
      this.deliveryRecords.delete(oldest);
    }
    this.deliveryRecords.set(this.deliveryRecordKey(fromSessionId, messageId), {
      fingerprint,
      state,
      ...(reason ? { reason } : {}),
      ...(code ? { code } : {}),
      retryable,
      createdAt: now,
    });
  }

  private pruneDeliveryRecords(now = Date.now()): void {
    for (const [key, record] of this.deliveryRecords) {
      if (now - record.createdAt > DELIVERY_RECORD_RETENTION_MS) this.deliveryRecords.delete(key);
    }
  }

  private updateDeliveryRecord(fromSessionId: string, messageId: string, state: DeliveryRecord["state"], reason?: string, code?: string): void {
    const record = this.deliveryRecords.get(this.deliveryRecordKey(fromSessionId, messageId));
    if (!record) return;
    record.state = state;
    record.reason = reason;
    record.code = code;
    record.retryable = false;
  }

  private retireSession(sessionId: string, socket: net.Socket): boolean {
    const existing = this.sessions.get(sessionId);
    if (existing?.socket !== socket) return false;
    this.opaqueDispatch.endpointDisconnected(sessionId);
    this.rememberDisconnectedSession(existing.info);
    this.sessions.delete(sessionId);
    this.clearMessageReceiptRoutesForSession(sessionId);
    this.broadcast({ type: "session_left", sessionId }, sessionId);
    this.recomputeNamespaceOwners();
    this.scheduleShutdownCheck();
    return true;
  }

  private opaqueEndpoint(sessionId: string): OpaqueEndpoint | undefined {
    this.pruneDisconnectedSessions();
    const connected = this.sessions.get(sessionId);
    if (connected) {
      return {
        sessionId,
        endpointEpoch: connected.info.endpointEpoch!,
        info: connected.info,
        extensions: connected.extensions,
        connected: true,
        write: (frame) => writeMessage(connected.socket, frame),
      };
    }
    const disconnected = this.disconnectedSessions.get(sessionId);
    if (!disconnected) return undefined;
    return {
      sessionId,
      endpointEpoch: disconnected.info.endpointEpoch!,
      info: disconnected.info,
      extensions: disconnected.info.opaqueDispatch?.namespaces.map((entry) => ({
        namespace: entry.namespace,
        ownerEligible: false,
        opaqueDispatch: { version: 1, roles: [...entry.roles] },
      })),
      connected: false,
    };
  }

  private rememberDisconnectedSession(info: SessionInfo, now = Date.now()): void {
    this.disconnectedSessions.set(info.id, { info: { ...info }, disconnectedAt: now });
    this.pruneDisconnectedSessions(now);
  }

  private pruneDisconnectedSessions(now = Date.now()): void {
    pruneDisconnectedSessions(this.disconnectedSessions, now, DISCONNECTED_SESSION_RETENTION_MS);
  }

  private brokerSessionInfo(receiptAt: number): SessionInfo {
    return {
      id: BROKER_SESSION_ID,
      name: "pi-intercom-broker",
      cwd: "",
      model: "broker",
      pid: process.pid,
      startedAt: BROKER_STARTED_AT,
      lastActivity: receiptAt,
      status: "broker",
      trustedLocal: typeof LISTEN_TARGET === "string" && process.platform !== "win32",
    };
  }

  private forwardMessageReceipt(socket: net.Socket, from: SessionInfo, receipt: MessageReceipt): void {
    // Rebuild from the validated schema so unrecognized inbound fields are never relayed.
    const forwarded: MessageReceipt = {
      messageId: receipt.messageId,
      status: receipt.status,
      timestamp: receipt.timestamp,
      ...(receipt.code ? { code: receipt.code } : {}),
      ...(receipt.detail !== undefined ? { detail: receipt.detail } : {}),
    };
    const frame = (nextReceipt: MessageReceipt) => ({ type: "message_receipt" as const, from, receipt: nextReceipt });
    try {
      writeMessage(socket, frame(forwarded));
      return;
    } catch (error) {
      if (!(error instanceof IntercomFrameTooLargeError)) throw error;
    }

    const { detail: _detail, ...receiptWithoutDetail } = forwarded;
    if (forwarded.detail !== undefined) {
      try {
        writeMessage(socket, frame({
          ...receiptWithoutDetail,
          detail: "Receipt detail omitted to fit the intercom frame limit",
        }));
        return;
      } catch (error) {
        if (!(error instanceof IntercomFrameTooLargeError)) throw error;
      }
    }

    try {
      writeMessage(socket, frame(receiptWithoutDetail));
    } catch (error) {
      // A receipt whose required identity plus receiver metadata cannot fit has no safe
      // correlated fallback. Dropping this diagnostic is preferable to disconnecting the
      // healthy receiver that submitted it.
      if (!(error instanceof IntercomFrameTooLargeError)) throw error;
    }
  }

  private emitBrokerReceipt(
    socket: net.Socket | undefined,
    messageId: string,
    status: MessageReceiptStatus,
    timestamp = Date.now(),
    detail?: string,
    code?: "E_DELIVERY_TOO_LARGE",
  ): void {
    if (!socket || socket.destroyed || socket.writableEnded || !socket.writable) {
      return;
    }
    writeMessage(socket, {
      type: "message_receipt",
      from: this.brokerSessionInfo(timestamp),
      receipt: { messageId, status, timestamp, ...(code ? { code } : {}), ...(detail ? { detail } : {}) },
    });
  }

  private pruneMailboxMessages(now = Date.now()): void {
    for (let index = this.mailboxMessages.length - 1; index >= 0; index -= 1) {
      const entry = this.mailboxMessages[index]!;
      if (now - entry.queuedAt > MAILBOX_MESSAGE_RETENTION_MS) {
        if (entry.message.expectsReply) {
          this.askEdges.delete(entry.message.id);
        }
        this.emitBrokerReceipt(this.sessions.get(entry.from.id)?.socket, entry.message.id, "expired", now);
        this.messageReceiptRoutes.delete(entry.message.id);
        this.updateDeliveryRecord(entry.from.id, entry.message.id, "failed", "Mailbox delivery expired", "E_DELIVERY_EXPIRED");
        this.mailboxMessages.splice(index, 1);
      }
    }
  }

  private queueMailboxMessage(from: SessionInfo, target: SessionInfo, message: Message, brokerReceivedAt: number): void {
    this.pruneMailboxMessages(brokerReceivedAt);
    while (this.mailboxMessages.length >= MAX_MAILBOX_MESSAGES) {
      const evicted = this.mailboxMessages.shift();
      if (!evicted) break;
      if (evicted.message.expectsReply) {
        this.askEdges.delete(evicted.message.id);
      }
      this.emitBrokerReceipt(this.sessions.get(evicted.from.id)?.socket, evicted.message.id, "expired", brokerReceivedAt);
      this.messageReceiptRoutes.delete(evicted.message.id);
      this.updateDeliveryRecord(evicted.from.id, evicted.message.id, "failed", "Mailbox capacity evicted the delivery", "E_DELIVERY_EVICTED");
    }
    this.mailboxMessages.push({
      from: { ...from },
      target: { ...target },
      message: { ...message, brokerReceivedAt },
      queuedAt: brokerReceivedAt,
    });
    this.emitBrokerReceipt(this.sessions.get(from.id)?.socket, message.id, "queued", brokerReceivedAt);
  }

  private flushMailboxForSession(session: ConnectedSession, now = Date.now()): void {
    this.pruneMailboxMessages(now);
    const sessionName = session.info.name?.toLowerCase();
    const uniqueMailboxIdentity = this.findLiveSessionsSharingMailboxIdentity(session.info).length === 1;

    for (let index = 0; index < this.mailboxMessages.length;) {
      const entry = this.mailboxMessages[index]!;
      const matchesId = entry.target.id === session.info.id;
      const matchesSenderIdentity = Boolean(
        sessionName
        && entry.from.name?.toLowerCase() === sessionName
        && sameCwd(entry.from.cwd, session.info.cwd),
      );
      const matchesUniqueName = Boolean(
        uniqueMailboxIdentity
        && sessionName
        && !matchesSenderIdentity
        && entry.target.name?.toLowerCase() === sessionName
        && sameCwd(entry.target.cwd, session.info.cwd),
      );
      if (!matchesId && !matchesUniqueName) {
        index += 1;
        continue;
      }

      const deliveredMessage: Message = {
        ...entry.message,
        brokerDeliveredAt: Date.now(),
      };
      // Write before removing the entry. Splicing first would lose the message permanently if the
      // write throws, and would also abort the flush for every remaining entry.
      try {
        writeMessage(session.socket, {
          type: "message",
          from: entry.from,
          message: deliveredMessage,
        });
      } catch (error) {
        if (error instanceof IntercomFrameTooLargeError) {
          this.mailboxMessages.splice(index, 1);
          if (entry.message.expectsReply) {
            this.askEdges.delete(entry.message.id);
            this.removePendingAskRecord(entry.message.id);
          }
          this.messageReceiptRoutes.delete(entry.message.id);
          this.updateDeliveryRecord(
            entry.from.id,
            entry.message.id,
            "failed",
            "Mailbox message is too large after broker metadata was added",
            "E_DELIVERY_TOO_LARGE",
          );
          this.emitBrokerReceipt(
            this.sessions.get(entry.from.id)?.socket,
            entry.message.id,
            "failed",
            now,
            "Mailbox message is too large after broker metadata was added",
            "E_DELIVERY_TOO_LARGE",
          );
          continue;
        }
        throw error;
      }
      this.mailboxMessages.splice(index, 1);
      const edge = this.askEdges.get(entry.message.id);
      if (edge?.to === entry.target.id) {
        // Must go through the owner so the pair index follows the retarget.
        this.askEdges.rekeyTarget(entry.message.id, session.info.id);
      }
      this.messageReceiptRoutes.set(entry.message.id, {
        from: entry.from.id,
        to: session.info.id,
        createdAt: now,
      });
      this.updateDeliveryRecord(entry.from.id, entry.message.id, "socket_delivered");
    }
  }

  private pruneAskEdges(now = Date.now()): void {
    this.prunePendingAskRecords(now);
    for (const messageId of this.askEdges.expireActiveOlderThan(this.askTimeoutMs, now)) {
      this.removePendingAskRecord(messageId);
    }
    // The caller's waiter expires at askTimeoutMs, but the extension documents late replies
    // as visible for its bounded stale-ask window. Keep reply authorization for that same window.
    this.askEdges.pruneOlderThan(this.askTimeoutMs + ASK_REPLY_AUTHORIZATION_RETENTION_MS, now);
  }

  private clearAskEdgesForSession(sessionId: string): void {
    for (const messageId of this.askEdges.deleteForSession(sessionId)) {
      this.removePendingAskRecord(messageId);
    }
  }

  private writePendingAskRecord(message: Message, from: SessionInfo, target: SessionInfo, createdAt: number): void {
    ensurePendingAskRecordDir();
    const record: PendingAskRecord = {
      askId: message.id,
      messageId: message.id,
      asker: { sessionId: from.id, name: from.name ?? null },
      target: { sessionId: target.id, name: target.name ?? null },
      question: message.content.text,
      createdAt,
      expiresAt: createdAt + this.askTimeoutMs,
    };
    const filePath = pendingAskRecordPath(message.id);
    writeFileSync(filePath, `${JSON.stringify(record, null, 2)}\n`, { mode: INTERCOM_RUNTIME_FILE_MODE });
    restrictIntercomRuntimeFile(filePath);
  }

  private removePendingAskRecord(messageId: string): void {
    try {
      unlinkSync(pendingAskRecordPath(messageId));
    } catch (error) {
      if (!isRecord(error) || error.code !== "ENOENT") throw error;
    }
  }

  private prunePendingAskRecords(now = Date.now()): void {
    ensurePendingAskRecordDir();
    for (const entry of readdirSync(PENDING_ASKS_DIR, { withFileTypes: true })) {
      if (!entry.isFile() || !entry.name.endsWith(".json")) continue;
      const filePath = join(PENDING_ASKS_DIR, entry.name);
      let parsed: unknown;
      try {
        parsed = JSON.parse(readFileSync(filePath, "utf8"));
      } catch {
        unlinkSync(filePath);
        continue;
      }
      if (!isPendingAskRecord(parsed) || now > parsed.expiresAt) unlinkSync(filePath);
    }
  }

  private pruneMessageReceiptRoutes(now = Date.now()): void {
    for (const [messageId, route] of this.messageReceiptRoutes) {
      if (now - route.createdAt > MESSAGE_RECEIPT_ROUTE_RETENTION_MS) {
        this.messageReceiptRoutes.delete(messageId);
      }
    }
  }

  private clearMessageReceiptRoutesForSession(sessionId: string): void {
    for (const [messageId, route] of this.messageReceiptRoutes) {
      if (route.from === sessionId || route.to === sessionId) {
        this.messageReceiptRoutes.delete(messageId);
      }
    }
  }

  private findSessions(nameOrId: string): ConnectedSession[] {
    const byId = this.sessions.get(nameOrId);
    if (byId) {
      return [byId];
    }

    const lowerName = nameOrId.toLowerCase();
    const byName = Array.from(this.sessions.values()).filter(session => session.info.name?.toLowerCase() === lowerName);
    if (byName.length > 0) {
      return byName;
    }

    return Array.from(this.sessions.entries())
      .filter(([id]) => id.startsWith(nameOrId))
      .map(([, session]) => session);
  }

  private findDisconnectedSessions(nameOrId: string): DisconnectedSession[] {
    this.pruneDisconnectedSessions();
    const byId = this.disconnectedSessions.get(nameOrId);
    if (byId) {
      return [byId];
    }

    const lowerName = nameOrId.toLowerCase();
    const byName = Array.from(this.disconnectedSessions.values()).filter(session => session.info.name?.toLowerCase() === lowerName);
    if (byName.length > 0) {
      return byName;
    }

    return Array.from(this.disconnectedSessions.entries())
      .filter(([id]) => id.startsWith(nameOrId))
      .map(([, session]) => session);
  }

  private findUniqueLiveSessionForDisconnectedSession(info: SessionInfo, senderId?: string): ConnectedSession | null {
    const matches = this.findLiveSessionsSharingMailboxIdentity(info)
      .filter((session) => session.info.id !== senderId);
    return matches.length === 1 ? matches[0]! : null;
  }

  /**
   * Mailbox identity is an explicit name plus directory, never name alone. A
   * runtime fallback alias is derived from the session id rather than chosen as
   * a durable identity, so it must not transfer mail to another process. This
   * also prevents two unnamed UUIDv7 sessions started close together from
   * inheriting each other's mailbox through a shared short alias.
   *
   * Directories compare through sameCwd so a relaunch that reports the same
   * directory differently (trailing slash, "."/"..", or a symlink such as macOS
   * /tmp vs /private/tmp) still matches.
   */
  private findLiveSessionsSharingMailboxIdentity(info: SessionInfo): ConnectedSession[] {
    const lowerName = info.name?.toLowerCase();
    if (!lowerName || info.runtimeFallbackAlias) {
      return [];
    }
    return Array.from(this.sessions.values()).filter(session =>
      !session.info.runtimeFallbackAlias
      && session.info.name?.toLowerCase() === lowerName
      && sameCwd(session.info.cwd, info.cwd)
    );
  }

  private broadcast(msg: BrokerMessage, exclude?: string): void {
    for (const [id, session] of this.sessions) {
      if (id !== exclude) {
        try {
          writeMessage(session.socket, msg);
        } catch (error) {
          if (error instanceof IntercomFrameTooLargeError) {
            // One recipient's oversize broadcast must not stop the rest from receiving it.
            console.error(`Skipping oversized ${msg.type} broadcast to ${id}: ${error.message}`);
            continue;
          }
          throw error;
        }
      }
    }
  }

  private validateExtensionCapability(cap: unknown): cap is ExtensionCapability {
    return isExtensionCapability(cap);
  }

  private validateNamespace(ns: string): boolean {
    // ^[a-z0-9][a-z0-9._/-]{0,63}$
    if (ns.length === 0 || ns.length > 64) {
      return false;
    }
    if (!/^[a-z0-9]/.test(ns)) {
      return false;
    }
    if (!/^[a-z0-9][a-z0-9._/-]*$/.test(ns)) {
      return false;
    }
    return true;
  }

  private recomputeNamespaceOwners(): void {
    const namespaces = new Set(this.namespaceOwners.keys());
    for (const session of this.sessions.values()) {
      for (const extension of session.extensions ?? []) {
        namespaces.add(extension.namespace);
      }
    }

    // For each namespace, elect owner by (startedAt, sessionId).
    for (const namespace of namespaces) {
      const candidates: Array<{ sessionId: string; session: ConnectedSession }> = [];
      for (const [sessionId, session] of this.sessions) {
        if (session.extensions) {
          const hasNamespace = session.extensions.some(
            (ext) => ext.namespace === namespace && ext.ownerEligible
          );
          if (hasNamespace) {
            candidates.push({ sessionId, session });
          }
        }
      }

      if (candidates.length === 0) {
        if (this.namespaceOwners.delete(namespace)) {
          for (const session of this.sessions.values()) {
            const isCapable = session.extensions?.some((extension) => extension.namespace === namespace);
            if (isCapable) {
              writeMessage(session.socket, { type: "extension_owner", namespace });
            }
          }
        }
        continue;
      }

      // Use broker-owned registration order so clients cannot seize authority
      // by backdating their advertised session start time. Stable-ID socket
      // replacements preserve the original order.
      candidates.sort((a, b) => {
        if (a.session.ownerOrder !== b.session.ownerOrder) {
          return a.session.ownerOrder - b.session.ownerOrder;
        }
        return a.sessionId.localeCompare(b.sessionId);
      });

      const winner = candidates[0];
      const existing = this.namespaceOwners.get(namespace);

      const ownerChanged = !existing || existing.sessionId !== winner.sessionId;
      const socketChanged = existing && existing.socket !== winner.session.socket;

      if (ownerChanged || socketChanged) {
        const epoch = randomUUID();
        this.namespaceOwners.set(namespace, {
          sessionId: winner.sessionId,
          socket: winner.session.socket,
          epoch,
        });

        for (const session of this.sessions.values()) {
          if (session.extensions?.length) {
            const isCapable = session.extensions.some((ext) => ext.namespace === namespace);
            if (isCapable) {
              writeMessage(session.socket, {
                type: "extension_owner",
                namespace,
                ownerId: winner.sessionId,
                ownerEpoch: epoch,
              });
            }
          }
        }
      }
    }
  }

  private handleExtensionPublish(
    socket: net.Socket,
    currentId: string | null,
    msg: Record<string, unknown>
  ): void {
    if (!currentId) {
      throw new Error("Received extension_publish before register");
    }

    const session = this.sessions.get(currentId);
    if (!session || session.socket !== socket) {
      writeMessage(socket, { type: "error", error: "Session not found" });
      return;
    }

    if (!session.extensions?.length) {
      writeMessage(socket, { type: "error", error: "Session has not advertised extension capability" });
      return;
    }

    const namespace = msg.namespace;
    const audience = msg.audience;
    const ownerOnly = msg.ownerOnly === true;
    const ownerEpoch = msg.ownerEpoch;
    const payload = msg.payload;

    if (typeof namespace !== "string" || !this.validateNamespace(namespace)) {
      writeMessage(socket, { type: "error", error: "Invalid namespace" });
      return;
    }

    if (audience !== "owner" && audience !== "capable") {
      writeMessage(socket, { type: "error", error: "Invalid audience" });
      return;
    }

    const payloadSize = serializedPayloadSize(payload);
    if (payloadSize === null || payloadSize > MAX_EXTENSION_MESSAGE_BYTES) {
      writeMessage(socket, { type: "error", error: "Invalid extension payload or payload exceeds 16 KiB limit" });
      return;
    }

    // Verify sender has capability for this namespace
    const hasCapability = session.extensions?.some((ext) => ext.namespace === namespace);
    if (!hasCapability) {
      writeMessage(socket, { type: "error", error: "Sender does not have capability for this namespace" });
      return;
    }

    const owner = this.namespaceOwners.get(namespace);
    if ((audience === "owner" || ownerOnly) && !owner) {
      writeMessage(socket, { type: "error", error: "No owner for this namespace" });
      return;
    }

    // For owner-only messages, validate exact socket and epoch
    if (ownerOnly && owner) {
      if (typeof ownerEpoch !== "string") {
        writeMessage(socket, { type: "error", error: "ownerEpoch required for owner-only messages" });
        return;
      }
      if (currentId !== owner.sessionId || socket !== owner.socket || ownerEpoch !== owner.epoch) {
        writeMessage(socket, { type: "error", error: "Owner validation failed" });
        return;
      }
    }

    // Route message to appropriate audience
    for (const [recipientId, recipientSession] of this.sessions) {
      if (!recipientSession.extensions?.length) {
        continue;
      }

      const isCapable = recipientSession.extensions.some((ext) => ext.namespace === namespace);
      if (!isCapable) {
        continue;
      }

      const shouldReceive =
        audience === "capable" ||
        (audience === "owner" && owner !== undefined &&
          recipientId === owner.sessionId &&
          recipientSession.socket === owner.socket);

      if (shouldReceive) {
        writeMessage(recipientSession.socket, {
          type: "extension_message",
          namespace,
          fromSessionId: currentId,
          ...(owner ? { ownerId: owner.sessionId, ownerEpoch: owner.epoch } : {}),
          payload,
        });
      }
    }
  }

  private handleExtensionStateCommit(
    socket: net.Socket,
    currentId: string | null,
    msg: Record<string, unknown>
  ): void {
    if (!currentId) {
      throw new Error("Received extension_state_commit before register");
    }

    const session = this.sessions.get(currentId);
    if (!session || session.socket !== socket) {
      writeMessage(socket, {
        type: "extension_state_result",
        namespace: String(msg.namespace || ""),
        committed: false,
        revision: 0,
        reason: "Session not found",
      });
      return;
    }

    if (!session.extensions?.length) {
      writeMessage(socket, {
        type: "extension_state_result",
        namespace: String(msg.namespace || ""),
        committed: false,
        revision: 0,
        reason: "Session has not advertised extension capability",
      });
      return;
    }

    const namespace = msg.namespace;
    const ownerEpoch = msg.ownerEpoch;
    const expectedRevision = msg.expectedRevision;
    const payload = msg.payload;

    if (typeof namespace !== "string" || !this.validateNamespace(namespace)) {
      writeMessage(socket, {
        type: "extension_state_result",
        namespace: String(namespace),
        committed: false,
        revision: 0,
        reason: "Invalid namespace",
      });
      return;
    }

    if (typeof ownerEpoch !== "string") {
      writeMessage(socket, {
        type: "extension_state_result",
        namespace,
        committed: false,
        revision: this.extensionStateManager.getCurrentRevision(namespace),
        reason: "Invalid ownerEpoch",
      });
      return;
    }

    if (typeof expectedRevision !== "number" || !Number.isSafeInteger(expectedRevision) || expectedRevision < 0) {
      writeMessage(socket, {
        type: "extension_state_result",
        namespace,
        committed: false,
        revision: this.extensionStateManager.getCurrentRevision(namespace),
        reason: "Invalid expectedRevision",
      });
      return;
    }

    const payloadSize = serializedPayloadSize(payload);
    if (payloadSize === null || payloadSize > MAX_EXTENSION_STATE_BYTES) {
      writeMessage(socket, {
        type: "extension_state_result",
        namespace,
        committed: false,
        revision: this.extensionStateManager.getCurrentRevision(namespace),
        reason: "Invalid extension state or payload exceeds 64 KiB limit",
      });
      return;
    }

    // Verify sender has capability for this namespace
    const hasCapability = session.extensions?.some((ext) => ext.namespace === namespace);
    if (!hasCapability) {
      writeMessage(socket, {
        type: "extension_state_result",
        namespace,
        committed: false,
        revision: this.extensionStateManager.getCurrentRevision(namespace),
        reason: "Sender does not have capability for this namespace",
      });
      return;
    }

    const owner = this.namespaceOwners.get(namespace);
    if (!owner) {
      writeMessage(socket, {
        type: "extension_state_result",
        namespace,
        committed: false,
        revision: this.extensionStateManager.getCurrentRevision(namespace),
        reason: "No owner for this namespace",
      });
      return;
    }

    // Validate owner, socket, and epoch
    if (currentId !== owner.sessionId || socket !== owner.socket || ownerEpoch !== owner.epoch) {
      writeMessage(socket, {
        type: "extension_state_result",
        namespace,
        committed: false,
        revision: this.extensionStateManager.getCurrentRevision(namespace),
        reason: "Owner validation failed",
      });
      return;
    }

    const result = this.extensionStateManager.commitState(namespace, expectedRevision, payload);

    // Send result to committer
    writeMessage(socket, {
      type: "extension_state_result",
      namespace,
      committed: result.committed,
      revision: result.revision,
      reason: result.reason,
    });

    // If committed, broadcast new state to all capable sessions
    if (result.committed) {
      for (const recipientSession of this.sessions.values()) {
        if (!recipientSession.extensions?.length) {
          continue;
        }

        const isCapable = recipientSession.extensions.some((ext) => ext.namespace === namespace);
        if (isCapable) {
          writeMessage(recipientSession.socket, {
            type: "extension_state",
            namespace,
            revision: result.revision,
            payload,
          });
        }
      }
    }
  }

  private shutdown(): void {
    console.log("Broker shutting down");
    
    for (const session of this.sessions.values()) {
      session.socket.end();
    }
    this.sessions.clear();
    this.askEdges.clear();
    this.messageReceiptRoutes.clear();
    this.disconnectedSessions.clear();
    this.mailboxMessages.length = 0;
    this.opaqueDispatch.shutdown();
    if (typeof LISTEN_TARGET === "string" && process.platform !== "win32") {
      try {
        unlinkSync(LISTEN_TARGET);
      } catch {
        // The socket may already be gone if shutdown started after a disconnect.
      }
    }
    try {
      unlinkSync(PORT_PATH);
    } catch {
      // The TCP endpoint file only exists when opt-in TCP transport is active.
    }
    try {
      unlinkSync(PID_PATH);
    } catch {
      // The PID file may already be gone if startup never completed.
    }
    this.server.close();
    process.exit(0);
  }
}

new IntercomBroker().start();
