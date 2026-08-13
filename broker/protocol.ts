import type {
  Attachment,
  ExtensionCapability,
  ExtensionStateSnapshot,
  Message,
  MessageControl,
  MessageReceipt,
  MessageReceiptStatus,
  OpaqueDispatchBrokerFrame,
  OpaqueDispatchClientFrame,
  OpaqueDispatchReason,
  OpaqueDispatchReceipt,
  OpaqueDispatchRole,
  OpaqueDispatchSender,
  OpaqueDispatchStatus,
  SessionInfo,
  SessionOpaqueCapability,
  SessionRegistration,
} from "../types.ts";

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function hasExactKeys(value: Record<string, unknown>, required: readonly string[], optional: readonly string[] = []): boolean {
  const allowed = new Set([...required, ...optional]);
  return required.every((key) => Object.hasOwn(value, key))
    && Object.keys(value).every((key) => allowed.has(key));
}

export function isBoundedId(value: unknown, maxBytes = 128): value is string {
  return typeof value === "string" && Buffer.byteLength(value, "utf8") > 0 && Buffer.byteLength(value, "utf8") <= maxBytes;
}

export function isNamespace(value: unknown): value is string {
  return typeof value === "string" && /^[a-z0-9][a-z0-9._/-]{0,63}$/.test(value);
}

export function isUuid(value: unknown): value is string {
  return typeof value === "string" && /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(value);
}

function isSafeInteger(value: unknown, minimum = 0, maximum = Number.MAX_SAFE_INTEGER): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= minimum && value <= maximum;
}

function isMessageReceiptStatus(value: unknown): value is MessageReceiptStatus {
  return value === "receiver_received" || value === "queued" || value === "injected" || value === "acknowledged"
    || value === "expired" || value === "cancelled" || value === "superseded" || value === "cancellation_requested";
}

export function isMessageReceipt(value: unknown): value is MessageReceipt {
  if (!isRecord(value)) return false;
  if (typeof value.messageId !== "string" || !isMessageReceiptStatus(value.status) || typeof value.timestamp !== "number") return false;
  return value.detail === undefined || typeof value.detail === "string";
}

export function isMessageControl(value: unknown): value is MessageControl {
  if (!isRecord(value) || Object.hasOwn(value, "detail")) return false;
  if (typeof value.messageId !== "string" || typeof value.timestamp !== "number") return false;
  if (value.action !== "cancel" && value.action !== "supersede") return false;
  return value.supersededBy === undefined || typeof value.supersededBy === "string";
}

function isAttachment(value: unknown): value is Attachment {
  if (!isRecord(value)) return false;
  if (value.type !== "file" && value.type !== "snippet" && value.type !== "context") return false;
  if (typeof value.name !== "string" || typeof value.content !== "string") return false;
  return value.language === undefined || typeof value.language === "string";
}

export function isMessage(value: unknown): value is Message {
  if (!isRecord(value)) return false;
  if (typeof value.id !== "string" || typeof value.timestamp !== "number") return false;
  for (const key of ["senderSequence", "brokerReceivedAt", "brokerDeliveredAt", "receiverReceivedAt", "injectedAt"] as const) {
    if (value[key] !== undefined && typeof value[key] !== "number") return false;
  }
  for (const key of ["supersedes", "retryOf", "replyTo"] as const) {
    if (value[key] !== undefined && typeof value[key] !== "string") return false;
  }
  if (value.expectsReply !== undefined && typeof value.expectsReply !== "boolean") return false;
  if (!isRecord(value.content) || typeof value.content.text !== "string") return false;
  return value.content.attachments === undefined
    || (Array.isArray(value.content.attachments) && value.content.attachments.every(isAttachment));
}

function isOpaqueRole(value: unknown): value is OpaqueDispatchRole {
  return value === "send" || value === "receive";
}

function areUniqueOpaqueRoles(value: unknown): value is OpaqueDispatchRole[] {
  return Array.isArray(value) && value.length > 0 && value.length <= 2
    && value.every(isOpaqueRole) && new Set(value).size === value.length;
}

export function isExtensionCapability(value: unknown): value is ExtensionCapability {
  if (!isRecord(value) || !hasExactKeys(value, ["namespace", "ownerEligible"], ["opaqueDispatch"])) return false;
  if (!isNamespace(value.namespace) || typeof value.ownerEligible !== "boolean") return false;
  if (value.opaqueDispatch === undefined) return true;
  return isRecord(value.opaqueDispatch)
    && hasExactKeys(value.opaqueDispatch, ["version", "roles"])
    && value.opaqueDispatch.version === 1
    && areUniqueOpaqueRoles(value.opaqueDispatch.roles);
}

export function opaqueSessionCapability(extensions: ExtensionCapability[] | undefined): SessionOpaqueCapability | undefined {
  const namespaces = (extensions ?? [])
    .filter((extension) => extension.opaqueDispatch !== undefined)
    .map((extension) => ({ namespace: extension.namespace, roles: [...extension.opaqueDispatch!.roles] }));
  return namespaces.length > 0 ? { version: 1, namespaces } : undefined;
}

export function isSessionOpaqueCapability(value: unknown): value is SessionOpaqueCapability {
  return isRecord(value) && hasExactKeys(value, ["version", "namespaces"]) && value.version === 1
    && Array.isArray(value.namespaces) && value.namespaces.length <= 32
    && value.namespaces.every((entry) => isRecord(entry) && hasExactKeys(entry, ["namespace", "roles"])
      && isNamespace(entry.namespace) && areUniqueOpaqueRoles(entry.roles));
}

export function isSessionInfo(value: unknown): value is SessionInfo {
  if (!isRecord(value)) return false;
  if (typeof value.id !== "string" || typeof value.cwd !== "string" || typeof value.model !== "string"
    || typeof value.pid !== "number" || typeof value.startedAt !== "number" || typeof value.lastActivity !== "number") return false;
  if (value.name !== undefined && typeof value.name !== "string") return false;
  if (value.runtimeFallbackAlias !== undefined && typeof value.runtimeFallbackAlias !== "boolean") return false;
  if (value.status !== undefined && typeof value.status !== "string") return false;
  if (value.peerUid !== undefined && typeof value.peerUid !== "number") return false;
  for (const key of ["contextPct", "contextTokens", "contextWindow"] as const) {
    if (value[key] !== undefined && typeof value[key] !== "number") return false;
  }
  if (value.opaqueDispatch !== undefined && !isSessionOpaqueCapability(value.opaqueDispatch)) return false;
  return value.trustedLocal === undefined || typeof value.trustedLocal === "boolean";
}

export function isSessionId(value: unknown): value is string {
  return isBoundedId(value, 256);
}

export function isSessionRegistration(value: unknown): value is SessionRegistration {
  if (!isRecord(value)) return false;
  if (typeof value.cwd !== "string" || typeof value.model !== "string" || typeof value.pid !== "number"
    || typeof value.startedAt !== "number" || typeof value.lastActivity !== "number") return false;
  if (value.name !== undefined && typeof value.name !== "string") return false;
  if (value.runtimeFallbackAlias !== undefined && typeof value.runtimeFallbackAlias !== "boolean") return false;
  if (value.extensions !== undefined && (!Array.isArray(value.extensions) || !value.extensions.every(isExtensionCapability))) return false;
  return value.status === undefined || typeof value.status === "string";
}

export function isExtensionStateSnapshot(value: unknown): value is ExtensionStateSnapshot {
  if (!isRecord(value) || !isNamespace(value.namespace) || !isSafeInteger(value.revision)) return false;
  if (value.present === false) return value.revision === 0 && hasExactKeys(value, ["namespace", "revision", "present"]);
  return value.present === true && value.revision >= 1 && hasExactKeys(value, ["namespace", "revision", "present", "payload"]);
}

const OPAQUE_REASONS = new Set<OpaqueDispatchReason>([
  "unsupported_host", "unsupported_broker", "unsupported_target", "unknown_exact_target", "self_dispatch_unsupported",
  "invalid_request", "invalid_frame", "request_conflict", "limit_exceeded", "rate_limited", "broker_epoch_changed",
  "claim_history_unavailable", "payload_too_large", "consumer_missing", "consumer_unloaded", "consumer_refused",
  "consumer_threw", "consumer_failed", "reservation_timeout", "claim_timeout", "malformed_consumer_result",
  "stale_reservation", "receiver_disconnected", "capability_invalidated", "queued_supersede_unsupported",
  "already_claimed", "already_terminal", "not_origin", "attempt_limit", "history_limit", "connection_lost",
  "uncorrelated_operation_pending",
]);
const OPAQUE_STATUSES = new Set<OpaqueDispatchStatus>([
  "queued", "reserved", "claimed", "refused", "expired", "cancelled", "superseded", "failed_closed",
]);

export function isOpaqueDispatchReason(value: unknown): value is OpaqueDispatchReason {
  return typeof value === "string" && OPAQUE_REASONS.has(value as OpaqueDispatchReason);
}

function isOpaqueSender(value: unknown): value is OpaqueDispatchSender {
  if (!isRecord(value) || !hasExactKeys(value, ["sessionId", "namespace", "trustedLocal"], ["owner"])) return false;
  if (!isSessionId(value.sessionId) || !isNamespace(value.namespace) || typeof value.trustedLocal !== "boolean") return false;
  return value.owner === undefined || (isRecord(value.owner) && hasExactKeys(value.owner, ["sessionId", "epoch"])
    && isSessionId(value.owner.sessionId) && isBoundedId(value.owner.epoch));
}

function isOpaqueReceipt(value: unknown): value is OpaqueDispatchReceipt {
  if (!isRecord(value) || !hasExactKeys(value, ["requestId", "messageId", "status", "at", "attempt", "sequence"], ["reason"])) return false;
  return isBoundedId(value.requestId) && isUuid(value.messageId)
    && typeof value.status === "string" && OPAQUE_STATUSES.has(value.status as OpaqueDispatchStatus)
    && isSafeInteger(value.at) && isSafeInteger(value.attempt, 1, 8) && isSafeInteger(value.sequence, 1, 20)
    && (value.reason === undefined || isOpaqueDispatchReason(value.reason));
}

function isClaimStatusResult(value: unknown): boolean {
  if (!isRecord(value) || typeof value.state !== "string") return false;
  if (value.state === "claimed" || value.state === "reserved") return hasExactKeys(value, ["state"]);
  if (value.state === "stale") return hasExactKeys(value, ["state", "code"]) && value.code === "stale_reservation";
  return value.state === "indeterminate" && hasExactKeys(value, ["state", "code"])
    && (value.code === "broker_epoch_changed" || value.code === "claim_history_unavailable");
}

export function isOpaqueDispatchClientFrame(value: unknown): value is OpaqueDispatchClientFrame {
  if (!isRecord(value) || typeof value.type !== "string") return false;
  switch (value.type) {
    case "opaque_dispatch_v1_send":
      return hasExactKeys(value, ["type", "operationId", "requestId", "senderNamespace", "toSessionId", "recipientNamespace", "payload"], ["supersedesMessageId"])
        && isBoundedId(value.operationId) && isBoundedId(value.requestId) && isNamespace(value.senderNamespace)
        && isSessionId(value.toSessionId) && isNamespace(value.recipientNamespace)
        && (value.supersedesMessageId === undefined || isUuid(value.supersedesMessageId));
    case "opaque_dispatch_v1_cancel":
      return hasExactKeys(value, ["type", "operationId", "senderNamespace", "messageId"])
        && isBoundedId(value.operationId) && isNamespace(value.senderNamespace) && isUuid(value.messageId);
    case "opaque_dispatch_v1_reservation_result":
      return hasExactKeys(value, ["type", "reservationId", "messageId", "decision"], ["reason"])
        && isUuid(value.reservationId) && isUuid(value.messageId)
        && (value.decision === "reserved" || value.decision === "refused" || value.decision === "failed_closed")
        && (value.reason === undefined || isOpaqueDispatchReason(value.reason));
    case "opaque_dispatch_v1_claim":
      return hasExactKeys(value, ["type", "operationId", "reservationId", "messageId"])
        && isBoundedId(value.operationId) && isUuid(value.reservationId) && isUuid(value.messageId);
    case "opaque_dispatch_v1_fail":
      return hasExactKeys(value, ["type", "operationId", "reservationId", "messageId", "reason"])
        && isBoundedId(value.operationId) && isUuid(value.reservationId) && isUuid(value.messageId) && value.reason === "consumer_failed";
    case "opaque_dispatch_v1_claim_status":
      return hasExactKeys(value, ["type", "operationId", "recipientNamespace", "brokerEpoch", "reservationId", "messageId"])
        && isBoundedId(value.operationId) && isNamespace(value.recipientNamespace) && isUuid(value.brokerEpoch)
        && isUuid(value.reservationId) && isUuid(value.messageId);
    case "opaque_dispatch_v1_peer_capability_get":
      return hasExactKeys(value, ["type", "operationId", "toSessionId", "recipientNamespace"])
        && isBoundedId(value.operationId) && isSessionId(value.toSessionId) && isNamespace(value.recipientNamespace);
    case "opaque_dispatch_v1_receipt_ack":
      return hasExactKeys(value, ["type", "senderNamespace", "messageId", "sequence"])
        && isNamespace(value.senderNamespace) && isUuid(value.messageId) && isSafeInteger(value.sequence, 1, 20);
    default:
      return false;
  }
}

export function isOpaqueDispatchBrokerFrame(value: unknown): value is OpaqueDispatchBrokerFrame {
  if (!isRecord(value) || typeof value.type !== "string") return false;
  switch (value.type) {
    case "opaque_dispatch_v1_ack":
      return hasExactKeys(value, ["type", "operationId", "requestId", "messageId", "brokerEpoch", "deliveryState"])
        && isBoundedId(value.operationId) && isBoundedId(value.requestId) && isUuid(value.messageId) && isUuid(value.brokerEpoch)
        && (value.deliveryState === "live" || value.deliveryState === "mailbox_queued");
    case "opaque_dispatch_v1_rejected":
      return hasExactKeys(value, ["type", "operationId", "code"], ["requestId", "messageId", "terminal"])
        && isBoundedId(value.operationId) && (value.requestId === undefined || isBoundedId(value.requestId))
        && (value.messageId === undefined || isUuid(value.messageId)) && isOpaqueDispatchReason(value.code)
        && (value.terminal === undefined || value.terminal === "refused" || value.terminal === "failed_closed");
    case "opaque_dispatch_v1_offer":
      return hasExactKeys(value, ["type", "reservationId", "requestId", "messageId", "attempt", "brokerEpoch", "toSessionId", "recipientNamespace", "sender", "payload", "reserveBy"])
        && isUuid(value.reservationId) && isBoundedId(value.requestId) && isUuid(value.messageId)
        && isSafeInteger(value.attempt, 1, 8) && isUuid(value.brokerEpoch) && isSessionId(value.toSessionId)
        && isNamespace(value.recipientNamespace) && isOpaqueSender(value.sender) && isSafeInteger(value.reserveBy);
    case "opaque_dispatch_v1_reservation_ended":
      return hasExactKeys(value, ["type", "messageId", "reservationId", "outcome"], ["reason"])
        && isUuid(value.messageId) && isUuid(value.reservationId)
        && (value.outcome === "expired" || value.outcome === "cancelled" || value.outcome === "superseded" || value.outcome === "failed_closed")
        && (value.reason === undefined || isOpaqueDispatchReason(value.reason));
    case "opaque_dispatch_v1_receipt":
      return hasExactKeys(value, ["type", "senderNamespace", "receipt"])
        && isNamespace(value.senderNamespace) && isOpaqueReceipt(value.receipt);
    case "opaque_dispatch_v1_claim_result":
      return hasExactKeys(value, ["type", "operationId", "reservationId", "messageId", "claimed"], ["code"])
        && isBoundedId(value.operationId) && isUuid(value.reservationId) && isUuid(value.messageId)
        && typeof value.claimed === "boolean" && (value.code === undefined || isOpaqueDispatchReason(value.code));
    case "opaque_dispatch_v1_fail_result":
      return hasExactKeys(value, ["type", "operationId", "reservationId", "messageId", "failedClosed"], ["code"])
        && isBoundedId(value.operationId) && isUuid(value.reservationId) && isUuid(value.messageId)
        && typeof value.failedClosed === "boolean" && (value.code === undefined || isOpaqueDispatchReason(value.code));
    case "opaque_dispatch_v1_claim_status_result":
      return hasExactKeys(value, ["type", "operationId", "brokerEpoch", "reservationId", "messageId", "result"])
        && isBoundedId(value.operationId) && isUuid(value.brokerEpoch) && isUuid(value.reservationId)
        && isUuid(value.messageId) && isClaimStatusResult(value.result);
    case "opaque_dispatch_v1_cancel_result":
      return hasExactKeys(value, ["type", "operationId", "messageId", "cancelled"], ["code"])
        && isBoundedId(value.operationId) && isUuid(value.messageId) && typeof value.cancelled === "boolean"
        && (value.code === undefined || isOpaqueDispatchReason(value.code));
    case "opaque_dispatch_v1_peer_capability_result":
      return hasExactKeys(value, ["type", "operationId", "toSessionId", "recipientNamespace", "state"], ["version"])
        && isBoundedId(value.operationId) && isSessionId(value.toSessionId) && isNamespace(value.recipientNamespace)
        && (value.state === "present" || value.state === "absent" || value.state === "unknown")
        && (value.version === undefined || value.version === 1) && (value.state === "present" ? value.version === 1 : value.version === undefined);
    default:
      return false;
  }
}
