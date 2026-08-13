import { createHash, randomUUID } from "node:crypto";
import type { ExtensionCapability, OpaqueDispatchBrokerFrame, OpaqueDispatchClientFrame, OpaqueDispatchReason, OpaqueDispatchReceipt, OpaqueDispatchStatus, SessionInfo } from "../types.ts";

export const OPAQUE_ACTIVE_TTL_MS = 24 * 60 * 60 * 1000;
export const OPAQUE_TOMBSTONE_TTL_MS = 60 * 60 * 1000;
export const OPAQUE_RESERVATION_TIMEOUT_MS = 5_000;
export const OPAQUE_CLAIM_TIMEOUT_MS = 30_000;
export const MAX_OPAQUE_ACTIVE_RECORDS = 256;
export const MAX_OPAQUE_PRINCIPAL_RECORDS = 32;
export const MAX_OPAQUE_TARGET_RECORDS = 32;
export const MAX_OPAQUE_TOMBSTONES = 512;
export const MAX_OPAQUE_PRINCIPAL_TOMBSTONES = 64;
export const MAX_OPAQUE_TARGET_TOMBSTONES = 64;
export const MAX_OPAQUE_ATTEMPTS = 8;
export const MAX_OPAQUE_RECEIPTS = 20;
export const MAX_OPAQUE_WAITERS = 8;
export const MAX_OPAQUE_PAYLOAD_BYTES = 64 * 1024;

const OPAQUE_CONSUMER_REASONS = new Set<OpaqueDispatchReason>([
  "consumer_refused", "consumer_failed", "consumer_threw", "consumer_unloaded", "consumer_missing", "malformed_consumer_result",
]);

export interface OpaqueEndpoint {
  sessionId: string;
  info: SessionInfo;
  extensions?: ExtensionCapability[];
  connected: boolean;
  write?: (frame: OpaqueDispatchBrokerFrame) => void;
}

export type OpaqueWriteRequirement =
  | { namespace: string; role: "send" | "receive" }
  | { anyOpaqueCapability: true };

export function writeOpaqueTo(
  endpoint: OpaqueEndpoint | undefined,
  requirement: OpaqueWriteRequirement,
  frame: OpaqueDispatchBrokerFrame,
): boolean {
  if (!endpoint?.connected || !endpoint.write) return false;
  const capable = "anyOpaqueCapability" in requirement
    ? Boolean(endpoint.extensions?.some((extension) => extension.opaqueDispatch?.version === 1))
    : hasRole(endpoint, requirement.namespace, requirement.role);
  if (!capable) return false;
  try {
    endpoint.write(frame);
    return true;
  } catch {
    return false;
  }
}

interface OpaqueDispatchHooks {
  brokerEpoch: string;
  endpoint(sessionId: string): OpaqueEndpoint | undefined;
  owner(namespace: string): { sessionId: string; epoch: string } | undefined;
  now?: () => number;
  activeTtlMs?: number;
  tombstoneTtlMs?: number;
  reservationTimeoutMs?: number;
  claimTimeoutMs?: number;
}

interface Waiter {
  operationId: string;
  requestId: string;
}

interface Reservation {
  id: string;
  targetSessionId: string;
  recipientNamespace: string;
  timer: NodeJS.Timeout;
}

interface RecordState {
  key: string;
  digest: string;
  originSessionId: string;
  senderNamespace: string;
  requestId: string;
  messageId: string;
  targetSessionId: string;
  recipientNamespace: string;
  createdAt: number;
  payload?: unknown;
  status: "queued" | "offered" | "reserved" | "claimed" | "terminal";
  terminalStatus?: Exclude<OpaqueDispatchStatus, "queued" | "reserved">;
  terminalReason?: OpaqueDispatchReason;
  attempt: number;
  reservation?: Reservation;
  waiters: Waiter[];
  receipts: OpaqueDispatchReceipt[];
  ackedThrough: number;
  activeTimer?: NodeJS.Timeout;
  tombstoneTimer?: NodeJS.Timeout;
  lastReservationId?: string;
}

export type CanonicalPayloadResult =
  | { ok: true; json: string; normalized: unknown }
  | { ok: false; code: "invalid_request" | "payload_too_large" };

function codePointCompare(a: string, b: string): number {
  const left = Array.from(a);
  const right = Array.from(b);
  const length = Math.min(left.length, right.length);
  for (let index = 0; index < length; index += 1) {
    const difference = left[index]!.codePointAt(0)! - right[index]!.codePointAt(0)!;
    if (difference !== 0) return difference;
  }
  return left.length - right.length;
}

function normalizeCanonical(value: unknown, ancestors: Set<object>): unknown {
  if (value === null || typeof value === "string" || typeof value === "boolean") return value;
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new Error("invalid");
    return Object.is(value, -0) ? 0 : value;
  }
  if (typeof value !== "object") throw new Error("invalid");
  if (ancestors.has(value)) throw new Error("invalid");
  ancestors.add(value);
  try {
    if (Array.isArray(value)) {
      for (let index = 0; index < value.length; index += 1) {
        if (!Object.hasOwn(value, index)) throw new Error("invalid");
      }
      return value.map((entry) => normalizeCanonical(entry, ancestors));
    }
    const prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) throw new Error("invalid");
    if (Object.hasOwn(value, "toJSON") || Object.getOwnPropertySymbols(value).length > 0) throw new Error("invalid");
    const descriptors = Object.getOwnPropertyDescriptors(value);
    const normalized = Object.create(null) as Record<string, unknown>;
    for (const key of Object.keys(descriptors).sort(codePointCompare)) {
      const descriptor = descriptors[key]!;
      if (!descriptor.enumerable || descriptor.get || descriptor.set || !("value" in descriptor)) throw new Error("invalid");
      normalized[key] = normalizeCanonical(descriptor.value, ancestors);
    }
    return normalized;
  } finally {
    ancestors.delete(value);
  }
}

export function canonicalizeOpaquePayload(value: unknown): CanonicalPayloadResult {
  try {
    const normalized = normalizeCanonical(value, new Set());
    const json = JSON.stringify(normalized);
    if (json === undefined) return { ok: false, code: "invalid_request" };
    if (Buffer.byteLength(json, "utf8") > MAX_OPAQUE_PAYLOAD_BYTES) return { ok: false, code: "payload_too_large" };
    return { ok: true, json, normalized };
  } catch {
    return { ok: false, code: "invalid_request" };
  }
}

function fingerprint(payloadJson: string, targetSessionId: string, recipientNamespace: string, supersedesMessageId?: string): string {
  const envelope = `{"payload":${payloadJson},"recipientNamespace":${JSON.stringify(recipientNamespace)},"supersedesMessageId":${JSON.stringify(supersedesMessageId ?? null)},"toSessionId":${JSON.stringify(targetSessionId)}}`;
  return createHash("sha256").update(envelope).digest("hex");
}

function hasRole(endpoint: OpaqueEndpoint | undefined, namespace: string, role: "send" | "receive"): boolean {
  return Boolean(endpoint?.extensions?.some((extension) => extension.namespace === namespace
    && extension.opaqueDispatch?.version === 1 && extension.opaqueDispatch.roles.includes(role)));
}

function principalKey(sessionId: string, namespace: string): string {
  return `${sessionId}\0${namespace}`;
}

function targetKey(sessionId: string, namespace: string): string {
  return `${sessionId}\0${namespace}`;
}

export class OpaqueDispatchManager {
  private readonly records = new Map<string, RecordState>();
  private readonly byMessageId = new Map<string, RecordState>();
  private readonly hooks: OpaqueDispatchHooks;

  constructor(hooks: OpaqueDispatchHooks) {
    this.hooks = hooks;
  }

  get activeCount(): number {
    return [...this.records.values()].filter((record) => record.status !== "terminal" && record.status !== "claimed").length;
  }

  get tombstoneCount(): number {
    return [...this.records.values()].filter((record) => record.status === "terminal" || record.status === "claimed").length;
  }

  handle(origin: OpaqueEndpoint, frame: OpaqueDispatchClientFrame): void {
    switch (frame.type) {
      case "opaque_dispatch_v1_send": this.send(origin, frame); break;
      case "opaque_dispatch_v1_cancel": this.cancel(origin, frame); break;
      case "opaque_dispatch_v1_reservation_result": this.reservationResult(origin, frame); break;
      case "opaque_dispatch_v1_claim": this.claim(origin, frame); break;
      case "opaque_dispatch_v1_fail": this.fail(origin, frame); break;
      case "opaque_dispatch_v1_claim_status": this.claimStatus(origin, frame); break;
      case "opaque_dispatch_v1_receipt_ack": this.receiptAck(origin, frame); break;
      case "opaque_dispatch_v1_peer_capability_get": break;
    }
  }

  rateLimited(endpoint: OpaqueEndpoint, frame: OpaqueDispatchClientFrame): void {
    if (frame.type === "opaque_dispatch_v1_reservation_result" || frame.type === "opaque_dispatch_v1_claim" || frame.type === "opaque_dispatch_v1_fail") {
      const record = this.authorizedReservation(endpoint, frame.messageId, frame.reservationId);
      if (!record) return;
      this.endReservation(record, "failed_closed", "rate_limited");
      this.rejectWaiters(record, "rate_limited", "failed_closed");
      this.terminalize(record, "failed_closed", "rate_limited");
      return;
    }
    if (!("operationId" in frame)) return;
    writeOpaqueTo(endpoint, { anyOpaqueCapability: true }, {
      type: "opaque_dispatch_v1_rejected",
      operationId: frame.operationId,
      ...("requestId" in frame ? { requestId: frame.requestId } : {}),
      ...("messageId" in frame ? { messageId: frame.messageId } : {}),
      code: "rate_limited",
    });
  }

  endpointAvailable(sessionId: string): void {
    for (const record of this.records.values()) {
      if (record.status === "queued" && record.targetSessionId === sessionId) this.offer(record);
      if (record.originSessionId === sessionId) this.replayReceipts(record);
    }
  }

  endpointDisconnected(sessionId: string): void {
    for (const record of this.records.values()) {
      const reservation = record.reservation;
      if (!reservation || reservation.targetSessionId !== sessionId) continue;
      clearTimeout(reservation.timer);
      record.reservation = undefined;
      if (record.status === "offered" || record.status === "reserved") {
        if (record.attempt >= MAX_OPAQUE_ATTEMPTS) this.terminalize(record, "failed_closed", "attempt_limit");
        else {
          if (record.status === "offered" && record.waiters.length > 0) this.ackWaiters(record, "mailbox_queued");
          record.status = "queued";
          this.receipt(record, "queued", "receiver_disconnected");
        }
      }
    }
  }

  capabilityChanged(sessionId: string): void {
    const pendingInvalidation: RecordState[] = [];
    for (const record of this.records.values()) {
      if (record.originSessionId === sessionId) this.replayReceipts(record);
      if (record.targetSessionId !== sessionId) continue;
      const endpoint = this.hooks.endpoint(sessionId);
      if (hasRole(endpoint, record.recipientNamespace, "receive")) {
        if (record.status === "queued") this.offer(record);
        continue;
      }
      if (record.status === "offered" || record.status === "reserved") pendingInvalidation.push(record);
    }
    if (pendingInvalidation.length === 0) return;
    setImmediate(() => {
      for (const record of pendingInvalidation) {
        if (record.status !== "offered" && record.status !== "reserved") continue;
        const endpoint = this.hooks.endpoint(sessionId);
        if (hasRole(endpoint, record.recipientNamespace, "receive")) continue;
        if (record.reservation) this.endReservation(record, "failed_closed", "capability_invalidated");
        this.terminalize(record, "failed_closed", "capability_invalidated");
      }
    });
  }

  shutdown(): void {
    for (const record of this.records.values()) {
      if (record.activeTimer) clearTimeout(record.activeTimer);
      if (record.tombstoneTimer) clearTimeout(record.tombstoneTimer);
      if (record.reservation) clearTimeout(record.reservation.timer);
    }
    this.records.clear();
    this.byMessageId.clear();
  }

  private now(): number { return this.hooks.now?.() ?? Date.now(); }
  private activeTtlMs(): number { return this.hooks.activeTtlMs ?? OPAQUE_ACTIVE_TTL_MS; }
  private tombstoneTtlMs(): number { return this.hooks.tombstoneTtlMs ?? OPAQUE_TOMBSTONE_TTL_MS; }
  private reservationTimeoutMs(): number { return this.hooks.reservationTimeoutMs ?? OPAQUE_RESERVATION_TIMEOUT_MS; }
  private claimTimeoutMs(): number { return this.hooks.claimTimeoutMs ?? OPAQUE_CLAIM_TIMEOUT_MS; }

  private send(origin: OpaqueEndpoint, frame: Extract<OpaqueDispatchClientFrame, { type: "opaque_dispatch_v1_send" }>): void {
    const reject = (code: OpaqueDispatchReason, messageId?: string) => writeOpaqueTo(origin, { anyOpaqueCapability: true }, {
      type: "opaque_dispatch_v1_rejected", operationId: frame.operationId, requestId: frame.requestId,
      ...(messageId ? { messageId } : {}), code,
    });
    if (!origin.connected || !hasRole(origin, frame.senderNamespace, "send")) return void reject("not_origin");
    if (origin.sessionId === frame.toSessionId) return void reject("self_dispatch_unsupported");
    const canonical = canonicalizeOpaquePayload(frame.payload);
    if (!canonical.ok) return void reject(canonical.code);
    const key = `${origin.sessionId}\0${frame.senderNamespace}\0${frame.requestId}`;
    const digest = fingerprint(canonical.json, frame.toSessionId, frame.recipientNamespace, frame.supersedesMessageId);
    const existing = this.records.get(key);
    if (existing) {
      if (existing.digest !== digest) return void reject("request_conflict", existing.messageId);
      if (existing.status === "offered") {
        if (existing.waiters.length >= MAX_OPAQUE_WAITERS) return void reject("limit_exceeded", existing.messageId);
        existing.waiters.push({ operationId: frame.operationId, requestId: frame.requestId });
        return;
      }
      return this.replayResult(existing, frame.operationId);
    }
    const target = this.hooks.endpoint(frame.toSessionId);
    if (!target) return void reject("unknown_exact_target");
    if (!hasRole(target, frame.recipientNamespace, "receive")) return void reject("unsupported_target");
    if (!this.hasCapacity(origin.sessionId, frame.senderNamespace, frame.toSessionId, frame.recipientNamespace)) return void reject("limit_exceeded");
    if (frame.supersedesMessageId) {
      const prior = this.byMessageId.get(frame.supersedesMessageId);
      if (!prior || prior.originSessionId !== origin.sessionId || prior.senderNamespace !== frame.senderNamespace
        || prior.targetSessionId !== frame.toSessionId || prior.recipientNamespace !== frame.recipientNamespace) return void reject("not_origin");
      if (prior.status === "queued") return void reject("queued_supersede_unsupported");
      if (prior.status === "claimed") return void reject("already_claimed");
      if (prior.status === "terminal") return void reject("already_terminal");
      this.endReservation(prior, "superseded");
      this.terminalize(prior, "superseded");
    }
    const now = this.now();
    const record: RecordState = {
      key, digest, originSessionId: origin.sessionId, senderNamespace: frame.senderNamespace,
      requestId: frame.requestId, messageId: randomUUID(), targetSessionId: frame.toSessionId,
      recipientNamespace: frame.recipientNamespace, createdAt: now, payload: canonical.normalized,
      status: "queued", attempt: 0,
      waiters: [{ operationId: frame.operationId, requestId: frame.requestId }], receipts: [], ackedThrough: 0,
    };
    record.activeTimer = setTimeout(() => this.expire(record), this.activeTtlMs());
    record.activeTimer.unref?.();
    this.records.set(key, record);
    this.byMessageId.set(record.messageId, record);
    if (target.connected) this.offer(record);
    else {
      this.ackWaiters(record, "mailbox_queued");
      this.receipt(record, "queued");
    }
  }

  private hasCapacity(originId: string, senderNamespace: string, targetId: string, recipientNamespace: string): boolean {
    let active = [...this.records.values()].filter((record) => record.status !== "terminal" && record.status !== "claimed");
    if (active.filter((record) => principalKey(record.originSessionId, record.senderNamespace) === principalKey(originId, senderNamespace)).length >= MAX_OPAQUE_PRINCIPAL_RECORDS) return false;
    if (active.filter((record) => targetKey(record.targetSessionId, record.recipientNamespace) === targetKey(targetId, recipientNamespace)).length >= MAX_OPAQUE_TARGET_RECORDS) return false;
    if (active.length >= MAX_OPAQUE_ACTIVE_RECORDS) {
      const oldestQueued = active.filter((record) => record.status === "queued").sort((a, b) => a.createdAt - b.createdAt)[0];
      if (!oldestQueued) return false;
      this.terminalize(oldestQueued, "expired", "limit_exceeded");
      active = [...this.records.values()].filter((record) => record.status !== "terminal" && record.status !== "claimed");
    }
    return active.length < MAX_OPAQUE_ACTIVE_RECORDS;
  }

  private offer(record: RecordState): void {
    const target = this.hooks.endpoint(record.targetSessionId);
    if (!target?.connected || !target.write || !hasRole(target, record.recipientNamespace, "receive") || record.payload === undefined) return;
    if (record.attempt >= MAX_OPAQUE_ATTEMPTS) return this.terminalize(record, "failed_closed", "attempt_limit");
    record.attempt += 1;
    const reservationId = randomUUID();
    const reserveBy = this.now() + this.reservationTimeoutMs();
    const timer = setTimeout(() => {
      if (record.reservation?.id !== reservationId || record.status !== "offered") return;
      this.endReservation(record, "failed_closed", "reservation_timeout");
      this.rejectWaiters(record, "reservation_timeout", "failed_closed");
      this.terminalize(record, "failed_closed", "reservation_timeout");
    }, this.reservationTimeoutMs());
    timer.unref?.();
    record.reservation = { id: reservationId, targetSessionId: record.targetSessionId, recipientNamespace: record.recipientNamespace, timer };
    record.status = "offered";
    const origin = this.hooks.endpoint(record.originSessionId);
    const offered = writeOpaqueTo(target, { namespace: record.recipientNamespace, role: "receive" }, {
      type: "opaque_dispatch_v1_offer", reservationId, requestId: record.requestId, messageId: record.messageId,
      attempt: record.attempt, brokerEpoch: this.hooks.brokerEpoch, toSessionId: record.targetSessionId,
      recipientNamespace: record.recipientNamespace,
      sender: {
        sessionId: record.originSessionId, namespace: record.senderNamespace,
        trustedLocal: origin?.info.trustedLocal === true,
        ...(this.hooks.owner(record.senderNamespace) ? { owner: this.hooks.owner(record.senderNamespace)! } : {}),
      },
      payload: record.payload,
      reserveBy,
    });
    if (!offered) {
      clearTimeout(timer);
      record.reservation = undefined;
      this.rejectWaiters(record, "receiver_disconnected", "failed_closed");
      this.terminalize(record, "failed_closed", "receiver_disconnected");
    }
  }

  private reservationResult(endpoint: OpaqueEndpoint, frame: Extract<OpaqueDispatchClientFrame, { type: "opaque_dispatch_v1_reservation_result" }>): void {
    const record = this.byMessageId.get(frame.messageId);
    const reservation = record?.reservation;
    if (!record || !reservation || reservation.id !== frame.reservationId || reservation.targetSessionId !== endpoint.sessionId
      || (frame.decision === "reserved" ? record.status !== "offered" : (record.status !== "offered" && record.status !== "reserved"))) return;
    // An exact current receiver may fail closed after its capability-removal frame
    // has been read; accepting only the negative settlement preserves dispose order.
    if (frame.decision === "reserved" && !hasRole(endpoint, reservation.recipientNamespace, "receive")) return;
    clearTimeout(reservation.timer);
    if (frame.decision !== "reserved") {
      const fallback = frame.decision === "refused" ? "consumer_refused" : "consumer_failed";
      const reason = frame.reason && OPAQUE_CONSUMER_REASONS.has(frame.reason) ? frame.reason : fallback;
      this.endReservation(record, "failed_closed", reason);
      this.rejectWaiters(record, reason, frame.decision === "refused" ? "refused" : "failed_closed");
      this.terminalize(record, frame.decision === "refused" ? "refused" : "failed_closed", reason);
      return;
    }
    reservation.timer = setTimeout(() => {
      if (record.reservation?.id !== reservation.id || record.status !== "reserved") return;
      this.endReservation(record, "failed_closed", "claim_timeout");
      this.terminalize(record, "failed_closed", "claim_timeout");
    }, this.claimTimeoutMs());
    reservation.timer.unref?.();
    record.status = "reserved";
    this.ackWaiters(record, "live");
    this.receipt(record, "reserved");
  }

  private claim(endpoint: OpaqueEndpoint, frame: Extract<OpaqueDispatchClientFrame, { type: "opaque_dispatch_v1_claim" }>): void {
    const known = this.byMessageId.get(frame.messageId);
    if (known?.status === "claimed" && known.lastReservationId === frame.reservationId
      && known.targetSessionId === endpoint.sessionId && hasRole(endpoint, known.recipientNamespace, "receive")) {
      writeOpaqueTo(endpoint, { namespace: known.recipientNamespace, role: "receive" }, { type: "opaque_dispatch_v1_claim_result", operationId: frame.operationId, reservationId: frame.reservationId, messageId: frame.messageId, claimed: true });
      return;
    }
    const record = this.authorizedReservation(endpoint, frame.messageId, frame.reservationId);
    if (!record || record.status !== "reserved") {
      writeOpaqueTo(endpoint, { anyOpaqueCapability: true }, { type: "opaque_dispatch_v1_claim_result", operationId: frame.operationId, reservationId: frame.reservationId, messageId: frame.messageId, claimed: false, code: "stale_reservation" });
      return;
    }
    clearTimeout(record.reservation!.timer);
    record.lastReservationId = frame.reservationId;
    record.reservation = undefined;
    record.status = "claimed";
    this.clearPayload(record);
    this.receipt(record, "claimed");
    this.startTombstone(record);
    this.enforceTombstoneCapacity();
    writeOpaqueTo(endpoint, { namespace: record.recipientNamespace, role: "receive" }, { type: "opaque_dispatch_v1_claim_result", operationId: frame.operationId, reservationId: frame.reservationId, messageId: frame.messageId, claimed: true });
  }

  private fail(endpoint: OpaqueEndpoint, frame: Extract<OpaqueDispatchClientFrame, { type: "opaque_dispatch_v1_fail" }>): void {
    const record = this.authorizedReservation(endpoint, frame.messageId, frame.reservationId);
    if (!record || record.status !== "reserved") {
      writeOpaqueTo(endpoint, { anyOpaqueCapability: true }, { type: "opaque_dispatch_v1_fail_result", operationId: frame.operationId, reservationId: frame.reservationId, messageId: frame.messageId, failedClosed: false, code: "stale_reservation" });
      return;
    }
    this.endReservation(record, "failed_closed", "consumer_failed");
    this.terminalize(record, "failed_closed", "consumer_failed");
    writeOpaqueTo(endpoint, { namespace: record.recipientNamespace, role: "receive" }, { type: "opaque_dispatch_v1_fail_result", operationId: frame.operationId, reservationId: frame.reservationId, messageId: frame.messageId, failedClosed: true });
  }

  private cancel(origin: OpaqueEndpoint, frame: Extract<OpaqueDispatchClientFrame, { type: "opaque_dispatch_v1_cancel" }>): void {
    const record = this.byMessageId.get(frame.messageId);
    const response = (cancelled: boolean, code?: OpaqueDispatchReason) => writeOpaqueTo(origin, { namespace: frame.senderNamespace, role: "send" }, { type: "opaque_dispatch_v1_cancel_result", operationId: frame.operationId, messageId: frame.messageId, cancelled, ...(code ? { code } : {}) });
    if (!record || record.originSessionId !== origin.sessionId || record.senderNamespace !== frame.senderNamespace) return void response(false, "not_origin");
    if (record.status === "claimed") return void response(false, "already_claimed");
    if (record.status === "terminal") return void response(false, "already_terminal");
    this.endReservation(record, "cancelled");
    this.terminalize(record, "cancelled");
    response(true);
  }

  private claimStatus(endpoint: OpaqueEndpoint, frame: Extract<OpaqueDispatchClientFrame, { type: "opaque_dispatch_v1_claim_status" }>): void {
    const record = this.byMessageId.get(frame.messageId);
    let result: Extract<OpaqueDispatchBrokerFrame, { type: "opaque_dispatch_v1_claim_status_result" }>["result"];
    if (frame.brokerEpoch !== this.hooks.brokerEpoch) result = { state: "indeterminate", code: "broker_epoch_changed" };
    else if (!record) result = { state: "indeterminate", code: "claim_history_unavailable" };
    else if (record.targetSessionId !== endpoint.sessionId || record.recipientNamespace !== frame.recipientNamespace || !hasRole(endpoint, frame.recipientNamespace, "receive")) result = { state: "indeterminate", code: "claim_history_unavailable" };
    else if (record.status === "claimed" && record.lastReservationId === frame.reservationId) result = { state: "claimed" };
    else if (record.status === "reserved" && record.reservation?.id === frame.reservationId) result = { state: "reserved" };
    else result = { state: "stale", code: "stale_reservation" };
    writeOpaqueTo(endpoint, { anyOpaqueCapability: true }, { type: "opaque_dispatch_v1_claim_status_result", operationId: frame.operationId, brokerEpoch: this.hooks.brokerEpoch, reservationId: frame.reservationId, messageId: frame.messageId, result });
  }

  private receiptAck(origin: OpaqueEndpoint, frame: Extract<OpaqueDispatchClientFrame, { type: "opaque_dispatch_v1_receipt_ack" }>): void {
    const record = this.byMessageId.get(frame.messageId);
    if (!record || record.originSessionId !== origin.sessionId || record.senderNamespace !== frame.senderNamespace) return;
    record.ackedThrough = Math.max(record.ackedThrough, frame.sequence);
  }

  private authorizedReservation(endpoint: OpaqueEndpoint, messageId: string, reservationId: string): RecordState | undefined {
    const record = this.byMessageId.get(messageId);
    return record?.reservation?.id === reservationId && record.targetSessionId === endpoint.sessionId
      && hasRole(endpoint, record.recipientNamespace, "receive") ? record : undefined;
  }

  private ackWaiters(record: RecordState, deliveryState: "live" | "mailbox_queued"): void {
    const origin = this.hooks.endpoint(record.originSessionId);
    for (const waiter of record.waiters.splice(0)) writeOpaqueTo(origin, { namespace: record.senderNamespace, role: "send" }, { type: "opaque_dispatch_v1_ack", operationId: waiter.operationId, requestId: waiter.requestId, messageId: record.messageId, brokerEpoch: this.hooks.brokerEpoch, deliveryState });
  }

  private rejectWaiters(record: RecordState, code: OpaqueDispatchReason, terminal: "refused" | "failed_closed"): void {
    const origin = this.hooks.endpoint(record.originSessionId);
    for (const waiter of record.waiters.splice(0)) writeOpaqueTo(origin, { namespace: record.senderNamespace, role: "send" }, { type: "opaque_dispatch_v1_rejected", operationId: waiter.operationId, requestId: waiter.requestId, messageId: record.messageId, code, terminal });
  }

  private replayResult(record: RecordState, operationId: string): void {
    const origin = this.hooks.endpoint(record.originSessionId);
    if (record.status === "queued") writeOpaqueTo(origin, { namespace: record.senderNamespace, role: "send" }, { type: "opaque_dispatch_v1_ack", operationId, requestId: record.requestId, messageId: record.messageId, brokerEpoch: this.hooks.brokerEpoch, deliveryState: "mailbox_queued" });
    else if (record.status === "reserved" || record.status === "claimed") writeOpaqueTo(origin, { namespace: record.senderNamespace, role: "send" }, { type: "opaque_dispatch_v1_ack", operationId, requestId: record.requestId, messageId: record.messageId, brokerEpoch: this.hooks.brokerEpoch, deliveryState: "live" });
    else if (record.status === "terminal") writeOpaqueTo(origin, { namespace: record.senderNamespace, role: "send" }, { type: "opaque_dispatch_v1_rejected", operationId, requestId: record.requestId, messageId: record.messageId, code: record.terminalReason ?? "already_terminal", terminal: record.terminalStatus === "refused" ? "refused" : "failed_closed" });
  }

  private receipt(record: RecordState, status: OpaqueDispatchStatus, reason?: OpaqueDispatchReason): void {
    if (record.receipts.length >= MAX_OPAQUE_RECEIPTS) return;
    if (record.receipts.length === MAX_OPAQUE_RECEIPTS - 1
      && status !== "claimed" && status !== "refused" && status !== "expired" && status !== "cancelled"
      && status !== "superseded" && status !== "failed_closed") {
      this.terminalize(record, "failed_closed", "history_limit");
      return;
    }
    const receipt: OpaqueDispatchReceipt = { requestId: record.requestId, messageId: record.messageId, status, at: this.now(), attempt: Math.max(1, record.attempt), sequence: record.receipts.length + 1, ...(reason ? { reason } : {}) };
    record.receipts.push(receipt);
    const origin = this.hooks.endpoint(record.originSessionId);
    writeOpaqueTo(origin, { namespace: record.senderNamespace, role: "send" }, { type: "opaque_dispatch_v1_receipt", senderNamespace: record.senderNamespace, receipt });
  }

  private replayReceipts(record: RecordState): void {
    const origin = this.hooks.endpoint(record.originSessionId);
    if (!origin?.connected || !hasRole(origin, record.senderNamespace, "send")) return;
    for (const receipt of record.receipts) if (receipt.sequence > record.ackedThrough) writeOpaqueTo(origin, { namespace: record.senderNamespace, role: "send" }, { type: "opaque_dispatch_v1_receipt", senderNamespace: record.senderNamespace, receipt });
  }

  private endReservation(record: RecordState, outcome: "expired" | "cancelled" | "superseded" | "failed_closed", reason?: OpaqueDispatchReason): void {
    const reservation = record.reservation;
    if (!reservation) return;
    clearTimeout(reservation.timer);
    const target = this.hooks.endpoint(record.targetSessionId);
    writeOpaqueTo(target, { namespace: record.recipientNamespace, role: "receive" }, { type: "opaque_dispatch_v1_reservation_ended", messageId: record.messageId, reservationId: reservation.id, outcome, ...(reason ? { reason } : {}) });
    record.reservation = undefined;
  }

  private expire(record: RecordState): void {
    if (record.status === "terminal" || record.status === "claimed") return;
    this.endReservation(record, "expired");
    this.rejectWaiters(record, "already_terminal", "failed_closed");
    this.terminalize(record, "expired");
  }

  private terminalize(record: RecordState, status: Exclude<OpaqueDispatchStatus, "queued" | "reserved" | "claimed">, reason?: OpaqueDispatchReason): void {
    if (record.status === "terminal" || record.status === "claimed") return;
    if (record.activeTimer) clearTimeout(record.activeTimer);
    record.status = "terminal";
    record.terminalStatus = status;
    record.terminalReason = reason;
    this.clearPayload(record);
    this.receipt(record, status, reason);
    this.startTombstone(record);
    this.enforceTombstoneCapacity();
  }

  private clearPayload(record: RecordState): void {
    delete record.payload;
  }

  private startTombstone(record: RecordState): void {
    if (record.activeTimer) clearTimeout(record.activeTimer);
    record.tombstoneTimer = setTimeout(() => this.deleteRecord(record), this.tombstoneTtlMs());
    record.tombstoneTimer.unref?.();
  }

  private enforceTombstoneCapacity(): void {
    const tombstones = [...this.records.values()].filter((record) => record.status === "terminal" || record.status === "claimed").sort((a, b) => a.createdAt - b.createdAt);
    const evict = (candidates: RecordState[]) => {
      const acknowledged = candidates.find((record) => record.ackedThrough >= record.receipts.length);
      const record = acknowledged ?? candidates[0];
      if (!record) return;
      const index = tombstones.indexOf(record);
      if (index >= 0) tombstones.splice(index, 1);
      this.deleteRecord(record);
    };
    for (const key of new Set(tombstones.map((record) => principalKey(record.originSessionId, record.senderNamespace)))) {
      let matching = tombstones.filter((record) => principalKey(record.originSessionId, record.senderNamespace) === key);
      while (matching.length > MAX_OPAQUE_PRINCIPAL_TOMBSTONES) {
        evict(matching);
        matching = tombstones.filter((record) => principalKey(record.originSessionId, record.senderNamespace) === key);
      }
    }
    for (const key of new Set(tombstones.map((record) => targetKey(record.targetSessionId, record.recipientNamespace)))) {
      let matching = tombstones.filter((record) => targetKey(record.targetSessionId, record.recipientNamespace) === key);
      while (matching.length > MAX_OPAQUE_TARGET_TOMBSTONES) {
        evict(matching);
        matching = tombstones.filter((record) => targetKey(record.targetSessionId, record.recipientNamespace) === key);
      }
    }
    while (tombstones.length > MAX_OPAQUE_TOMBSTONES) evict(tombstones);
  }

  private deleteRecord(record: RecordState): void {
    if (record.tombstoneTimer) clearTimeout(record.tombstoneTimer);
    this.records.delete(record.key);
    this.byMessageId.delete(record.messageId);
  }
}
