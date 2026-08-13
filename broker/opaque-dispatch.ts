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
export const MAX_OPAQUE_ATTEMPTS = 8;
export const MAX_OPAQUE_RECEIPTS = 20;
export const MAX_OPAQUE_WAITERS = 8;
export const MAX_OPAQUE_PAYLOAD_BYTES = 64 * 1024;

export interface OpaqueEndpoint {
  sessionId: string;
  info: SessionInfo;
  extensions?: ExtensionCapability[];
  connected: boolean;
  write?: (frame: OpaqueDispatchBrokerFrame) => void;
}

interface OpaqueDispatchHooks {
  brokerEpoch: string;
  endpoint(sessionId: string): OpaqueEndpoint | undefined;
  owner(namespace: string): { sessionId: string; epoch: string } | undefined;
  now?: () => number;
}

interface Waiter {
  operationId: string;
  requestId: string;
}

interface Reservation {
  id: string;
  targetSessionId: string;
  recipientNamespace: string;
  attempt: number;
  phase: "offered" | "reserved";
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
  canonicalBytes?: number;
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
    endpoint.write?.({
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
      if (record.status === "offered" && record.waiters.length > 0) {
        this.rejectWaiters(record, "receiver_disconnected", "failed_closed");
      }
      if (record.status === "offered" || record.status === "reserved") {
        if (record.attempt >= MAX_OPAQUE_ATTEMPTS) this.terminalize(record, "failed_closed", "attempt_limit");
        else {
          record.status = "queued";
          this.receipt(record, "queued", "receiver_disconnected");
        }
      }
    }
  }

  capabilityChanged(sessionId: string): void {
    for (const record of this.records.values()) {
      if (record.targetSessionId !== sessionId) continue;
      const endpoint = this.hooks.endpoint(sessionId);
      if (hasRole(endpoint, record.recipientNamespace, "receive")) {
        if (record.status === "queued") this.offer(record);
        continue;
      }
      if (record.reservation) this.endReservation(record, "failed_closed", "capability_invalidated");
      if (record.status === "offered" || record.status === "reserved") this.terminalize(record, "failed_closed", "capability_invalidated");
    }
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

  private send(origin: OpaqueEndpoint, frame: Extract<OpaqueDispatchClientFrame, { type: "opaque_dispatch_v1_send" }>): void {
    const reject = (code: OpaqueDispatchReason, messageId?: string) => origin.write?.({
      type: "opaque_dispatch_v1_rejected", operationId: frame.operationId, requestId: frame.requestId,
      ...(messageId ? { messageId } : {}), code,
    });
    if (!origin.connected || !hasRole(origin, frame.senderNamespace, "send")) return reject("unsupported_broker");
    if (origin.sessionId === frame.toSessionId) return reject("self_dispatch_unsupported");
    const canonical = canonicalizeOpaquePayload(frame.payload);
    if (!canonical.ok) return reject(canonical.code);
    const key = `${origin.sessionId}\0${frame.senderNamespace}\0${frame.requestId}`;
    const digest = fingerprint(canonical.json, frame.toSessionId, frame.recipientNamespace, frame.supersedesMessageId);
    const existing = this.records.get(key);
    if (existing) {
      if (existing.digest !== digest) return reject("request_conflict", existing.messageId);
      if (existing.status === "offered") {
        if (existing.waiters.length >= MAX_OPAQUE_WAITERS) return reject("limit_exceeded", existing.messageId);
        existing.waiters.push({ operationId: frame.operationId, requestId: frame.requestId });
        return;
      }
      return this.replayResult(existing, frame.operationId);
    }
    const target = this.hooks.endpoint(frame.toSessionId);
    if (!target) return reject("unknown_exact_target");
    if (!hasRole(target, frame.recipientNamespace, "receive")) return reject("unsupported_target");
    if (!this.hasCapacity(origin.sessionId, frame.senderNamespace, frame.toSessionId, frame.recipientNamespace)) return reject("limit_exceeded");
    if (frame.supersedesMessageId) {
      const prior = this.byMessageId.get(frame.supersedesMessageId);
      if (!prior || prior.originSessionId !== origin.sessionId || prior.senderNamespace !== frame.senderNamespace
        || prior.targetSessionId !== frame.toSessionId || prior.recipientNamespace !== frame.recipientNamespace) return reject("not_origin");
      if (prior.status === "queued") return reject("queued_supersede_unsupported");
      if (prior.status === "claimed") return reject("already_claimed");
      if (prior.status === "terminal") return reject("already_terminal");
      this.endReservation(prior, "superseded");
      this.terminalize(prior, "superseded");
    }
    const now = this.now();
    const record: RecordState = {
      key, digest, originSessionId: origin.sessionId, senderNamespace: frame.senderNamespace,
      requestId: frame.requestId, messageId: randomUUID(), targetSessionId: frame.toSessionId,
      recipientNamespace: frame.recipientNamespace, createdAt: now, payload: canonical.normalized,
      canonicalBytes: Buffer.byteLength(canonical.json, "utf8"), status: "queued", attempt: 0,
      waiters: [{ operationId: frame.operationId, requestId: frame.requestId }], receipts: [], ackedThrough: 0,
    };
    record.activeTimer = setTimeout(() => this.expire(record), OPAQUE_ACTIVE_TTL_MS);
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
    const active = [...this.records.values()].filter((record) => record.status !== "terminal" && record.status !== "claimed");
    if (active.length >= MAX_OPAQUE_ACTIVE_RECORDS) {
      const oldestQueued = active.filter((record) => record.status === "queued").sort((a, b) => a.createdAt - b.createdAt)[0];
      if (!oldestQueued) return false;
      this.terminalize(oldestQueued, "expired", "limit_exceeded");
    }
    if (active.filter((record) => principalKey(record.originSessionId, record.senderNamespace) === principalKey(originId, senderNamespace)).length >= MAX_OPAQUE_PRINCIPAL_RECORDS) return false;
    return active.filter((record) => targetKey(record.targetSessionId, record.recipientNamespace) === targetKey(targetId, recipientNamespace)).length < MAX_OPAQUE_TARGET_RECORDS;
  }

  private offer(record: RecordState): void {
    const target = this.hooks.endpoint(record.targetSessionId);
    if (!target?.connected || !target.write || !hasRole(target, record.recipientNamespace, "receive") || record.payload === undefined) return;
    if (record.attempt >= MAX_OPAQUE_ATTEMPTS) return this.terminalize(record, "failed_closed", "attempt_limit");
    record.attempt += 1;
    const reservationId = randomUUID();
    const reserveBy = this.now() + OPAQUE_RESERVATION_TIMEOUT_MS;
    const timer = setTimeout(() => {
      if (record.reservation?.id !== reservationId || record.status !== "offered") return;
      this.endReservation(record, "failed_closed", "reservation_timeout");
      this.rejectWaiters(record, "reservation_timeout", "failed_closed");
      this.terminalize(record, "failed_closed", "reservation_timeout");
    }, OPAQUE_RESERVATION_TIMEOUT_MS);
    timer.unref?.();
    record.reservation = { id: reservationId, targetSessionId: record.targetSessionId, recipientNamespace: record.recipientNamespace, attempt: record.attempt, phase: "offered", timer };
    record.status = "offered";
    const origin = this.hooks.endpoint(record.originSessionId);
    target.write({
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
  }

  private reservationResult(endpoint: OpaqueEndpoint, frame: Extract<OpaqueDispatchClientFrame, { type: "opaque_dispatch_v1_reservation_result" }>): void {
    const record = this.byMessageId.get(frame.messageId);
    const reservation = record?.reservation;
    if (!record || !reservation || reservation.id !== frame.reservationId || reservation.targetSessionId !== endpoint.sessionId
      || !hasRole(endpoint, reservation.recipientNamespace, "receive") || record.status !== "offered") return;
    clearTimeout(reservation.timer);
    if (frame.decision !== "reserved") {
      this.endReservation(record, "failed_closed", frame.reason ?? (frame.decision === "refused" ? "consumer_refused" : "consumer_failed"));
      this.rejectWaiters(record, frame.reason ?? (frame.decision === "refused" ? "consumer_refused" : "consumer_failed"), frame.decision === "refused" ? "refused" : "failed_closed");
      this.terminalize(record, frame.decision === "refused" ? "refused" : "failed_closed", frame.reason ?? (frame.decision === "refused" ? "consumer_refused" : "consumer_failed"));
      return;
    }
    reservation.phase = "reserved";
    reservation.timer = setTimeout(() => {
      if (record.reservation?.id !== reservation.id || record.status !== "reserved") return;
      this.endReservation(record, "failed_closed", "claim_timeout");
      this.terminalize(record, "failed_closed", "claim_timeout");
    }, OPAQUE_CLAIM_TIMEOUT_MS);
    reservation.timer.unref?.();
    record.status = "reserved";
    this.ackWaiters(record, "live");
    this.receipt(record, "reserved");
  }

  private claim(endpoint: OpaqueEndpoint, frame: Extract<OpaqueDispatchClientFrame, { type: "opaque_dispatch_v1_claim" }>): void {
    const record = this.authorizedReservation(endpoint, frame.messageId, frame.reservationId);
    if (!record || record.status !== "reserved") return endpoint.write?.({ type: "opaque_dispatch_v1_claim_result", operationId: frame.operationId, reservationId: frame.reservationId, messageId: frame.messageId, claimed: false, code: "stale_reservation" });
    clearTimeout(record.reservation!.timer);
    record.lastReservationId = frame.reservationId;
    record.reservation = undefined;
    record.status = "claimed";
    this.clearPayload(record);
    this.receipt(record, "claimed");
    this.startTombstone(record);
    endpoint.write?.({ type: "opaque_dispatch_v1_claim_result", operationId: frame.operationId, reservationId: frame.reservationId, messageId: frame.messageId, claimed: true });
  }

  private fail(endpoint: OpaqueEndpoint, frame: Extract<OpaqueDispatchClientFrame, { type: "opaque_dispatch_v1_fail" }>): void {
    const record = this.authorizedReservation(endpoint, frame.messageId, frame.reservationId);
    if (!record || record.status !== "reserved") return endpoint.write?.({ type: "opaque_dispatch_v1_fail_result", operationId: frame.operationId, reservationId: frame.reservationId, messageId: frame.messageId, failedClosed: false, code: "stale_reservation" });
    this.endReservation(record, "failed_closed", "consumer_failed");
    this.terminalize(record, "failed_closed", "consumer_failed");
    endpoint.write?.({ type: "opaque_dispatch_v1_fail_result", operationId: frame.operationId, reservationId: frame.reservationId, messageId: frame.messageId, failedClosed: true });
  }

  private cancel(origin: OpaqueEndpoint, frame: Extract<OpaqueDispatchClientFrame, { type: "opaque_dispatch_v1_cancel" }>): void {
    const record = this.byMessageId.get(frame.messageId);
    const response = (cancelled: boolean, code?: OpaqueDispatchReason) => origin.write?.({ type: "opaque_dispatch_v1_cancel_result", operationId: frame.operationId, messageId: frame.messageId, cancelled, ...(code ? { code } : {}) });
    if (!record || record.originSessionId !== origin.sessionId || record.senderNamespace !== frame.senderNamespace) return response(false, "not_origin");
    if (record.status === "claimed") return response(false, "already_claimed");
    if (record.status === "terminal") return response(false, "already_terminal");
    this.endReservation(record, "cancelled");
    this.terminalize(record, "cancelled");
    response(true);
  }

  private claimStatus(endpoint: OpaqueEndpoint, frame: Extract<OpaqueDispatchClientFrame, { type: "opaque_dispatch_v1_claim_status" }>): void {
    const record = this.byMessageId.get(frame.messageId);
    let result: Extract<OpaqueDispatchBrokerFrame, { type: "opaque_dispatch_v1_claim_status_result" }>["result"];
    if (frame.brokerEpoch !== this.hooks.brokerEpoch) result = { state: "indeterminate", code: "broker_epoch_changed" };
    else if (!record) result = { state: "indeterminate", code: "claim_history_unavailable" };
    else if (record.targetSessionId !== endpoint.sessionId || record.recipientNamespace !== frame.recipientNamespace || !hasRole(endpoint, frame.recipientNamespace, "receive")) result = { state: "stale", code: "stale_reservation" };
    else if (record.status === "claimed" && record.lastReservationId === frame.reservationId) result = { state: "claimed" };
    else if (record.status === "reserved" && record.reservation?.id === frame.reservationId) result = { state: "reserved" };
    else result = { state: "stale", code: "stale_reservation" };
    endpoint.write?.({ type: "opaque_dispatch_v1_claim_status_result", operationId: frame.operationId, brokerEpoch: this.hooks.brokerEpoch, reservationId: frame.reservationId, messageId: frame.messageId, result });
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
    for (const waiter of record.waiters.splice(0)) origin?.write?.({ type: "opaque_dispatch_v1_ack", operationId: waiter.operationId, requestId: waiter.requestId, messageId: record.messageId, brokerEpoch: this.hooks.brokerEpoch, deliveryState });
  }

  private rejectWaiters(record: RecordState, code: OpaqueDispatchReason, terminal: "refused" | "failed_closed"): void {
    const origin = this.hooks.endpoint(record.originSessionId);
    for (const waiter of record.waiters.splice(0)) origin?.write?.({ type: "opaque_dispatch_v1_rejected", operationId: waiter.operationId, requestId: waiter.requestId, messageId: record.messageId, code, terminal });
  }

  private replayResult(record: RecordState, operationId: string): void {
    const origin = this.hooks.endpoint(record.originSessionId);
    if (record.status === "queued") origin?.write?.({ type: "opaque_dispatch_v1_ack", operationId, requestId: record.requestId, messageId: record.messageId, brokerEpoch: this.hooks.brokerEpoch, deliveryState: "mailbox_queued" });
    else if (record.status === "reserved" || record.status === "claimed") origin?.write?.({ type: "opaque_dispatch_v1_ack", operationId, requestId: record.requestId, messageId: record.messageId, brokerEpoch: this.hooks.brokerEpoch, deliveryState: "live" });
    else if (record.status === "terminal") origin?.write?.({ type: "opaque_dispatch_v1_rejected", operationId, requestId: record.requestId, messageId: record.messageId, code: record.terminalReason ?? "already_terminal", terminal: record.terminalStatus === "refused" ? "refused" : "failed_closed" });
  }

  private receipt(record: RecordState, status: OpaqueDispatchStatus, reason?: OpaqueDispatchReason): void {
    if (record.receipts.length >= MAX_OPAQUE_RECEIPTS) {
      if (record.status !== "terminal" && record.status !== "claimed") this.terminalize(record, "failed_closed", "history_limit");
      return;
    }
    const receipt: OpaqueDispatchReceipt = { requestId: record.requestId, messageId: record.messageId, status, at: this.now(), attempt: Math.max(1, record.attempt), sequence: record.receipts.length + 1, ...(reason ? { reason } : {}) };
    record.receipts.push(receipt);
    const origin = this.hooks.endpoint(record.originSessionId);
    if (origin?.connected && hasRole(origin, record.senderNamespace, "send")) origin.write?.({ type: "opaque_dispatch_v1_receipt", senderNamespace: record.senderNamespace, receipt });
  }

  private replayReceipts(record: RecordState): void {
    const origin = this.hooks.endpoint(record.originSessionId);
    if (!origin?.connected || !hasRole(origin, record.senderNamespace, "send")) return;
    for (const receipt of record.receipts) if (receipt.sequence > record.ackedThrough) origin.write?.({ type: "opaque_dispatch_v1_receipt", senderNamespace: record.senderNamespace, receipt });
  }

  private endReservation(record: RecordState, outcome: "expired" | "cancelled" | "superseded" | "failed_closed", reason?: OpaqueDispatchReason): void {
    const reservation = record.reservation;
    if (!reservation) return;
    clearTimeout(reservation.timer);
    const target = this.hooks.endpoint(record.targetSessionId);
    if (target?.connected && hasRole(target, record.recipientNamespace, "receive")) target.write?.({ type: "opaque_dispatch_v1_reservation_ended", messageId: record.messageId, reservationId: reservation.id, outcome, ...(reason ? { reason } : {}) });
    record.reservation = undefined;
  }

  private expire(record: RecordState): void {
    if (record.status === "terminal" || record.status === "claimed") return;
    this.endReservation(record, "expired");
    this.rejectWaiters(record, "reservation_timeout", "failed_closed");
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
    delete record.canonicalBytes;
  }

  private startTombstone(record: RecordState): void {
    if (record.activeTimer) clearTimeout(record.activeTimer);
    record.tombstoneTimer = setTimeout(() => this.deleteRecord(record), OPAQUE_TOMBSTONE_TTL_MS);
    record.tombstoneTimer.unref?.();
  }

  private enforceTombstoneCapacity(): void {
    const tombstones = [...this.records.values()].filter((record) => record.status === "terminal" || record.status === "claimed").sort((a, b) => a.createdAt - b.createdAt);
    while (tombstones.length > MAX_OPAQUE_TOMBSTONES) {
      const acknowledgedIndex = tombstones.findIndex((record) => record.ackedThrough >= record.receipts.length);
      const [record] = tombstones.splice(acknowledgedIndex >= 0 ? acknowledgedIndex : 0, 1);
      if (record) this.deleteRecord(record);
    }
  }

  private deleteRecord(record: RecordState): void {
    if (record.tombstoneTimer) clearTimeout(record.tombstoneTimer);
    this.records.delete(record.key);
    this.byMessageId.delete(record.messageId);
  }
}
