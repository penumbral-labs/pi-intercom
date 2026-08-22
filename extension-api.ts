import type {
  OpaqueDispatchReason as ProtocolOpaqueDispatchReason,
  OpaqueDispatchStatus as ProtocolOpaqueDispatchStatus,
  SessionInfo,
} from "./types.ts";

export const INTERCOM_EXTENSION_REGISTER_EVENT = "intercom:extension-register";
export const INTERCOM_EXTENSION_REGISTRY_READY_EVENT = "intercom:extension-registry-ready";

export interface IntercomExtensionOwner {
  sessionId: string;
  epoch: string;
}

export interface IntercomExtensionState {
  revision: number;
  payload: unknown;
}

export type ExtensionStateSnapshot =
  | { namespace: string; revision: 0; present: false }
  | { namespace: string; revision: number; present: true; payload: unknown };

export type ExtensionStateRefreshResult =
  | { ok: true; state: ExtensionStateSnapshot }
  | { ok: false; code: "unsupported_broker" | "connection_lost" };

export type OpaqueDispatchRole = "send" | "receive";
export type OpaqueDispatchStatus = ProtocolOpaqueDispatchStatus;
export type OpaqueDispatchReason = ProtocolOpaqueDispatchReason;

export interface OpaqueDispatchSender {
  sessionId: string;
  namespace: string;
  trustedLocal: boolean;
  owner?: IntercomExtensionOwner;
}

export interface OpaqueDispatchReceipt {
  requestId: string;
  messageId: string;
  status: OpaqueDispatchStatus;
  at: number;
  attempt: number;
  sequence: number;
  reason?: OpaqueDispatchReason;
}

export interface OpaqueDispatchEvent {
  requestId: string;
  messageId: string;
  attempt: number;
  brokerEpoch: string;
  endpointEpoch: string;
  toSessionId: string;
  recipientNamespace: string;
  sender: OpaqueDispatchSender;
  payload: unknown;
  receivedAt: number;
  reserveBy: number;
}

export interface OpaqueDispatchReservation {
  readonly messageId: string;
  readonly reservationId: string;
  readonly attempt: number;
  readonly signal: AbortSignal;
  claim(): Promise<{ claimed: true } | { claimed: false; code: OpaqueDispatchReason }>;
  fail(): Promise<{ failedClosed: true } | { failedClosed: false; code: OpaqueDispatchReason }>;
}

export interface OpaqueDispatchTransportIndeterminateEvent {
  requestId: string;
  messageId: string;
  previousBrokerEpoch: string;
  currentBrokerEpoch: string;
}

export interface OpaqueDispatchRegistration {
  version: 1;
  roles: OpaqueDispatchRole[];
  onReserve?(event: OpaqueDispatchEvent, reservation: OpaqueDispatchReservation): "reserved" | "refused";
  onReceipt?(receipt: OpaqueDispatchReceipt): void;
  onTransportIndeterminate?(event: OpaqueDispatchTransportIndeterminateEvent): void;
}

export type SendOpaqueResult =
  | { accepted: true; requestId: string; messageId: string; brokerEpoch: string; deliveryState: "live" | "mailbox_queued" }
  | { accepted: false; requestId: string; messageId?: string; code: OpaqueDispatchReason; terminal?: "refused" | "failed_closed" };

export type IntercomExtensionEvent =
  | { type: "connection"; connected: boolean; supported: boolean }
  | { type: "owner"; owner?: IntercomExtensionOwner }
  | { type: "message"; fromSessionId: string; owner?: IntercomExtensionOwner; payload: unknown }
  | { type: "state"; state: IntercomExtensionState }
  | { type: "state_result"; committed: boolean; revision: number; reason?: string }
  | { type: "session_joined"; session: SessionInfo }
  | { type: "session_left"; sessionId: string }
  | { type: "presence_update"; session: SessionInfo };

export interface ExtensionChannelSnapshot {
  connected: boolean;
  supported: boolean;
  brokerEpoch?: string;
  capabilities: {
    extensionBus: boolean;
    extensionStateRefreshVersion?: 1;
    opaqueDispatchVersion?: 1;
  };
  owner?: IntercomExtensionOwner;
  state?: IntercomExtensionState;
}

export interface IntercomExtensionChannel {
  readonly namespace: string;
  snapshot(): ExtensionChannelSnapshot;
  publish(payload: unknown, options?: { audience?: "owner" | "capable"; ownerOnly?: boolean }): void;
  commitState(payload: unknown, expectedRevision?: number): void;
  refreshState(): Promise<ExtensionStateRefreshResult>;
  listSessions(): Promise<SessionInfo[]>;
  peerCapability(sessionId: string, recipientNamespace: string): Promise<
    { state: "present"; version: 1; endpointEpoch: string }
    | { state: "absent"; endpointEpoch: string }
    | { state: "unknown" }
  >;
  sendOpaqueDispatch(input: {
    requestId: string;
    toSessionId: string;
    recipientNamespace: string;
    payload: unknown;
    supersedesMessageId?: string;
  }): Promise<SendOpaqueResult>;
  cancelMessage(messageId: string): Promise<{ cancelled: true } | { cancelled: false; code: OpaqueDispatchReason }>;
  reconcileClaim(input: { brokerEpoch: string; endpointEpoch: string; messageId: string; reservationId: string }): Promise<
    { state: "claimed" } | { state: "reserved" } | { state: "stale"; code: "stale_reservation" }
    | { state: "indeterminate"; code: "broker_epoch_changed" | "claim_history_unavailable" }
  >;
  dispose(): void;
}

export interface IntercomExtensionRegistration {
  namespace: string;
  ownerEligible: boolean;
  opaqueDispatch?: OpaqueDispatchRegistration;
  onEvent(event: IntercomExtensionEvent): void;
  onReady(channel: IntercomExtensionChannel): void;
  onUnavailable?(reason: "unsupported_host"): void;
}
