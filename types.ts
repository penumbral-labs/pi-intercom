export const EXTENSION_BUS_FEATURE = "extension-bus-v1";
export const EXACT_SEND_FEATURE = "exact-send-v1";
export const CORRELATED_OPERATIONS_FEATURE = "correlated-operations-v1";
export const EXTENSION_STATE_REFRESH_FEATURE = "extension-state-refresh-v1";
export const OPAQUE_DISPATCH_FEATURE = "opaque-dispatch-v1";
export const BROKER_SESSION_ID = "__pi_intercom_broker__";

export type DeliveryState = "socket_delivered" | "queued" | "failed" | "unknown";

export interface DeliveryDetails {
  delivery: DeliveryState;
  code?: string;
  retryable: boolean;
  outcomeKnown: boolean;
}

export interface SessionInfo {
  id: string;
  /** Broker-owned lifetime of this live endpoint. */
  endpointEpoch?: string;
  name?: string;
  /** True only when the extension synthesized name for an unnamed runtime. */
  runtimeFallbackAlias?: boolean;
  cwd: string;
  model: string;
  pid: number;
  startedAt: number;
  lastActivity: number;
  status?: string;
  peerUid?: number;
  trustedLocal?: boolean;
  /** Live context-window usage, pushed via presence from the source session's
   *  getContextUsage(). contextPct is 0..100 (rounded); contextTokens /
   *  contextWindow are raw token counts. All optional: unknown right after a
   *  compaction (before the next assistant response), when no model is selected,
   *  or on older clients that never report it. */
  contextPct?: number;
  contextTokens?: number;
  contextWindow?: number;
  /** tmux pane id (e.g. "%212") of the session's terminal. */
  tmuxPane?: string;
  opaqueDispatch?: SessionOpaqueCapability;
}

export interface Message {
  id: string;
  timestamp: number;
  senderSequence?: number;
  brokerReceivedAt?: number;
  brokerDeliveredAt?: number;
  receiverReceivedAt?: number;
  injectedAt?: number;
  supersedes?: string;
  retryOf?: string;
  replyTo?: string;
  expectsReply?: boolean;
  content: {
    text: string;
    attachments?: Attachment[];
  };
}

export interface Attachment {
  type: "file" | "snippet" | "context";
  name: string;
  content: string;
  language?: string;
}

export type MessageReceiptStatus = "receiver_received" | "queued" | "injected" | "acknowledged" | "expired" | "cancelled" | "superseded" | "failed" | "cancellation_requested";

export interface MessageReceipt {
  messageId: string;
  status: MessageReceiptStatus;
  timestamp: number;
  code?: "E_DELIVERY_TOO_LARGE";
  detail?: string;
}

export type MessageControlAction = "cancel" | "supersede";

export interface MessageControl {
  messageId: string;
  action: MessageControlAction;
  timestamp: number;
  supersededBy?: string;
}

export type OpaqueDispatchRole = "send" | "receive";

export interface OpaqueCapabilityAdvertisement {
  version: 1;
  roles: OpaqueDispatchRole[];
}

export interface SessionOpaqueCapability {
  version: 1;
  namespaces: Array<{ namespace: string; roles: OpaqueDispatchRole[] }>;
}

export interface ExtensionCapability {
  namespace: string;
  ownerEligible: boolean;
  opaqueDispatch?: OpaqueCapabilityAdvertisement;
}

export type ExtensionStateSnapshot =
  | { namespace: string; revision: 0; present: false }
  | { namespace: string; revision: number; present: true; payload: unknown };

export type SessionRegistration = Omit<SessionInfo, "id" | "endpointEpoch" | "peerUid" | "trustedLocal"> & {
  extensions?: ExtensionCapability[];
};

export type ClientMessage =
  | { type: "register"; session: SessionRegistration; sessionId?: string; stateId?: string }
  | { type: "unregister" }
  | { type: "extension_capabilities_update"; extensions: ExtensionCapability[] }
  | { type: "list"; requestId: string }
  | { type: "send"; to: string; message: Message; operationId?: string; targetId?: string; targetEpoch?: string }
  | { type: "message_receipt"; receipt: MessageReceipt }
  | { type: "cancel_message"; messageId: string; operationId?: string }
  | { type: "cancel_ask"; messageId: string }
  | { type: "presence"; name?: string; runtimeFallbackAlias?: boolean; status?: string; model?: string; contextPct?: number | null; contextTokens?: number | null; contextWindow?: number | null }
  | {
      type: "extension_publish";
      namespace: string;
      audience: "owner" | "capable";
      ownerEpoch?: string;
      ownerOnly?: boolean;
      payload: unknown;
    }
  | {
      type: "extension_state_commit";
      namespace: string;
      ownerEpoch: string;
      expectedRevision: number;
      payload: unknown;
    }
  | { type: "extension_state_get"; requestId: string; namespace: string }
  | OpaqueDispatchClientFrame;

export type BrokerMessage =
  | { type: "registered"; sessionId: string; features?: string[]; brokerEpoch?: string; endpointEpoch?: string }
  | { type: "sessions"; requestId: string; sessions: SessionInfo[] }
  | { type: "sessions_failed"; requestId: string; code: "response_too_large"; error: string }
  | { type: "message"; from: SessionInfo; message: Message; control?: MessageControl }
  | { type: "presence_update"; session: SessionInfo }
  | { type: "session_joined"; session: SessionInfo }
  | { type: "session_left"; sessionId: string }
  | { type: "error"; error: string }
  | ({ type: "delivered"; messageId: string; operationId?: string } & Partial<DeliveryDetails>)
  | ({ type: "delivery_failed"; messageId: string; operationId?: string; reason: string } & Partial<DeliveryDetails>)
  | { type: "message_receipt"; from: SessionInfo; receipt: MessageReceipt }
  | { type: "message_control"; from: SessionInfo; control: MessageControl }
  | { type: "extension_owner"; namespace: string; ownerId?: string; ownerEpoch?: string }
  | {
      type: "extension_message";
      namespace: string;
      fromSessionId: string;
      ownerId?: string;
      ownerEpoch?: string;
      payload: unknown;
    }
  | {
      type: "extension_state";
      namespace: string;
      revision: number;
      payload: unknown;
    }
  | {
      type: "extension_state_result";
      namespace: string;
      committed: boolean;
      revision: number;
      reason?: string;
    }
  | { type: "extension_state_snapshot"; requestId: string; snapshot: ExtensionStateSnapshot }
  | OpaqueDispatchBrokerFrame;

export const OPAQUE_DISPATCH_STATUSES = [
  "queued", "reserved", "claimed", "refused", "expired", "cancelled", "superseded", "failed_closed",
] as const;
export type OpaqueDispatchStatus = typeof OPAQUE_DISPATCH_STATUSES[number];

export const OPAQUE_DISPATCH_REASONS = [
  "unsupported_host", "unsupported_broker", "unsupported_target", "unknown_exact_target", "self_dispatch_unsupported",
  "invalid_request", "invalid_frame", "request_conflict", "limit_exceeded", "rate_limited", "broker_epoch_changed",
  "target_rebound", "endpoint_epoch_changed", "claim_history_unavailable", "payload_too_large", "consumer_missing",
  "consumer_unloaded", "consumer_refused", "consumer_threw", "consumer_failed", "reservation_timeout", "claim_timeout",
  "malformed_consumer_result", "stale_reservation", "receiver_disconnected", "capability_invalidated",
  "queued_supersede_unsupported", "already_claimed", "already_terminal", "not_origin", "attempt_limit", "history_limit",
  "connection_lost", "uncorrelated_operation_pending",
] as const;
export type OpaqueDispatchReason = typeof OPAQUE_DISPATCH_REASONS[number];

export interface OpaqueDispatchSender {
  sessionId: string;
  namespace: string;
  trustedLocal: boolean;
  owner?: { sessionId: string; epoch: string };
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

export type OpaqueDispatchClientFrame =
  | { type: "opaque_dispatch_v1_send"; operationId: string; requestId: string; senderNamespace: string; toSessionId: string; targetEpoch: string; recipientNamespace: string; payload: unknown; supersedesMessageId?: string }
  | { type: "opaque_dispatch_v1_cancel"; operationId: string; senderNamespace: string; messageId: string }
  | { type: "opaque_dispatch_v1_reservation_result"; endpointEpoch: string; reservationId: string; messageId: string; decision: "reserved" | "refused" | "failed_closed"; reason?: OpaqueDispatchReason }
  | { type: "opaque_dispatch_v1_claim"; operationId: string; endpointEpoch: string; reservationId: string; messageId: string }
  | { type: "opaque_dispatch_v1_fail"; operationId: string; endpointEpoch: string; reservationId: string; messageId: string; reason: "consumer_failed" }
  | { type: "opaque_dispatch_v1_claim_status"; operationId: string; recipientNamespace: string; brokerEpoch: string; endpointEpoch: string; reservationId: string; messageId: string }
  | { type: "opaque_dispatch_v1_peer_capability_get"; operationId: string; toSessionId: string; recipientNamespace: string }
  | { type: "opaque_dispatch_v1_receipt_ack"; senderNamespace: string; messageId: string; sequence: number };

export type OpaqueDispatchBrokerFrame =
  | { type: "opaque_dispatch_v1_ack"; operationId: string; requestId: string; messageId: string; brokerEpoch: string; deliveryState: "live" | "mailbox_queued" }
  | { type: "opaque_dispatch_v1_rejected"; operationId: string; requestId?: string; messageId?: string; code: OpaqueDispatchReason; terminal?: "refused" | "failed_closed" }
  | { type: "opaque_dispatch_v1_offer"; reservationId: string; requestId: string; messageId: string; attempt: number; brokerEpoch: string; endpointEpoch: string; toSessionId: string; recipientNamespace: string; sender: OpaqueDispatchSender; payload: unknown; reserveBy: number }
  | { type: "opaque_dispatch_v1_reservation_ended"; messageId: string; reservationId: string; outcome: "expired" | "cancelled" | "superseded" | "failed_closed"; reason?: OpaqueDispatchReason }
  | { type: "opaque_dispatch_v1_receipt"; senderNamespace: string; receipt: OpaqueDispatchReceipt }
  | { type: "opaque_dispatch_v1_claim_result"; operationId: string; reservationId: string; messageId: string; claimed: boolean; code?: OpaqueDispatchReason }
  | { type: "opaque_dispatch_v1_fail_result"; operationId: string; reservationId: string; messageId: string; failedClosed: boolean; code?: OpaqueDispatchReason }
  | { type: "opaque_dispatch_v1_claim_status_result"; operationId: string; brokerEpoch: string; reservationId: string; messageId: string; result: { state: "claimed" } | { state: "reserved" } | { state: "stale"; code: "stale_reservation" } | { state: "indeterminate"; code: "broker_epoch_changed" | "claim_history_unavailable" } }
  | { type: "opaque_dispatch_v1_cancel_result"; operationId: string; messageId: string; cancelled: boolean; code?: OpaqueDispatchReason }
  | ({ type: "opaque_dispatch_v1_peer_capability_result"; operationId: string; toSessionId: string; recipientNamespace: string } & (
      | { state: "present"; version: 1; endpointEpoch: string }
      | { state: "absent"; endpointEpoch: string }
      | { state: "unknown" }
    ));
