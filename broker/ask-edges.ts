import { STALE_ASK_RETENTION_MS } from "../config.ts";
import { scopedSessionKey } from "./opaque-dispatch.ts";

// Sole owner of pending ask-edge state.
//
// An "ask edge" records that `from` is awaiting a reply to message `id` from `to`. The broker
// uses it for two decisions: whether a `replyTo` names an authorized ask, and whether an active
// ask would form a mutual-ask deadlock (A waiting on B while B waits on A). Timed-out asks remain
// as reply-only authorization for a bounded window, but leave the active indexes immediately.
//
// Extracted into a module for three reasons:
//
// 1. Reverse-edge lookup was a full scan of every pending edge on each ask. The pair index makes
// it O(1).
// 2. The edge set had no capacity limit, so a misbehaving session could grow it without bound.
// 3. Mailbox redelivery rewrites an edge's target in place. With a pair index that rewrite must
// go through `rekeyTarget`, or the index silently desynchronizes from the edges and the
// mutual-ask check starts returning wrong answers. Making this a module with no public map
// means no call site can mutate the edges directly and forget the index.
//
// `IntercomBroker` is not exported and importing broker.ts starts a broker, so a module is also
// the only seam where this logic can be unit-tested.

export interface AskEdge {
  readonly from: string;
  readonly to: string;
  // Routing scope both parties registered under; absent for the default unscoped routing space.
  readonly scopeId?: string;
  readonly createdAt: number;
}

// An edge that left the map, with the scope needed to locate its durable pending-ask record.
// Bulk removals can span scopes, so the scope cannot be supplied by the caller.
export interface AskEdgeRemoval {
  readonly messageId: string;
  readonly scopeId?: string;
}

interface StoredAskEdge {
  // Retained because the map is keyed by the scoped key, and bulk removals must report the raw id.
  messageId: string;
  from: string;
  to: string;
  scopeId?: string;
  createdAt: number;
  // Cached `${from}\0${to}` key so active counters can be maintained without recomputing.
  pairKey: string;
  active: boolean;
  insertionOrder: number;
}

export interface AskEdgeCapacityRefusal {
  ok: false;
  reason: string;
}

export type AskEdgeCapacity = { ok: true } | AskEdgeCapacityRefusal;

export const MAX_PENDING_ASK_EDGES_PER_SESSION = 16;
export const ASK_REPLY_AUTHORIZATION_RETENTION_MS = STALE_ASK_RETENTION_MS;

function pairKey(from: string, to: string): string {
  return `${from}\0${to}`;
}

// Message IDs are caller-controlled, so per-message broker state must be keyed by routing scope as
// well: otherwise one scope can observe, replace, or evict another scope's entry for the same id.
// Delegates to the session-key builder so scoped keys have exactly one encoding.
export function scopedMessageKey(scopeId: string | undefined, messageId: string): string {
  return scopedSessionKey(scopeId, messageId);
}

function removalOf(edge: StoredAskEdge): AskEdgeRemoval {
  return { messageId: edge.messageId, ...(edge.scopeId ? { scopeId: edge.scopeId } : {}) };
}

export class AskEdges {
  private readonly edges = new Map<string, StoredAskEdge>();
  private readonly activeByAsker = new Map<string, number>();
  private readonly activeByPair = new Map<string, number>();
  private activeCount = 0;
  private replyOnlyCount = 0;
  private nextInsertionOrder = 0;

  constructor(
    private readonly maxGlobal: number,
    private readonly maxPerSession = MAX_PENDING_ASK_EDGES_PER_SESSION,
    private readonly maxReplyOnly = maxGlobal,
  ) {}

  get size(): number {
    return this.edges.size;
  }

  get activeSize(): number {
    return this.activeCount;
  }

  get replyOnlySize(): number {
    return this.replyOnlyCount;
  }

  get(scopeId: string | undefined, messageId: string): AskEdge | undefined {
    return this.edges.get(scopedMessageKey(scopeId, messageId));
  }

  has(scopeId: string | undefined, messageId: string): boolean {
    return this.edges.has(scopedMessageKey(scopeId, messageId));
  }

  // Whether adding an edge from `from` is allowed.
  //
  // `replacingMessageIds` names validated edges this add will retire after successful delivery.
  // Replacing active edges preserves global capacity, but preserves this asker's capacity only for
  // edges belonging to that asker. Reply-only authorizations do not affect active capacity.
  canAdd(scopeId: string | undefined, from: string, replacingMessageIds?: string | readonly string[]): AskEdgeCapacity {
    const replacementIds = typeof replacingMessageIds === "string"
      ? [replacingMessageIds]
      : replacingMessageIds ?? [];
    const replaced = new Set<StoredAskEdge>();
    for (const messageId of replacementIds) {
      const edge = this.edges.get(scopedMessageKey(scopeId, messageId));
      if (edge?.active) replaced.add(edge);
    }
    const replacedActiveCount = replaced.size;
    if (this.activeCount - replacedActiveCount >= this.maxGlobal) {
      return { ok: false, reason: "Too many pending intercom asks" };
    }
    let replacedForAsker = 0;
    for (const edge of replaced) {
      if (edge.from === from) replacedForAsker += 1;
    }
    const askerCountAfterReplacement = (this.activeByAsker.get(from) ?? 0) - replacedForAsker;
    if (askerCountAfterReplacement >= this.maxPerSession) {
      return { ok: false, reason: "Too many pending intercom asks from this session" };
    }
    return { ok: true };
  }

  // Adds an edge, replacing any edge already stored under the same scope and message id.
  add(scopeId: string | undefined, messageId: string, from: string, to: string, now = Date.now()): void {
    this.delete(scopeId, messageId);
    const key = pairKey(from, to);
    this.edges.set(scopedMessageKey(scopeId, messageId), {
      messageId,
      from,
      to,
      ...(scopeId ? { scopeId } : {}),
      pairKey: key,
      createdAt: now,
      active: true,
      insertionOrder: this.nextInsertionOrder++,
    });
    this.activeCount += 1;
    this.increment(this.activeByAsker, from);
    this.increment(this.activeByPair, key);
  }

  delete(scopeId: string | undefined, messageId: string): boolean {
    return this.deleteKey(scopedMessageKey(scopeId, messageId));
  }

  // Repoints an existing edge at a new target, keeping the pair index consistent.
  //
  // Mailbox redelivery can hand a queued ask to a different session than the one it was addressed
  // to (a reconnect under a new session id resolving to the same mailbox identity). Rewriting
  // `edge.to` directly would leave the pair counters describing the old target.
  rekeyTarget(scopeId: string | undefined, messageId: string, nextTo: string): boolean {
    const edge = this.edges.get(scopedMessageKey(scopeId, messageId));
    if (!edge || edge.to === nextTo) {
      return false;
    }
    if (edge.active) this.decrement(this.activeByPair, edge.pairKey);
    edge.to = nextTo;
    edge.pairKey = pairKey(edge.from, nextTo);
    if (edge.active) this.increment(this.activeByPair, edge.pairKey);
    return true;
  }

  // Whether `to` is already awaiting a reply from `from` — i.e. adding from→to would be mutual.
  //
  // `excludingMessageId` omits one edge from consideration, so replying to an ask does not count
  // that same ask as the blocking reverse edge.
  hasReverse(scopeId: string | undefined, from: string, to: string, excludingMessageId?: string): boolean {
    const reverse = pairKey(to, from);
    let count = this.activeByPair.get(reverse) ?? 0;
    if (excludingMessageId !== undefined) {
      const excluded = this.edges.get(scopedMessageKey(scopeId, excludingMessageId));
      if (excluded?.active && excluded.pairKey === reverse) {
        count -= 1;
      }
    }
    return count > 0;
  }

  // Removes timed-out asks from deadlock and active-capacity accounting while retaining a bounded
  // set for late-reply authorization. Returns newly expired and evicted IDs so durable waiter
  // records can be removed without scanning their directory on every send.
  expireActiveOlderThan(maxAgeMs: number, now = Date.now()): AskEdgeRemoval[] {
    const expired: AskEdgeRemoval[] = [];
    const reported = new Set<string>();
    for (const [key, edge] of this.edges) {
      if (edge.active && now - edge.createdAt > maxAgeMs) {
        this.deactivate(edge);
        reported.add(key);
        expired.push(removalOf(edge));
      }
    }
    if (this.replyOnlyCount <= this.maxReplyOnly) return expired;

    const replyOnly = Array.from(this.edges.entries())
      .filter(([, edge]) => !edge.active)
      .sort(([, left], [, right]) => left.createdAt - right.createdAt || left.insertionOrder - right.insertionOrder);
    for (const [key, edge] of replyOnly) {
      if (this.replyOnlyCount <= this.maxReplyOnly) break;
      const removal = removalOf(edge);
      this.deleteKey(key);
      if (!reported.has(key)) {
        expired.push(removal);
      }
    }
    return expired;
  }

  // Drops reply authorization older than `maxAgeMs`.
  pruneOlderThan(maxAgeMs: number, now = Date.now()): void {
    for (const [key, edge] of this.edges) {
      if (now - edge.createdAt > maxAgeMs) {
        this.deleteKey(key);
      }
    }
  }

  // Drops every edge where `sessionKey` is either party and returns their message IDs.
  deleteForSession(sessionKey: string): AskEdgeRemoval[] {
    const deleted: AskEdgeRemoval[] = [];
    for (const [key, edge] of this.edges) {
      if (edge.from === sessionKey || edge.to === sessionKey) {
        const removal = removalOf(edge);
        this.deleteKey(key);
        deleted.push(removal);
      }
    }
    return deleted;
  }

  clear(): void {
    this.edges.clear();
    this.activeByAsker.clear();
    this.activeByPair.clear();
    this.activeCount = 0;
    this.replyOnlyCount = 0;
  }

  private deleteKey(key: string): boolean {
    const edge = this.edges.get(key);
    if (!edge) {
      return false;
    }
    this.edges.delete(key);
    if (edge.active) this.releaseActive(edge);
    else this.replyOnlyCount -= 1;
    return true;
  }

  private deactivate(edge: StoredAskEdge): void {
    this.releaseActive(edge);
    edge.active = false;
    this.replyOnlyCount += 1;
  }

  private releaseActive(edge: StoredAskEdge): void {
    this.activeCount -= 1;
    this.decrement(this.activeByAsker, edge.from);
    this.decrement(this.activeByPair, edge.pairKey);
  }

  private increment(map: Map<string, number>, key: string): void {
    map.set(key, (map.get(key) ?? 0) + 1);
  }

  private decrement(map: Map<string, number>, key: string): void {
    const count = map.get(key);
    if (count === undefined || count <= 1) {
      map.delete(key);
      return;
    }
    map.set(key, count - 1);
  }
}
