// Sole owner of pending ask-edge state.
//
// An "ask edge" records that `from` is awaiting a reply to message `id` from `to`. The broker
// uses it for two decisions: whether a `replyTo` names a real pending ask, and whether a new ask
// would form a mutual-ask deadlock (A waiting on B while B waits on A).
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
  from: string;
  to: string;
  createdAt: number;
}

interface StoredAskEdge extends AskEdge {
  // Cached `${from}\0${to}` key so counters can be maintained without recomputing.
  pairKey: string;
}

export interface AskEdgeCapacityRefusal {
  ok: false;
  reason: string;
}

export type AskEdgeCapacity = { ok: true } | AskEdgeCapacityRefusal;

export const MAX_PENDING_ASK_EDGES_PER_SESSION = 16;

function pairKey(from: string, to: string): string {
  return `${from}\0${to}`;
}

export class AskEdges {
  private readonly edges = new Map<string, StoredAskEdge>();
  private readonly byAsker = new Map<string, number>();
  private readonly byPair = new Map<string, number>();

  constructor(private readonly maxGlobal: number, private readonly maxPerSession = MAX_PENDING_ASK_EDGES_PER_SESSION) {}

  get size(): number {
    return this.edges.size;
  }

  get(messageId: string): AskEdge | undefined {
    return this.edges.get(messageId);
  }

  has(messageId: string): boolean {
    return this.edges.has(messageId);
  }

  // Whether adding an edge from `from` is allowed.
  //
  // `replacingMessageId` names an edge this add would replace. Any replacement preserves global
  // capacity, but it preserves this asker's capacity only when the replaced edge belongs to the
  // same asker. Replacing a peer-owned ask must not let an already-capped sender add a 17th edge.
  canAdd(from: string, replacingMessageId?: string): AskEdgeCapacity {
    const replaced = replacingMessageId === undefined ? undefined : this.edges.get(replacingMessageId);
    if (!replaced && this.edges.size >= this.maxGlobal) {
      return { ok: false, reason: "Too many pending intercom asks" };
    }
    const askerCountAfterReplacement = (this.byAsker.get(from) ?? 0) - (replaced?.from === from ? 1 : 0);
    if (askerCountAfterReplacement >= this.maxPerSession) {
      return { ok: false, reason: "Too many pending intercom asks from this session" };
    }
    return { ok: true };
  }

  // Adds an edge, replacing any edge already stored under the same message id.
  add(messageId: string, from: string, to: string, now = Date.now()): void {
    this.delete(messageId);
    const key = pairKey(from, to);
    this.edges.set(messageId, { from, to, pairKey: key, createdAt: now });
    this.increment(this.byAsker, from);
    this.increment(this.byPair, key);
  }

  delete(messageId: string): boolean {
    const edge = this.edges.get(messageId);
    if (!edge) {
      return false;
    }
    this.edges.delete(messageId);
    this.decrement(this.byAsker, edge.from);
    this.decrement(this.byPair, edge.pairKey);
    return true;
  }

  // Repoints an existing edge at a new target, keeping the pair index consistent.
  //
  // Mailbox redelivery can hand a queued ask to a different session than the one it was addressed
  // to (a reconnect under a new session id resolving to the same mailbox identity). Rewriting
  // `edge.to` directly would leave the pair counters describing the old target.
  rekeyTarget(messageId: string, nextTo: string): boolean {
    const edge = this.edges.get(messageId);
    if (!edge || edge.to === nextTo) {
      return false;
    }
    this.decrement(this.byPair, edge.pairKey);
    edge.to = nextTo;
    edge.pairKey = pairKey(edge.from, nextTo);
    this.increment(this.byPair, edge.pairKey);
    return true;
  }

  // Whether `to` is already awaiting a reply from `from` — i.e. adding from→to would be mutual.
  //
  // `excludingMessageId` omits one edge from consideration, so replying to an ask does not count
  // that same ask as the blocking reverse edge.
  hasReverse(from: string, to: string, excludingMessageId?: string): boolean {
    const reverse = pairKey(to, from);
    let count = this.byPair.get(reverse) ?? 0;
    if (excludingMessageId !== undefined) {
      const excluded = this.edges.get(excludingMessageId);
      if (excluded?.pairKey === reverse) {
        count -= 1;
      }
    }
    return count > 0;
  }

  // Drops edges older than `maxAgeMs`.
  pruneOlderThan(maxAgeMs: number, now = Date.now()): void {
    for (const [messageId, edge] of this.edges) {
      if (now - edge.createdAt > maxAgeMs) {
        this.delete(messageId);
      }
    }
  }

  // Drops every edge where `sessionId` is either party.
  deleteForSession(sessionId: string): void {
    for (const [messageId, edge] of this.edges) {
      if (edge.from === sessionId || edge.to === sessionId) {
        this.delete(messageId);
      }
    }
  }

  clear(): void {
    this.edges.clear();
    this.byAsker.clear();
    this.byPair.clear();
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
