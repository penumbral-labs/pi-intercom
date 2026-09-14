import test from "node:test";
import assert from "node:assert/strict";
import { ASK_REPLY_AUTHORIZATION_RETENTION_MS, AskEdges, MAX_PENDING_ASK_EDGES_PER_SESSION } from "./ask-edges.ts";

const GLOBAL_CAP = 512; // MAX_SESSIONS * 4 in the broker

test("add stores an edge and get returns its parties", () => {
  const edges = new AskEdges(GLOBAL_CAP);
  edges.add(undefined, "m1", "a", "b", 1000);
  const edge = edges.get(undefined, "m1");
  assert.equal(edge?.from, "a");
  assert.equal(edge?.to, "b");
  assert.equal(edge?.createdAt, 1000);
  assert.equal(edges.size, 1);
  assert.equal(edges.has(undefined, "m1"), true);
});

test("delete removes the edge and reports whether it existed", () => {
  const edges = new AskEdges(GLOBAL_CAP);
  edges.add(undefined, "m1", "a", "b");
  assert.equal(edges.delete(undefined, "m1"), true);
  assert.equal(edges.delete(undefined, "m1"), false);
  assert.equal(edges.size, 0);
  assert.equal(edges.get(undefined, "m1"), undefined);
});

test("hasReverse detects a mutual ask in O(1) without scanning", () => {
  const edges = new AskEdges(GLOBAL_CAP);
  edges.add(undefined, "m1", "b", "a");
  // a now wants to ask b, but b is already awaiting a.
  assert.equal(edges.hasReverse(undefined, "a", "b"), true);
  // The forward direction is not itself a reverse edge.
  assert.equal(edges.hasReverse(undefined, "b", "a"), false);
});

test("hasReverse excludes the ask being replied to", () => {
  const edges = new AskEdges(GLOBAL_CAP);
  edges.add(undefined, "m1", "b", "a");
  // Replying to m1 must not count m1 as the blocking reverse edge.
  assert.equal(edges.hasReverse(undefined, "a", "b", "m1"), false);
  // Any other pending edge in that direction still blocks.
  edges.add(undefined, "m2", "b", "a");
  assert.equal(edges.hasReverse(undefined, "a", "b", "m1"), true);
});

test("hasReverse stays correct after the last edge for a pair is deleted", () => {
  const edges = new AskEdges(GLOBAL_CAP);
  edges.add(undefined, "m1", "b", "a");
  edges.delete(undefined, "m1");
  assert.equal(edges.hasReverse(undefined, "a", "b"), false);
});

test("rekeyTarget repoints an edge and moves it in the pair index", () => {
  const edges = new AskEdges(GLOBAL_CAP);
  edges.add(undefined, "m1", "a", "b");
  assert.equal(edges.hasReverse(undefined, "b", "a"), true, "precondition: a->b is a reverse edge for b->a");

  assert.equal(edges.rekeyTarget(undefined, "m1", "c"), true);
  assert.equal(edges.get(undefined, "m1")?.to, "c");

  // This is the assertion that catches an in-place `edge.to = …` rewrite: the pair index must
  // follow the retarget, so the old target no longer sees a reverse edge and the new one does.
  assert.equal(edges.hasReverse(undefined, "b", "a"), false, "old target must no longer register a reverse edge");
  assert.equal(edges.hasReverse(undefined, "c", "a"), true, "new target must register the reverse edge");
});

test("rekeyTarget is a no-op for an unknown id or an unchanged target", () => {
  const edges = new AskEdges(GLOBAL_CAP);
  edges.add(undefined, "m1", "a", "b");
  assert.equal(edges.rekeyTarget(undefined, "missing", "c"), false);
  assert.equal(edges.rekeyTarget(undefined, "m1", "b"), false);
  assert.equal(edges.get(undefined, "m1")?.to, "b");
});

test("per-session cap refuses the 17th concurrent ask from one asker", () => {
  const edges = new AskEdges(GLOBAL_CAP);
  for (let i = 0; i < MAX_PENDING_ASK_EDGES_PER_SESSION; i += 1) {
    assert.equal(edges.canAdd(undefined, "a").ok, true, `ask ${i + 1} should be allowed`);
    edges.add(undefined, `m${i}`, "a", "b");
  }
  const refusal = edges.canAdd(undefined, "a");
  assert.equal(refusal.ok, false);
  assert.match(refusal.ok === false ? refusal.reason : "", /from this session/);

  // The cap is per asker, not global.
  assert.equal(edges.canAdd(undefined, "other").ok, true);
});

test("global cap refuses a new asker once the table is full", () => {
  const edges = new AskEdges(3, 100);
  edges.add(undefined, "m1", "a", "x");
  edges.add(undefined, "m2", "b", "x");
  edges.add(undefined, "m3", "c", "x");
  const refusal = edges.canAdd(undefined, "d");
  assert.equal(refusal.ok, false);
  assert.match(refusal.ok === false ? refusal.reason : "", /Too many pending intercom asks$/);
});

test("replacing an existing edge discounts only capacity owned by the same asker", () => {
  const edges = new AskEdges(3, 1);
  edges.add(undefined, "own-ask", "a", "b");
  edges.add(undefined, "peer-ask", "b", "a");

  assert.equal(edges.canAdd(undefined, "a", "own-ask").ok, true, "re-arming the asker's own edge preserves its capacity");

  const peerReplacement = edges.canAdd(undefined, "a", "peer-ask");
  assert.equal(peerReplacement.ok, false, "replacing a peer-owned edge would add another edge for the capped asker");
  assert.match(peerReplacement.ok === false ? peerReplacement.reason : "", /from this session/);

  const full = new AskEdges(1, 100);
  full.add(undefined, "m1", "a", "b");
  assert.equal(full.canAdd(undefined, "z").ok, false, "global cap refuses a genuinely new edge");
  assert.equal(full.canAdd(undefined, "z", "m1").ok, true, "any replacement preserves global capacity");
});

test("capacity checks discount multiple active edges retired by one accepted replacement", () => {
  const edges = new AskEdges(2, 1);
  edges.add(undefined, "reply-target", "target", "asker");
  edges.add(undefined, "superseded", "asker", "target");

  assert.equal(edges.canAdd(undefined, "asker", "reply-target").ok, false);
  assert.equal(edges.canAdd(undefined, "asker", ["reply-target", "superseded"]).ok, true);
});

test("add replaces an existing id without double-counting capacity", () => {
  const edges = new AskEdges(GLOBAL_CAP, 2);
  edges.add(undefined, "m1", "a", "b");
  edges.add(undefined, "m1", "a", "c");
  assert.equal(edges.size, 1);
  assert.equal(edges.get(undefined, "m1")?.to, "c");
  // If the replace had leaked a counter, "a" would already be at its cap of 2.
  assert.equal(edges.canAdd(undefined, "a").ok, true);
});

test("timed-out asks retain reply authorization without blocking reverse asks", () => {
  const edges = new AskEdges(GLOBAL_CAP);
  const waiterTimeoutMs = 50;
  edges.add(undefined, "timed-out", "a", "b", 1000);

  assert.deepEqual(edges.expireActiveOlderThan(waiterTimeoutMs, 1000 + waiterTimeoutMs), []);
  assert.equal(edges.has(undefined, "timed-out"), true);
  assert.equal(edges.hasReverse(undefined, "b", "a"), true, "the ask remains active through its timeout boundary");

  assert.deepEqual(edges.expireActiveOlderThan(waiterTimeoutMs, 1001 + waiterTimeoutMs), [{ messageId: "timed-out" }]);
  assert.deepEqual(edges.expireActiveOlderThan(waiterTimeoutMs, 1002 + waiterTimeoutMs), [], "expiry is reported only once");
  assert.equal(edges.has(undefined, "timed-out"), true, "the timed-out ask remains authorized for a late reply");
  assert.equal(edges.hasReverse(undefined, "b", "a"), false, "reply-only authorization must not block a reverse ask");
});

test("timed-out asks do not consume active capacity", () => {
  const edges = new AskEdges(GLOBAL_CAP, 1);
  edges.add(undefined, "timed-out", "a", "b", 1000);
  assert.equal(edges.canAdd(undefined, "a").ok, false);

  edges.expireActiveOlderThan(50, 1051);
  assert.equal(edges.activeSize, 0);
  assert.equal(edges.size, 1, "reply authorization remains stored separately from active capacity");
  assert.equal(edges.canAdd(undefined, "a").ok, true);
  assert.equal(edges.canAdd(undefined, "c").ok, true);
});

test("reply-only capacity evicts the deterministic oldest authorizations without changing active caps", () => {
  const edges = new AskEdges(2, 1, 2);
  edges.add(undefined, "old-by-time", "a", "x", 999);
  edges.add(undefined, "old-by-order", "b", "x", 1000);
  edges.expireActiveOlderThan(0, 1001);

  edges.add(undefined, "new-by-order", "c", "x", 1000);
  assert.deepEqual(edges.expireActiveOlderThan(0, 1001), [{ messageId: "new-by-order" }, { messageId: "old-by-time" }]);
  assert.equal(edges.has(undefined, "old-by-time"), false);
  assert.equal(edges.has(undefined, "old-by-order"), true);
  assert.equal(edges.has(undefined, "new-by-order"), true);

  edges.add(undefined, "newest", "d", "x", 1001);
  assert.deepEqual(edges.expireActiveOlderThan(0, 1002), [{ messageId: "newest" }, { messageId: "old-by-order" }]);
  assert.equal(edges.has(undefined, "old-by-order"), false, "equal timestamps are ordered by insertion");
  assert.equal(edges.has(undefined, "new-by-order"), true);
  assert.equal(edges.has(undefined, "newest"), true);
  assert.equal(edges.activeSize, 0);
  assert.equal(edges.replyOnlySize, 2);
  assert.equal(edges.canAdd(undefined, "a").ok, true, "reply-only eviction must not alter per-session active accounting");
  assert.equal(edges.canAdd(undefined, "e").ok, true, "reply-only eviction must not alter global active accounting");
});

test("deleting more than the reply-only cap of active edges does not inflate reply-only accounting", () => {
  const replyOnlyCap = 2;
  const edges = new AskEdges(10, 10, replyOnlyCap);
  for (let index = 0; index <= replyOnlyCap; index += 1) {
    edges.add(undefined, `deleted-${index}`, "a", "b", index);
    assert.equal(edges.delete(undefined, `deleted-${index}`), true);
  }
  assert.equal(edges.replyOnlySize, 0);

  edges.add(undefined, "oldest-valid", "a", "b", 100);
  edges.add(undefined, "newest-valid", "a", "b", 101);
  assert.deepEqual(edges.expireActiveOlderThan(0, 102), [{ messageId: "oldest-valid" }, { messageId: "newest-valid" }]);
  assert.equal(edges.replyOnlySize, replyOnlyCap);
  assert.equal(edges.has(undefined, "oldest-valid"), true);
  assert.equal(edges.has(undefined, "newest-valid"), true);
});

test("pruneOlderThan retains reply authorization for the bounded late-reply window", () => {
  const edges = new AskEdges(GLOBAL_CAP);
  const waiterTimeoutMs = 50;
  const authorizationAgeMs = waiterTimeoutMs + ASK_REPLY_AUTHORIZATION_RETENTION_MS;
  edges.add(undefined, "old", "a", "b", 1000);
  edges.add(undefined, "new", "a", "c", 5000);
  edges.expireActiveOlderThan(waiterTimeoutMs, 1001 + waiterTimeoutMs);
  edges.pruneOlderThan(authorizationAgeMs, 1000 + authorizationAgeMs);
  assert.equal(edges.has(undefined, "old"), true, "the full late-reply window remains authorized after waiter timeout");
  edges.pruneOlderThan(authorizationAgeMs, 1001 + authorizationAgeMs);
  assert.equal(edges.has(undefined, "old"), false);
  assert.equal(edges.has(undefined, "new"), true);
  // Active counters follow expiration, so "a" is not stuck at a phantom count.
  assert.equal(edges.hasReverse(undefined, "b", "a"), false);
  assert.equal(edges.hasReverse(undefined, "c", "a"), true);
});

test("deleteForSession removes edges where the session is either party", () => {
  const edges = new AskEdges(GLOBAL_CAP);
  edges.add(undefined, "m1", "a", "b");
  edges.add(undefined, "m2", "c", "a");
  edges.add(undefined, "m3", "c", "d");
  assert.deepEqual(edges.deleteForSession("a"), [{ messageId: "m1" }, { messageId: "m2" }]);
  assert.equal(edges.has(undefined, "m1"), false);
  assert.equal(edges.has(undefined, "m2"), false);
  assert.equal(edges.has(undefined, "m3"), true);
  assert.equal(edges.size, 1);
});

test("clear empties edges and both indexes", () => {
  const edges = new AskEdges(GLOBAL_CAP);
  edges.add(undefined, "m1", "a", "b");
  edges.add(undefined, "m2", "b", "a");
  edges.clear();
  assert.equal(edges.size, 0);
  assert.equal(edges.hasReverse(undefined, "a", "b"), false);
  assert.equal(edges.hasReverse(undefined, "b", "a"), false);
  assert.equal(edges.canAdd(undefined, "a").ok, true);
});

test("removals report the scope that owns the edge's durable ask record", () => {
  const edges = new AskEdges(GLOBAL_CAP);
  edges.add("team-1", "scoped", "scoped-a", "scoped-b", 1000);
  edges.add(undefined, "unscoped", "plain-a", "plain-b", 1000);
  assert.equal(edges.get("team-1", "scoped")?.scopeId, "team-1");
  assert.equal(edges.get(undefined, "unscoped")?.scopeId, undefined);

  assert.deepEqual(edges.deleteForSession("scoped-a"), [{ messageId: "scoped", scopeId: "team-1" }]);
  assert.deepEqual(edges.expireActiveOlderThan(0, 1001), [{ messageId: "unscoped" }]);
});

test("reply-only eviction reports the evicted edge's scope", () => {
  const edges = new AskEdges(2, 2, 1);
  edges.add("team-1", "first", "a", "x", 1000);
  edges.add("team-2", "second", "b", "x", 1001);
  assert.deepEqual(
    edges.expireActiveOlderThan(0, 1002),
    [{ messageId: "first", scopeId: "team-1" }, { messageId: "second", scopeId: "team-2" }],
  );
  assert.equal(edges.has("team-1", "first"), false, "the oldest reply-only authorization is evicted at capacity");
  assert.equal(edges.has("team-2", "second"), true);
});

test("the same message id in two scopes is two independent edges", () => {
  const edges = new AskEdges(GLOBAL_CAP);
  edges.add("team-1", "shared-id", "team-1-asker", "team-1-target", 1000);
  edges.add("team-2", "shared-id", "team-2-asker", "team-2-target", 1001);
  assert.equal(edges.size, 2, "a caller-controlled id must not be able to replace another scope's edge");
  assert.equal(edges.get("team-1", "shared-id")?.to, "team-1-target");
  assert.equal(edges.get("team-2", "shared-id")?.to, "team-2-target");
  assert.equal(edges.get(undefined, "shared-id"), undefined, "the unscoped routing space has no such edge");

  // Only the addressed scope's edge is repointed, retired, or excluded from the deadlock check.
  assert.equal(edges.rekeyTarget("team-1", "shared-id", "team-1-replacement"), true);
  assert.equal(edges.get("team-2", "shared-id")?.to, "team-2-target");
  assert.equal(edges.hasReverse("team-2", "team-2-target", "team-2-asker", "shared-id"), false);
  assert.equal(edges.hasReverse("team-1", "team-2-target", "team-2-asker", "shared-id"), true, "another scope's id must not exclude this edge");
  assert.equal(edges.delete("team-1", "shared-id"), true);
  assert.equal(edges.has("team-2", "shared-id"), true);
  assert.equal(edges.delete(undefined, "shared-id"), false);
});

test("capacity replacement only discounts edges in the caller's scope", () => {
  const edges = new AskEdges(2, 1);
  edges.add("team-1", "shared-id", "asker", "target", 1000);
  edges.add("team-2", "shared-id", "other-asker", "other-target", 1000);

  assert.equal(edges.canAdd("team-1", "asker", "shared-id").ok, true, "the asker's own scoped edge is retired by the replacement");
  const crossScope = edges.canAdd(undefined, "asker", "shared-id");
  assert.equal(crossScope.ok, false, "an id that only exists in another scope discounts nothing");
});
