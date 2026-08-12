import test from "node:test";
import assert from "node:assert/strict";
import { AskEdges, MAX_PENDING_ASK_EDGES_PER_SESSION } from "./ask-edges.ts";

const GLOBAL_CAP = 512; // MAX_SESSIONS * 4 in the broker

test("add stores an edge and get returns its parties", () => {
  const edges = new AskEdges(GLOBAL_CAP);
  edges.add("m1", "a", "b", 1000);
  const edge = edges.get("m1");
  assert.equal(edge?.from, "a");
  assert.equal(edge?.to, "b");
  assert.equal(edge?.createdAt, 1000);
  assert.equal(edges.size, 1);
  assert.equal(edges.has("m1"), true);
});

test("delete removes the edge and reports whether it existed", () => {
  const edges = new AskEdges(GLOBAL_CAP);
  edges.add("m1", "a", "b");
  assert.equal(edges.delete("m1"), true);
  assert.equal(edges.delete("m1"), false);
  assert.equal(edges.size, 0);
  assert.equal(edges.get("m1"), undefined);
});

test("hasReverse detects a mutual ask in O(1) without scanning", () => {
  const edges = new AskEdges(GLOBAL_CAP);
  edges.add("m1", "b", "a");
  // a now wants to ask b, but b is already awaiting a.
  assert.equal(edges.hasReverse("a", "b"), true);
  // The forward direction is not itself a reverse edge.
  assert.equal(edges.hasReverse("b", "a"), false);
});

test("hasReverse excludes the ask being replied to", () => {
  const edges = new AskEdges(GLOBAL_CAP);
  edges.add("m1", "b", "a");
  // Replying to m1 must not count m1 as the blocking reverse edge.
  assert.equal(edges.hasReverse("a", "b", "m1"), false);
  // Any other pending edge in that direction still blocks.
  edges.add("m2", "b", "a");
  assert.equal(edges.hasReverse("a", "b", "m1"), true);
});

test("hasReverse stays correct after the last edge for a pair is deleted", () => {
  const edges = new AskEdges(GLOBAL_CAP);
  edges.add("m1", "b", "a");
  edges.delete("m1");
  assert.equal(edges.hasReverse("a", "b"), false);
});

test("rekeyTarget repoints an edge and moves it in the pair index", () => {
  const edges = new AskEdges(GLOBAL_CAP);
  edges.add("m1", "a", "b");
  assert.equal(edges.hasReverse("b", "a"), true, "precondition: a->b is a reverse edge for b->a");

  assert.equal(edges.rekeyTarget("m1", "c"), true);
  assert.equal(edges.get("m1")?.to, "c");

  // This is the assertion that catches an in-place `edge.to = …` rewrite: the pair index must
  // follow the retarget, so the old target no longer sees a reverse edge and the new one does.
  assert.equal(edges.hasReverse("b", "a"), false, "old target must no longer register a reverse edge");
  assert.equal(edges.hasReverse("c", "a"), true, "new target must register the reverse edge");
});

test("rekeyTarget is a no-op for an unknown id or an unchanged target", () => {
  const edges = new AskEdges(GLOBAL_CAP);
  edges.add("m1", "a", "b");
  assert.equal(edges.rekeyTarget("missing", "c"), false);
  assert.equal(edges.rekeyTarget("m1", "b"), false);
  assert.equal(edges.get("m1")?.to, "b");
});

test("per-session cap refuses the 17th concurrent ask from one asker", () => {
  const edges = new AskEdges(GLOBAL_CAP);
  for (let i = 0; i < MAX_PENDING_ASK_EDGES_PER_SESSION; i += 1) {
    assert.equal(edges.canAdd("a").ok, true, `ask ${i + 1} should be allowed`);
    edges.add(`m${i}`, "a", "b");
  }
  const refusal = edges.canAdd("a");
  assert.equal(refusal.ok, false);
  assert.match(refusal.ok === false ? refusal.reason : "", /from this session/);

  // The cap is per asker, not global.
  assert.equal(edges.canAdd("other").ok, true);
});

test("global cap refuses a new asker once the table is full", () => {
  const edges = new AskEdges(3, 100);
  edges.add("m1", "a", "x");
  edges.add("m2", "b", "x");
  edges.add("m3", "c", "x");
  const refusal = edges.canAdd("d");
  assert.equal(refusal.ok, false);
  assert.match(refusal.ok === false ? refusal.reason : "", /Too many pending intercom asks$/);
});

test("replacing an existing edge does not consume capacity at either cap", () => {
  const edges = new AskEdges(2, 1);
  edges.add("m1", "a", "b");
  // "a" is at its per-session cap of 1 and the table is not full, but re-arming m1 is a
  // replacement, so it must be allowed.
  assert.equal(edges.canAdd("a", "m1").ok, true);

  const full = new AskEdges(1, 100);
  full.add("m1", "a", "b");
  assert.equal(full.canAdd("z").ok, false, "global cap refuses a genuinely new edge");
  assert.equal(full.canAdd("z", "m1").ok, true, "replacing an existing edge is still allowed at the global cap");
});

test("add replaces an existing id without double-counting capacity", () => {
  const edges = new AskEdges(GLOBAL_CAP, 2);
  edges.add("m1", "a", "b");
  edges.add("m1", "a", "c");
  assert.equal(edges.size, 1);
  assert.equal(edges.get("m1")?.to, "c");
  // If the replace had leaked a counter, "a" would already be at its cap of 2.
  assert.equal(edges.canAdd("a").ok, true);
});

test("pruneOlderThan drops only edges past the age bound", () => {
  const edges = new AskEdges(GLOBAL_CAP);
  edges.add("old", "a", "b", 1000);
  edges.add("new", "a", "c", 5000);
  edges.pruneOlderThan(1000, 5500);
  assert.equal(edges.has("old"), false);
  assert.equal(edges.has("new"), true);
  // Counters follow the prune, so "a" is not stuck at a phantom count.
  assert.equal(edges.hasReverse("b", "a"), false);
  assert.equal(edges.hasReverse("c", "a"), true);
});

test("deleteForSession removes edges where the session is either party", () => {
  const edges = new AskEdges(GLOBAL_CAP);
  edges.add("m1", "a", "b");
  edges.add("m2", "c", "a");
  edges.add("m3", "c", "d");
  edges.deleteForSession("a");
  assert.equal(edges.has("m1"), false);
  assert.equal(edges.has("m2"), false);
  assert.equal(edges.has("m3"), true);
  assert.equal(edges.size, 1);
});

test("clear empties edges and both indexes", () => {
  const edges = new AskEdges(GLOBAL_CAP);
  edges.add("m1", "a", "b");
  edges.add("m2", "b", "a");
  edges.clear();
  assert.equal(edges.size, 0);
  assert.equal(edges.hasReverse("a", "b"), false);
  assert.equal(edges.hasReverse("b", "a"), false);
  assert.equal(edges.canAdd("a").ok, true);
});
