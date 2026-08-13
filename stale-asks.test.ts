import test from "node:test";
import assert from "node:assert/strict";
import { MAX_STALE_ASKS, STALE_ASK_RETENTION_MS, StaleAsks } from "./stale-asks.ts";

test("stale asks classify only for the same principal", () => {
  const stale = new StaleAsks();
  stale.record("ask-1", "session-a", "cancelled", 10);
  assert.equal(stale.classify("ask-1", "session-a", 11), "cancelled");
  assert.equal(stale.classify("ask-1", "session-b", 11), undefined);
});

test("stale asks distinguish cancellation, supersession, and plain timeout", () => {
  const stale = new StaleAsks();
  stale.record("cancelled", "session-a", "cancelled", 1);
  stale.record("superseded", "session-a", "superseded", 2);
  stale.record("timed-out", "session-a", "timed_out", 3);
  assert.equal(stale.classify("cancelled", "session-a", 4), "cancelled");
  assert.equal(stale.classify("superseded", "session-a", 4), "superseded");
  assert.equal(stale.classify("timed-out", "session-a", 4), "timed_out");
});

test("stale ask history is bounded and expires", () => {
  const stale = new StaleAsks();
  for (let index = 0; index <= MAX_STALE_ASKS; index += 1) {
    stale.record(`ask-${index}`, "session-a", "cancelled", index);
  }
  assert.equal(stale.classify("ask-0", "session-a", MAX_STALE_ASKS), undefined);
  assert.equal(stale.classify(`ask-${MAX_STALE_ASKS}`, "session-a", MAX_STALE_ASKS), "cancelled");
  assert.equal(stale.classify(`ask-${MAX_STALE_ASKS}`, "session-a", MAX_STALE_ASKS + STALE_ASK_RETENTION_MS + 1), undefined);
});
