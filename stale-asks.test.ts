import test from "node:test";
import assert from "node:assert/strict";
import { MAX_STALE_ASKS, STALE_ASK_RETENTION_MS, StaleAsks } from "./stale-asks.ts";

test("stale asks classify names case-insensitively", () => {
  const stale = new StaleAsks();
  stale.record("ask-1", { type: "session_name", value: "Planner" }, "cancelled", 10);
  assert.equal(stale.classify("ask-1", { type: "session_name", value: "pLaNnEr" }, 11), "cancelled");
  assert.equal(stale.classify("ask-1", { type: "session_name", value: "worker" }, 11), undefined);
});

test("stale asks preserve exact opaque session IDs and separate ID and name namespaces", () => {
  const stale = new StaleAsks();
  stale.record("ask-1", { type: "session_id", value: "Session-A" }, "cancelled", 10);
  assert.equal(stale.classify("ask-1", { type: "session_id", value: "Session-A" }, 11), "cancelled");
  assert.equal(stale.classify("ask-1", { type: "session_id", value: "session-a" }, 11), undefined);
  assert.equal(stale.classify("ask-1", { type: "session_name", value: "Session-A" }, 11), undefined);

  stale.record("ask-2", { type: "session_name", value: "Session-A" }, "superseded", 12);
  assert.equal(stale.classify("ask-2", { type: "session_name", value: "session-a" }, 13), "superseded");
  assert.equal(stale.classify("ask-2", { type: "session_id", value: "Session-A" }, 13), undefined);
});

test("stale asks distinguish cancellation, supersession, and plain timeout", () => {
  const stale = new StaleAsks();
  const principal = { type: "session_id", value: "session-a" } as const;
  stale.record("cancelled", principal, "cancelled", 1);
  stale.record("superseded", principal, "superseded", 2);
  stale.record("timed-out", principal, "timed_out", 3);
  assert.equal(stale.classify("cancelled", principal, 4), "cancelled");
  assert.equal(stale.classify("superseded", principal, 4), "superseded");
  assert.equal(stale.classify("timed-out", principal, 4), "timed_out");
});

test("stale ask history is bounded and expires", () => {
  const stale = new StaleAsks();
  const principal = { type: "session_id", value: "session-a" } as const;
  for (let index = 0; index <= MAX_STALE_ASKS; index += 1) {
    stale.record(`ask-${index}`, principal, "cancelled", index);
  }
  assert.equal(stale.classify("ask-0", principal, MAX_STALE_ASKS), undefined);
  assert.equal(stale.classify(`ask-${MAX_STALE_ASKS}`, principal, MAX_STALE_ASKS), "cancelled");
  assert.equal(stale.classify(`ask-${MAX_STALE_ASKS}`, principal, MAX_STALE_ASKS + STALE_ASK_RETENTION_MS + 1), undefined);
});
