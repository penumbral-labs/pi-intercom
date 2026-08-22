import test from "node:test";
import assert from "node:assert/strict";
import { pruneDisconnectedSessions } from "./disconnected-sessions.ts";
import type { SessionInfo } from "../types.ts";

function session(id: string): SessionInfo {
  return {
    id,
    name: id,
    cwd: "/tmp",
    model: "test-model",
    pid: 1,
    startedAt: 1,
    lastActivity: 1,
  };
}

test("disconnected session pruning uses an injected timestamp at the retention boundary", () => {
  const disconnected = new Map([
    ["expired", { info: session("expired"), disconnectedAt: 1_000 }],
    ["boundary", { info: session("boundary"), disconnectedAt: 1_001 }],
    ["recent", { info: session("recent"), disconnectedAt: 1_500 }],
  ]);

  pruneDisconnectedSessions(disconnected, 2_001, 1_000);

  assert.deepEqual([...disconnected.keys()], ["boundary", "recent"]);
});
