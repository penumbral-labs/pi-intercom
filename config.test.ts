import test from "node:test";
import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import {
  getAskTimeoutMs,
  getConfigPath,
  getPendingAskPruneIntervalMs,
  loadConfig,
  MAX_ASK_TIMEOUT_MS,
} from "./config.ts";

async function withAgentDir<T>(agentDir: string, fn: () => T | Promise<T>): Promise<T> {
  const previousAgentDir = process.env.PI_CODING_AGENT_DIR;
  process.env.PI_CODING_AGENT_DIR = agentDir;
  try {
    return await fn();
  } finally {
    if (previousAgentDir === undefined) delete process.env.PI_CODING_AGENT_DIR;
    else process.env.PI_CODING_AGENT_DIR = previousAgentDir;
  }
}

test("getConfigPath uses the centralized intercom runtime directory", () => {
  assert.equal(getConfigPath("/tmp/pi-agent/intercom"), join("/tmp/pi-agent", "intercom", "config.json"));
});

test("loadConfig reads config below PI_CODING_AGENT_DIR", async () => {
  const root = mkdtempSync(join(tmpdir(), "pi-intercom-config-"));

  try {
    const intercomDir = join(root, "intercom");
    mkdirSync(intercomDir, { recursive: true });
    writeFileSync(join(intercomDir, "config.json"), JSON.stringify({ status: "platform-test" }));

    await withAgentDir(root, () => {
      assert.equal(loadConfig().status, "platform-test");
    });
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("loadConfig defaults inboundTrigger to current auto-trigger behavior", async () => {
  const root = mkdtempSync(join(tmpdir(), "pi-intercom-config-"));
  try {
    await withAgentDir(root, () => {
      assert.equal(loadConfig().inboundTrigger, "always");
    });
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("loadConfig accepts inboundTrigger replies policy", async () => {
  const root = mkdtempSync(join(tmpdir(), "pi-intercom-config-"));
  try {
    mkdirSync(join(root, "intercom"), { recursive: true });
    writeFileSync(join(root, "intercom", "config.json"), JSON.stringify({ inboundTrigger: "replies" }));
    await withAgentDir(root, () => {
      assert.equal(loadConfig().inboundTrigger, "replies");
    });
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("loadConfig accepts a restart-stable intercom id", async () => {
  const root = mkdtempSync(join(tmpdir(), "pi-intercom-config-"));
  try {
    mkdirSync(join(root, "intercom"), { recursive: true });
    writeFileSync(join(root, "intercom", "config.json"), JSON.stringify({ stableId: " pinned-worker " }));
    await withAgentDir(root, () => {
      assert.equal(loadConfig().stableId, "pinned-worker");
    });
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("loadConfig rejects invalid inboundTrigger values", async () => {
  const root = mkdtempSync(join(tmpdir(), "pi-intercom-config-"));
  try {
    mkdirSync(join(root, "intercom"), { recursive: true });
    writeFileSync(join(root, "intercom", "config.json"), JSON.stringify({ inboundTrigger: "prompt" }));

    await withAgentDir(root, () => {
      assert.throws(
        () => loadConfig(),
        /Failed to load intercom config.*"inboundTrigger" must be "always", "replies", or "never"/,
      );
    });
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("getPendingAskPruneIntervalMs preserves valid delays and defaults unsafe timer values", () => {
  assert.equal(getPendingAskPruneIntervalMs("1"), 1);
  assert.equal(getPendingAskPruneIntervalMs(String(MAX_ASK_TIMEOUT_MS)), MAX_ASK_TIMEOUT_MS);
  for (const raw of [undefined, "", "NaN", "0", "-1", "1.5", String(MAX_ASK_TIMEOUT_MS + 1)]) {
    assert.equal(getPendingAskPruneIntervalMs(raw), 60_000);
  }
});

test("getAskTimeoutMs accepts the largest delay setTimeout can represent", () => {
  const previous = process.env.PI_INTERCOM_ASK_TIMEOUT_MS;
  process.env.PI_INTERCOM_ASK_TIMEOUT_MS = String(MAX_ASK_TIMEOUT_MS);
  try {
    assert.equal(getAskTimeoutMs(), MAX_ASK_TIMEOUT_MS);
    assert.equal(MAX_ASK_TIMEOUT_MS, 2 ** 31 - 1);
  } finally {
    if (previous === undefined) delete process.env.PI_INTERCOM_ASK_TIMEOUT_MS;
    else process.env.PI_INTERCOM_ASK_TIMEOUT_MS = previous;
  }
});

test("getAskTimeoutMs rejects a delay setTimeout would fire immediately", () => {
  const previous = process.env.PI_INTERCOM_ASK_TIMEOUT_MS;
  process.env.PI_INTERCOM_ASK_TIMEOUT_MS = String(MAX_ASK_TIMEOUT_MS + 1);
  try {
    assert.throws(() => getAskTimeoutMs(), new RegExp(String(MAX_ASK_TIMEOUT_MS)));
  } finally {
    if (previous === undefined) delete process.env.PI_INTERCOM_ASK_TIMEOUT_MS;
    else process.env.PI_INTERCOM_ASK_TIMEOUT_MS = previous;
  }
});
