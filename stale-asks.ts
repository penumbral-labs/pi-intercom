import { STALE_ASK_RETENTION_MS } from "./config.ts";

export type StaleAskTier = "cancelled" | "superseded" | "timed_out";

export type StaleAskPrincipal =
  | { type: "session_id"; value: string }
  | { type: "session_name"; value: string };

interface StaleAskEntry {
  principalKey: string;
  tier: StaleAskTier;
  recordedAt: number;
}

function principalKey(principal: StaleAskPrincipal): string {
  const value = principal.type === "session_name" ? principal.value.toLowerCase() : principal.value;
  return `${principal.type}\0${value}`;
}

export const MAX_STALE_ASKS = 256;
export { STALE_ASK_RETENTION_MS };

export class StaleAsks {
  private entries = new Map<string, StaleAskEntry>();

  record(messageId: string, principal: StaleAskPrincipal, tier: StaleAskTier, now = Date.now()): void {
    this.prune(now);
    this.entries.delete(messageId);
    this.entries.set(messageId, { principalKey: principalKey(principal), tier, recordedAt: now });
    while (this.entries.size > MAX_STALE_ASKS) {
      const oldest = this.entries.keys().next().value;
      if (typeof oldest !== "string") break;
      this.entries.delete(oldest);
    }
  }

  classify(messageId: string, principal: StaleAskPrincipal, now = Date.now()): StaleAskTier | undefined {
    this.prune(now);
    const entry = this.entries.get(messageId);
    return entry?.principalKey === principalKey(principal) ? entry.tier : undefined;
  }

  delete(messageId: string): void {
    this.entries.delete(messageId);
  }

  clear(): void {
    this.entries.clear();
  }

  private prune(now: number): void {
    for (const [messageId, entry] of this.entries) {
      if (now - entry.recordedAt > STALE_ASK_RETENTION_MS) {
        this.entries.delete(messageId);
      }
    }
  }
}
