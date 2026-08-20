export type StaleAskTier = "cancelled" | "superseded" | "timed_out";

interface StaleAskEntry {
  principal: string;
  tier: StaleAskTier;
  recordedAt: number;
}

export const MAX_STALE_ASKS = 256;
export const STALE_ASK_RETENTION_MS = 60 * 60 * 1000;

export class StaleAsks {
  private entries = new Map<string, StaleAskEntry>();

  record(messageId: string, principal: string, tier: StaleAskTier, now = Date.now()): void {
    this.prune(now);
    this.entries.delete(messageId);
    this.entries.set(messageId, { principal, tier, recordedAt: now });
    while (this.entries.size > MAX_STALE_ASKS) {
      const oldest = this.entries.keys().next().value;
      if (typeof oldest !== "string") break;
      this.entries.delete(oldest);
    }
  }

  classify(messageId: string, principal: string, now = Date.now()): StaleAskTier | undefined {
    this.prune(now);
    const entry = this.entries.get(messageId);
    return entry?.principal === principal ? entry.tier : undefined;
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
