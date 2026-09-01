export interface RetainedSession {
  disconnectedAt: number;
}

export function pruneDisconnectedSessions<T extends RetainedSession>(
  sessions: Map<string, T>,
  now: number,
  retentionMs: number,
): void {
  for (const [sessionId, session] of sessions) {
    if (now - session.disconnectedAt > retentionMs) sessions.delete(sessionId);
  }
}
