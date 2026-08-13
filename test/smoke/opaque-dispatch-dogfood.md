# Opaque dispatch dogfood

Tested package source: local `pi-intercom` checkout at commit recorded by the run operator after chunks 7–10.

Run without changing the loaded runtime:

```bash
PI_CODING_AGENT_DIR="$(mktemp -d)" npx tsx test/smoke/opaque-dispatch-dogfood.ts
```

The smoke consumer receives a private offer, records the broker epoch, message ID, reservation ID, and payload to a
scratch file, calls `fsync`, and only then claims. It verifies the claim result, same-epoch reconciliation, claimed
receipt, and durable sentinel before deleting the scratch directory.

The broader automated suite covers exact live/offline targets, queued restart delivery, cancellation, supersession,
refusal, reservation and claim expiry, capability invalidation, state refresh, receipt replay/ack, bounds, malformed
frames, broker epoch mismatch, and privacy fencing. Runtime replacement, publication, push, and loaded v0.6 mutation are
not part of this run; cutover remains a separate human action.
