# Opaque dispatch dogfood

Tested package source: local `pi-intercom` checkout at the current `HEAD`; record `git rev-parse HEAD` with the run
output.

Run without changing the loaded runtime:

```bash
DOGFOOD_DIR="$(mktemp -d)"
PI_CODING_AGENT_DIR="$DOGFOOD_DIR" npx tsx test/smoke/opaque-dispatch-dogfood.ts
git rev-parse HEAD
rm -rf "$DOGFOOD_DIR"
```

The script uses `spawnBrokerIfNeeded` to launch the production broker in the scratch `PI_CODING_AGENT_DIR`, then
connects separate sender and receiver `IntercomClient` instances over the real socket. The receiver gets a private
offer, records the broker epoch, message ID, reservation ID, and payload to a scratch file, calls `fsync`, and only then
claims. It verifies the claim result, same-epoch reconciliation, durable sentinel, absence of the sentinel from
ordinary-message and generic broker callbacks, and ordinary socket usability after the opaque flow.

This is an equivalent two-client production-broker dogfood, not a replacement of the currently loaded Pi extension.
Runtime replacement, publication, push, and loaded-runtime mutation are not part of this run; cutover remains a separate
human action.
