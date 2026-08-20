# Opaque dispatch dogfood residual

The hermetic dogfood uses the production broker and two independent socket clients. It can verify the broker/client wire
contract, durable fsync-before-claim custody, live claim, same-epoch reconciliation, receipt acknowledgement, privacy
isolation, and ordinary traffic survival without replacing the loaded Pi runtime.

The approved PRD also asks for two real Pi runtimes and watched exercises covering offline process restart, new broker
epoch, state refresh, callback-specific consumer failures, timeouts, capability loss, and every limit. Those
host/runtime behaviors cannot be represented truthfully by two `IntercomClient` instances alone. They remain an explicit
true-Pi cutover residual and require a separate human-run validation before loaded-runtime replacement. This repository
change does not claim that gate has passed.
