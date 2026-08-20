# Reconciliation deviations

## State refresh result

The approved reconciliation PRD shows `refreshState(): Promise<ExtensionStateSnapshot>`. The shipped type and
implementation return `Promise<ExtensionStateRefreshResult>` so unsupported brokers and connection loss are typed
without an unsupported wire write. README and `extension-api.ts` are the public API source of truth. The approved PRD
remains unchanged as planning history.

## True-Pi dogfood

The hermetic production-broker dogfood uses two independent socket clients, not two loaded Pi runtimes. The host
callback, process restart, and cutover-only residual is listed in `opaque-dispatch-residual.md`; it must be validated
separately before runtime replacement.
