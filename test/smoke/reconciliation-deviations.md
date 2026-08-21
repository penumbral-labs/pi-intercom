# Reconciliation deviations

## State refresh result

The approved reconciliation PRD shows `refreshState(): Promise<ExtensionStateSnapshot>`. The shipped type and
implementation return `Promise<ExtensionStateRefreshResult>` so unsupported brokers and connection loss are typed
without an unsupported wire write. README and `extension-api.ts` are the public API source of truth. The approved PRD
remains unchanged as planning history.

## Certified lineage

The reconciliation was verified against the certified opaque baseline when its history was available. That ancestry
check belongs in release/review validation rather than `npm test`: exported source archives and shallow checkouts may
not contain the certified commit graph. A full checkout can still run `git merge-base --is-ancestor 0685e199 HEAD` for
the reconciliation baseline and `git merge-base --is-ancestor 763770b HEAD` for the reviewed transport-fix lineage.

## Repo-consistency review

The review's error-channel finding was real: opaque sends mixed thrown errors, typed results, and list failures. Opaque
send outcomes now use one typed result channel. The shared-retirement finding was also real: close and unregister had
duplicated cleanup while in-place replacement skipped opaque retirement. All three paths now use `retireSession`, with
replacement retiring the old endpoint before map replacement.

The remaining reported findings were false positives against ported or upstream code: multi-commit authorship reflected
the required integration merge; the matching key-helper bodies came from the certified opaque baseline; comment style
matched existing repository usage; and session finder duplication was unchanged upstream v0.11 code. The final
repo-consistency diff check reported all 38 selected files `ok` with zero findings.

## True-Pi dogfood

The hermetic production-broker dogfood uses two independent socket clients, not two loaded Pi runtimes. The host
callback, process restart, and cutover-only residual is listed in `opaque-dispatch-residual.md`; it must be validated
separately before runtime replacement.
