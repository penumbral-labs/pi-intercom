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

The nine repeated findings map to these adjudications:

1. Cultivated authorship/multiple commits in `broker/broker.ts`: false positive caused by the required certified
   multi-commit port.
2. Throws-on-error in `broker/client.ts`: real and fixed by the unified typed opaque result channel.
3. Unique function bodies for `principalKey` and `targetKey`: false positive; these are semantically distinct helpers
   from the certified opaque baseline.
4. Cultivated authorship in `broker/broker.ts`: duplicate of finding 1 and the same false positive.
5. Unique function bodies for session retirement: real; duplicated close and unregister cleanup allowed replacement to
   skip opaque retirement. Close, unregister, and replacement now use `retireSession`, with replacement retiring the old
   endpoint before map replacement.
6. Line comments in `broker/ask-edges.ts`: false positive; the comments were ported byte-for-byte and JSDoc is an
   established repository style.
7. Unique function bodies for `findSessions` and `findDisconnectedSessions`: false positive; this family is unchanged
   upstream v0.11 code.
8. Line comments in `broker/ask-edges.ts`: duplicate of finding 6 and the same false positive.
9. Line comments in `broker/ask-edges.ts`: duplicate of finding 6 and the same false positive.

The final repo-consistency diff check reported all 38 selected files `ok` with zero findings.

A later fresh baseline check at `5d52d09` reported duplicate function bodies in `broker/broker.ts` and
`broker/client.ts`, plus block-comment style in `config.ts`. The normalized baseline itself classified all six files it
considered for semantic duplication as duplicates (`0 of 6` unique), so it did not establish a dominant unique-body
pattern. The `3aa605b..5d52d09` diff adds no function body to `broker/broker.ts` and only strengthens a predicate in the
existing `IntercomClient.handleBrokerMessage` body. It also leaves the pre-existing `MAX_ASK_TIMEOUT_MS` JSDoc in
`config.ts` unchanged and adds no block comment. These unlocated, file-level findings therefore do not identify drift
introduced by that diff.

## True-Pi dogfood

The hermetic production-broker dogfood uses two independent socket clients, not two loaded Pi runtimes. The host
callback, process restart, and cutover-only residual is listed in `opaque-dispatch-residual.md`; it must be validated
separately before runtime replacement.
