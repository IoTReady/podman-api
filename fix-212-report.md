# Fix report — IoTReady/podman-api#212

Branch `fix/212-stats-timeout`, worktree `/home/tej/projects/podman-api/.worktrees/fix-212-stats-budget`.
Single commit: `c984e12 fix(inventory,server): give container stats its own timeout (#212)`.

## Diagnosis check (done before writing code)

The issue's causal chain is real, verified in the code, not assumed:

- `internal/inventory/poller.go:112-161` (pre-fix): one `hctx, cancel := context.WithTimeout(ctx, p.Timeout)`
  per host, used for `RefreshHost` **and** then for `RefreshHostStats`.
- The `err == nil` guard means a slow-but-successful refresh still proceeds to stats — with whatever
  is left of `Timeout`.
- `internal/podman/real.go:235-245` `opCtxFor` does
  `ctx, cancel := context.WithTimeout(c, callTimeout); stop := context.AfterFunc(parent, cancel)`.
  The libpod call's context is *not* derived from the caller's — it is rooted at the cached
  connection context and merely **bridged** to the parent via `AfterFunc`. So an already-expired
  parent fires the cancel immediately and the error surfaces as `context canceled`, exactly as the
  production log shows, rather than `context deadline exceeded`. The diagnosis matches the code.

## What changed

### `internal/inventory/poller.go`

- New exported field `Poller.StatsTimeout time.Duration`.
- New `const defaultStatsTimeout = 5 * time.Second` and unexported `(*Poller).statsTimeout()`,
  which maps `<= 0` to the default. A zero field must never mean "unbounded": `tick` blocks the
  ticker, so an unbounded stats call would hang the poll loop — the failure mode a careless caller
  would otherwise get for free.
- `tick` now derives the stats context from the **tick** context:
  `sctx, scancel := context.WithTimeout(ctx, p.statsTimeout())`, used for `RefreshHostStats`,
  cancelled immediately after. Deriving from `ctx` (not `context.Background()`) keeps shutdown
  prompt: a SIGTERM still aborts an in-flight sample.
- Unchanged by design: a host whose refresh **failed** is still not sampled at all, and its cached
  samples are still dropped so the series go absent rather than stale.

### `server/server.go`

- New flag `-container-stats-timeout` (`time.Duration`, default `5s`), wired into
  `inventory.Poller{... StatsTimeout: *containerStatsTO}`.
- New `statsBudgetWarning(interval, timeout, statsTimeout, statsEnabled)` returning the warning line
  or `""`, logged as `WARNING: …` (same shape and call style as the existing
  `pollerDisabledMetricsWarning`). Fires when `timeout + statsTimeout >= interval`; names all three
  flags, prints the actual values and their sum, states the consequence (a slow host can spend a
  whole interval on one tick, stretching the poll cadence fleet-wide and inflating
  `podman_api_inventory_age_seconds`) and the remedy. **Warns, never fails.**
- The call sits directly in the `*inventoryInterval > 0` block, immediately before
  `invPoller.Start`, *not* nested inside the `if *containerStats` collector block — so it is
  evaluated on the poller-enabled start path independent of where the sampler's wiring lives.
- `container stats sampling enabled` log line now reports the per-host timeout, matching the
  volume-usage line's phrasing.

## Deliberate judgement call on requirement 3

The brief said "put the check where it runs regardless of whether the stats sampler is enabled, if
that reads naturally". The **call site** is unconditional within the poller block, but the helper
returns `""` when `-container-stats=false` (and when the poller is off). Reason: with the sampler
disabled `Poller.Stats` is nil, no stats call is ever made, and the second budget is unspendable —
warning about a sum that cannot occur is noise. This is the same philosophy the neighbouring
`pollerDisabledMetricsWarning` already documents at length ("the operator wants no metrics, so
there is nothing to warn about"). Both silent cases are pinned by table entries in the new test. If
the reviewer wants it to fire unconditionally, it is a one-line change plus two test rows.

## TDD / mutation evidence

Test written first, implementation second.

1. **Red (compile).** With the inverted test in place and no implementation,
   `make test` → `FAIL github.com/iotready/podman-api/internal/inventory [build failed]`
   (`StatsTimeout` / `defaultStatsTimeout` undefined), every other package `ok`.

2. **Green.** After the poller change:
   ```
   === RUN   TestPollerStatsGetsItsOwnBudgetIndependentOfRefresh
   --- PASS: TestPollerStatsGetsItsOwnBudgetIndependentOfRefresh (0.70s)
   === RUN   TestPollerStatsTimeoutDefaultsWhenUnset
   --- PASS: TestPollerStatsTimeoutDefaultsWhenUnset (0.00s)
   ```

3. **Mutation.** Reverted the single line to the old behaviour
   (`sctx, scancel := context.WithCancel(hctx)`) and re-ran:
   ```
   poller_test.go:266: stats context has only 298.966929ms left, no more than the 1s per-host
       refresh budget: it appears to still share hctx rather than getting its own StatsTimeout of 5s
   --- FAIL: TestPollerStatsGetsItsOwnBudgetIndependentOfRefresh (0.71s)
   poller_test.go:293: stats context has 59m59.999988259s left, more than the 5s default
   --- FAIL: TestPollerStatsTimeoutDefaultsWhenUnset (0.00s)
   ```
   Both new tests fail on the mutant — they pin the fix, not the scaffolding. The `298ms` figure is
   the bug reproduced in miniature: 700ms of refresh out of a 1s budget leaves the sampler ~300ms.
   Restored from a scratchpad copy and re-verified green.

### The inverted test

`TestPollerStatsSharesThePerHostBudgetWithRefresh` → `TestPollerStatsGetsItsOwnBudgetIndependentOfRefresh`.
Same fixtures (`fakeRefresher.delay`, `statsRec.firstRemaining`), inverted assertion. Parameters
chosen so the two designs cannot be confused by scheduling noise: `Timeout: 1s`, refresh delay
`700ms`, `StatsTimeout: 5s`. A shared budget can only ever produce `< 1s`; the test fails at
`left <= 1s`, so the gap between pass and fail is ~4.7s, not a few milliseconds. The upper bound
(`left > StatsTimeout`) additionally rules out "no deadline at all" passing by accident.

`TestPollerStatsTimeoutDefaultsWhenUnset` is new: a `Poller` with `Timeout: time.Hour` and
`StatsTimeout` unset must see ~5s, not an hour and not infinity. It calls `tick` directly (the same
style as `TestPollerPrunesStatsStateForRemovedHosts`) so it costs no wall-clock time.

### Server-side test

`server/stats_budget_warning_test.go`, modelled on `server/poller_disabled_warning_test.go` (same
table shape, same `want []string` substring style, same "empty means want \"\"" convention). Five
cases: shipped defaults silent; sum **equal** to interval warns (the boundary is already too tight);
sum over interval warns; sampler disabled silent; poller disabled silent.

## Comments and docs corrected for the design change

Stale comments asserting the opposite of the code were the specific risk; every one #210 wrote has
been rewritten, not deleted:

1. **`poller.go` `Poller.Stats` field** — new sibling doc comment on `StatsTimeout` explaining the
   zero-value default and pointing at `tick` for the rationale.
2. **`poller.go` `tick`, the long block comment** (was: "under the SAME hctx deadline… That shared
   budget is load-bearing… Sharing hctx keeps it at exactly Timeout… The cost is that a
   slow-but-successful refresh leaves the sampler little or no time"). Rewritten: states the new
   derivation, records *why* the shared budget was chosen, records what it actually cost in
   production with the #212 numbers, and says where the cadence invariant is now enforced
   (`server.statsBudgetWarning`).
3. **`poller.go` `tick`, the failed-refresh paragraph** (was: "hctx is most likely already expired,
   and calling anyway would only log a second failure"). The first half of that reason evaporated
   with the shared budget — with an independent budget the sample would get a *fresh* 5s to spend
   discovering the host is down. Reworded to the reason that survives.
4. **`poller.go` `logStatsTransition` godoc** (was: "Expect this pair of messages to alternate on a
   host whose inventory refresh chronically consumes most of Timeout… That is the shared budget
   working as designed… the fix is… a larger `-inventory-refresh-timeout`"). This one was actively
   misleading operator guidance: it pointed at the wrong flag. Rewritten to say the messages no
   longer track the refresh's leftover time and the lever is `-container-stats-timeout`.
5. **`README.md`, the sawtooth paragraph** — replaced, not dropped. The operator guidance is kept
   and corrected: the sample has its own `-container-stats-timeout`; the historical symptom is
   described as the *bug it was* (with the `context canceled` string an operator would grep, and the
   advice to check the build if they see it); repeated unavailable/available now points at
   `-container-stats-timeout`; the "metrics stay honest — absent, never stale" line is preserved.
6. **`README.md`, new second paragraph** — the invariant the operator now owns:
   `-inventory-refresh-timeout + -container-stats-timeout` under `-inventory-refresh-interval`,
   defaults satisfy it, exceeding it warns rather than refuses, consequence is a stretched cadence.
7. **`server/server.go` `statsBudgetWarning` godoc** — records the #210 trade-off and why the
   enforcement is a warning rather than a fatal, plus why it is silent when the sampler/poller is off.

The README line above the rewritten paragraph ("A host whose refresh fails is not also charged a
second timeout for stats; instead its cached samples are dropped") was checked and is **still
accurate** — a failed refresh skips sampling entirely — so it was left alone.

Deliberately **not** edited: `docs/superpowers/plans/2026-07-28-container-resource-metrics.md` and
`docs/superpowers/specs/2026-07-28-container-resource-metrics-design.md`. Those are dated
plan/spec records of what #209/#210 decided at the time; rewriting history there would be a lie of
a different kind. Neither is operator-facing documentation. The #212 issue itself is the record of
the reversal. Flag them if the repo's convention is to amend specs in place.

## Commands run, with real output

```
$ make test                       # after writing the inverted test, before the fix
FAIL	github.com/iotready/podman-api/internal/inventory [build failed]
make: *** [Makefile:14: test] Error 1
(all other packages ok)

$ go test -tags "…" ./internal/inventory/ -run TestPollerStats -v   # after the fix
--- PASS: TestPollerStatsErrorDoesNotAffectReachability (0.01s)
--- PASS: TestPollerStatsGetsItsOwnBudgetIndependentOfRefresh (0.70s)
--- PASS: TestPollerStatsTimeoutDefaultsWhenUnset (0.00s)
ok  	github.com/iotready/podman-api/internal/inventory	0.710s

$ (mutant: WithTimeout(ctx, statsTimeout) -> WithCancel(hctx))
--- FAIL: TestPollerStatsGetsItsOwnBudgetIndependentOfRefresh (0.71s)
--- FAIL: TestPollerStatsTimeoutDefaultsWhenUnset (0.00s)

$ go test -tags "…" ./server/ -run 'TestStatsBudgetWarning|TestPollerDisabled' -v
--- PASS: TestPollerDisabledMetricsWarning (0.00s)   [5 subtests]
--- PASS: TestStatsBudgetWarning (0.00s)             [5 subtests]
ok  	github.com/iotready/podman-api/server	0.010s

$ make vet
gofmt -l … (no output)
go vet -tags "…" ./...            # clean

$ make build
go build -tags "…" -o bin/podman-api ./cmd/podman-api

$ ./bin/podman-api --help | grep -A2 container-stats
  -container-stats
    	sample per-container … ; requires the inventory poller (default true)
  -container-stats-timeout duration
    	per-host timeout for one container stats sample, independent of
    	-inventory-refresh-timeout; keep it plus -inventory-refresh-timeout
    	under -inventory-refresh-interval (default 5s)

$ make test                       # full suite, final
ok  … all packages (internal/inventory 0.820s, server 3.523s); templates & cmd have no test files

$ go test -race -count=3 -tags "…" ./internal/inventory/ ./server/
ok  	github.com/iotready/podman-api/internal/inventory	3.480s
ok  	github.com/iotready/podman-api/server	12.779s

$ git diff --stat (staged into c984e12)
 README.md                         | 28 ++++++++-----
 internal/inventory/poller.go      | 82 ++++++++++++++++++++++++++-------------
 internal/inventory/poller_test.go | 58 +++++++++++++++++++++------
 server/server.go                  | 47 +++++++++++++++++++++-
```
(`server/stats_budget_warning_test.go` is new and shows in the commit, not in `git diff --stat` of
tracked files.)

## Self-review findings

- **`ctx` vs `Background()` for the stats context.** Deriving from the tick `ctx` was deliberate: a
  `Background()`-rooted context would survive SIGTERM and could hold shutdown for up to
  `StatsTimeout` per host, re-introducing the noisy-shutdown problem #209 fixed for the volume walk.
  Derived from `ctx`, shutdown still aborts the sample promptly, and the tick ctx has no deadline of
  its own, so nothing else can starve it.
- **`scancel()` is called explicitly, not deferred.** It is the last statement in that branch, so
  the timer is released immediately rather than at the end of the host goroutine.
  `go vet`'s `lostcancel` check is clean. A panic inside `RefreshHostStats` would leak the timer
  for at most `StatsTimeout` — but a panic in a per-host goroutine takes the process down anyway
  (the `recover` lives in `runTick`, on the ticker goroutine), so there is nothing to protect.
- **Ordering of the warning.** Logged before `invPoller.Start`, so on a misconfigured start it
  appears above the `inventory poller enabled …` line rather than buried after several ticks.
- **Boundary chosen as `>=`, not `>`.** At exactly `timeout + statsTimeout == interval` a single
  slow host consumes the entire interval, which is already the condition worth naming. Pinned by
  the `sum equal to the interval warns` case.
- **The zero-value trap is tested, not just asserted.** `TestPollerStatsTimeoutDefaultsWhenUnset`
  fails loudly on the mutant with `59m59.99s left` — i.e. it genuinely detects "inherited the
  caller's context" as well as "no deadline".
- Existing tests that exercise the stats path (`TestPollerStatsErrorDoesNotAffectReachability`,
  `TestPollerPrunesStatsStateForRemovedHosts`, the drop-on-failure tests) pass unmodified: none of
  them depended on which context the sampler received.

## Concerns

1. **Per-host worst case rises from `Timeout` to `Timeout + StatsTimeout`** (20s → 25s at the
   defaults, against a 30s interval). That is the trade the issue asks for and the margin is real,
   but it is thinner than before, and hosts are swept concurrently so the fleet is unaffected — only
   a single pathological host can push its own tick to 25s. The warning is the guardrail; nothing
   *prevents* an operator tuning past it, by design.
2. **The warning is startup-only.** Hosts are reloaded on SIGHUP but these three values are flags,
   so they cannot drift at runtime — a one-shot check is sufficient today. It would not survive a
   future move of these knobs into reloadable config.
3. **Not validated against a live host.** The fix is verified by unit test and mutation only. The
   production confirmation is a deploy to engine-infra and checking that
   `podman_api_container_cpu_seconds_total{host="engine-1"}` appears with ~115 series, and that
   `inventory: host engine-1 container stats available again` shows up once in the log. Worth doing
   before tagging, since the whole issue is about behaviour only load reveals.
4. **`5s` is a default, not a measurement-derived bound.** 92ms on the worst host means ~50x
   headroom, but a host that grows well past 115 containers has no automatic safety margin — it
   would fail as a clean `deadline exceeded` on the stats call alone, visibly, and the flag exists
   to raise it. That is the intended failure mode rather than an oversight.
5. **Dated spec/plan docs still describe the shared budget** (see above). Intentional; raise it if
   this repo prefers specs amended in place.

---

# Review round 2 — findings addressed

Fixed in a second commit on the same branch. `make vet` clean, `make test` fully green,
`-race -count=2` clean on `internal/inventory` + `server`.

## 1. Stale comment contradicting the test it precedes (important) — fixed

Correct and my fault. `internal/inventory/poller_test.go:234-239` still carried the original #210
paragraph ("Refresh and stats share ONE per-host budget: the sampler runs under the same hctx…
two independent Timeouts would make the bound 2*Timeout (40s at the 30s/20s defaults)…") asserting
the old design as present fact, immediately above the new paragraph asserting the opposite. Deleted;
the new paragraph stands alone as the complete rationale.

**Why it slipped, concretely.** My audit grep was
`grep -rn "shared budget|sawtooth|hctx|…" --include=*.go --include=*.md . | grep -v _test.go`.
I excluded test files on the assumption that inverting the test body covered its comment. The one
stale comment in the repo was in the one file I filtered out — and my report then asserted the whole
class was clear. The completeness claim was the real error, not the miss.

**Re-audit, enumerated.** Re-ran without the `_test.go` filter and over a wider vocabulary
(`share|shared|sawtooth|2\*Timeout|2 × timeout|one per-host budget|same hctx|second timeout|leftover`)
across all `*.go` and `*.md`. Every surviving hit in the touched area, and its verdict:

| Location | Text | Verdict |
|---|---|---|
| `internal/inventory/poller.go:56` | "why this is a separate budget rather than a share of Timeout (#212)" | correct, describes current design |
| `internal/inventory/poller.go:120` | "It **used to** share hctx, to keep the per-host bound at…" | correct, explicitly past tense |
| `internal/inventory/poller.go:129` | "Not a sawtooth: permanent starvation" | correct, describes the bug |
| `internal/inventory/poller.go:244` (`logStatsTransition`) | "no longer track the refresh's leftover time — **until #212 they did**" | correct, past tense |
| `internal/inventory/poller_test.go:239,256,258,261` | "a **shared budget would** leave ~300ms", "it appears to still share hctx" | correct — counterfactual describing the failure the assertion detects |
| `server/server.go:557` | "**Until #212** the stats sample shared the refresh's hctx" | correct, past tense |
| `server/stats_budget_warning_test.go:40` | "the tuning the **old** shared budget made impossible" | correct, past tense |
| `README.md:112` | "A host whose refresh fails is not also charged a second timeout for stats" | still true — a failed refresh skips sampling entirely |
| `README.md:120` | "It **briefly shared** the refresh's budget" | correct, past tense |

The only remaining present-tense descriptions of the shared budget are in
`docs/superpowers/plans/` and `docs/superpowers/specs/`, deliberately untouched as dated records
(flagged in the original report, unchanged).

## 2. `statsBudgetWarning` ignored the zero-defaulting (important) — fixed

Correct, and the reviewer's worked example is exact. The fix, as suggested, is a shared helper:

- `internal/inventory/poller.go`: extracted **exported** `EffectiveStatsTimeout(d time.Duration) time.Duration`
  (`<= 0 → defaultStatsTimeout`). `(*Poller).statsTimeout()` is now a one-line call to it. Exported
  deliberately, with a doc comment saying why: the normalisation is not an internal detail once
  anything outside the package reasons about the per-host budget.
- `server/server.go`: `statsBudgetWarning` computes `eff := inventory.EffectiveStatsTimeout(statsTimeout)`
  and uses it for **both** the comparison and the printed value, so
  `-container-stats-timeout=0 -inventory-refresh-timeout=28s -inventory-refresh-interval=30s` now
  warns and reports `-container-stats-timeout (5s) = 33s`, rather than silently computing 28s while
  the poller spends 33s.

### On `timeout` — deliberately NOT normalised, saying so rather than widening scope

`Poller.Timeout` is passed to `context.WithTimeout` verbatim, so `-inventory-refresh-timeout=0`
means an already-expired context, not a default. Adding a default there would change refresh
behaviour, which is outside #212 and would be a silent semantic change to a flag this PR otherwise
does not touch. It is also a *loud* pre-existing edge — every refresh on every host fails
immediately — unlike the stats one, which was silent. The `statsBudgetWarning` godoc now states
this asymmetry explicitly (raw `timeout` is the honest sum precisely because the poller uses it
raw), so the next reader does not read the inconsistency as an oversight. Flagging rather than
fixing; happy to take it as a separate issue.

### New test cases (3, not 2)

- `zero stats timeout is normalised to the default` — the reviewer's exact scenario; asserts the
  message contains `-container-stats-timeout (5s)` and `33s`, i.e. the effective value, not `0`.
- `negative stats timeout is normalised to the default` — `-1s`, which `flag.Duration` accepts.
- `zero stats timeout still silent when the default fits` — added unprompted, to pin that the
  normalisation does not manufacture a warning when `10s + 5s < 30s`. Without it, a helper that
  always warned on `statsTO == 0` would pass the other two.

**Mutation-checked.** Reverting `eff := inventory.EffectiveStatsTimeout(statsTimeout)` to
`eff := statsTimeout`:

```
--- FAIL: TestStatsBudgetWarning/zero_stats_timeout_is_normalised_to_the_default
        warning "" missing "-container-stats-timeout (5s)"
        warning "" missing "33s"
--- FAIL: TestStatsBudgetWarning/negative_stats_timeout_is_normalised_to_the_default
        warning "" missing "-container-stats-timeout (5s)"
```
The empty-string `got` is the bug itself: silence where the poller will overrun. Restored, green.

## 3. Help string (minor) — fixed

```
-container-stats-timeout duration
    per-host timeout for one container stats sample, independent of
    -inventory-refresh-timeout; 0 or negative means the 5s default, never
    unbounded or instant; keep it plus -inventory-refresh-timeout under
    -inventory-refresh-interval (default 5s)
```
Verified from `./bin/podman-api --help`.

## Round-2 commands

```
$ go test -tags "…" ./server/ -run TestStatsBudgetWarning -v
--- PASS: TestStatsBudgetWarning (0.00s)   [8 subtests, all PASS]

$ (mutant: EffectiveStatsTimeout(statsTimeout) -> statsTimeout)
--- FAIL: TestStatsBudgetWarning  [2 new subtests fail]

$ make vet     # gofmt clean, go vet clean
$ make test    # all packages ok (internal/inventory 0.820s, server 3.398s)
$ go test -race -count=2 -tags "…" ./internal/inventory/ ./server/
ok  	github.com/iotready/podman-api/internal/inventory	2.655s
ok  	github.com/iotready/podman-api/server	8.862s
$ make build && ./bin/podman-api --help | grep -A2 container-stats-timeout   # as above
```

## Calibration note

Taken. The lesson is not "grep harder" — it is that a claim of the form "every X was checked" has to
ship the enumeration that backs it, so the filter I applied is visible and falsifiable. The re-audit
above is written that way: every hit, its text, and its verdict, including the ones I judged correct.
