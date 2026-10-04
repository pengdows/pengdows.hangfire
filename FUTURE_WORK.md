# Future work

Open items for pengdows.hangfire, recorded 2026-10-03 from the review of a4a3681 (plus b4851fc and the
main merge). Verified still present at a9de83b.

## Before tagging 2.0.6

### CLOCK-001 (high): a fast node clock is never corrected

`StorageClock`'s constructor seeds the anchor with node time (`_anchorDb = DateTime.UtcNow`). The first
refresh then clamps the database reading against that seed (`if (midpoint < oldAtEnd) midpoint = oldAtEnd`),
so when the node runs ahead of the database the clock keeps the node's time, and every later refresh clamps
again. A fast clock is the dangerous direction: it takes live locks early and sees live claims as stale.

The `finally` also clears `_needsInitialRefresh` when the first database read fails, so a node that starts
while the database is unreachable runs on node time until the next refresh, and the clamp keeps it.

Fix: clamp only after the first real database anchor, and clear `_needsInitialRefresh` only on success.
Test: the database 3 minutes behind the node; the first reading is within 1 second of database time. The
existing tests put the database ahead of the node, where the clamp never fires.

### CLOCK-002 (low): document forward-only correction

After the first anchor, Stopwatch drift ahead of the database is never corrected, only held until the
database catches up (parts per million: seconds per day at worst). Document the clock as "monotonic,
corrects forward only".

### CLOCK-003 (low): gateways fall back to node time silently

`JobGateway`, `ServerGateway`, `HashGateway`, `JobQueueGateway`, `SetGateway`, `AggregatedCounterGateway`
and `ListGateway` take `utcNow ?? (() => DateTime.UtcNow)`. A gateway constructed without the storage clock
silently uses node time. Make the clock a required constructor argument; tests pass an explicit fake.

### CLOCK-004 (low): lock expiry read with `Convert.ToDateTime`

`DistributedLockGateway.GetOwnedVersionAsync` (line ~310) reads `expires_at` with `Convert.ToDateTime`, then
`SpecifyKind(Utc)`. A provider returning an ISO string ending in `Z` gets `Kind=Local`, shifted to the
machine's zone, which `SpecifyKind` then mislabels. Normalize by kind as `GetUtcDateTime` does
(`Local` goes through `ToUniversalTime()`).

## 3.0 (deferred): multi-tenant storage

Notes from the 2026-10-03 design discussion; not started.

- Shape: one composite `JobStorage` routing to each tenant's database through `ITenantContextRegistry`,
  plus a small control-plane database.
- Job identity: the tenant encoded in the job ID (`acme:12345`), validated against the registry. First check
  that the dashboard and extensions never parse job IDs as integers.
- Fetch: measure plain fan-out polling with backoff for idle tenants first; build a work-hint table in the
  control plane (with a slow full sweep as the backstop) only if it's needed (likely above ~50 tenants).
- Fairness: round-robin across tenants with work, with a per-tenant concurrency cap.
- One `StorageClock` per tenant context, created with the context.
- Contexts are disposed only at application shutdown (pengdows.crud has no tenant rotation), so plans must
  not assume rotation. A fetched job still holds a context lease for its lifetime.
- Background processes (watchdog, expiration, counter aggregation) iterate tenants in batches; no process per
  tenant.
- Locks: per-tenant locks in the tenant database, global locks in the control plane, scope explicit in the
  resource name.
- Dashboard: one tenant per view; a cross-tenant view only as an opt-in, admin-only, audited feature.
- Recurring jobs: stored per tenant; a control-plane job that fans out for "every tenant" schedules.
- Sequencing: ship 2.0.6 first and let it collect users before 3.0 changes the job-ID format.
