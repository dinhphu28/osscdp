# 15 — Future Enhancements

This document specifies admin-API capabilities that are **not yet implemented** but were identified
as gaps during hands-on use. Each enhancement lists what exists **today**, what to **add** (with
file/line anchors into the current code), and an **interim workaround** using direct database access
until the endpoint is built.

Nothing here is wired into the running system yet — treat it as an implementation backlog.

---

## Background: canonical IDs and identity resolution

A **canonical ID** (`customer_<uuidv7>`) is the stable identifier of a *resolved person*. The
identity graph groups every identifier a person uses (email, phone, `user_id`, `anonymous_id`) into
one **identity cluster**; each cluster is minted exactly one canonical ID at creation
(`internal/identity/repo.go:80-93`), and one `customer_profile` row is keyed to that canonical ID.

A single person can end up with **multiple** canonical IDs for two reasons:

1. **Identifiers sent inside `traits` instead of top-level.** Identity linking only reads top-level
   `email`/`phone` fields (`internal/events/normalize.go:58-59` → `internal/identity/model.go:42`).
   Email/phone placed inside `traits` become profile *display* attributes but never identity nodes,
   so separate `/identify` calls never link. **Fix (usage):** always send `email`/`phone` as
   top-level fields, siblings of `user_id` — not inside `traits`.
2. **Orphan profiles after a merge.** When two clusters merge, the *loser* cluster's already-created
   `customer_profile` row is not deleted or repointed (`identity_cluster_id` is set only at
   creation — `internal/profile/service.go:92`). New traffic uses the survivor's canonical ID, but
   the old row lingers with a now-dead canonical ID and a `merged`, zero-node cluster. See
   [Enhancement D](#enhancement-d--reparent-profiles-on-cluster-merge).

Diagnostic — detect residual same-person splits (should return 0 rows):

```sh
docker exec -i deploy-postgres-1 psql -U cdp -d cdp -c "
SELECT p.traits_json->>'email' AS email, count(*) AS active_clusters
FROM customer_profile p JOIN identity_cluster ic ON ic.id = p.identity_cluster_id
WHERE p.tenant_id='<tenant>' AND ic.status='active'
GROUP BY 1 HAVING count(*) > 1;"
```

---

## Enhancement A — Unsubscribe (disable) a subscription ✅ Implemented

Detach one destination from one segment without deleting the destination.

**Status:** Shipped. `DELETE /admin/v1/tenants/{tenantID}/destinations/{destinationID}/subscriptions/{subscriptionID}`
(requires `destination:write`) soft-disables the subscription and returns `200` with the updated row
(`status=disabled`), or `404` if no matching subscription exists under that tenant + destination.

What was added:

- **Repo** — `DisableSubscription(ctx, tenantID, destinationID, subscriptionID uuid.UUID) (Subscription, error)`
  in `internal/activation/repo.go`, using `UPDATE … RETURNING`:
  ```sql
  UPDATE destination_subscription SET status='disabled', updated_at=now()
  WHERE tenant_id=$1 AND destination_id=$2 AND id=$3
  RETURNING id, tenant_id, destination_id, trigger_type, segment_id, event_name, status
  ```
  Returns `ErrNotFound` on `pgx.ErrNoRows`. Scoped by `destination_id` so the nested route's
  `{destinationID}` must match. Idempotent — disabling an already-disabled subscription returns it
  unchanged. Reuses the `StatusDisabled` constant (`internal/activation/model.go`).
- **Handler** — `DisableSubscription` in `internal/activation/handler.go`, mirroring
  `CreateSubscription` for param parsing and `UpdateDestination` for the `ErrNotFound` → `404` branch.
- **Route** — registered in `cmd/cdp-api/main.go` under `rbac.PermDestinationWrite`.
- **Test** — `internal/activation/repo_integration_test.go` (testcontainers): seed → disable →
  assert `ActiveSubscriptionsForSegment` no longer returns it, the row is retained (soft-disable),
  disabling twice is idempotent, and unknown ids / mismatched destination return `ErrNotFound`.
- **Spec** — documented in `api/openapi.yaml`.

**Why soft-disable, not a row delete:** `activation_task.subscription_id` is a non-cascading foreign
key (`migrations/00008_activation.sql:36`), so a hard `DELETE` would be blocked by existing task rows.
Setting `status='disabled'` is enough — the sender only dispatches `status='active'` subscriptions
(`ActiveSubscriptionsForSegment`), and picks up the change on its next tick with no restart.

**Interim workaround (historical — endpoint now exists):**

```sh
docker exec -i deploy-postgres-1 psql -U cdp -d cdp -c "
UPDATE destination_subscription SET status='disabled', updated_at=now()
WHERE tenant_id='<tenant>' AND id='<subscription_id>';"
```

---

## Enhancement B — List destinations connected to a segment ✅ Implemented

See every destination wired to a segment, including disabled subscriptions.

**Status:** Shipped. `GET /admin/v1/tenants/{tenantID}/segments/{segmentID}/destinations`
(requires `destination:read`) returns `{"destinations": [...]}` — one entry per subscription joined
to its destination, **all statuses** (so disabled subscriptions/destinations are visible, unlike the
sender's active-only `ActiveSubscriptionsForSegment`). A segment with no subscriptions returns an
empty array.

What was added:

- **Repo** — `SubscriptionsBySegment(ctx, tenantID, segmentID uuid.UUID) ([]SegmentDestination, error)`
  in `internal/activation/repo.go`:
  ```sql
  SELECT s.id, s.status, d.id, d.name, d.type, d.status
  FROM destination_subscription s
  JOIN destination d ON d.id = s.destination_id
  WHERE s.tenant_id=$1 AND s.segment_id=$2
  ORDER BY d.name
  ```
  Returns the `SegmentDestination{SubscriptionID, SubscriptionStatus, DestinationID, Name, Type,
  DestinationStatus}` struct (`internal/activation/model.go`). Backed by the existing
  `idx_destination_subscription_segment` index.
- **Handler** — `ListSegmentDestinations` in `internal/activation/handler.go` (wraps the rows in a
  `{"destinations": [...]}` envelope, mirroring `Deliveries`).
- **Route** — registered in `cmd/cdp-api/main.go` alongside the segment routes, under
  `rbac.PermDestinationRead`.
- **Test** — `internal/activation/repo_integration_test.go` (testcontainers): two destinations on one
  segment are both listed; a disabled subscription still appears with `status=disabled`; an unknown
  segment yields an empty slice.
- **Spec** — documented in `api/openapi.yaml`.

**Interim workaround (historical — endpoint now exists):**

```sh
docker exec -i deploy-postgres-1 psql -U cdp -d cdp -c "
SELECT d.name, d.type, s.id AS subscription_id, s.status AS sub_status, d.status AS dest_status
FROM destination_subscription s JOIN destination d ON d.id = s.destination_id
WHERE s.tenant_id='<tenant>' AND s.segment_id='<segment_id>'
ORDER BY d.name;"
```

---

## Enhancement C — View all of a person's identifiers · Tier 1 ✅ Implemented

Show every phone/email/`user_id` a person has ever used, not just the latest trait value.

**Status:** Tier 1 shipped. `GET /admin/v1/tenants/{tenantID}/profiles/{canonicalUserID}/identifiers`
(requires `profile:read`) returns `{"canonical_user_id", "total", "by_namespace": {...}}` — a
per-namespace **count** of every identity node linked to the person (e.g. `{"email": 2, "phone": 3}`),
which the last-write-wins profile traits cannot show. Counts only — no values or hashes exposed. `404`
if the profile does not exist.

What was added (Tier 1):

- **Service** — `Service.Identifiers(ctx, tenantID, canonicalUserID) (IdentifierInventory, error)` in
  `internal/governance/governance.go`, resolving the profile via `GetByCanonical` (→ `ErrNotFound`)
  then aggregating the same cluster-node join used by `Export`, `GROUP BY namespace`.
- **Handler** — `Identifiers` in `internal/governance/handler.go`, mirroring `Export`.
- **Route** — registered in `cmd/cdp-api/main.go` next to the export route, under `rbac.PermProfileRead`.
- **Test** — `internal/governance/governance_integration_test.go` (testcontainers): seed a person with
  2 emails / 3 phones / 1 user_id → assert `total=6` and the per-namespace map; unknown canonical id
  → `ErrNotFound`.
- **Spec** — documented in `api/openapi.yaml`.

**Tier 2 (real plaintext values) is still pending** — see below. Tier 1 answers "how many, which
namespaces"; it does not reveal the identifier strings.

**Today (for Tier 2):** The profile endpoints (`cmd/cdp-api/main.go:133-134`) return only last-write-wins traits
(`customer_profile.traits_json` — one `phone`, one `email`) plus computed attributes. The `List`
endpoint even searches by `traits_json->>key`, so an *older* phone won't match. The only place all
identity nodes surface is the governance **Export** endpoint
(`GET .../profiles/{canonicalUserID}/export`, `cmd/cdp-api/main.go:138`), which lists every node in
the cluster (query at `internal/governance/governance.go:62-66`) — **but each node is a SHA-256
`value_hash`, not the original string** (`internal/governance/governance.go:30-34`).

Constraints to be aware of:

- The hash is one-way: `sha256(tenant_id | namespace | value)` (`internal/identity/hash.go:14-22`).
- The `identity_node.value_encrypted` column exists (`migrations/00005_identity.sql:12`) but is
  **never written** — `upsertNode` inserts only `id, tenant_id, namespace, value_hash`
  (`internal/identity/repo.go:32-44`). It is effectively dead today.
- The only place original identifier strings survive is `raw_event.payload_json`
  (`migrations/00003_raw_event.sql`), exposed via `GET .../events` — but keyed by event, not joined
  to a canonical ID.

**Two tiers (Tier 1 done, Tier 2 pending):**

- **Tier 1 — count inventory (small). ✅ Implemented** as `GET .../profiles/{canonicalUserID}/identifiers`
  (see status above). Returns every node grouped by namespace with counts, reusing the Export
  cluster-node join, behind `PermProfileRead`. Answers "how many, which namespaces" but not the
  plaintext values.
- **Tier 2 — real values (larger). Pending.** Start populating `identity_node.value_encrypted` in `upsertNode`
  (`internal/identity/repo.go:32-44`) using the existing `crypto.Cipher` (already used for encrypting
  destination secrets — see `internal/activation/handler.go`), then decrypt on read behind a
  PII-scoped permission with masking (mirror `maskTraits` in `internal/profile`).
  **Caveat:** this only covers identifiers ingested *after* the change; historical nodes remain
  hash-only unless backfilled by re-hashing plaintext recovered from `raw_event.payload_json`.

**Interim workaround (no code):** Call the Export endpoint for the hashed node inventory, or inspect
`raw_event.payload_json` via `GET .../events` for plaintext values (not joined to the canonical ID):

```sh
# hashed node inventory for a person
curl -s -X GET 'http://localhost:18080/admin/v1/tenants/<tenant>/profiles/<canonicalUserID>/export' \
  -H 'Authorization: Bearer <admin-token>'
```

---

## Enhancement D — Reparent profiles on cluster merge

Removes the root cause of orphan profiles described in the background section.

**Today:** When clusters merge, only identity-graph rows move to the survivor
(`pickSurvivor`/`mergeClusters`, `internal/identity/repo.go`); the loser's `customer_profile` row is
left untouched (`internal/profile/service.go:92` sets `identity_cluster_id` only at creation and never
updates it). Result: a stale profile under a dead canonical ID.

**Add (design sketch — larger, cross-package):** on merge, either (a) repoint the loser profile's
`identity_cluster_id`/`canonical_user_id` to the survivor and merge its traits via the existing
`MergeTraits` (`internal/profile/merge.go`), or (b) mark the loser profile `merged` and redirect
lookups. Requires coordination between `internal/identity` (emit which canonical IDs merged) and
`internal/profile` (apply the reparent). Include idempotency and an audit record.

**Interim workaround (no code):** delete orphan profile rows (they point to `merged`, zero-node
clusters and receive no live traffic):

```sh
docker exec -i deploy-postgres-1 psql -U cdp -d cdp -c "
DELETE FROM customer_profile p
USING identity_cluster ic
WHERE p.identity_cluster_id = ic.id
  AND ic.status='merged'
  AND p.tenant_id='<tenant>';"
```

---

## Summary

| # | Capability | Exists today? | Size | Interim workaround |
|---|------------|---------------|------|--------------------|
| A | Unsubscribe / disable a subscription | ✅ Implemented (`DELETE …/subscriptions/{subscriptionID}`) | Small | `UPDATE destination_subscription SET status='disabled'` |
| B | List destinations by segment | ✅ Implemented (`GET …/segments/{segmentID}/destinations`) | Small | JOIN query on `destination_subscription` + `destination` |
| C | View all of a person's identifiers | ✅ Tier 1 (`GET …/profiles/{id}/identifiers`, counts); Tier 2 (plaintext) pending | Tier 1 small / Tier 2 larger | Export endpoint (hashed) or `raw_event.payload_json` |
| D | Reparent profiles on cluster merge | No | Larger | Delete orphan rows under `merged` clusters |
