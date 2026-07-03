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

## Enhancement A — Unsubscribe (disable) a subscription

Detach one destination from one segment without deleting the destination.

**Today:** No endpoint. Five activation routes exist (`cmd/cdp-api/main.go:146-150`); none
delete or disable a subscription. `CreateSubscription` hardcodes `status='active'`
(`internal/activation/repo.go:90-93`) and no update/delete method exists. The schema already supports
a soft-disable: `destination_subscription.status` (`migrations/00008_activation.sql:17-30`), and the
sender only dispatches active subscriptions (`ActiveSubscriptionsForSegment`,
`internal/activation/repo.go:113`, filters `s.status='active' AND d.status='active'`).

Use **soft-disable**, not a row delete: `activation_task.subscription_id` has a foreign key
(`migrations/00008_activation.sql:36`), so a hard `DELETE` is blocked by existing task rows.

**Add:**

- **Repo** — `DeactivateSubscription(ctx, tenantID, subscriptionID uuid.UUID) error` in
  `internal/activation/repo.go`:
  ```sql
  UPDATE destination_subscription SET status='disabled', updated_at=now()
  WHERE tenant_id=$1 AND id=$2
  ```
  Return `ErrNotFound` when `RowsAffected()==0`. Reuse the `StatusDisabled` constant
  (`internal/activation/model.go:19-23`).
- **Handler** — `DisableSubscription` in `internal/activation/handler.go`, mirroring
  `CreateSubscription`: use `parseTenant` and read `{subscriptionID}` via `chi.URLParam`. Return
  `204 No Content` on success, `404` on `ErrNotFound`.
- **Route** — in the activation block of `cmd/cdp-api/main.go` (after line 149):
  ```go
  admin.With(auth.Require(rbac.PermDestinationWrite)).
      Delete("/admin/v1/tenants/{tenantID}/destinations/{destinationID}/subscriptions/{subscriptionID}",
          activationHandler.DisableSubscription)
  ```
- **Test** — in `internal/activation` (see `unit_test.go` for the pattern): create a subscription,
  disable it, assert `ActiveSubscriptionsForSegment` no longer returns it.

**Interim workaround (no code):**

```sh
docker exec -i deploy-postgres-1 psql -U cdp -d cdp -c "
UPDATE destination_subscription SET status='disabled', updated_at=now()
WHERE tenant_id='<tenant>' AND id='<subscription_id>';"
```

The sender picks up the change on its next tick — no restart needed.

---

## Enhancement B — List destinations connected to a segment

See every destination wired to a segment, including disabled subscriptions.

**Today:** No endpoint. The closest code is `ActiveSubscriptionsForSegment`
(`internal/activation/repo.go:107`), but it is internal-only (not wired to any route), returns
*active-only* rows, and carries no destination detail (name/type/status). The index
`idx_destination_subscription_segment` on `(tenant_id, segment_id, status)`
(`migrations/00008_activation.sql`) already fits the query.

**Add:**

- **Repo** — `SubscriptionsBySegment(ctx, tenantID, segmentID uuid.UUID)` in
  `internal/activation/repo.go`, returning one row per subscription joined to its destination,
  **all statuses** (so admins can see disabled ones):
  ```sql
  SELECT s.id, s.status, d.id, d.name, d.type, d.status
  FROM destination_subscription s
  JOIN destination d ON d.id = s.destination_id
  WHERE s.tenant_id=$1 AND s.segment_id=$2
  ORDER BY d.name
  ```
  Return a small struct (e.g. `SegmentDestination{SubscriptionID, SubscriptionStatus, DestinationID,
  Name, Type, DestinationStatus}`).
- **Handler** — `ListSegmentDestinations` in `internal/activation/handler.go`.
- **Route** — in `cmd/cdp-api/main.go` (near the segment routes at 142-144, or the activation block):
  ```go
  admin.With(auth.Require(rbac.PermDestinationRead)).
      Get("/admin/v1/tenants/{tenantID}/segments/{segmentID}/destinations",
          activationHandler.ListSegmentDestinations)
  ```
- **Test** — create two destinations + subscriptions on one segment, assert both are listed; disable
  one and assert it still appears with `status=disabled`.

**Interim workaround (no code):**

```sh
docker exec -i deploy-postgres-1 psql -U cdp -d cdp -c "
SELECT d.name, d.type, s.id AS subscription_id, s.status AS sub_status, d.status AS dest_status
FROM destination_subscription s JOIN destination d ON d.id = s.destination_id
WHERE s.tenant_id='<tenant>' AND s.segment_id='<segment_id>'
ORDER BY d.name;"
```

---

## Enhancement C — View all of a person's identifiers

Show every phone/email/`user_id` a person has ever used, not just the latest trait value.

**Today:** The profile endpoints (`cmd/cdp-api/main.go:133-134`) return only last-write-wins traits
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

**Add — two tiers (do Tier 1 first):**

- **Tier 1 — hashed inventory (small).** New `GET .../profiles/{canonicalUserID}/identifiers`
  returning every node grouped by namespace with counts (e.g. `{"phone": 3, "email": 2, ...}`).
  Add a repo method `IdentityNodesForCluster(ctx, tenantID, clusterID)` reusing the Export
  cluster-node query (`internal/governance/governance.go:62-66`), wired behind `PermProfileRead`.
  This answers "how many, which namespaces" but not the plaintext values.
- **Tier 2 — real values (larger).** Start populating `identity_node.value_encrypted` in `upsertNode`
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
| A | Unsubscribe / disable a subscription | No | Small | `UPDATE destination_subscription SET status='disabled'` |
| B | List destinations by segment | No | Small | JOIN query on `destination_subscription` + `destination` |
| C | View all of a person's identifiers | Hashed only (Export) | Tier 1 small / Tier 2 larger | Export endpoint (hashed) or `raw_event.payload_json` |
| D | Reparent profiles on cluster merge | No | Larger | Delete orphan rows under `merged` clusters |
