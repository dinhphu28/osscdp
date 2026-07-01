# Usage Guide

A task-oriented walkthrough of the CDP: start it, authenticate, send events, and drive the full flow
through to a signed webhook delivery. Commands are copy-pasteable `curl`.

For architecture see `01-architecture-overview.md`; for operations see `13-operations.md`.

## 0. Concepts & mental model

Read this first — it's the model everything else assumes.

**Core objects**
- **Tenant** — the isolation boundary. Every object, query, and token is scoped to one tenant; nothing
  crosses tenants.
- **Source** — an authenticated origin of events (e.g. your web backend), with an **ingest API key**.
- **Event** — `track` (a behavior; needs `event_name`), `identify` (attach traits like email/name),
  `alias` (link two IDs), `batch` (many at once).
- **Identity → `canonical_user_id`** — you send messy identifiers (`user_id`, `anonymous_id`, `email`…);
  the CDP resolves them into one identity cluster with a stable `canonical_user_id` (`customer_…`) — the
  real customer key that survives email/phone changes.
- **Profile** — the unified customer: merged **traits** + **computed attributes** (`total_events`,
  `total_orders`, `last_product_viewed`, …).
- **Segment** — an audience defined by a JSON rule over `profile.*` / `event.*`; membership updates in
  real time.
- **Destination + subscription** — where a segment change is delivered (webhook or Kafka).
- **Consent** — activation is skipped when consent is `denied` for the destination's channel/purpose.

**Two kinds of auth (don't mix them)**
- **Source API key** (`cdp_…`) → send events (`/v1/*`).
- **Admin token** → manage everything (`/admin/*`). The static `ADMIN_API_TOKEN` is the bootstrap
  `SUPER_ADMIN`; from it you mint role-scoped tokens (VIEWER, MARKETER, TENANT_ADMIN, …), each limited by
  permission and tenant.

**It is asynchronous.** Ingest returns `202` immediately and tells you nothing about the customer —
identity → profile → segmentation → activation happen in the background (seconds later). After sending
an event, **wait, then query**; don't expect an instant profile or membership. This keeps ingress fast
and durable.

**Rules that bite if you forget them**
- `tenant_id`/`source_id` come from the API key — values in the request body are ignored.
- **Idempotent by `event_id`**: same id + same payload → `duplicate`; same id + different payload →
  `409`. Omit `event_id` and the server generates one. Retries are safe.
- **Limits:** 64 KB/event, 500 events/batch; a per-source rate limit returns `429` + `Retry-After`.
- **Secrets are shown once:** the source API key and minted admin tokens are returned exactly once —
  store them (lost a source key? rotate it, §5). Destination secrets are encrypted at rest.
- **PII masking:** profile `email`/`phone`/`name` are masked unless your token holds `pii:read`
  (SUPER_ADMIN/TENANT_ADMIN).
- **v1 scope:** identity matching is deterministic only (no fuzzy/ML); segmentation is
  stateless/profile-attribute (no time-window rules yet).

**Minimal first success:** create tenant → create source → `POST /v1/identify` → `POST /v1/events/track`
→ wait ~5s → `GET /admin/v1/tenants/{tid}/profiles/{canonical_user_id}` → see the unified profile.

## 0.5 Onboarding a new tenant (who does what)

The CDP is an **API-only backend** (no self-serve UI yet) and onboarding has **two actors**.

### Operator (you, running the platform) — provisions the tenant

Tenant creation is `SUPER_ADMIN`-only; a client can't self-provision. Using the static
`ADMIN_API_TOKEN`:

1. Create the tenant → note its id.
2. Mint a `TENANT_ADMIN` token for the client and hand it over securely (shown once):
   `POST /admin/v1/admin-tokens {"name":"acme-admin","role":"TENANT_ADMIN","tenant_id":"<TID>"}`.

The client now self-serves everything below with that token, scoped to just their tenant.

### Client / tenant — configures ingress, audiences, and activation

1. **Connect ingress.** Create a **source** (§5) → get the ingest API key (shown once). Then
   **instrument your app** to POST events (`identify`/`track`/`alias`) with that key (§6). Choose an
   identifier strategy: `user_id` when logged in, `anonymous_id` before login, `alias` to stitch the
   two at login. *(Your engineering owns this — there's no SDK; it's plain HTTP/JSON.)*
2. **Verify identity.** Send an `identify` + a `track`, wait a few seconds, then fetch the profile
   (§8) — a unified customer with a `canonical_user_id` should appear.
3. **Define an audience.** Write a JSON rule and create a **segment** (§9); membership updates in real
   time as events arrive.
4. **Connect a destination.** Create a **webhook** (or Kafka) destination with a signing `secret`, and
   **subscribe** it to the segment (§10). Then **build a receiver** on your side that verifies the
   `X-CDP-Signature` HMAC. *(The second integration you own.)* Watch `…/deliveries` for status.
5. **Govern.** Set **consent** so you don't activate opted-out users (§11); handle export/delete for
   privacy requests (§12); mint **scoped tokens** for teammates — `MARKETER`, `ANALYST`, `VIEWER`
   (only admins see un-masked PII).
6. **Operate.** Watch the dashboard/metrics; retry from the DLQ if something fails (§13–14).

### Client checklist

1. Get your tenant + `TENANT_ADMIN` token from the operator.
2. Create a source; **send events from your app**.
3. Confirm a profile forms.
4. Define a segment.
5. Create a destination + subscription; **build a signature-verifying webhook receiver**.
6. Set consent; use scoped tokens for your team.

> Two gaps to expect today: **no self-serve console** (everything is the admin API — curl or your own
> tooling) and **no client SDK** (you POST JSON directly). A tenant sign-up UI and language SDKs are
> the natural next additions.

## 1. Prerequisites & start

Requirements: Docker (+ Compose), Go 1.24+.

```bash
cp .env.example .env
# Required secrets:
#   ADMIN_API_TOKEN   — the bootstrap SUPER_ADMIN token
#   CDP_ENCRYPTION_KEY — base64 32 bytes: openssl rand -base64 32
$EDITOR .env
```

Two ways to run:

```bash
# A) Host dev: infra in Docker, apps via go run (auto-migrate on boot)
make up            # Postgres + Redpanda
make run-api       # cdp-api  (HTTP + admin API)     :8080
make run-worker    # cdp-worker (pipeline + metrics) :9100

# B) Full stack in Docker (adds Prometheus + Grafana + Alertmanager)
make stack-up      # cdp-api on :18080, Grafana :3000, Prometheus :9090
```

> Local ports may collide. This repo's dev env uses `POSTGRES_PORT=5433`, `HTTP_ADDR=:18080`,
> `METRICS_ADDR=:9100`. The examples below assume the API at `http://localhost:18080`.

Set shell variables used throughout:

```bash
B=http://localhost:18080
A="Authorization: Bearer $ADMIN_API_TOKEN"   # SUPER_ADMIN (from .env)
```

## 2. The flow

```
POST /v1/events/*  →  event_outbox  →  relay  →  cdp.events
      →  raw_event (stored)  +  identity (cluster)  →  customer_profile
      →  segmentation (membership)  →  activation (webhook / kafka)
```
Everything is tenant-isolated and idempotent; failures retry then dead-letter.

## 3. Admin auth & RBAC

The admin API is guarded by role-bearing tokens. The static `ADMIN_API_TOKEN` authenticates as
`SUPER_ADMIN` (use it to bootstrap). Mint scoped tokens:

```bash
# SUPER_ADMIN mints a tenant-scoped role token (shown once)
curl -s -XPOST $B/admin/v1/admin-tokens -H "$A" \
  -d '{"name":"analyst-1","role":"ANALYST","tenant_id":"<TENANT_ID>"}'
# → {"api_token":"cdpadm_...","role":"ANALYST"}
```

Roles: `SUPER_ADMIN, TENANT_ADMIN, MARKETER, ANALYST, OPERATOR, VIEWER`. Each admin route requires a
permission (e.g. `segment:write`, `profile:delete`, `pii:read`); a token scoped to tenant A cannot act
on tenant B (403). Only `SUPER_ADMIN`/`TENANT_ADMIN` hold `pii:read` (see §7).

## 4. Tenant & source

```bash
# Create a tenant (SUPER_ADMIN only)
TID=$(curl -s -XPOST $B/admin/v1/tenants -H "$A" -d '{"name":"acme"}' | jq -r .id)

# Create a source → returns the ingest API key ONCE
KEY=$(curl -s -XPOST $B/admin/v1/tenants/$TID/sources -H "$A" \
       -d '{"name":"web","type":"server"}' | jq -r .api_key)
K="Authorization: Bearer $KEY"

# Rotate the key (old key invalidated immediately)
curl -s -XPOST $B/admin/v1/tenants/$TID/sources/<SOURCE_ID>/rotate-key -H "$A"
```

## 5. Send events (ingest)

Authenticate with the **source API key** (`$K`). `tenant_id`/`source_id` come from the key, never the
body. Ingress returns `202` fast; heavy work is async.

```bash
# track — a behavior event (event_name required, ≥1 identifier)
curl -s -XPOST $B/v1/events/track -H "$K" \
  -d '{"event_id":"e1","user_id":"u1","event_name":"product_viewed",
       "properties":{"product_id":"p001","category":"phone"},
       "context":{"page":{"url":"https://shop/p001"}}}'

# identify — attach traits to a customer
curl -s -XPOST $B/v1/identify -H "$K" \
  -d '{"user_id":"u1","anonymous_id":"a1","traits":{"email":"u@x.com","name":"Ann","country":"VN"}}'

# alias — link two identifiers (previous_id + user_id)
curl -s -XPOST $B/v1/alias -H "$K" -d '{"previous_id":"a1","user_id":"u1"}'

# batch — up to 500 events, per-item results
curl -s -XPOST $B/v1/events/batch -H "$K" \
  -d '{"events":[{"event_id":"b1","user_id":"u1","event_name":"page_viewed"}]}'
```

Behavior:
- `event_id` omitted → server generates one; server always sets `received_at`.
- Same `event_id` + same payload → `202 {"status":"duplicate"}` (idempotent). Same `event_id` +
  different payload → `409 conflict`.
- Over the per-source rate limit → `429` with `Retry-After`.

## 6. Raw events (query & replay)

```bash
# by event id
curl -s $B/admin/v1/tenants/$TID/events/e1 -H "$A"
# list by customer identifier (keyset-paginated: use next_cursor)
curl -s "$B/admin/v1/tenants/$TID/events?identifier_key=user_id:u1&limit=50" -H "$A"
# replay one / all events for an identifier (republishes to the pipeline)
curl -s -XPOST $B/admin/v1/tenants/$TID/events/e1/replay -H "$A"
curl -s -XPOST "$B/admin/v1/tenants/$TID/replay?identifier_key=user_id:u1" -H "$A"
```

## 7. Customer profiles

After the pipeline runs, a unified profile exists per customer (`canonical_user_id` like
`customer_…`).

```bash
CUID=customer_...    # from psql or a segment_membership_changed / profile_updated event
curl -s $B/admin/v1/tenants/$TID/profiles/$CUID -H "$A"
curl -s "$B/admin/v1/tenants/$TID/profiles?email=u@x.com" -H "$A"
```

Traits (`email`, `phone`, `name`) are **masked** unless the caller holds `pii:read`
(`u***@x.com`, `+8490****567`). Computed attributes: `total_events`, `total_orders`,
`last_product_viewed`, `last_event_name`, etc.

## 8. Segments (audiences)

Rules are a JSON DSL. Logical nodes: `and`/`or`/`not`. Leaf ops: `eq, neq, gt, gte, lt, lte, contains,
not_contains, in, not_in, exists, not_exists`. Fields: `profile.traits.*`,
`profile.computed_attributes.*`, `event.event_name`, `event.properties.*`, `event.context.*`.

```bash
SEG=$(curl -s -XPOST $B/admin/v1/tenants/$TID/segments -H "$A" -d '{
  "name":"vn-phone-viewers",
  "rule":{"operator":"and","conditions":[
    {"field":"profile.traits.country","op":"eq","value":"VN"},
    {"field":"event.event_name","op":"eq","value":"product_viewed"}]}}' | jq -r .id)

# editing creates a new version (current_version repointed)
curl -s -XPUT $B/admin/v1/tenants/$TID/segments/$SEG -H "$A" \
  -d '{"rule":{"field":"profile.computed_attributes.total_orders","op":"gt","value":3}}'

# who's in it
curl -s $B/admin/v1/tenants/$TID/segments/$SEG/members -H "$A"
```

A customer enters/exits in real time as events update the profile; each transition emits
`segment_membership_changed`.

## 9. Destinations & activation

Membership changes are delivered to destinations. Webhook or Kafka.

```bash
# webhook destination; `secret` is encrypted at rest and used to HMAC-sign deliveries
DID=$(curl -s -XPOST $B/admin/v1/tenants/$TID/destinations -H "$A" -d '{
  "type":"webhook","name":"crm","secret":"whsec_123",
  "channel":"webhook","purpose":"marketing",
  "config":{"url":"https://example.com/hook","timeout_ms":5000}}' | jq -r .id)

# connect the segment
curl -s -XPOST $B/admin/v1/tenants/$TID/destinations/$DID/subscriptions -H "$A" \
  -d "{\"trigger_type\":\"segment_membership\",\"segment_id\":\"$SEG\"}"

# delivery log
curl -s $B/admin/v1/tenants/$TID/destinations/$DID/deliveries -H "$A"

# disable a destination
curl -s -XPUT $B/admin/v1/tenants/$TID/destinations/$DID -H "$A" -d '{"status":"disabled"}'
```

Each webhook carries `Idempotency-Key`, `X-CDP-*` headers, and `X-CDP-Signature: sha256=<hmac(secret,
body)>`. Failures retry with exponential backoff (10s→15min, max 5); a flapping destination trips a
**circuit breaker** (deliveries pause for a cooldown instead of hammering it).

## 10. Consent

Activation is skipped when consent is denied for the destination's channel/purpose.

```bash
curl -s -XPUT $B/admin/v1/tenants/$TID/profiles/$CUID/consent -H "$A" \
  -d '{"channel":"webhook","purpose":"marketing","status":"denied"}'   # granted|denied|unknown
curl -s $B/admin/v1/tenants/$TID/profiles/$CUID/consent -H "$A"
```
`denied` → the activation task is recorded `skipped` (not sent). `granted`/`unknown` proceed.

## 11. Governance (GDPR export / delete)

```bash
# export everything for a customer (profile + identity + memberships + consent)
curl -s $B/admin/v1/tenants/$TID/profiles/$CUID/export -H "$A"
# delete / anonymize (one transaction; raw_event retained per retention policy)
curl -s -XDELETE $B/admin/v1/tenants/$TID/profiles/$CUID -H "$A"
```
Export, delete, key rotation, admin-token creation, and DLQ retry/discard are all written to
`audit_log`.

## 12. DLQ operations

```bash
curl -s "$B/admin/v1/tenants/$TID/dlq?status=open" -H "$A"                    # list
curl -s -XPOST $B/admin/v1/tenants/$TID/dlq/<ID>/retry   -H "$A"              # republish to cdp.events
curl -s -XPOST $B/admin/v1/tenants/$TID/dlq/<ID>/discard -H "$A"              # mark discarded
```

## 13. Observability

- `curl localhost:18080/metrics` (cdp-api: `events_received/validated/rejected/rate_limited_total`).
- `curl localhost:9100/metrics` (cdp-worker: pipeline, identity/profile/segment/activation,
  `processing_lag_seconds`, `dlq_total`, `activation_circuit_open_total`).
- With `make stack-up`: Grafana **CDP Overview** dashboard at http://localhost:3000, Prometheus at
  :9090, alert rules per `13-operations.md`.

## 14. End-to-end walkthrough

Copy-paste (needs `jq`; API at `$B`, admin `$A`, a local webhook sink at `http://127.0.0.1:18090`):

```bash
TID=$(curl -s -XPOST $B/admin/v1/tenants -H "$A" -d '{"name":"demo"}' | jq -r .id)
KEY=$(curl -s -XPOST $B/admin/v1/tenants/$TID/sources -H "$A" -d '{"name":"web","type":"server"}' | jq -r .api_key)
K="Authorization: Bearer $KEY"
SEG=$(curl -s -XPOST $B/admin/v1/tenants/$TID/segments -H "$A" -d '{"name":"vn","rule":{"field":"profile.traits.country","op":"eq","value":"VN"}}' | jq -r .id)
DID=$(curl -s -XPOST $B/admin/v1/tenants/$TID/destinations -H "$A" -d '{"type":"webhook","name":"sink","secret":"shh","config":{"url":"http://127.0.0.1:18090","timeout_ms":3000}}' | jq -r .id)
curl -s -XPOST $B/admin/v1/tenants/$TID/destinations/$DID/subscriptions -H "$A" -d "{\"trigger_type\":\"segment_membership\",\"segment_id\":\"$SEG\"}"

# drive the customer into the segment
curl -s -XPOST $B/v1/identify -H "$K" -d '{"event_id":"w1","user_id":"buyer","traits":{"country":"VN"}}'
curl -s -XPOST $B/v1/events/track -H "$K" -d '{"event_id":"w2","user_id":"buyer","event_name":"product_viewed"}'

sleep 5
# canonical_user_id is emitted on profile_updated / segment_membership_changed; here we read it from the DB
CUID=$(docker exec deploy-postgres-1 psql -U cdp -d cdp -t -A \
        -c "SELECT canonical_user_id FROM customer_profile WHERE tenant_id='$TID'")
curl -s $B/admin/v1/tenants/$TID/profiles/$CUID -H "$A" | jq '{traits,computed_attributes}'
curl -s $B/admin/v1/tenants/$TID/segments/$SEG/members -H "$A" | jq '.members|length'
curl -s $B/admin/v1/tenants/$TID/destinations/$DID/deliveries -H "$A" | jq '.deliveries[0]'
```

The webhook sink receives the `segment_membership_changed` payload with a valid `X-CDP-Signature`.
