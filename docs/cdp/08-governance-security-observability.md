# Governance, Security, and Observability

## Purpose

A CDP handles customer data, identifiers, behavior events, and activation to external systems. Governance, security, and observability are not optional.

This document defines the minimum requirements.

## Tenant isolation

Rules:

- Every event belongs to exactly one tenant.
- Every profile belongs to exactly one tenant.
- Every identity node belongs to exactly one tenant.
- Every segment belongs to exactly one tenant.
- Every destination belongs to exactly one tenant.
- Every query must include tenant scope.

Database rule:

```text
tenant_id is mandatory in all business tables
```

Application rule:

```text
Never trust tenant_id from request body when API key or session already determines tenant.
```

## RBAC

Initial roles:

```text
SUPER_ADMIN
TENANT_ADMIN
MARKETER
ANALYST
OPERATOR
VIEWER
```

Suggested permissions:

| Permission | Description |
|---|---|
| `source:read` | View sources. |
| `source:write` | Create/update/disable sources. |
| `event:read` | View events. |
| `profile:read` | View profiles. |
| `profile:delete` | Delete customer profile. |
| `segment:read` | View segments. |
| `segment:write` | Create/update segments. |
| `destination:read` | View destinations. |
| `destination:write` | Create/update destinations. |
| `activation:read` | View delivery logs. |
| `dlq:read` | View DLQ. |
| `dlq:retry` | Retry DLQ events. |
| `audit:read` | View audit logs. |

## PII protection

PII examples:

```text
email
phone
name
address
ip
raw device identifiers
external IDs that identify a person
```

Rules:

- Do not log PII in plain text.
- Hash identifiers for lookup.
- Encrypt sensitive values if they must be displayed or exported.
- Mask PII in admin UI based on permission.
- Do not expose API keys or destination secrets after creation.

Example masking:

```text
user@example.com -> u***@example.com
+84901234567 -> +8490****567
```

## Consent model

```sql
customer_consent (
  id,
  tenant_id,
  customer_profile_id,
  channel,
  purpose,
  status,
  source,
  updated_at
)
```

Channels:

```text
email
sms
push
ads
webhook
```

Purposes:

```text
marketing
analytics
personalization
transactional
```

Statuses:

```text
granted
denied
unknown
```

Rule:

```text
Activation must check consent before sending.
```

## Audit log

Audit all sensitive operations:

- Tenant created/updated.
- Source created/disabled.
- API key created/rotated/revoked.
- Segment created/updated/published/disabled.
- Destination created/updated/disabled.
- Destination secret changed.
- DLQ event retried/discarded.
- Customer profile viewed/exported/deleted.
- Consent changed.
- RBAC changed.

Audit log model:

```sql
audit_log (
  id,
  tenant_id,
  actor_id,
  actor_type,
  action,
  resource_type,
  resource_id,
  before_json,
  after_json,
  ip_address,
  user_agent,
  created_at
)
```

## Data retention

Define retention by data type:

| Data | Suggested initial policy |
|---|---|
| Raw events | 90-365 days depending on tenant plan. |
| Aggregated profiles | Keep while customer exists. |
| Delivery logs | 90 days. |
| Audit logs | 1 year or more. |
| DLQ events | Until resolved or manually discarded. |

Retention should be configurable later.

## Customer data deletion

The system must eventually support:

```http
DELETE /v1/admin/profiles/{profile_id}
```

Deletion should remove or anonymize:

- Customer profile.
- Identity nodes if no longer referenced.
- Segment membership.
- Consent records.
- Activation payloads where possible.
- Raw events depending on retention/legal policy.

## Observability

### Metrics

Required metrics:

```text
events_received_total
events_validated_total
events_rejected_total
kafka_publish_failed_total
identity_resolved_total
identity_merge_total
profile_updated_total
segment_evaluated_total
segment_matched_total
activation_sent_total
activation_failed_total
dlq_total
processing_lag_seconds
```

### Logs

Use structured JSON logs.

Example:

```json
{
  "level": "INFO",
  "component": "identity-worker",
  "tenant_id": "tenant_001",
  "event_id": "evt_001",
  "source_id": "src_web",
  "message": "identity resolved"
}
```

### Traces

Trace event processing with correlation IDs:

```text
trace_id
event_id
tenant_id
source_id
customer_profile_id
```

### Dashboards

Minimum dashboards:

- Ingress volume.
- Validation failure rate.
- Kafka publish errors.
- Kafka consumer lag.
- Worker processing latency.
- DLQ count.
- Identity merge count.
- Profile update count.
- Segment evaluation/match count.
- Activation success/failure rate.

## DLQ operations

DLQ viewer must show:

- Tenant.
- Event ID.
- Source.
- Component.
- Error code.
- Error message.
- Original payload.
- Retry count.
- Failed time.

Operator actions:

```text
Retry
Discard
Export
Mark as resolved
```

## Implementation notes (Phase 9b — access governance)

- **RBAC** — admin callers are role-bearing admin tokens (`admin_token` table, SHA-256 hash lookup).
  The static `ADMIN_API_TOKEN` authenticates as `SUPER_ADMIN` (bootstrap to mint the first tokens).
  `internal/rbac` defines the 6 roles, a permission set per role, and `Has(role, perm)`.
- **Authorization** — `auth.Authenticate` resolves a `Principal{role, tenant_id}` into context;
  `auth.Require(perm)` gates each route on the permission and, for `{tenantID}` routes, enforces that a
  non-super principal matches that tenant (`403` otherwise). `POST /admin/v1/tenants` is super-admin only.
- **Admin tokens** — `POST /admin/v1/admin-tokens` (perm `admin:write`): SUPER_ADMIN mints any
  role/tenant; TENANT_ADMIN mints only non-super roles scoped to its own tenant. Plaintext returned once;
  audited.
- **PII masking** — profile/export responses mask `traits.email/phone/name` (`u***@x.com`,
  `+8490****567`) unless the principal holds `pii:read` (SUPER_ADMIN/TENANT_ADMIN). Stored data is
  untouched.
- **Deferred:** user accounts / SSO / JWT, token expiry/rotation, per-field PII policy, raw-event
  payload masking, row-level DB security.

## Implementation notes (Phase 9a — data governance)

Phase 9 is split: **9a (data governance)** is implemented; **9b (access governance: RBAC + field-level
PII masking)** is deferred.

- **Encryption at rest** — `internal/crypto` AES-256-GCM keyed by `CDP_ENCRYPTION_KEY` (base64 32
  bytes). Destination secrets are encrypted into `destination.secret_ref` (never returned by the API).
- **Webhook signing** — when a destination has a secret, the sender adds
  `X-CDP-Signature: sha256=<hmac(secret, body)>`.
- **Consent** — `customer_consent` (channel/purpose/status). Activation checks consent for the
  destination's channel/purpose; `denied` → the task is recorded `skipped` (`consent_denied`, never
  sent); `granted`/`unknown` proceed. Admin: `PUT/GET …/profiles/{cuid}/consent`.
- **API-key rotation** — `POST …/sources/{id}/rotate-key`; old key invalid immediately; audited.
- **Export/delete** — `GET …/profiles/{cuid}/export` (profile + identity + memberships + consent);
  `DELETE …/profiles/{cuid}` erases customer-scoped rows in one transaction (raw_event retained,
  retention-governed). Both audited.
- **Deferred (9b/later):** RBAC (the admin API still uses a single static token), field-level PII
  masking in responses, retention enforcement, KMS/envelope encryption, key-rotation grace period,
  raw-event erasure.

## Acceptance criteria

- [ ] Tenant isolation enforced in code and database queries.
- [ ] RBAC permissions defined and enforced.
- [ ] PII is masked in logs and UI.
- [ ] API keys are hashed/encrypted.
- [ ] Destination secrets are encrypted.
- [ ] Consent is checked before activation.
- [ ] Audit log records sensitive operations.
- [ ] Metrics are exported.
- [ ] Structured logs are implemented.
- [ ] DLQ viewer and retry exist.
- [ ] Retention policy exists.
