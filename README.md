# osscdp — Customer Data Platform

A production-grade CDP (Go): event ingress → identity resolution → unified profiles → stateless
segmentation → activation, with governance (encryption, consent, RBAC, PII masking, GDPR
export/delete) and operability (metrics, dashboards, alerts, DLQ admin, rate limiting, circuit
breaker).

## Getting started

```bash
cp .env.example .env      # set ADMIN_API_TOKEN and CDP_ENCRYPTION_KEY (openssl rand -base64 32)
make up                   # Postgres + Redpanda
make run-api              # admin + ingress API
make run-worker           # pipeline worker
# or: make stack-up       # full stack in Docker incl. Prometheus + Grafana
make test                 # unit + integration (testcontainers)
```

**→ Read [docs/cdp/14-usage.md](docs/cdp/14-usage.md) for a hands-on walkthrough with `curl`
examples.** Operations: [docs/cdp/13-operations.md](docs/cdp/13-operations.md). Design:
[docs/cdp/00-index.md](docs/cdp/00-index.md).

**API reference:** interactive docs at `http://localhost:18080/docs` (Redoc); the OpenAPI 3 spec is
[`api/openapi.yaml`](api/openapi.yaml) (also served at `/openapi.yaml` — generate client SDKs from it
with `openapi-generator`).

## Documentation Pack

Copy the `docs/cdp` directory into your repository.

Recommended target path:

```text
your-repo/docs/cdp
```

Start reading from:

```text
docs/cdp/00-index.md
docs/cdp/11-ai-agent-instructions.md
```

For AI agents, include this instruction in your task prompt:

```text
Before coding, read /docs/cdp/00-index.md and /docs/cdp/11-ai-agent-instructions.md. Preserve tenant isolation, idempotency, PII safety, and component boundaries. Add tests and update docs when behavior changes.
```
