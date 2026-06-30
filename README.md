# CDP Documentation Pack

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
