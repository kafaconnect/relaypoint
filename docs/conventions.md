# Conventions

> Proprietary project. All dependencies must be 100% OSS — no exceptions.

## Comments

Comments are **rare**. Most code should explain itself through good names and small functions. Write a comment only when:

- **(a)** the code genuinely cannot — a non-obvious rationale, constraint, invariant, trade-off, gotcha, or "why it's done this weird way"; or
- **(b)** it references a change/story/task/issue — e.g. `// see openspec change m1-unified-inbox` or `// #123`.

**Never** restate what the code already says. No narrating obvious steps. No decorative/section-banner comments. No "what" comments — only "why" comments. Filler and token-wasting comments are treated as a **review defect**.

Prefer fixing the code (rename, extract a function) over explaining it.

**Go**

```go
// BAD: restates the code
i++ // increment i
ttl := 5 * time.Minute // set ttl to 5 minutes

// GOOD: explains the why
// pgx caches prepared statements per-conn; reset after DDL or we get stale plans. #412
conn.Reset()
```

**TypeScript**

```ts
// BAD: narrates the obvious
const next = page + 1 // add one to page
setOpen(true) // open the dialog

// GOOD: explains a non-obvious constraint
// zca-js drops the WS if we send before the login ack; gate on ready. see change m2-relay
await waitForReady()
```

## Naming

- **Go**: short, idiomatic, lowercase locals; no stutter (`user.New`, not `user.NewUser`). Exported names carry package context, not repeat it.
- **TS**: descriptive `camelCase` for values/functions, `PascalCase` for components and types. Hooks start with `use`.
- **SQL**: `snake_case` for tables, columns, indexes. Plural table names.

## Formatting & Lint

- Go: `gofmt` (enforced) + `golangci-lint`.
- TS/React: `eslint` + `prettier`.
- CI **fails** on any lint or format violation. Run locally before pushing.

## Errors

- **Go**: wrap with `%w` for context (`fmt.Errorf("load tenant: %w", err)`). No `panic` in library/`internal/` code. Handle errors — never ignore with `_`.
- **TS**: typed errors, no silent `catch`. Catch only to add context, recover, or surface — then rethrow or report. Never swallow.

## Structure

- **Contract-first**: change `contracts/` (OpenAPI / proto / AsyncAPI), then regenerate. Never edit generated clients/servers by hand.
- Services communicate **only** via contracts — no reaching into another service's internals.
- Shared React lives in `packages/ui`; Go domain logic lives under `internal/`.

## Database & Migrations

- `golang-migrate` — every migration has **both** `up` and `down`.
- Every tenant-owned table has a `tenant_id` column **and** an RLS policy. No exceptions.
- Data access via `sqlc`-generated code — **never** hand-edit generated files; change the query/schema and regenerate.
- No destructive migration (drop/rename/type-narrow) without an explicit note in the PR describing impact and rollback.

## Tests

- Go: **table-driven**.
- Every spec scenario has a test tagged `// @spec:<scenario-id>`.
- Tests are **deterministic** — no real clocks, network, or random without seeding/injection.

## Logging & Security

- `slog` structured key/values only — no string-formatted log lines.
- **Never** log secrets, tokens, or PII.
- `tenant_id` belongs in the log context for tenant-scoped operations.
- Config via **env**; no secrets committed to the repo.

## Commits & PRs

- **Conventional commits** (`feat:`, `fix:`, `chore:`, ...).
- Each PR references the OpenSpec change **and** the issue.
- Keep diffs focused — one concern per PR.

## Generated Code

- Committed to the repo, but **never** hand-edited.
- Regenerate from the source of truth (`contracts/`, SQL schema/queries) when it changes.

---

`AGENTS.md` enforces these conventions for AI agents.
