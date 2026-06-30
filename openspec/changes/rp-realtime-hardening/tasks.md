# Tasks — rp-realtime-hardening

> GENERATED from tasks/*.md frontmatter by scripts/tasks-index.sh — do not edit.

## RH

- [x] RH-01 — CRITICAL — OCC token is the broker-committed stream seq, not ++ on the shared stream ([tasks/RH-01-occ-committed-stream-seq.md](tasks/RH-01-occ-committed-stream-seq.md))
- [x] RH-02 — CRITICAL — fence lease renewal within the TTL budget, stop-the-world on overdue renew, and review the 2 folded perf commits ([tasks/RH-02-lease-fencing-budget.md](tasks/RH-02-lease-fencing-budget.md))
- [x] RH-03 — HIGH — partially-applied participant.transfer must re-drive idempotently (subset, not exact-equality) ([tasks/RH-03-transfer-partial-apply.md](tasks/RH-03-transfer-partial-apply.md))
- [x] RH-04 — HIGH — gate publish/tombstone Nak on MaxDeliver to DLQ, roster failure retries unbounded ([tasks/RH-04-nak-dlq-guard.md](tasks/RH-04-nak-dlq-guard.md))
- [x] RH-05 — HIGH — empty roster (200, empty agents) must soft-fail and never be cached ([tasks/RH-05-empty-roster-soft-fail.md](tasks/RH-05-empty-roster-soft-fail.md))
- [x] RH-06 — HIGH — health/readiness surface on each binary plus a least-privilege projector NATS user ([tasks/RH-06-health-probes-nats-user.md](tasks/RH-06-health-probes-nats-user.md))
- [x] RH-07 — MED — no-wait rebuild fetch plus bounded in-process router state ([tasks/RH-07-rebuild-fetch-state-ttl.md](tasks/RH-07-rebuild-fetch-state-ttl.md))
- [x] RH-08 — MED — auth-callout fail-closed unknown role plus least-privilege JS.API/presence/HMAC ([tasks/RH-08-authcallout-hardening.md](tasks/RH-08-authcallout-hardening.md))
- [x] RH-09 — MED — wire OTLP export in deploy plus a metrics surface ([tasks/RH-09-otlp-metrics.md](tasks/RH-09-otlp-metrics.md))
- [x] RH-10 — MED — enable HA replicas (queue group / KV lease) plus delete duplicated mutable-tag deploy defs ([tasks/RH-10-ha-replicas-deploy-dedup.md](tasks/RH-10-ha-replicas-deploy-dedup.md))
- [x] RH-11 — LOW cluster — assigned emit/drop, stream ceiling, fail-loud password, fanout config, plus contract/comment/doc notes ([tasks/RH-11-low-cluster.md](tasks/RH-11-low-cluster.md))
- [x] RH-12 — DoD — spec-tree sync, anchor RDL ids, ADRs, cross-reviews, CI green ([tasks/RH-12-dod-closeout.md](tasks/RH-12-dod-closeout.md))
