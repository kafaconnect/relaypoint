---
name: docs-writer
description: Author human- AND AI-readable documentation as self-contained HTML (never Markdown). Use when creating or updating any human-facing doc — architecture, guides, references, runbooks, onboarding. Produces one self-contained .html per topic from the shared template, with a machine-readable metadata block and a table of contents that mirrors the headings.
license: Proprietary
compatibility:
  openspec: ">=1.4.1"
metadata:
  version: "1.0.0"
  owner: platform
  template: docs/assets/doc-template.html
---

# docs-writer

Human-facing docs are **HTML, not Markdown**. Markdown is for tooling; HTML reads well
for humans in a browser and parses cleanly for AI (semantic landmarks + a metadata block).

**Scope — what is HTML vs what stays Markdown**
- **HTML** (use this skill): architecture (C4), guides, API/contract references, runbooks,
  onboarding, design explainers — anything a person reads. These live in `docs/`.
- **Stays Markdown** (tooling requires it, do NOT convert): `AGENTS.md`, everything under
  `openspec/` (project.md, config.yaml, specs, changes), the skill files, and the root
  `README.md` (GitHub renders it). When unsure which a file is, ask.

## What needs a doc
- A new capability/service → an architecture page (context + containers + key flows).
- A public/contract surface (OpenAPI/proto/AsyncAPI) → a reference page.
- A recurring operational task → a runbook.
- Onboarding / "how we work" → a guide.

Do NOT re-document OpenSpec specs — link to them. Don't restate code in prose.

## How to write
1. Copy `docs/assets/doc-template.html` → `docs/<kebab-title>.html` (or `docs/<area>/<kebab>.html`).
2. Fill `<script type="application/json" id="doc-meta">`: title, summary, version, updated (YYYY-MM-DD), status, tags.
3. One `<h1>` + one `<p class="summary">`. Body lives only inside `<section>`s, each `aria-labelledby` its heading id.
4. `<h2>`/`<h3>` get **kebab-case, stable** ids + the `#` anchor link; never skip a heading level. ids are permanent — add a new one rather than renaming (renaming breaks deep links).
5. The `nav.toc` MUST mirror every `<h2>`/`<h3>` id in document order (h3 links get `class="lvl-3"`).
6. Each `<section>` must read in isolation — no "as above", no unscoped pronouns (AI chunks per section).
7. **Semantic line breaks** in the HTML source: one sentence per line (and after long clauses) → Git diffs touch only the changed sentence. Rendered output is unaffected.
8. Tables for >1 option; callouts (`note`/`tip`/`warn`/`danger`) always carry a text label.
9. Update `doc-meta.updated` and the footer date on every content change.
10. Add a link from `docs/index.html` (the hub).

## Format rules (enforced)
- **Self-contained**: all CSS inline, **no CDN, no build, no network**.
- Required landmarks, once each: `header.site`, `nav.toc`, `main>article`, `footer.site`.
- Filename = kebab-case of the doc title + `.html`.
- One topic per file — don't merge unrelated topics or split one topic across files.

## Guardrails
- Never emit a human-facing doc as `.md`.
- Don't add an external dependency, font CDN, or JS framework — the template is the whole toolchain.
- For heavy AI ingestion, optionally also maintain `docs/llms.txt` (one line per doc: path + summary); the HTML stays the source of truth.
