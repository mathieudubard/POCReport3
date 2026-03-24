# Custom model reports — sample project and platform direction

## Purpose of this page

This describes what the Hanmi / Model Report Generator sample project does and how it fits a broader pattern: many custom report models that share the same data concepts and delivery shape, but differ in business logic—with a path to generate that logic (e.g. via an LLM) from templates or instructions.

---

## What this sample project is

- A single custom Python model in a standard package layout (`model/run.py` entry, I/O layer, core logic, tests, `meta/`).
- It reads analysis inputs (e.g. instrument result, reporting, reference data, analysis details) from cloud storage or local folders, aligned with a shared datamodel.
- It produces structured report outputs (e.g. quarterly summary JSON, Hanmi ACL quarterly JSON, per-analysis analysisDetails copies, optional debug JSON, zipped report export) for downstream systems or documents.
- Analysis identity is driven by a list of analysis IDs; time periods (current vs prior quarter, multi-quarter tables) can be resolved from dates in analysis details, not from ad hoc metadata files.

In short: one concrete model implemented as code (several JSON artifacts from one run), packaged so it can run in the same execution environment as other models.

---

## Generalization — the target pattern

| Layer | Role |
|--------|------|
| Shared contract | Same ideas: analyses, instruments, shared datamodel fields, core ingestion rules (where inputs live, how IDs join). |
| Variable layer | Per-report business logic: sections, aggregations, joins, narrative structure, extra calculations—whatever the specific report needs. |
| Package shape | Each model is still a similar repo layout (e.g. `run.py`, I/O, `model.py`, tests, `meta/`) so automation can build, test, register, and deploy consistently. |

Different goals (regulatory narrative, management summary, deep drill-down) become different models that reuse the same plumbing and datamodel, not one giant script with endless `if report_type`.

---

## LLM-assisted model authoring (vision)

1. Inputs to the LLM: Report template (Word/Markdown), PDF, or written instructions—plus pointers to the canonical datamodel and non-negotiable rules (join keys, required outputs, file naming).
2. Output: Python (and tests) that implement only the variable business logic, still fitting the standard project skeleton.
3. Human + CI: Review, run pytest, validate against sample data, then promote through model registry / deployment pipelines.

The LLM does not replace governance; it accelerates drafting and refactoring of the logic that differs per report.

---

## Why this matters

- Scalability: Many custom reports without forking the entire platform each time.
- Consistency: Same I/O, auth, and data semantics; fewer one-off bugs in how we read storage.
- Speed: New report types go from spec → generated scaffold → reviewed code → deployed model.
- Traceability: Logic lives in versioned Python with tests, not only in spreadsheets or PDFs.

---

## This repo vs “any” custom report

- This repo = one worked example (quarterly-style summaries, Hanmi ACL-oriented outputs, etc.).
- Generalization = same pattern, different `model.py` (and related helpers), possibly extra steps or outputs—still analyses + datamodel + standard run contract.

---

## Related artifacts (in the sample project)

- `README.md` — how to run locally or with S3 parameters.
- `AGENTS.md` — compact technical flow for developers/agents.
- `datamodel/` — data dictionary reference.
- `sample/` — use cases / payload examples.
- `tests/` — unit tests that new generated logic should extend or mirror.

---

## Summary

We are proving one custom model end-to-end while documenting a repeatable package so that automation and (optionally) LLM assistance can produce many such models—same backbone, tailored business logic for each report goal.

---

## Pasting into Confluence

1. Add a Markdown macro: type `/markdown` (Confluence Cloud), insert it, then click inside the macro’s editor panel so the caret is there—not in the main page body.
2. Paste there. The main document canvas is a rich-text editor: it does not treat `#` or `|` as Markdown, so it will look “flat” or wrong if you paste outside the macro.

### If Markdown disappears when you paste from Cursor / VS Code

Those editors often put HTML on the clipboard (syntax highlighting). Confluence may use that instead of plain text, so headings and tables never run through Markdown.

Workarounds (any one):

- Paste as plain text in the browser — With focus in the Markdown macro, try `Ctrl+Shift+V` (Chrome/Edge/Firefox often paste “without formatting”). If that still fails, use another option below.
- Copy from a plain-text path — Open this file in Notepad, or run `Get-Content .\docs\CONFLUENCE_WIKI.md -Raw | Set-Clipboard` in PowerShell, then paste into the Markdown macro (PowerShell puts plain text on the clipboard).
- Console — Same idea as above: `type` / `Get-Content` output copied from the terminal is usually plain text only, which is why that worked for you.

If you do not have a Markdown macro, you have to recreate headings and tables with the normal Confluence editor; pasting raw Markdown into the default page body will not render it.
