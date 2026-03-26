# Input Sources – Export CSV, macro parquet, metadata

Short reference for tenant S3 inputs and optional analysis metadata for the Hanmi ACL Quarterly report.

---

## 1. Export CSV (instrument* + analysisDetails)

Bucket-root paths. One CSV per category per analysis for **instrumentResult**, **instrumentReporting**, and **instrumentReference** only.

| File | S3 key |
|------|--------|
| analysisDetails | `export/analysisidentifier={id}/analysisDetails.json` |
| instrumentResult | `export/analysisidentifier={id}/instrumentResult/instrumentResult.csv` |
| instrumentReporting | `export/analysisidentifier={id}/instrumentReporting/instrumentReporting.csv` |
| instrumentReference | `export/analysisidentifier={id}/instrumentReference/instrumentReference.csv` |

**Macro is not exported here** — see §2.

Column names should match the Impairment Studio datamodel. The model applies the same column allowlist as for parquet where possible.

**Local mirror:** `inputPaths` per category; filename `{category}.csv` in that category directory.

**Settings:** `settings.liveS3InputsByAnalysisId` + `settings.exportCsvInputs` (default **true**). If `exportCsvInputs` is false, the run raises with a pointer to this doc.

---

## 2. macroEconomicVariableInput (parquet under `input/`)

**Not** provided as export CSV. Downloads remain under the **input** root, **Baseline** scenario, using **asOfDate** from the main (current) analysis’s `analysisDetails` (BASE scenario), same as before:

- `input/macroeconomicVariableInput/asofdate=YYYY-MM-DD/scenarioidentifier=BASE/*.parquet`
- Fallback if date missing: `input/macroeconomicVariableInput/analysisidentifier={id}/scenarioidentifier=BASE/` (legacy).

Only the **current/main** analysis index receives macro parquet (Hanmi ACL reads macro for current only).

`model.Model._load_parquet_for_analysis` loads `**/*.parquet` in the macro local dir (no `{category}.csv` from export).

---

## 3. Optional analysis metadata file

- Purpose: Define which analysis ID is current, prior, prior year, and optional quarters.
- When: Optional. If absent: first analysisId = current, second = prior.
- Sources: optional file under execution `inputPath`, or `settings.analysisRoles` / `settings.analyses`.
- Example shape: see previous revisions of this doc or `docs/REPORT_MAPPING.md`.

---

## 4. Legacy: `output/` parquet shards

See **`docs/INPUT_LAYOUT_OUTPUT_PARQUET_LEGACY.md`** for the old multi-parquet layout under `output/` for instrument*. The model still **reads parquet** locally if `{category}.csv` is absent (e.g. tests).

---

## 5. Path summary

| Data | Root | Pattern |
|------|------|---------|
| instrumentResult / Reporting / Reference | export | `export/analysisidentifier={id}/.../{category}.csv` |
| analysisDetails | export | `export/analysisidentifier={id}/analysisDetails.json` |
| macroEconomicVariableInput | input | `input/macroeconomicVariableInput/asofdate={date}/scenarioidentifier=BASE/*.parquet` |
| Optional payload | execution inputPath | As today under `settings.inputPath` |

---

*See `docs/REPORT_MAPPING.md` and `docs/IMPLEMENTATION_PLAN.md`.*
