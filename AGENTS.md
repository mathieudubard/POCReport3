# Model Report Generator – Agent Context

Compact context for agents. See README.md for full project layout.

---

## What this repo does

- Entry: `model/run.py` — run with `-j JWT -s <s3_key_to_modelRunParameter.json>` for S3 mode; `-L <local_folder>` for local.
- Flow: Load model run parameters → list/fetch inputs from S3 (Cappy) → process instrumentResult → write report outputs → upload outputs to S3.
- Report output: Quarterly summary: `quarterly_summary_report.json` (structured JSON for all 6 doc sections per `sample/useCase.txt`). Plus `analysisDetails_{id}.json` per analysis (from `export/analysisidentifier={id}/analysisDetails.json`). `report_export.zip` contains all report files. Current/prior/priorYear and quarter order are derived from analysisDetails.reportingDate (no metadata file); first analysisId in the array is not assumed to be latest.

---

## What’s implemented (done so far)

1. S3 input source (per analysis, tenant bucket)  
   **Primary:** bucket-root `export/` — one CSV per category per analysis for **instrumentResult, instrumentReporting, instrumentReference** (see `docs/INPUT_SOURCES.md`); `export/.../analysisDetails.json` → report dir as `analysisDetails_{id}.json`.
   **Macro:** not in export — parquet under `input/macroeconomicVariableInput/asofdate=.../scenarioidentifier=BASE/` (asOfDate from current analysis `analysisDetails`), same as before; only main/current analysis index is downloaded.
   - `settings.exportCsvInputs` defaults true (with `liveS3InputsByAnalysisId`).  
   **Legacy instrument* parquet:** multi-parquet under `output/` — `docs/INPUT_LAYOUT_OUTPUT_PARQUET_LEGACY.md`. Local runs can still use parquet if `{category}.csv` is absent (`model.py` falls back to `**/*.parquet`).

2. Callback mode  
   When `settingsCallbackUrl` is present in the run parameters, the callback is run (no need for `settingsCallbackUrl` in `datasets.settings`). That populates `inputPaths` and `analysisIds`.

3. getSourceInputFiles (iosession.py)  
   - **Export CSV mode (default for live S3 per analysis):** download fixed keys under `export/...` for instrument* CSVs and `analysisDetails.json`. **Macro:** list/download parquet from `input/macroeconomicVariableInput/asofdate=.../BASE/` (not export). See `docs/INPUT_SOURCES.md`.
   - create_io_directories creates local dirs for `instrumentResult`, `instrumentReporting`, `instrumentReference`, and `macroEconomicVariableInput` from `analysisIds` when using per-analysis inputs.
   - Non-callback path: still uses `inputPath` with `custInputs` and downloads from execution input.

4. Report step (model.py)  
   - build_quarterly_summary_report() (Step 3): Builds `quarterly_summary_report.json`. Resolves current/prior/priorYear and chronological quarters from analysisDetails dates via `_resolve_analysis_roles_from_dates()`; then uses `_get_analysis_roles()`. Section (1) Changes to ACL uses prior/current reserves and chargeOffs/recoveries/provision from instrumentReporting. Sections (2)–(3) join instrumentResult with instrumentReference on instrumentIdentifier for ascImpairmentEvaluation. Helper: `_load_parquet_for_analysis()` (loads `{category}.csv` when present, else parquet glob), `_find_column()`, `_filter_summary_scenario()`, `_safe_sum()`.
   - build_hanmi_acl_quarterly_report() (Step 3b): Builds `hanmi_acl_quarterly_report.json` with sections per `docs/REPORT_MAPPING.md` (segmentMethodology, collectivelyEvaluatedByMethodology, quantitativeLossRatesBySegment, netChargeOffsQuarterly, qualitativeReservesBySegment, macroeconomicBaseline from macroEconomicVariableInput, individualAnalysis, unfundedBySegment, unfundedTrend; parametersInventory/peerRatios/hanmiSummaryMetrics stubbed).
   - analysisDetails (iosession.py): For each analysis ID, download `export/analysisidentifier={id}/analysisDetails.json` into report dir as `analysisDetails_{id}.json` (getSourceInputFiles, with category CSVs).
   - create_report_export_zip() (Step 4): Zips all files in report dir (including `hanmi_acl_quarterly_report.json`) into `report_dir/report_export.zip`; zip is then uploaded with the rest of the report output.

5. Tests  
   `tests/test_model.py`: unit tests for Model helpers, _get_analysis_roles, build_quarterly_summary_report, build_hanmi_acl_quarterly_report, create_report_export_zip, run.py argument parsing. Run: `python -m pytest tests/ -v`. No R; all logic is Python.

6. Interactive API (FastAPI)  
   `runner/api/main.py` + `process.py`: `POST {api.prefix}/v1/execute` with Bearer JWT; body is either `modelRunParameter` (+ optional `settingsPatch`) or `mrpS3Key`. Calls `run_model_batch(..., return_model=True)`; returns merged `report_response_payload` when `returnReportsInResponse` is set. Image: `dockerbuild/api/Dockerfile`. HOCON: `conf/application.conf`. `model/__init__.py` lazy-loads `Model` so importing the API stack does not require moodyscappy until a run executes.

7. Library entry — `interactive_run`  
   `model/interactive.py`: `interactive_run(jwt, analysis_ids, ...)` uses `build_interactive_mrp()` (`libraryMode`, `liveS3InputsByAnalysisId`, `returnReportsInResponse`), temp `modelRunParameter.json`, then `run_model_batch`. **`settings.libraryMode`**: `Model.run()` skips zip, Step 5–6 S3 uploads, and log upload in `cleanUp()`; only JWT-driven S3 **downloads** (inputs). Returns **one dict** (one JSON): `quarterly_summary_report` + `hanmi_acl_quarterly_report` (or `null`). Tests: `tests/test_interactive.py`.

8. Listing / diagnostics  
   - Step 1: `list_and_print_s3_folders()` lists bucket/prefix (from `inputPath`).
   - In callback branch: list and print files under each `output/.../scenarioidentifier=Summary/` for each analysis ID.

9. Print logging  
   Steps and instrumentResult read/aggregate/write are logged with `[Model run]` / `[instrumentResult]` / `[getSourceInputFiles]` prefixes.

---

## Key paths and constants

| Where | What |
|-------|------|
| iosession.py | `EXPORT_BASE = "export"` (primary CSV + analysisDetails); `OUTPUT_ROOT_BASE` / `SCENARIO_SUMMARY_SEGMENT` kept for docs/legacy only |
| iosession | Per-analysis download: fixed keys under `export/...`; `_downloadFile()` uses Cappy or boto3; listing helpers still used for diagnostics |
| model.py | `build_quarterly_summary_report()` (Step 3); `_load_parquet_for_analysis()` (CSV preferred, parquet fallback), `_find_column()`, `_filter_summary_scenario()`, `_safe_sum()` |
| Report dir | `io_session.local_directories['outputPaths']['report']` |
| Datamodel | `datamodel/ImpairmentStudio-DataDictionary.csv` — Instrument Result: `analysisIdentifier`, `assetClass`, `lossAllowanceDelta` |
| Column lookup | `docs/DATAMODEL_COLUMNS.md` — Report section → category → canonical attribute name. All report/debug lookups use `_resolve_column(df, canonical_name, *variants)` so parquet with lowercase/spaced column names still match. |

---

## Column resolution (avoid empty sections)

- Canonical names come from `datamodel/ImpairmentStudio-DataDictionary.csv` (Attribute Name column). Parquet may expose columns as lowercase (e.g. `portfolioidentifier`) or with spaces; the code tries canonical first, then `_resolve_column` with variants (`portfolioidentifier`, `Portfolio Identifier`, `portfolio_identifier` etc.).
- Use `_resolve_column` for any report-related column lookup (portfolio, ascImpairmentEvaluation, lossRateModelName, pdModelName, lgdModelName). Use `_find_column` for join keys (instrumentIdentifier, analysisIdentifier) and scenarioIdentifier (case-insensitive match is enough).
- Same resolution in debug: `_build_debug_all_data_summary` uses `_resolve_column` for port/asc/lr/pd/lgd so debug and report find the same columns. If segmentMethodology is empty but debug_all_data_summary has groupBy data, the run was likely using an older build; re-run with current code.

---

## Run parameters (from wrapper)

- `settings.analysisIds`: list of analysis IDs.
- `settings.settingsCallbackUrl`: if present, callback runs and returns `inputPaths` (and optionally more).
- `settings.inputPath`: execution input prefix (e.g. `apps/reports/executionId=...-report/input`); used for custInputs and for Step 1 listing.
- `settings.outputPaths.report`: target for report outputs (JSON files).
- `settings.fetchAdjustmentDetails` (default **true**): after reports, GET Impairment Studio [`/adjustment/1.0/analyses/{currentAnalysisId}/adjustmentdetails`](https://qa-api.impairmentstudio.moodysanalytics.net/adjustment/docs/swagger-ui/index.html) with the same Bearer JWT as Cappy; writes `adjustment_details.json` (array or error stub). Override base URL with env **`IMPAIRMENT_STUDIO_API_BASE`** (default QA host). `interactive_run` adds top-level **`adjustment_details`** to the returned JSON.

---

## Dependencies

- Python: moodyscappy (Cappy), pandas, pyarrow (for `read_parquet`), `requests` (adjustment API + callback).
- Auth: JWT or username/password via `-j` / `-u`; Cappy uses it for S3.
- Batch entry point: `model/run.py` → `run_model_batch()` (ManagedBatch / S3 or local MRP). For API-style runs without a settings callback, set `settings.liveS3InputsByAnalysisId: true` with `settings.analysisIds` so parquet is fetched live from S3 (JWT). To return all report `*.json` files in an API response, set `settings.returnReportsInResponse: true`; after `run()`, read `model.report_response_payload` or use `run_model_batch(..., return_model=True)`. Details: `docs/HANMI_BATCH_AND_INTERACTIVE.md`.

---

## If you change behavior

- Change S3 input path: Edit `OUTPUT_ROOT_BASE` / `SCENARIO_SUMMARY_SEGMENT` in `iosession.py` and the prefix built in `getSourceInputFiles`.
- Change aggregation or report format: Edit `build_quarterly_summary_report()` in `model.py`; section contract and field names from `sample/useCase.txt`; column names follow datamodel (case-insensitive).
- Add another output category: Ensure `meta/model.json` and run parameters define the output path; write files under `local_directories['outputPaths'][key]` so upload picks them up.

---

## Planned / Hanmi ACL report (see docs/)

- Target report: Hanmi3 Q4 2025 ACL Quarterly Analysis and Supplemental Exhibits. Full mapping and JSON section contract: `docs/REPORT_MAPPING.md`.
- Optional analysis metadata: Input file (e.g. under `inputPath`) may define which analysis is current, prior, prior year, and quarters; see `docs/INPUT_SOURCES.md`.
- macroEconomicVariableInput: Path `input/macroeconomicVariableInput/asofdate=YYYY-MM-DD/scenarioidentifier=BASE/`; asOfDate from analysisDetails for main analysis (current): `scenarios[].name === "BASE"` → `asOfDate` (see `sample/analysisDetails.json`). Variable names from `sample/macroeconomicVariable.csv`.
- Report logic: instrumentResult filtered to Summary scenario only (already done via `_load_parquet_for_analysis(..., filter_summary=True)`). Joins: result/reporting to instrumentReference on instrumentIdentifier. Attribute precedence: reporting > result > reference.
- Implementation phases: `docs/IMPLEMENTATION_PLAN.md` (metadata, macro input, new report sections, tests).
