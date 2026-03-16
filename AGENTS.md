# Model Report Generator – Agent Context

Compact context for agents. See README.md for full project layout.

---

## What this repo does

- **Entry:** `model/run.py` — run with `-j JWT -s <s3_key_to_modelRunParameter.json>` for S3 mode; `-L <local_folder>` for local.
- **Flow:** Load model run parameters → list/fetch inputs from S3 (Cappy) → process instrumentResult → write report outputs → upload outputs to S3.
- **Report output:** **Quarterly summary:** `quarterly_summary_report.json` (structured JSON for all 6 doc sections per `sample/useCase.txt`). Plus `analysisDetails_{id}.json` per analysis (from `export/analysisidentifier={id}/analysisDetails.json`). `report_export.zip` contains all report files. **Current/prior/priorYear and quarter order** are derived from **analysisDetails.reportingDate** (no metadata file); first analysisId in the array is not assumed to be latest.

---

## What’s implemented (done so far)

1. **S3 input source (single path)**  
   Inputs are read only from bucket-root **`output/`**, same analysisIdentifier pattern for all three categories:
   - **instrumentResult:** `output/instrumentResult/analysisidentifier={id}/scenarioidentifier=Summary/` (parquet under that prefix, e.g. `.../adjusted=true/*.snappy.parquet`).
   - **instrumentReporting:** `output/instrumentReporting/analysisidentifier={id}/` — parquet files directly under that prefix (no extra partition).
   - **instrumentReference:** `output/instrumentReference/analysisidentifier={id}/` — partitioned by portfolio; all parquet from all subfolders are downloaded (subfolder structure preserved locally to avoid overwrites).
   - **macroEconomicVariableInput:** `input/macroeconomicVariableInput/asofdate=YYYY-MM-DD/scenarioidentifier=BASE/` — asOfDate from main analysis’s analysisDetails (BASE scenario); downloaded when callback is True.

2. **Callback mode**  
   When `settingsCallbackUrl` is present in the run parameters, the callback is run (no need for `settingsCallbackUrl` in `datasets.settings`). That populates `inputPaths` and `analysisIds`.

3. **getSourceInputFiles (iosession.py)**  
   - **instrumentResult:** For each analysis ID, list/download from `output/instrumentResult/analysisidentifier={id}/scenarioidentifier=Summary/` into that analysis’s local `instrumentResult` dir.
   - **instrumentReporting:** For each analysis ID, list all `*.parquet` under `output/instrumentReporting/analysisidentifier={id}/` (no subfolder) and download into `inputPaths/instrumentReporting` dir per analysis.
   - **instrumentReference:** For each analysis ID, list all `*.parquet` under `output/instrumentReference/analysisidentifier={id}/` (including all `portfolioidentifier=.../` subfolders) and download into `inputPaths/instrumentReference` dir per analysis; relative subpath preserved locally to avoid name clashes.
   - **create_io_directories** creates local dirs for `instrumentResult`, `instrumentReporting`, `instrumentReference`, and `macroEconomicVariableInput` from `analysisIds` when callback is True.
   - Non-callback path: still uses `inputPath` with `custInputs` and downloads from execution input.

4. **Report step (model.py)**  
   - **build_quarterly_summary_report()** (Step 3): Builds `quarterly_summary_report.json`. Resolves current/prior/priorYear and chronological quarters from analysisDetails dates via `_resolve_analysis_roles_from_dates()`; then uses `_get_analysis_roles()`. Section (1) Changes to ACL uses prior/current reserves and chargeOffs/recoveries/provision from **instrumentReporting**. Sections (2)–(3) join **instrumentResult** with **instrumentReference** on instrumentIdentifier for ascImpairmentEvaluation. Helper: `_load_parquet_for_analysis()`, `_find_column()`, `_filter_summary_scenario()`, `_safe_sum()`.
   - **build_hanmi_acl_quarterly_report()** (Step 3b): Builds `hanmi_acl_quarterly_report.json` with sections per `docs/REPORT_MAPPING.md` (segmentMethodology, collectivelyEvaluatedByMethodology, quantitativeLossRatesBySegment, netChargeOffsQuarterly, qualitativeReservesBySegment, macroeconomicBaseline from **macroEconomicVariableInput**, individualAnalysis, unfundedBySegment, unfundedTrend; parametersInventory/peerRatios/hanmiSummaryMetrics stubbed).
   - **analysisDetails (iosession.py):** For each analysis ID, download `export/analysisidentifier={id}/analysisDetails.json` (bucket root) into report dir as `analysisDetails_{id}.json` (done in getSourceInputFiles after parquet download).
   - **create_report_export_zip()** (Step 4): Zips all files in report dir (including `hanmi_acl_quarterly_report.json`) into `report_dir/report_export.zip`; zip is then uploaded with the rest of the report output.

5. **Tests**  
   `tests/test_model.py`: unit tests for Model helpers, _get_analysis_roles, build_quarterly_summary_report, build_hanmi_acl_quarterly_report, create_report_export_zip, run.py argument parsing. Run: `python -m pytest tests/ -v`. No R; all logic is Python.

6. **Listing / diagnostics**  
   - Step 1: `list_and_print_s3_folders()` lists bucket/prefix (from `inputPath`).
   - In callback branch: list and print files under each `output/.../scenarioidentifier=Summary/` for each analysis ID.

7. **Print logging**  
   Steps and instrumentResult read/aggregate/write are logged with `[Model run]` / `[instrumentResult]` / `[getSourceInputFiles]` prefixes.

---

## Key paths and constants

| Where | What |
|-------|------|
| **iosession.py** | `OUTPUT_ROOT_BASE = "output"`, `SCENARIO_SUMMARY_SEGMENT = "scenarioidentifier=Summary"`, `EXPORT_BASE = "export"` (for analysisDetails) |
| **iosession** | `_list_s3_at_prefix()`, `_get_s3_object_keys()` for listing; `_downloadFile()` uses Cappy |
| **model.py** | `build_quarterly_summary_report()` (Step 3); `_load_parquet_for_analysis()`, `_find_column()`, `_filter_summary_scenario()`, `_safe_sum()` |
| **Report dir** | `io_session.local_directories['outputPaths']['report']` |
| **Datamodel** | `datamodel/ImpairmentStudio-DataDictionary.csv` — Instrument Result: `analysisIdentifier`, `assetClass`, `lossAllowanceDelta` |
| **Column lookup** | **`docs/DATAMODEL_COLUMNS.md`** — Report section → category → canonical attribute name. All report/debug lookups use **`_resolve_column(df, canonical_name, *variants)`** so parquet with lowercase/spaced column names still match. |

---

## Column resolution (avoid empty sections)

- **Canonical names** come from `datamodel/ImpairmentStudio-DataDictionary.csv` (Attribute Name column). Parquet may expose columns as lowercase (e.g. `portfolioidentifier`) or with spaces; the code tries canonical first, then **`_resolve_column`** with variants (`portfolioidentifier`, `Portfolio Identifier`, `portfolio_identifier` etc.).
- **Use `_resolve_column`** for any report-related column lookup (portfolio, ascImpairmentEvaluation, lossRateModelName, pdModelName, lgdModelName). Use **`_find_column`** for join keys (instrumentIdentifier, analysisIdentifier) and scenarioIdentifier (case-insensitive match is enough).
- **Same resolution in debug:** `_build_debug_all_data_summary` uses `_resolve_column` for port/asc/lr/pd/lgd so debug and report find the same columns. If segmentMethodology is empty but debug_all_data_summary has groupBy data, the run was likely using an older build; re-run with current code.

---

## Run parameters (from wrapper)

- `settings.analysisIds`: list of analysis IDs.
- `settings.settingsCallbackUrl`: if present, callback runs and returns `inputPaths` (and optionally more).
- `settings.inputPath`: execution input prefix (e.g. `apps/reports/executionId=...-report/input`); used for custInputs and for Step 1 listing.
- `settings.outputPaths.report`: target for report outputs (JSON files).

---

## Dependencies

- **Python:** moodyscappy (Cappy), pandas, pyarrow (for `read_parquet`).
- **Auth:** JWT or username/password via `-j` / `-u`; Cappy uses it for S3.

---

## If you change behavior

- **Change S3 input path:** Edit `OUTPUT_ROOT_BASE` / `SCENARIO_SUMMARY_SEGMENT` in `iosession.py` and the prefix built in `getSourceInputFiles`.
- **Change aggregation or report format:** Edit `build_quarterly_summary_report()` in `model.py`; section contract and field names from `sample/useCase.txt`; column names follow datamodel (case-insensitive).
- **Add another output category:** Ensure `meta/model.json` and run parameters define the output path; write files under `local_directories['outputPaths'][key]` so upload picks them up.

---

## Planned / Hanmi ACL report (see docs/)

- **Target report:** Hanmi3 Q4 2025 ACL Quarterly Analysis and Supplemental Exhibits. Full mapping and JSON section contract: **`docs/REPORT_MAPPING.md`**.
- **Optional analysis metadata:** Input file (e.g. under `inputPath`) may define which analysis is current, prior, prior year, and quarters; see **`docs/INPUT_SOURCES.md`**.
- **macroEconomicVariableInput:** Path `input/macroeconomicVariableInput/asofdate=YYYY-MM-DD/scenarioidentifier=BASE/`; **asOfDate** from **analysisDetails** for main analysis (current): `scenarios[].name === "BASE"` → `asOfDate` (see `sample/analysisDetails.json`). Variable names from **`sample/macroeconomicVariable.csv`**.
- **Report logic:** instrumentResult filtered to **Summary** scenario only (already done via `_load_parquet_for_analysis(..., filter_summary=True)`). Joins: result/reporting to instrumentReference on **instrumentIdentifier**. Attribute precedence: reporting > result > reference.
- **Implementation phases:** **`docs/IMPLEMENTATION_PLAN.md`** (metadata, macro input, new report sections, tests).
