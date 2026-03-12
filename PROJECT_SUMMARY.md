# Model Report Generator – Project Summary & Template Notes

This document summarizes what the project does, how it works, and how to reuse it as a template for other report/model work.

---

## 1. Purpose and Scope

**What it does:** A Python model that runs inside a wrapper (e.g. ImpairmentStudio). It fetches input data from S3 (or local), builds a **quarterly summary report** (Allowance for Credit Losses), and uploads report outputs back to S3.

**Domain:** Financial reporting (ACL – Allowance for Credit Losses). Report structure and field mappings are defined in `sample/useCase.txt` and follow the datamodel in `datamodel/ImpairmentStudio-DataDictionary.csv`.

**Use as template:** The same pattern (load MRP → fetch inputs from S3 → run business logic → write outputs → upload) can be reused for other report or analytics models by swapping the report logic and keeping the I/O and run flow.

---

## 2. High-Level Architecture

```
run.py (entry)
    → config (logging, env)
    → Model(credentials, proxy, model_run_parameters_path, local_mode)
        → IOSession (S3/local I/O, temp dirs, MRP parsing)
        → model.run()
            Step 1: List S3 (diagnostics)
            Step 2: getSourceInputFiles() – download inputs
            Step 3: build_quarterly_summary_report() – produce report JSON
            Step 4: create_report_export_zip() – zip report dir
            Step 5: createLocalModelRunParameters() – write local MRP
            Step 6: createFileDicts + uploadFiles – upload outputs
        → model.cleanUp()
```

- **run.py:** CLI, config, instantiate `Model`, run, cleanup, exit code.
- **Model (model/model.py):** Orchestrates the run; owns report-building logic and helpers.
- **IOSession (model/iosession.py):** S3/list/download/upload via Cappy; temp dirs; parses and stores model run parameters (MRP).

---

## 3. Entry Point and Invocation

**Entry:** `model/run.py`

**Required arguments:**
- **Mode (one of):** `-s <s3_key>` (S3) or `-L <local_path>` (local).
- **Credentials (one of):** `-j <jwt>` or `-u <username> <password>`.

**Optional:** `-l` loglevel, `-k` keeptemp, `-o`/`-c` config file, `-t`/`-p` proxy.

**Examples:**
```bash
# S3 (typical when run by wrapper)
python model/run.py -j <JWT> -s <s3_key_to_modelRunParameters.json>

# Local (testing)
python model/run.py -L <path_to_folder_with_modelRunParameters.json> -j <JWT>
```

The wrapper provides the path to **model run parameters** (JSON). That JSON contains `settings` (e.g. `analysisIds`, `inputPath`, `outputPaths`, `settingsCallbackUrl`, `inputPaths` after callback) and is used to decide where to read inputs and write outputs.

---

## 4. Modes and Input Sources

### 4.1 Local vs S3

- **local_mode=True** (`-L`): MRP path is a local file; “download” is a local copy; no real S3.
- **local_mode=False** (`-s`): MRP is an S3 key; Cappy is used for all S3 access (list, get, put).

### 4.2 Callback vs non-callback

- **Callback:** If `settings.settingsCallbackUrl` is present, the model **POSTs** the MRP to that URL and merges the response into `settings`. The response typically adds `inputPaths` (per-category S3 path lists) and may add or confirm `analysisIds`. The model then uses **fixed bucket-root paths** (see below) for actual data, not the raw callback paths for parquet.
- **Non-callback:** Uses `settings.inputPath` and, when present, `inputPath.custInputs` (list of local paths after download). **Payload files under `inputPath` are optional:** if the S3 prefix is empty, `create_io_directories` uses `resp.get('Contents') or []` so the run does not fail.

### 4.3 Where input data really comes from (callback mode)

All real input data is read from **bucket-root** paths, not from the execution “input” folder (that folder is only for optional payload files):

| Category            | S3 pattern (bucket root)                                                                 | Notes |
|---------------------|-------------------------------------------------------------------------------------------|--------|
| instrumentResult    | `output/instrumentResult/analysisidentifier={id}/scenarioidentifier=Summary/`            | Parquet; subdirs e.g. `adjusted=true/`. |
| instrumentReporting | `output/instrumentReporting/analysisidentifier={id}/`                                     | Parquet directly under prefix. |
| instrumentReference | `output/instrumentReference/analysisidentifier={id}/`                                     | Parquet under `portfolioidentifier=.../` subdirs; all collected. |
| analysisDetails     | `export/analysisidentifier={id}/analysisDetails.json`                                     | One JSON per analysis. |

Constants: `OUTPUT_ROOT_BASE = "output"`, `SCENARIO_SUMMARY_SEGMENT = "scenarioidentifier=Summary"`, `EXPORT_BASE = "export"` (in `iosession.py`).

---

## 5. Run Flow (What Happens Step by Step)

1. **Load MRP:** IOSession downloads (or copies) the model run parameters JSON and parses it. Creates a temp directory and builds `local_directories` (inputPath, outputPaths, logPath; and when callback, inputPaths per category and per analysis ID).
2. **Optional payload:** For non-callback, lists S3 at `inputPath`; if there are objects, their keys are in `custInputs` and get downloaded. If there are **no** objects, `Contents` is missing/empty and the code uses `[]` so no failure.
3. **List S3:** Step 1 lists the bucket/prefix for diagnostics.
4. **getSourceInputFiles (callback):** For each `analysisId`, downloads parquet from the three categories above and writes into `local_directories['inputPaths'][category][idx]`. Downloads each `analysisDetails.json` into the report output dir as `analysisDetails_{id}.json`.
5. **build_quarterly_summary_report:** Uses **first** analysisId as current, **second** as prior. Loads parquet via `_load_parquet_for_analysis(category, id, filter_summary=...)`, joins instrumentResult with instrumentReference where needed, aggregates per section, writes `quarterly_summary_report.json` into the report dir.
6. **create_report_export_zip:** Zips all files in the report dir into `report_export.zip` (excluding the zip itself).
7. **createLocalModelRunParameters:** Writes a new MRP JSON under the temp dir with paths replaced by local temp paths.
8. **createFileDicts + uploadFiles:** Walks the temp dir, builds file dicts, and uploads each to the corresponding S3 output path from MRP (e.g. `outputPaths.report`).
9. **cleanUp:** Deletes temp dirs (unless `keep_temp`) and optionally uploads log file.

---

## 6. Outputs Produced

All under the **report** output path (from `settings.outputPaths.report`):

- **quarterly_summary_report.json** – Structured report with 6 sections (changes to ACL, collectively evaluated, individually evaluated, total reserve, unfunded, QTD credit loss). Uses current/prior analysis IDs and datamodel field names.
- **analysisDetails_{id}.json** – One per analysis ID (from `export/analysisidentifier={id}/analysisDetails.json`).
- **report_export.zip** – Zip of all of the above (and any other files in the report dir).

---

## 7. Key Modules and Files

| Path | Role |
|------|------|
| **model/run.py** | CLI, config load, Model instantiation, run, cleanup, exit code. |
| **model/model.py** | `Model`: run loop, `build_quarterly_summary_report`, `create_report_export_zip`, `createLocalModelRunParameters`, helpers (`_load_parquet_for_analysis`, `_find_column`, `_filter_summary_scenario`, `_safe_sum`). |
| **model/iosession.py** | `IOSession`, `ModelRunParameters`; `create_io_directories` (temp dirs, optional payload via `Contents` or []); `getSourceInputFiles` (callback: download parquet + analysisDetails; non-callback: custInputs); `list_and_print_s3_folders`; `createFileDicts`; `uploadFiles`; `_get_s3_object_keys`, `_downloadFile`, etc. |
| **config/config.py** | Logging and env config (e.g. from `local.ini`). |
| **sample/useCase.txt** | Report instructions and section/field contract for the quarterly summary. |
| **datamodel/ImpairmentStudio-DataDictionary.csv** | Canonical attribute names (e.g. for instrumentResult, instrumentReporting, instrumentReference). |
| **meta/model.json** | Model metadata (name, outputs, permissions). |

---

## 8. Configuration and Parameters (from wrapper)

- **settings.inputPath** – S3 prefix for “execution input” (optional payload); also used for listing in Step 1.
- **settings.outputPaths** – Map of output category → S3 path (e.g. `report`, `error`).
- **settings.settingsCallbackUrl** – If present, callback is run and response is merged into settings.
- **settings.analysisIds** – List of analysis IDs (current = first, prior = second for the report).
- **settings.inputPaths** – After callback, per-category list of paths (used to create local dirs and to know how many analyses); actual download uses the fixed bucket-root paths above.

---

## 9. Tests

- **Location:** `tests/test_model.py`.
- **Run:** From repo root: `python -m pytest tests/ -v`.
- **Scope:** Model helpers (`_find_column`, `_safe_sum`, `_filter_summary_scenario`), `build_quarterly_summary_report` (skip when no callback/analysisIds; writes JSON when callback+ids), `create_report_export_zip`, and run.py argument parsing. Cappy and boto3 are mocked so tests need no S3 or moodyscappy.

---

## 10. Template Notes for Future Work

**Reuse as-is:**
- Entry and CLI (`run.py`), config loading, Model/IOSession split.
- IOSession: temp dirs, MRP parsing, optional payload (`Contents` or []), callback vs non-callback, S3 list/download/upload via Cappy, `createFileDicts`/upload.
- Run skeleton: list → getSourceInputFiles → [your step] → zip (optional) → local MRP → upload.

**Customize per new project:**
- **S3 paths:** Change `OUTPUT_ROOT_BASE`, `SCENARIO_SUMMARY_SEGMENT`, `EXPORT_BASE` and the prefix logic in `getSourceInputFiles` if your data lives elsewhere.
- **Input categories:** Adjust which categories are created in `create_io_directories` and which are downloaded in `getSourceInputFiles` (and their path shapes).
- **Report/business logic:** Replace or extend `build_quarterly_summary_report` with your own aggregation/report; keep using `_load_parquet_for_analysis` (or similar) and `local_directories['outputPaths'][...]` for writing.
- **Outputs:** Write into the appropriate `outputPaths` dirs so `createFileDicts` and `uploadFiles` pick them up; add or drop the zip step as needed.
- **Datamodel/spec:** Point to your own use-case doc and data dictionary; keep column resolution case-insensitive if you use the same helper pattern.

**Important behaviors to preserve when forking:**
- Treat **payload under `inputPath` as optional** (use `resp.get('Contents') or []` when listing).
- Keep **callback detection** (e.g. presence of `settingsCallbackUrl`) and merge of callback response into settings.
- Keep **local_mode** so the same code path can run from a local folder for tests.

---

## 11. Dependencies

- **Python 3.x**
- **pandas**, **pyarrow** (parquet)
- **moodyscappy (Cappy)** for S3 when not in local mode (often provided by the runtime)
- **pytest** for tests
- **config** and env (e.g. `config/local.ini`) for logging and environment

---

## 12. Short Reference

| Concept | Where / How |
|--------|--------------|
| Entry | `model/run.py`; `-s` or `-L` + `-j` or `-u` |
| Input data (callback) | `output/` and `export/` at bucket root; per-category, per–analysis ID |
| Optional payload | `inputPath` prefix; empty → `Contents` or [] → no failure |
| Report output dir | `local_directories['outputPaths']['report']` |
| Current/prior | First two `analysisIds` |
| Add new output category | Write under `outputPaths[category]`; ensure MRP defines that path |

This summary plus `AGENTS.md` and `README.md` give a full picture of the project and how to use it as a template.
