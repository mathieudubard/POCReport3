# Hanmi report model: batch today vs Interactive (API) mode

## What works today (ManagedBatch)

- Entry: `python model/run.py` with `-s` (S3 key to `modelRunParameter.json`) or `-L` (local folder with MRP), plus JWT or username/password.
- `Model` uses `iosession.IOSession` + **Cappy** to list/download **parquet** (instrumentResult, instrumentReporting, instrumentReference), optional **callback** (`settingsCallbackUrl`) for `analysisIds` / `inputPaths`, **analysisDetails** from export, macro CSV paths, then builds JSON reports and **uploads** outputs to S3.
- Registry: `meta/model.json` uses **`apiType`: `ManagedBatch`** (Blaze passes an MRP path; inputs live on S3).

This path is unchanged by the refactor in `model/run.py` (`run_model_batch`).

## Metadata-only API / Interactive (no input tables in HTTP body)

If you only pass **metadata** over HTTP (e.g. `name`, **`analysisIds`**, optional `analyses` / roles) and use **JWT** so Cappy reads **live from S3** (same bucket layout as today: `output/instrumentResult/...`, `export/.../analysisDetails.json`, etc.), you do **not** need parquet rows in the payload.

Implemented in code:

- **`settings.liveS3InputsByAnalysisId`**: when `true` and **`settings.analysisIds`** is non-empty, the model uses the **same per-analysis S3 download path** as after a successful **settings callback** (`getSourceInputFiles` + report steps), **without** calling `settingsCallbackUrl`.
- **`settings.inputPaths`** is optional in that mode: if omitted or empty, local temp dirs are still created per analysis for `instrumentResult`, `instrumentReporting`, `instrumentReference`, and `macroEconomicVariableInput`.

You still need a valid **modelRunParameter.json** shape for everything else (`datasets`, `outputPaths`, `inputPath` execution prefix, `logPath`, dates, etc.) — either loaded from S3 (`-s`) as today or merged from the API body into a full MRP before `IOSession` runs.

`ModelRunParameters.use_per_analysis_s3_download()` is true when **`callBack`** is true **or** when **`liveS3InputsByAnalysisId`** + **`analysisIds`** are set (see `model/iosession.py`).

### Returning report JSON in the HTTP response

Set **`settings.returnReportsInResponse`: `true`** in the merged MRP/settings. After Steps 3–4, the model loads every **`.json`** file in the report output directory (e.g. `quarterly_summary_report.json`, `hanmi_acl_quarterly_report.json`, `analysisDetails_*.json`, `debug_all_data_summary.json`) into:

- **`model.report_response_payload`** → `{ "reports": { "<filename>": <parsed JSON object>, ... } }`

The binary **`report_export.zip`** is not embedded (redundant with the JSON entries). Parse errors become `{ "_parseError": "...", "_file": "<filename>" }`.

From a wrapper, call **`run_model_batch(args, return_model=True)`** to get **`(exit_code, model)`**, then if **`exit_code == 0`** return **`model.report_response_payload`** as the API body (or merge with your envelope). CLI batch behavior is unchanged when **`return_model`** is false (default).

## What Interactive mode means elsewhere (reference)

The Public Firm **API branch** (`model-ttc2pit-public` / `ci-v-9-1-0-5-api-mode`) uses:

- **FastAPI** (`runner/api`), **`commonfastapi`** (JWT against SSO), **`conf/application.conf`**
- **`modelexecutor.get_session(..., payload_api_mode=...)`** with **`APIModeSession`**: settings + tabular **`data`** in the HTTP body; **`get_final_response()`** for JSON output
- **Separate API Dockerfile** + **CircleCI** second image; CAP registration with **`apiType`: `Interactive`** and a **`/v1/run`** URL

For side-by-side study, clone or open the **`model-ttc2pit-public`** (Public Firm) API branch in a separate workspace — it is not vendored in this repo.

## Gap for Hanmi (why Interactive is not a small copy-paste)

| Area | Public Firm (API branch) | Hanmi (this repo) |
|------|--------------------------|-------------------|
| I/O abstraction | **`modelexecutor.IOSession`** (`get_settings`, `get_dataframe`, `upload_*`) | **Custom `iosession.IOSession`** (Cappy, parquet paths, callback) |
| Inputs | Row lists per category in JSON | **Large parquet** trees under `output/…`, **per-analysis** dirs |
| Orchestration | Single **`Model(io_session)`** around converter | **Steps 1–6**: list S3, download, two report builders, zip, upload |

To support **true** Interactive (body = settings + data, response = JSON, no S3 input):

1. **Either** migrate to **`modelexecutor`** with an implementation that can hydrate Hanmi’s expected layout from the payload (heavy),  
2. **Or** add a **parallel session** that writes payload tables to temp parquet / fake `inputPaths` and reuses as much of `getSourceInputFiles` as possible (still large; payload design is non-trivial for multi-analysis parquet).

So: **one Git repo can ship both batch and API** (two Dockerfiles, two registrations), but Hanmi **business logic** stays shared; the **extra work** is almost all **I/O and packaging**, not `build_quarterly_summary_report` / `build_hanmi_acl_quarterly_report`.

## Recommended direction (single code version, two registrations)

1. Keep **`ManagedBatch`** as today: `run_model_batch()` + current Dockerfile/CI.
2. **Interactive (metadata + live S3)**: use the FastAPI service in this repo (no **`modelexecutor`** required for that pattern).
3. Register a second version with **`apiType`: `Interactive`** and processor URL to **`…/v1/execute`** (see internal Confluence “Enabling API (Interactive) Mode”).

## FastAPI service (Interactive shell)

| Piece | Location |
|-------|----------|
| App + lifespan (SSO JWKs, model `config` bootstrap) | `runner/api/main.py` |
| **`POST …/v1/execute`** — inline `modelRunParameter` or `mrpS3Key`; JWT from `Authorization` | `runner/api/process.py` |
| JWT (RS256) vs SSO **`/auth/certs`** | `commonfastapi/security.py`, `commonfastapi/sso.py` |
| **`GET …/v1/ping`** | `commonfastapi/health.py` |
| HOCON: **`api.prefix`**, **`url.sso`** | `conf/application.conf` (env: **`API_PREFIX`**, **`GLOBAL_SSO_API_SERVICE_URL`**) |
| API image | `dockerbuild/api/Dockerfile`; deps: `dockerbuild/api-requirements.txt` + **moodyscappy** (Cappy) from Git |

Default **`api.prefix`** is **`/model-report-generator`** → **`POST /model-report-generator/v1/execute`**. Response body is **`{ "exitCode": 0, "reports": { ... } }`** when **`settings.returnReportsInResponse`** is true (see above).

## Code hook

- **`model.run_model_batch(args)`** — single entry for CLI batch execution; same behavior as before. The API calls **`run_model_batch(args, return_model=True)`** and returns **`model.report_response_payload`**.

## What we still need from you / platform

- **Deploy** the API image and wire **Helios / CAP** with **`apiType`: `Interactive`** and the public **`/v1/execute`** URL.
- **Tabular data in the HTTP body** instead of S3 remains a separate, larger design (see gap table above).

If you later want **tabular data in the body** instead of S3, that is a separate, larger payload design (see gap table below).
