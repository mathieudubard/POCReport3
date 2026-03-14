# Multi-Quarter Support – One Config, Any Number of Quarters (and Years)

One input structure describes **an arbitrary number of past quarters**, **ordered**. **Tags are optional**: the model can infer current/prior/prior year from each analysis’s **reportingDate** in `analysisDetails`. It supports quarterly tables, yearly tables (by grouping the same analyses by year), and other reports; the model **produces JSON data** only.

---

## 1. Config shape

**analyses**: array of entries in **chronological order** (oldest first). Each entry:

| Field | Required | Description |
|-------|----------|-------------|
| **analysisId** | Yes | Analysis ID (string or number). |
| **quarterLabel** | No | Display label (e.g. `"Q3 2025"`). If omitted, derived from `analysisDetails` → `reportingDate`. |
| **tags** | No | Optional. Array of strings (e.g. `current`, `prior`, `priorYear`). If omitted, the model **infers** roles from dates (see §1.1). |

**Backward compatibility**: if an entry has **role** (string) instead of **tags**, it is treated as `tags: [role]`.

- **Order** = quarter order for multi-quarter tables (e.g. columns Q4 2024 … Q4 2025).
- After normalization the model sets **analysisIds** (same order), **quarters** (full list), and **quarterLabels**. For **analysisRoles** (current/prior/priorYear): if tags are present they are used; otherwise the model **infers** from `analysisDetails` reportingDate when building the report.

### 1.1 Inferring current / prior / prior year from dates

If **tags** are not set (or current/prior/priorYear are missing), the model reads **reportingDate** from `analysisDetails_{analysisId}.json` for each analysis and infers:

- **Current** = analysis with the **latest** reportingDate. If multiple share the same date, the **first in the list** (in `analyses` order) is used.
- **Prior** = analysis whose reportingDate falls in the **calendar quarter immediately before** the current quarter (e.g. current Q4 2025 → prior is Q3 2025).
- **Prior year** = analysis whose reportingDate is in the **same calendar quarter, previous year** (e.g. current Q4 2025 → prior year is Q4 2024).

So you can omit tags and pass only **analysisId** (and optional **quarterLabel**); the model infers roles from dates. You can still supply tags to override or when analysisDetails are not available.

### 1.2 Yearly tables

Reports may include **yearly** tables (e.g. net charge-offs by year, reserves by year). The same **analyses** list is used: each analysis has a **reportingDate** in analysisDetails, so the model can derive **year** (and quarter). For yearly tables, the builder can group analyses by year and output yearly aggregates in the JSON (e.g. one object per year). No separate config is required; the same ordered list of analyses supports both quarterly and yearly views.

---

## 2. Waterfall

- **Config provided** (payload or file): use **analyses** (ordered, optional tags/labels). Model normalizes and builds JSON for all quarters in the list.
- **Config not provided**: use **analysisIds** only. Model infers current = first, prior = second, quarters = `[prior, current]` or `[current]`. Only supports two periods by position.

---

## 3. Examples (same structure, arbitrary N)

### 3.1 Minimal: no tags (inferred from analysisDetails dates)

List analyses in chronological order; roles are inferred from reportingDate.

```json
{
  "analyses": [
    { "analysisId": "4647909", "quarterLabel": "Q3 2025" },
    { "analysisId": "4647997", "quarterLabel": "Q4 2025" }
  ]
}
```

Latest date → current; calendar quarter before → prior; same quarter prior year → priorYear. If multiple share a date, first in list wins.

**Optional**: you can still set **tags** (or legacy **role**) to pin current/prior/priorYear when you don’t want to rely on dates (e.g. analysisDetails not yet available).

---

### 3.2 Three periods (prior year + prior quarter + current, YoY)

```json
{
  "analyses": [
    { "analysisId": "4647000", "quarterLabel": "Q4 2024", "tags": ["priorYear"] },
    { "analysisId": "4647909", "quarterLabel": "Q3 2025", "tags": ["prior"] },
    { "analysisId": "4647997", "quarterLabel": "Q4 2025", "tags": ["current"] }
  ]
}
```

---

### 3.3 Five past quarters (e.g. “quarterly net loss trends by segment for the past five quarters”)

Columns: Q4 2024, Q1 2025, Q2 2025, Q3 2025, Q4 2025. One config.

```json
{
  "analyses": [
    { "analysisId": "4647000", "quarterLabel": "Q4 2024" },
    { "analysisId": "4647100", "quarterLabel": "Q1 2025" },
    { "analysisId": "4647200", "quarterLabel": "Q2 2025" },
    { "analysisId": "4647909", "quarterLabel": "Q3 2025", "tags": ["prior"] },
    { "analysisId": "4647997", "quarterLabel": "Q4 2025", "tags": ["current"] }
  ]
}
```

Tags only where needed for this report (current/prior); others are “just” ordered quarters. Same quarter last year can be tagged for YoY: e.g. add `"tags": ["priorYear"]` on Q4 2024.

---

### 3.4 Arbitrary N quarters (3, 8, 12, …)

Same shape: **analyses** length = N, order = chronological. Use **tags** only when a report needs to identify current/prior/priorYear (or custom roles). All N entries are used for multi-quarter tables and JSON output.

```json
{
  "analyses": [
    { "analysisId": "id1", "quarterLabel": "Q1 2024" },
    { "analysisId": "id2", "quarterLabel": "Q2 2024" },
    { "analysisId": "id3", "quarterLabel": "Q3 2024" },
    { "analysisId": "id4", "quarterLabel": "Q4 2024", "tags": ["priorYear"] },
    { "analysisId": "id5", "quarterLabel": "Q1 2025" },
    { "analysisId": "id6", "quarterLabel": "Q2 2025" },
    { "analysisId": "id7", "quarterLabel": "Q3 2025", "tags": ["prior"] },
    { "analysisId": "id8", "quarterLabel": "Q4 2025", "tags": ["current"] }
  ]
}
```

---

### 3.5 Custom tags for other reports

Tags are strings; this report uses `current`, `prior`, `priorYear`. Other reports can use the same **analyses** and interpret different tags (e.g. `baseline`, `forecast`, `quarterlyTrend`). The normalizer only maps `current`/`prior`/`priorYear` into **analysisRoles**; custom tags remain in the config for downstream use if needed.

---

## 4. Payload: upload a file or callback

- **Upload a file (recommended):** Put **`analysis_metadata.csv`** or **`analysis_metadata.json`** in the execution **input** directory (the path used as run input). The model looks for **CSV first** (helps when the wrapper has issues with JSON), then JSON.
  - **CSV:** `analysis_metadata.csv` with header row and columns **analysisId** (required), **quarterLabel** (optional), **tags** (optional, comma-separated: `current`, `prior`, `priorYear`), **role** (optional, single value). Case-insensitive headers. See `sample/analysis_metadata.csv`.
  - **JSON:** `analysis_metadata.json` with an **`analyses`** array (no `settings` wrapper). See `sample/analysis_metadata.json`.
- **Callback:** Alternatively, the callback can return **analyses** in `settings`. Model normalizes and builds JSON.

**inputPaths** must have one entry per category per analysis, in the same order as **analyses** (from callback or from run parameters).

---

## 5. Output

The model **creates JSON data** only (e.g. `hanmi_acl_quarterly_report.json`). Multi-quarter sections (quantitative by segment, net charge-offs by quarter, unfunded trend, etc.) include one row or column per quarter in **analyses** order, with **quarterLabel** when available. No PDF/HTML is generated here; downstream can render tables (e.g. “quarterly net loss trends by segment”, $ in thousands) from this JSON.

---

## 6. File reference

| Item | Location |
|------|----------|
| Normalize analyses (tags → analysisRoles, quarters, quarterLabels) | `model/iosession.py` → `IOSession.normalize_analyses_to_settings(settings)` |
| Infer current/prior/priorYear from analysisDetails dates | `model/model.py` → `_infer_analysis_roles_from_dates(report_dir, analysis_ids)` |
| Role resolution (current/prior/priorYear from analysisRoles) | `model/model.py` → `_get_analysis_roles()` |
| Quarter list + labels for tables | `model/model.py` → `_get_quarters_for_tables()`, `_get_quarter_label()` |
| Metadata file load | `model/iosession.py` → `_load_analysis_metadata_from_input()` |
| JSON report build | `model/model.py` → `build_hanmi_acl_quarterly_report()` |
| Sample CSV (two quarters) | `sample/analysis_metadata.csv` |
| Sample JSON (two quarters) | `sample/analysis_metadata.json` |
| Sample (5 past quarters) | `sample/analysis_metadata_five_quarters.json` |
