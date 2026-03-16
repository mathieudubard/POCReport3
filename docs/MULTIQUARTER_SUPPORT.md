# Multi-Quarter Support – analysisIds Array + Date-Based Automation

The model uses **only the provided array of analysis IDs** (e.g. from the callback). It does **not** read analysis metadata from files. **Current**, **prior**, **prior year**, **quarter order**, and **quarter labels** are derived automatically by scanning **analysisDetails** (reportingDate) for each analysis. The first ID in the array is **not** assumed to be latest – the model sorts by date. It supports quarterly tables, yearly tables (by grouping by year from reportingDate), and other reports; the model **produces JSON data** only.

---

## 1. Input: array of analysis IDs

- **analysisIds**: provided by the callback (or run parameters). Any order; the model **sorts by reportingDate** from `analysisDetails_{id}.json` to get chronological order.
- **Quarter labels**: derived from each analysis’s **reportingDate** (e.g. `Q2 2025`). No separate metadata file.

### 1.1 Automatic mapping from analysisDetails dates

The model reads **reportingDate** from `analysisDetails_{analysisId}.json` for each analysis and:

- **Sorts** all analysis IDs by date **ascending (oldest first)** → this is the **quarters** list used for multi-quarter tables.
- **Current** = analysis with the **latest** reportingDate (last after sort). First in the provided array is not assumed to be “main”.
- **Prior** = analysis whose reportingDate is in the **calendar quarter immediately before** the current quarter (e.g. current Q2 2025 → prior is Q1 2025).
- **Prior year** = analysis whose reportingDate is in the **same calendar quarter, previous year** (e.g. current Q2 2025 → prior year is Q2 2024).
- **Quarter labels** = `Q{n} {year}` from reportingDate for each analysis.

If an analysis has no analysisDetails or no reportingDate, it is still included in the quarters list (ordered after dated analyses); current/prior/priorYear fall back to the first IDs when no dates are found.

### 1.2 Yearly tables

Reports may include **yearly** tables (e.g. net charge-offs by year, reserves by year). The same **analyses** list is used: each analysis has a **reportingDate** in analysisDetails, so the model can derive **year** (and quarter). For yearly tables, the builder can group analyses by year and output yearly aggregates in the JSON (e.g. one object per year). No separate config is required; the same ordered list of analyses supports both quarterly and yearly views.

---

## 2. Waterfall

- **analysisIds** from callback (or run parameters): model downloads analysisDetails for each, then **resolves** current/prior/priorYear and **chronological quarters** from reportingDate. All quarters in the list are used for multi-quarter tables. No file-based metadata.

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

## 4. Payload: callback (no metadata file)

- The **callback** returns **analysisIds** (and **inputPaths**). The model does **not** read analysis metadata from any file. It uses only the analysisIds array and derives roles and quarter order from **analysisDetails** (reportingDate) after downloading them.
- **inputPaths** must have one entry per category per analysis, in the same order as **analysisIds**.

---

## 5. Output

The model **creates JSON data** only (e.g. `hanmi_acl_quarterly_report.json`). Multi-quarter sections (quantitative by segment, net charge-offs by quarter, unfunded trend, etc.) include one row or column per quarter in **chronological** order (from dates), with **quarterLabel** when available. No PDF/HTML is generated here; downstream can render tables (e.g. “quarterly net loss trends by segment”, $ in thousands) from this JSON.

---

## 6. File reference

| Item | Location |
|------|----------|
| Resolve roles + chronological quarters from analysisDetails dates | `model/model.py` → `_resolve_analysis_roles_from_dates(report_dir, analysis_ids)` |
| Read current/prior/priorYear and quarters from settings | `model/model.py` → `_get_analysis_roles()` |
| Quarter list + labels for tables | `model/model.py` → `_get_quarters_for_tables()`, `_get_quarter_label()` |
| JSON report build | `model/model.py` → `build_hanmi_acl_quarterly_report()` |
