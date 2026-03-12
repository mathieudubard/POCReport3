# Input Sources – Metadata and Macro Variables

Short reference for the **optional analysis metadata** and **macroEconomicVariableInput** used by the Hanmi ACL Quarterly report.

---

## 1. Optional analysis metadata file

- **Purpose:** Define which analysis ID is **current**, **prior**, **prior year**, and optionally more quarters (for multi-quarter tables).
- **When:** Optional. If absent, keep current behavior: first analysisId = current, second = prior.
- **Where it can come from:**
  - Optional file under execution **inputPath** (e.g. `analysis_metadata.json`) – same optional payload that we already treat as non-fatal when empty.
  - Or a key in model run parameters (e.g. `settings.analysisRoles`).
- **Proposed shape (example):**
  ```json
  {
    "current": "<analysis_id_for_Q4_25>",
    "prior": "<analysis_id_for_Q3_25>",
    "priorYear": "<analysis_id_for_Q4_24>",
    "quarters": ["<Q1_25>", "<Q2_25>", "<Q3_25>", "<Q4_25>"]
  }
  ```
- **Usage:** Report builder uses **current** and **prior** to label columns and compute Q→Q and Y→Y changes. **priorYear** is the analysis ID for the same quarter last year. **quarters** is the ordered list of analysis IDs for prior quarters we retrieve data from to build multi-quarter tables (e.g. Q4 ’24, Q3 ’25, Q4 ’25); these IDs should be among the analysis IDs returned by the callback (or otherwise available in inputPaths).

---

## 2. macroEconomicVariableInput (Baseline)

- **Category:** **macroEconomicVariableInput**
- **Root:** **input** (not `output/` or `export/`). Full path uses **asOfDate** from the main analysis’s **analysisDetails** and **scenarioidentifier=BASE**.
- **Path:** `input/macroeconomicVariableInput/asofdate=YYYY-MM-DD/scenarioidentifier=BASE/`
  - **asOfDate** is taken from the **BASE** scenario in **analysisDetails** for the main (current) analysis: `analysisDetails.scenarios[]` where `name === "BASE"` → use `asOfDate`. See `sample/analysisDetails.json`. If missing, fallback to `analysisDetails.reportingDate`, then legacy path without asofdate.
- **Filter:** Use **Baseline** scenario only (`scenarioidentifier=BASE`).
- **Attributes:** At least `macroeconomicVariableName`, `valueDate`, `macroeconomicVariableValue`. Optional: `asOfDate`, `databuffetMnemonic`, etc. (see datamodel).
- **Variable names:** Resolve report labels via `sample/macroeconomicVariable.csv` (column `macroeconomicVariableName` and any mnemonic columns).

---

## 3. S3/local path summary

| Data | Root | Path pattern (conceptual) |
|------|------|---------------------------|
| instrumentResult | output | `output/instrumentResult/analysisidentifier={id}/scenarioidentifier=Summary/` |
| instrumentReporting | output | `output/instrumentReporting/analysisidentifier={id}/` |
| instrumentReference | output | `output/instrumentReference/analysisidentifier={id}/` |
| analysisDetails | export | `export/analysisidentifier={id}/analysisDetails.json` |
| **macroEconomicVariableInput** | **input** | `input/macroeconomicVariableInput/asofdate=YYYY-MM-DD/scenarioidentifier=BASE/` (date from analysisDetails main analysis, BASE scenario) |
| Optional payload (e.g. metadata) | execution inputPath | As today: list/download under `settings.inputPath`; optional (empty = no failure). |

---

*See `docs/REPORT_MAPPING.md` for how these feed into each report section and `docs/IMPLEMENTATION_PLAN.md` for implementation order.*
