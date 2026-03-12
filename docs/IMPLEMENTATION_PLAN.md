# Implementation Plan – Hanmi ACL Quarterly Report

Phased work plan to support the new report (Hanmi3 Q4 2025 ACL Quarterly Analysis and SuppExh), optional metadata file, and macroEconomicVariableInput. Use this to restart work and manage context.

---

## Phase 1: Inputs and configuration

### 1.1 Optional analysis metadata file
- **Goal:** Accept an optional input (e.g. JSON) that defines analysis roles: which analysis is **current**, **prior**, **prior year**, and optionally a list of quarter analysis IDs.
- **Where:** 
  - **iosession.py:** In callback mode, after we have `analysisIds`, allow an override from metadata (e.g. from optional payload under `inputPath` or a dedicated key in run parameters). If no metadata, keep current behavior: `analysisIds[0]` = current, `analysisIds[1]` = prior.
  - **model.py:** Read “current” and “prior” (and “priorYear”, “quarters”) from settings or a small helper so report builder uses the right IDs for each column (Q4 ’24, Q3 ’25, Q4 ’25, etc.).
- **Deliverable:** Documented contract for metadata shape; code that applies it when present without breaking existing runs.

### 1.2 macroEconomicVariableInput under input root
- **Goal:** In callback mode (or when analysis IDs are known), download **macroEconomicVariableInput** from the **input** root (not `output/` or `export/`), partitioned by **scenarioIdentifier**, and keep only **Baseline** (e.g. `scenarioIdentifier = "BASE"`).
- **Where:**
  - **iosession.py:** Add constant for input root (e.g. `INPUT_ROOT_BASE = "input"`). Add listing/download for `input/macroEconomicVariableInput/analysisidentifier={id}/scenarioidentifier=BASE/` (or equivalent). Store in `local_directories['inputPaths']['macroEconomicVariableInput']` per analysis if needed, or single path for current analysis.
  - **sample/scenario.csv:** BASE = Baseline (already documented in REPORT_MAPPING.md).
  - **sample/macroeconomicVariable.csv:** Use to resolve variable display names to `macroeconomicVariableName` (or mnemonics) when building the macro section.
- **Deliverable:** Parquet (or CSV) for Baseline macro variables available to the report step; doc in AGENTS.md/PROJECT_SUMMARY.

### 1.3 Verify Summary scenario filter
- **Goal:** Ensure we only use **Summary** scenario for **instrumentResult** everywhere.
- **Where:** **model.py** – `_load_parquet_for_analysis(..., filter_summary=True)` is already used for instrumentResult. Confirm no code path uses unfiltered result for report aggregates.
- **Deliverable:** Comment or assertion that report logic uses Summary-only result; no code change if already correct.

---

## Phase 2: Report JSON structure and sections

### 2.1 New report builder entry point
- **Goal:** Replace or extend `build_quarterly_summary_report()` to produce the new JSON payload with sections matching `docs/REPORT_MAPPING.md` §5.
- **Where:** **model/model.py** – New function (e.g. `build_hanmi_acl_quarterly_report()`) or refactor existing to output the new structure. Write single JSON file (e.g. `hanmi_acl_quarterly_report.json`) to report dir.
- **Deliverable:** One JSON file with at least: `reportMetadata`, `segmentMethodology`, `collectivelyEvaluatedByMethodology`, `quantitativeLossRatesBySegment`, `netChargeOffsQuarterly`/`Annual`, `qualitativeReservesBySegment`, `individualAnalysis`, `unfundedBySegment`/`unfundedTrend`, `macroeconomicBaseline`. Placeholders OK for sections that need metadata/external data.

### 2.2 Section-by-section implementation (order)
Implement each section so it can be tested independently:

1. **Segment / methodology** – From instrumentReference (portfolioIdentifier, lossRateModelName).
2. **Collectively evaluated by methodology** – Join result + reference, filter collective, aggregate by lossRateModelName (quant/qual/total).
3. **Quantitative loss rates by segment** – By portfolio (and CRE sub-segment), amortized cost + onBalanceSheetReserveUnadjusted, for current/prior/priorYear analyses; Q→Q and Y→Y deltas.
4. **Net charge-offs** – From instrumentReporting by segment and quarter/year.
5. **Qualitative reserves by segment** – (onBalanceSheetReserveAdjusted − onBalanceSheetReserveUnadjusted) by segment/quarter.
6. **Qualitative factors** – From instrumentResult adjustment fields; map to report factor names (document assumptions).
7. **Macro Baseline** – From macroEconomicVariableInput (Baseline), selected variables and valueDate/macroeconomicVariableValue.
8. **Individual analysis** – Filter individually evaluated, aggregate by sub-type (DCF/Collateral Dependent/Other) if available.
9. **Unfunded** – offBalanceSheetEADAmountLifetime, offBalanceSheetReserve by segment; trend from reserves/provision by quarter.
10. **Parameters inventory / Peer ratios** – From analysisDetails and metadata where possible; document gaps.

---

## Phase 3: Integration and tests

### 3.1 Run flow
- **model/run.py:** Keep same steps; ensure Step 2 fetches macro input when configured; Step 3 calls new report builder; Step 4 zip still includes all report outputs.
- **Optional payload:** Continue to treat payload under `inputPath` as optional (no crash when empty). If metadata file is provided there (e.g. `analysis_metadata.json`), parse it in getSourceInputFiles or at start of report step.

### 3.2 Tests
- **tests/test_model.py:** Add tests for: (1) metadata parsing (current/prior/priorYear); (2) macro Baseline variable selection; (3) at least one new section (e.g. collectively evaluated by methodology or quantitative by segment) with mock parquet/DataFrames.
- Reuse existing helpers: `_find_column`, `_safe_sum`, `_filter_summary_scenario`, `_load_parquet_for_analysis`.

### 3.3 Documentation
- **AGENTS.md:** Update “What’s implemented” with: optional analysis metadata, macroEconomicVariableInput under input root (Baseline), new report sections and output file name.
- **PROJECT_SUMMARY.md:** Add subsection on input metadata and macro input path.

---

## Part completion checklist

- [x] Phase 1.1 – Metadata file support (analysis_metadata.json under inputPath or settings.analysisRoles; _get_analysis_roles() in model)
- [x] Phase 1.2 – macroEconomicVariableInput (input root, Baseline); download in getSourceInputFiles when callback
- [x] Phase 1.3 – Summary filter verified (already in place)
- [x] Phase 2.1 – New report JSON builder (build_hanmi_acl_quarterly_report, hanmi_acl_quarterly_report.json)
- [x] Phase 2.2 – All sections implemented (or stubbed with “from metadata”)
- [x] Phase 3.1 – Run flow: Step 3b calls build_hanmi_acl_quarterly_report; zip includes new report
- [x] Phase 3.2 – Unit tests: TestBuildHanmiAclQuarterlyReport, zip contains hanmi report
- [ ] Phase 3.3 – AGENTS.md and PROJECT_SUMMARY updated

---

*Reference: `docs/REPORT_MAPPING.md` for field-level mapping and JSON shape.*
