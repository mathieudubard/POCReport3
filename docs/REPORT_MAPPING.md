# Hanmi ACL Quarterly Analysis and Supplemental Exhibits – Report-to-Data Mapping

This document maps the **Hanmi3 Q4 2025 ACL Quarterly Analysis and SuppExh** report (target PDF) to the ImpairmentStudio datamodel and to the data we can build from **instrumentResult**, **instrumentReporting**, **instrumentReference**, and **macroEconomicVariableInput**. It is intended for humans to validate mappings and for implementation.

---

## 1. Data source precedence and filters

- **Attribute precedence:** When an attribute exists in more than one category, use: **instrumentReporting** > **instrumentResult** > **instrumentReference**.
- **instrumentResult:** Partitioned by scenario. Use **scenarioIdentifier = "Summary"** only (weighted average). Optionally filter **adjustmentIdentifier = "0"** where we need base quantitative (e.g. for collective quantitative vs qualitative split).
- **Joins:** Join **instrumentResult** and **instrumentReporting** to **instrumentReference** on **instrumentIdentifier** (and **analysisIdentifier** where applicable). Reference is the source for segment/metadata (portfolio, methodology, ascImpairmentEvaluation).

---

## 2. Analysis metadata (prior / current / last year / quarters)

- **Requirement:** An optional **input metadata file** (e.g. JSON) will describe which analysis is **prior**, **current**, **last year**, and optionally more quarters back.
- **Current behavior:** Today we assume `analysisIds[0]` = current and `analysisIds[1]` = prior. The metadata file would replace this with explicit roles, e.g. `{ "current": "<id>", "prior": "<id>", "priorYear": "<id>", "quarters": ["<q1_id>", "<q2_id>", ...] }`.
- **Use:** Drive which analysis IDs we use for “Q4 ’24”, “Q3 ’25”, “Q4 ’25”, and YoY comparisons in each report section.

---

## 3. Macroeconomic variable input

- **Category:** **macroEconomicVariableInput**
- **Location:** Under **input** root (not `output/` or `export/`), partitioned by **scenarioIdentifier** (subfolders `scenarioidentifier=<value>/`).
- **Filter:** Use **Baseline** scenario only. In `sample/scenario.csv`, Baseline is **BASE** (`scenarioIdentifier = "BASE"`), description "MEDC Baseline Scenario".
- **Attributes (datamodel):** `analysisIdentifier`, `scenarioIdentifier`, `asOfDate`, `macroeconomicVariableName`, `valueDate`, `macroeconomicVariableValue`, `importSource`, `databuffetMnemonic`, etc.
- **Variable names:** Resolve display names (e.g. “Unemployment Rate”, “USA Real GDP Growth”) via `sample/macroeconomicVariable.csv` (columns: `macroeconomicVariableName`, `scenarioStudioMnemonics`, `databuffetMnemonic`, etc.).

**Report section 1.1.2 / 1.2 macro table (Moody’s Baseline, 4-quarter historical + 4-quarter forecast):**

| Report variable / column   | Datamodel source                    | Sample/metadata name (macroeconomicVariable.csv) |
|----------------------------|-------------------------------------|--------------------------------------------------|
| Unemployment Rate          | macroEconomicVariableInput          | USA Unemployment Rate (or USA Unemployment Rate Monthly) |
| USA Real GDP Growth (Annualized) | macroEconomicVariableInput    | USA Real GDP Growth |
| USA BBB Spread (7–10 Year) | macroEconomicVariableInput          | USA BBB Spread |
| US Treasury 3 Year         | macroEconomicVariableInput          | US Treasury 3 Year |
| USA CRE Price Index Growth | macroEconomicVariableInput          | USA Commercial Real Estate Price Index or USA CRE Price Index Growth |
| FHFA All Transactions Home Price Index | macroEconomicVariableInput | USA House Price Index FHFA |
| Retail Sales (YoY % Chg)   | macroEconomicVariableInput          | USA Retail Sales and Food Services Growth (or similar YoY) |

Output: time series by **valueDate** (and optionally scenario) for each variable, for the Baseline scenario only.

**Implementation:** The report builder loads macro data from Baseline, then filters to **only** the variables in the table above (see `REPORT_MACRO_VARIABLE_NAMES` in `model/model.py`) and to **valueDate** within **4 quarters back and 4 quarters forward** of the current analysis’s `reportingDate` from analysisDetails. No other variables or dates are included in the report.

---

## 4. Report sections and table mappings

### 4.1 Section 1.1.1 – Segmentations and Methodologies (Segments → Methodology)

- **Report:** Table mapping **Segments** (e.g. All Commercial and Industrial, All CRE Investor and Construction, Residential Mortgage, SBA, EFAs) to **Methodology** (e.g. C&I Loss Rate Model, CRE Loss Rate Model, ECCL).
- **Source:** **instrumentReference** (and optionally **instrumentResult** for consistency).
  - **Segment:** **portfolioIdentifier** (and, for CRE sub-segments, **assetSubClass1** or **assetSubClass2** / custom segment attributes). Map portfolio + sub-type to the report “segment” label (e.g. “Commercial Line of Credit”, “Commercial Real Estate (Gas Station)”).
  - **Methodology:** **instrumentReference.lossRateModelName** (or **pdModelName** / **ttcPDModelName** if that carries the methodology). Precedence: if present in **instrumentResult** (e.g. a model name field), use that; else **instrumentReference**.
- **Output:** List of `{ "segment": "...", "methodology": "..." }`. Can be derived by distinct (portfolioIdentifier, lossRateModelName) from reference (or result) for the current analysis.

---

### 4.2 Section 1.1.1 – Collectively Evaluated by Methodology

- **Report:** Table: Methodology, Amortized Cost, Quantitative Reserve, Qualitative Reserve, Total Reserve, % of Evaluation Type.
- **Logic:**
  - Restrict to **ascImpairmentEvaluation** = “Collectively Evaluated” (from **instrumentReference** after join).
  - Join **instrumentResult** (Summary scenario) to **instrumentReference** on **instrumentIdentifier** (and analysisIdentifier).
  - **Methodology:** **instrumentReference.lossRateModelName** (or result’s model name if available).
  - **Amortized Cost:** SUM(**instrumentResult.amortizedCost**).
  - **Quantitative Reserve:** SUM(**instrumentResult.onBalanceSheetReserveUnadjusted**). Optionally filter **adjustmentIdentifier = "0"** for base quantitative row if needed.
  - **Qualitative Reserve:** SUM(**instrumentResult.onBalanceSheetReserveAdjusted** − **instrumentResult.onBalanceSheetReserveUnadjusted**).
  - **Total Reserve:** SUM(**instrumentResult.onBalanceSheetReserveAdjusted**).
  - **% of Evaluation Type:** (row total reserve) / (total collective reserve).
- **Output:** One row per methodology (and optionally segment) with the above aggregates.

---

### 4.3 Section 1.1.3 – Loss rates by segment (quantitative)

- **Report:** Two tables:
  - **Main:** Segment (Commercial Line of Credit, Commercial Term - Unsecured, SBA, SBA-TLTB, Commercial Real Estate, Construction, Residential Mortgage, Equipment Financing Agreements, Total Quantitative). Columns: Balance (Q4 ’24, Q3 ’25, Q4 ’25), Loss rate (same quarters), Incr/Decr Q→Q and Y→Y, Quantitative Reserve (same quarters), Incr/Decr.
  - **CRE sub-segments:** Gas Station, Industrial, Mixed Use, Hospitality, Multifamily, Office, Others, Retail, Total CRE.
- **Sources:**
  - **Balance (Total Pooled Balance):** **instrumentResult.amortizedCost** (Summary), summed by segment. Segment = **instrumentReference.portfolioIdentifier** (and **assetSubClass1** / **assetSubClass2** for CRE sub-segments). Map portfolio + sub-type to report labels (e.g. “Commercial Real Estate (Office)”).
  - **Loss rate:** (Quantitative Reserve / Amortized Cost) for the period. Quantitative Reserve = **instrumentResult.onBalanceSheetReserveUnadjusted** (Summary).
  - **Quantitative Reserve:** SUM(**onBalanceSheetReserveUnadjusted**) by segment and analysis (quarter).
- **Periods:** Use metadata to map analysis IDs to Q4 ’24, Q3 ’25, Q4 ’25 (and prior year if needed). Load **instrumentResult** (Summary) and **instrumentReference** per analysis, join, then aggregate by segment; compute Q→Q and Y→Y deltas.
- **Output:** JSON: e.g. `quantitativeLossRatesBySegment`: list of segments with balances, rates, reserves, and changes by quarter.

---

### 4.4 Net charge-offs (quarterly and annual) by segment

- **Report:** Quarterly net charge-offs (e.g. Q4 2024 … Q4 2025) by segment; annual net charge-offs (2021–2025) by segment.
- **Source:** **instrumentReporting** (primary for charge-offs). **instrumentReporting.netChargeOffAmount** (or **grossChargeOffAmount** − **recoveryAmount** if net not present). Segment from **instrumentReporting.portfolioIdentifier** (or join to **instrumentReference** on **instrumentIdentifier** for portfolio).
- **Precedence:** **instrumentReporting** for charge-off/recovery; segment from reference if not in reporting.
- **Output:** 
  - `netChargeOffsQuarterly`: by segment and quarter (analysisId → quarter label via metadata).
  - `netChargeOffsAnnual`: by segment and year (aggregate reporting by year or use reporting date).

---

### 4.5 Section 1.2 – Qualitative component (rates and reserves by segment)

- **Report:** Same segment structure as 1.1.3 but for **qualitative** reserves and rates.
- **Sources:**
  - **Qualitative Reserve:** SUM(**instrumentResult.onBalanceSheetReserveAdjusted** − **instrumentResult.onBalanceSheetReserveUnadjusted**) by segment (and quarter).
  - **Qualitative Rate:** (Qualitative Reserve / Amortized Cost) by segment.
  - Segment and amortized cost as in 4.3.
- **Output:** `qualitativeReservesBySegment`: same shape as quantitative table but qualitative amounts and rates.

---

### 4.6 Qualitative factors matrix and adjustment breakdown

- **Report:** (1) Matrix: Qualitative factors (e.g. Adjustment to Moody’s Model Output, Remaining time to maturity, CRE Concentration, Policy exceptions, Loan review, Management experience, 2025 Geopolitical) × Segments (C&I, CRE, Const, SBA, SBA-TLTB, Resi, EFAs). (2) Table: Qualitative adjustment by factor with Q4 ’24, Q3 ’25, Q4 ’25 and changes.
- **Source:** **instrumentResult** has **adjustmentCategory**, **adjustmentReason**, **adjustmentValue**, **adjustmentIdentifier**. Qualitative breakdown may be in result rows with non-zero adjustment (adjusted − unadjusted). Map **adjustmentCategory** / **adjustmentReason** to report factor names. Segment from joined **instrumentReference**.
- **Note:** Exact mapping of each report factor to adjustmentCategory/adjustmentReason may require client-specific rules; document in implementation.
- **Output:** `qualitativeFactorsMatrix`: which factors apply to which segments; `qualitativeAdjustmentByFactor`: amounts by factor and quarter.

---

### 4.7 Section 1.3 – Individual component (DCF, Collateral Dependent, Other)

- **Report:** Individually evaluated: Amortized Cost, Specific Reserve, % of Evaluation Type; by type (DCF, Collateral Dependent, Other) and by quarter (Q→Q and Y→Y).
- **Source:** Filter **instrumentReference.ascImpairmentEvaluation** = “Individually Evaluated”. Join **instrumentResult** (Summary). **Amortized Cost:** **instrumentResult.amortizedCost**. **Specific Reserve:** **instrumentResult.onBalanceSheetReserveAdjusted** (or **onBalanceSheetReserve**). Sub-type (DCF vs Collateral Dependent vs Other) may come from **instrumentReference** or result (e.g. **riskClassification**, or a custom field); if not in datamodel, may need to be supplied by metadata or left as “Other”.
- **Output:** `individualAnalysis`: by evaluation sub-type and quarter, with amortized cost, reserve, and percentages.

---

### 4.8 Section 1.4 – Unfunded commitment liability

- **Report:** (1) By segment: Unfunded Commitment Liability (Available Credit), Reserve. (2) Five-quarter trend: Beginning Reserve, Required Reserve, Provision, Total Unfunded Commitment, Quarter-to-Quarter Change.
- **Sources:**
  - **Unfunded exposure (Available Credit):** **instrumentResult.offBalanceSheetEADAmountLifetime** (Summary), by segment (join **instrumentReference** for **portfolioIdentifier**).
  - **Unfunded reserve:** **instrumentResult.offBalanceSheetReserve** (Summary), by segment.
  - **Provision (unfunded):** May be in **instrumentReporting** or **instrumentResult** (e.g. **allowanceProvisionDelta** split or off-balance-sheet provision). If no separate OBS provision field, document as “derive from reporting or reserve flow”.
- **Output:** `unfundedBySegment`: segment, availableCredit, reserve; `unfundedTrend`: by quarter, beginningReserve, requiredReserve, provision, totalUnfunded, quarterChange.

---

### 4.9 Section 1.5 – Sensitivity analysis

- **Report:** Narrative (e.g. S2 vs midpoint, S1 vs midpoint). No direct table from instrument data; optionally pull scenario weights or labels from metadata. Omit from data payload or add a placeholder section if we have scenario metadata.

---

### 4.10 Exhibit 1 – Parameters inventory

- **Report:** Various parameters (CRE S/D, C&I S/D, Loss estimation method, Model versions, Scenario, Weight, etc.) by quarter.
- **Source:** Largely **metadata** and **analysisDetails** (from `export/analysisidentifier={id}/analysisDetails.json`). Some values (e.g. model names, scenario names) can be inferred from **instrumentReference** (e.g. **lossRateModelName**) and from run/analysis settings. Macro scenario = Baseline from **macroEconomicVariableInput**.
- **Output:** `parametersInventory`: key-value or table form from analysisDetails + metadata; document which keys we can populate from datamodel vs metadata only.

---

### 4.11 Exhibit 2 – Peer ratios and Hanmi summary metrics

- **Report:** ACL %, NPL %, 30–89 DPD %, NCO % (Hanmi and peers) by quarter; Hanmi-only summary.
- **Source:** Typically **external** (peer data) or **metadata**. If NPL/NCO/DPD are present in **instrumentReporting** or another category, they can be aggregated; otherwise mark as “from metadata or external input”.
- **Output:** `peerRatios` and `hanmiSummaryMetrics` if we have source fields; else document as metadata/external.

---

## 5. JSON output structure (proposed)

High-level payload with one block per major table/section:

```json
{
  "reportMetadata": {
    "reportTitle": "Allowance for Credit Losses – Quarterly Analysis and Supplemental Exhibits",
    "asOfDate": "...",
    "analysisRoles": { "current": "...", "prior": "...", "priorYear": "..." }
  },
  "segmentMethodology": [ { "segment": "...", "methodology": "..." } ],
  "collectivelyEvaluatedByMethodology": [ { "methodology": "...", "amortizedCost": 0, "quantitativeReserve": 0, "qualitativeReserve": 0, "totalReserve": 0, "pctOfEvaluationType": 0 } ],
  "quantitativeLossRatesBySegment": { "main": [...], "creSubSegments": [...] },
  "netChargeOffsQuarterly": [ { "segment": "...", "quarters": { "Q4 2024": 0, ... } } ],
  "netChargeOffsAnnual": [ { "segment": "...", "years": { "2021": 0, ... } } ],
  "qualitativeReservesBySegment": { "main": [...], "creSubSegments": [...] },
  "qualitativeFactorsMatrix": { "factors": [...], "segments": [...], "applicability": [...] },
  "qualitativeAdjustmentByFactor": [ { "factor": "...", "Q4_24": 0, "Q3_25": 0, "Q4_25": 0, "changeQtoQ": 0, "changeYtoY": 0 } ],
  "macroeconomicBaseline": [ { "variableName": "...", "valueDate": "...", "value": 0 } ],
  "individualAnalysis": [ { "evaluationType": "DCF|Collateral Dependent|Other", "amortizedCost": 0, "specificReserve": 0, "pctOfType": 0, "quarters": {...} } ],
  "unfundedBySegment": [ { "segment": "...", "availableCredit": 0, "reserve": 0 } ],
  "unfundedTrend": [ { "quarter": "...", "beginningReserve": 0, "requiredReserve": 0, "provision": 0, "totalUnfunded": 0, "quarterChange": 0 } ],
  "parametersInventory": { ... },
  "peerRatios": { ... },
  "hanmiSummaryMetrics": { ... }
}
```

Sections that cannot be built from the datamodel (e.g. peer data, some parameters) can be omitted or populated from the optional metadata file.

---

## 6. Summary of datamodel attributes used

| Category                    | Key attributes |
|----------------------------|----------------|
| **instrumentResult**       | analysisIdentifier, instrumentIdentifier, scenarioIdentifier, adjustmentIdentifier, portfolioIdentifier, amortizedCost, onBalanceSheetReserve, onBalanceSheetReserveAdjusted, onBalanceSheetReserveUnadjusted, offBalanceSheetReserve, offBalanceSheetEADAmountLifetime, allowanceProvisionDelta, adjustmentCategory, adjustmentReason, lossRateAdjusted, lossRateUnadjusted |
| **instrumentReporting**    | analysisIdentifier, instrumentIdentifier, portfolioIdentifier, grossChargeOffAmount, recoveryAmount, netChargeOffAmount, allowanceProvision, allowanceBeginBalance, allowanceEndingBalance |
| **instrumentReference**    | analysisIdentifier, instrumentIdentifier, portfolioIdentifier, ascImpairmentEvaluation, lossRateModelName, assetClass, assetSubClass1, assetSubClass2 |
| **macroEconomicVariableInput** | analysisIdentifier, scenarioIdentifier, macroeconomicVariableName, valueDate, macroeconomicVariableValue |

---

## 7. Files to reference

- **Datamodel:** `datamodel/ImpairmentStudio-DataDictionary.csv`
- **Scenarios (Baseline):** `sample/scenario.csv` → BASE = Baseline
- **Macro variable names:** `sample/macroeconomicVariable.csv`
- **Current report logic:** `model/model.py` (`build_quarterly_summary_report`), `model/iosession.py` (`getSourceInputFiles`)

---

*Document version: 1.0. Next: implement metadata file support, macroEconomicVariableInput under input root (Baseline), and the new report builder producing the JSON sections above.*
