# Report → Data Dictionary Column Mapping

Canonical attribute names from datamodel/ImpairmentStudio-DataDictionary.csv (Attribute Name column). The report builder resolves columns with datamodel name first, then flexible variants (lowercase, spaces/underscores), so parquet written with different casing still matches.

---

## instrumentReference

| Report use | Datamodel attribute name | Notes |
|------------|--------------------------|--------|
| Join key | `instrumentIdentifier` | Required for all result/reporting joins |
| Join key (optional) | `analysisIdentifier` | When present in both sides |
| Segment / portfolio | `assetClass` (then `portfolioIdentifier`, …) | First-preference segment dimension; see Segmentation dimensions below |
| Evaluation type | `ascImpairmentEvaluation` | Collectively vs Individually evaluated |
| Methodology (PD) | `pdModelName` | Fallback when lossRateModelName null |
| Methodology (LGD) | `lgdModelName` | Fallback when lossRateModelName null |
| Methodology (loss rate) | `lossRateModelName` | First choice for methodology label |

---

## Segmentation dimensions (instrumentReference)

The report uses one segment dimension per dataset: the first candidate below that exists in the ref parquet and has at least one non-null value. Used for segmentMethodology, quantitativeLossRatesBySegment, qualitativeReservesBySegment, unfundedBySegment (and debug groupBy). Order of preference:

| Order | Datamodel attribute name | Notes |
|-------|---------------------------|--------|
| 1 | `assetClass` | Asset class (preferred when present; aligns with typical CRE / Retail / SME exhibits) |
| 2 | `portfolioIdentifier` | Portfolio / segment label |
| 3 | `assetSubClass1` | Sub asset class 1 |
| 4 | `assetSubClass2` | Sub asset class 2 |
| 5 | `portfolioSubType1` | Portfolio sub-type 1 (e.g. Residential mortgage, Consumer loan, Commercial mortgage) |
| 6 | `portfolioSubType2` | Portfolio sub-type 2 (e.g. Commercial and Industrial, CRE-construction, Consumer-credit card) |
| 7 | `instrumentSubType` | Instrument sub-type (e.g. Commercial real estate, Retail, C&I) |
| 8 | `consumerProductCategory` | Consumer product category (e.g. First Mortgage, HELOC, Bankcard) |
| 9 | `productType` | Product type for reporting (e.g. Real estate-mortgage, Installment loans) |
| 10 | `usRegion` | US region (Midwest, Northeast, South, West) |
| 11 | `entityType` | Entity type (Private Firm, Public Firm, Sovereign, etc.) |
| 12 | `locationType` | Location (Domestic, Foreign, Unallocated) |
| 13 | `instrumentType` | Instrument type (Loan, Line of Credit, Bond, etc.) |

Defined in `model.py` as `SEGMENT_DIMENSION_CANDIDATES`; resolved via `_get_segment_column(df)`. Debug JSON (`debug_all_data_summary.json`) includes per-analysis `refColumns`, `segmentationCandidates` (found, column, distinct, sample per candidate), and `segmentDimensionUsed`.

---

## instrumentResult

| Report use | Datamodel attribute name | Notes |
|------------|--------------------------|--------|
| Join key | `instrumentIdentifier` | |
| Scenario filter | `scenarioIdentifier` | Filter to "Summary" only |
| Reserve (adjusted) | `onBalanceSheetReserveAdjusted` | Collective/individual totals |
| Reserve (unadjusted) | `onBalanceSheetReserveUnadjusted` | Quantitative reserve |
| Exposure | `amortizedCost` | |
| Off-balance reserve | `onBalanceSheetReserve` | Unfunded section |
| Off-balance EAD | `offBalanceSheetEADAmountLifetime` | Unfunded section |

---

## instrumentReporting

| Report use | Datamodel attribute name | Notes |
|------------|--------------------------|--------|
| Join key | `instrumentIdentifier` | |
| Segment | `portfolioIdentifier` | netChargeOffs by segment |
| Net charge-offs | `netChargeOffAmount` | |
| Gross charge-offs | `grossChargeOffAmount` | Fallback for net |
| Recoveries | `recoveryAmount` | |
| Provision | `allowanceProvision` | Quarterly summary Section 1 |

---

## macroeconomicVariableInput

| Report use | Datamodel attribute name | Notes |
|------------|--------------------------|--------|
| Variable label | `macroeconomicVariableName` | Filter to REPORT_MACRO_VARIABLE_NAMES |
| Date | `valueDate` | Filter to reportingDate ± quarters |
| Value | `macroeconomicVariableValue` | |

---

## Column resolution (model.py)

- `_find_column(df, name)`: Case-insensitive exact match (e.g. `portfolioIdentifier` matches `portfolioidentifier`).
- `_resolve_column(df, canonical_name, *variants)`: Tries `_find_column(canonical_name)` then `_find_column_flexible(canonical_name, *variants)` so that datamodel camelCase and platform lowercase/spaced names both resolve.
- Report and debug use `_resolve_column` for portfolioIdentifier, ascImpairmentEvaluation, and methodology columns (lossRateModelName, pdModelName, lgdModelName) so one resolution strategy is used everywhere and segmentMethodology matches the debug summary.

When adding new report fields, use the attribute name from the data dictionary and resolve with `_resolve_column(df, "attributeName", "attributename")` (and any other known variants) to avoid empty sections when parquet column names differ.
