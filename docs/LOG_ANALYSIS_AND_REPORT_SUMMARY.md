# Log Analysis and Report Summary

This document interprets run logs and summarizes what the Hanmi ACL report expects from the data vs what is not implemented or not expected yet.

---

## 1. Log review (from 03/12/2026 run)

### 1.1 What is **expected** (working as intended)

| Item | Log evidence | Meaning |
|------|----------------|--------|
| Callback and analysis IDs | `analysisIds=[4647997, 4647909]`, `callback: analysisIds count=2` | Callback ran; we have current and prior analysis. |
| S3 listing for instrumentResult | `instrumentResult analysisId=4647997: 93 object(s), 92 parquet` (and 39 for 4647909) | Parquet files exist at `output/instrumentResult/.../scenarioidentifier=Summary/`. |
| instrumentReporting download | `instrumentReporting analysisidentifier=4647997: downloading 42 parquet` (and 39 for prior) | Reporting data downloaded; used for net charge-offs. |
| instrumentReference download | `instrumentReference analysisidentifier=4647997: downloading 50 parquet file(s) (all portfolios)` (both analyses) | Reference data downloaded. |
| analysisDetails | `Downloading analysisDetails for analysisidentifier=4647997 <- export/...` | analysisDetails fetched from export for macro date and metadata. |
| Macro path and download | `macroEconomicVariableInput: using asofdate=2025-06-30 from analysisDetails`, `downloading 36 parquet file(s)` | Macro date taken from BASE scenario in analysisDetails; parquet found and downloaded. |
| Macro filter | `macro: filtered to 8 report variables, date range 4q back / 4q forward -> 63 rows` | Only report variables and date range used; 63 rows in report. |
| netChargeOffsQuarterly | `netChargeOffs=1` in sections, one row in JSON | Built from instrumentReporting; segment and net charge-offs present. |
| macroeconomicBaseline | `macroVars=63` | 63 variable/date/value rows in report. |
| Upload | `Uploading 6 output file(s)` | Only report outputs uploaded, not input parquet. |

### 1.2 What is **not** expected (bugs / gaps)

| Item | Log evidence | Cause | Fix |
|------|----------------|--------|-----|
| **instrumentResult never downloaded** | No line `instrumentResult analysisidentifier=...: downloading ... parquet`. Then `result current=0, prior=0` and `[_load_parquet]` never logs for instrumentResult in Step 3/3b. | Callback only returned `inputPaths.instrumentReference`. We only ensured dirs for `instrumentReporting`, `instrumentReference`, `macroEconomicVariableInput` — not `instrumentResult`. So `instrument_result_paths` was empty and the instrumentResult download loop never ran. | **Fixed:** `create_io_directories` now also ensures `instrumentResult` so we create local dirs and download instrumentResult from `output/.../scenarioidentifier=Summary/`. |
| **portfolioIdentifier “MISSING” in ref** | `ref current: portfolioIdentifier=MISSING, ... sample port=[]`, `segmentMethodology: SKIP - portfolioIdentifier column not found in ref (cols: ['couponrate_s1', ...])` | Ref parquet may use a different column name (or the column is beyond the first 12 we log). Datamodel expects `portfolioIdentifier`. | **Check:** Inspect actual ref parquet column names (e.g. `portfolioidentifier`, `portfolio_identifier`, or segment column). If name differs, add a fallback in `_find_column` or map that column in the report. |

### 1.3 Cascading effect of missing instrumentResult

Because **instrumentResult** was not downloaded (0 rows):

- **collectivelyByMethodology:** SKIP – result empty (needs result + ref, filter “collective”).
- **quantitativeLossRatesBySegment:** SKIP for both analyses – result=0 (needs result + ref, group by portfolio).
- **qualitativeReservesBySegment:** SKIP – result or ref empty (needs result + ref for adj/unadj reserves).
- **individualAnalysis:** SKIP – result empty (needs result + ref, filter “individual”).
- **unfundedBySegment:** SKIP – result empty (needs result + ref, off-balance-sheet columns).
- **unfundedTrend:** SKIP – result None for both analyses (needs result with offBalanceSheetReserve / offBalanceSheetEADAmountLifetime).

After the fix, instrumentResult should load (92 + 39 parquet files). If **portfolioIdentifier** is still missing in ref, segmentMethodology (and any section that groups by segment/portfolio) will need the ref column name fixed or mapped.

---

## 2. Report basis: what we get from the data vs not yet

### 2.1 What we **try to get** from the data (and from where)

| Report section | Source data | What we use | Purpose |
|----------------|------------|-------------|---------|
| **reportMetadata** | settings / analysisIds | current, prior, priorYear, quarters | Identify analyses and period labels. |
| **segmentMethodology** | instrumentReference | portfolioIdentifier, lossRateModelName or pdModelName | Segment → methodology (one row per distinct segment + methodology). |
| **collectivelyEvaluatedByMethodology** | instrumentResult + instrumentReference | Join on instrumentIdentifier; ascImpairmentEvaluation = “Collectively Evaluated”; methodology; amortizedCost, onBalanceSheetReserveUnadjusted/Adjusted | By methodology: AC, quantitative reserve, qualitative reserve, total, % of type. |
| **quantitativeLossRatesBySegment** | instrumentResult + instrumentReference (current and prior) | Join on instrumentIdentifier; portfolioIdentifier; amortizedCost, onBalanceSheetReserveUnadjusted | By segment and period: balance, quantitative reserve, loss rate %. |
| **netChargeOffsQuarterly** | instrumentReporting | portfolioIdentifier (or total), netChargeOffAmount (or grossChargeOff − recovery) | Net charge-offs by segment (or total) for the quarter. |
| **qualitativeReservesBySegment** | instrumentResult + instrumentReference | Join; portfolioIdentifier; onBalanceSheetReserveAdjusted − Unadjusted, amortizedCost | By segment: qualitative reserve and rate %. |
| **macroeconomicBaseline** | macroEconomicVariableInput (Baseline, asofdate from analysisDetails) | Filter: report variables only, valueDate in 4q back/forward of reportingDate | Time series of macro variables for the table. |
| **individualAnalysis** | instrumentResult + instrumentReference | Join; ascImpairmentEvaluation = “Individually Evaluated”; amortizedCost, onBalanceSheetReserve(Adjusted) | Totals for individually evaluated. |
| **unfundedBySegment** | instrumentResult + instrumentReference | Join; portfolioIdentifier; offBalanceSheetEADAmountLifetime, offBalanceSheetReserve | By segment: unfunded exposure and reserve. |
| **unfundedTrend** | instrumentResult (per analysis) | offBalanceSheetReserve, offBalanceSheetEADAmountLifetime | Required reserve and total unfunded per period. |
| **parametersInventory** | — | — | Stubbed; not from parquet yet. |
| **peerRatios** | — | — | Stubbed; not from parquet yet. |
| **hanmiSummaryMetrics** | — | — | Stubbed; not from parquet yet. |

### 2.2 What we **do not expect to get yet**

- **parametersInventory, peerRatios, hanmiSummaryMetrics:** Placeholders only; no data sources wired. Filled with `{}` in the JSON.
- **priorYear / quarters:** Only populated if optional analysis metadata (or settings) provides `priorYear` and `quarters`; otherwise `priorYearAnalysisId: null`, `quartersAnalysisIds: []`. Multi-quarter tables (e.g. 4 quarters) depend on that.
- **CRE sub-segments:** quantitativeLossRatesBySegment.creSubSegments and any CRE-only breakdown require assetSubClass or similar in reference; currently not implemented, so `creSubSegments` stays `[]`.

---

## 3. Expected state after the instrumentResult fix

- **instrumentResult** is downloaded for both analyses (logs should show `instrumentResult analysisidentifier=4647997: downloading 92 parquet file(s)` and same for 4647909).
- **result current / prior** row counts become non-zero in “data loaded” and in `[_load_parquet] instrumentResult ... N files -> M rows`.
- Sections that need result + ref (collectively, quantitative, qualitative, individual, unfunded) can populate **provided**:
  - Ref has a column we can use as portfolio (e.g. `portfolioIdentifier` or a known alias).
  - ascImpairmentEvaluation and reserve columns exist and match expected names/casing.

If **portfolioIdentifier** is still reported as MISSING after the fix, the next step is to inspect the ref parquet schema and either add a column alias or document the actual segment column name for the report.
