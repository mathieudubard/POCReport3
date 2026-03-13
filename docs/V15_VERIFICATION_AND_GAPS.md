# V15 Generated Data Verification & Remaining Gaps

Run used **callback only** — **no input JSON analysis payload** was provided. Roles (current/prior) and quarter labels were inferred from `analysisDetails.reportingDate` for the two analysis IDs returned by the callback.

---

## 1. What v15 Contains (Verified)

| Section | v15 content | Status |
|--------|-------------|--------|
| **reportMetadata** | current=4647997, prior=4647909, priorYear=null, quarters=[4647997, 4647909] | ✓ Correct for 2 analyses; priorYear null expected |
| **segmentMethodology** | 8 rows (SME, Retail, CRE, one `""` segment) → methodology (Instrument Preset, EDF-X, MA TTC converters) | ✓ Populated; one empty segment (see gaps) |
| **collectivelyEvaluatedByMethodology** | 4 methodologies, AC, quant/qual/total reserve, pctOfEvaluationType | ✓ Full |
| **quantitativeLossRatesBySegment.main** | By segment (CRE, Retail, SME, nan) and by quarter (Q2 2025, Q1 2025); amortizedCost, quantitativeReserve, lossRatePct | ✓ Multi-quarter; quarterLabel present |
| **quantitativeLossRatesBySegment.creSubSegments** | `[]` | Empty (CRE sub-segments not built) |
| **netChargeOffsQuarterly** | 2 rows (one per quarter), segment "US South Regional Bank Demo (ISv2)", netChargeOffs | ✓ By quarter; single segment from reporting |
| **qualitativeReservesBySegment.main** | CRE, Retail, SME; qualitativeReserve 0, qualitativeRatePct 0 | ✓ Structure OK (all zero in this run) |
| **macroeconomicBaseline** | 63 rows; 8 variables (Unemployment, Real GDP, BBB Spread, Treasury 3Y, CRE Price, FHFA, Retail Sales, etc.) by valueDate | ✓ 4q back/forward range |
| **individualAnalysis** | 1 row "Individually Evaluated", amortizedCost, specificReserve, pctOfType | ✓ Single aggregate; no sub-type (DCF/Collateral/Other) |
| **unfundedBySegment** | CRE, Retail, SME, nan → availableCredit, reserve | ✓ Populated; "nan" segment (see gaps) |
| **unfundedTrend** | 2 rows (Q2 2025, Q1 2025): requiredReserve, totalUnfunded | Partial — missing columns below |
| **parametersInventory** | `{}` | Not populated |
| **peerRatios** | `{}` | Not populated |
| **hanmiSummaryMetrics** | `{}` | Not populated |

**quarterly_summary_report.json** (6 sections): changesToAcl, collectivelyEvaluated, individuallyEvaluated, totalReserveRequirements, unfundedCommitmentLiabilities, qtdCreditLossExpense — all present with values.

**debug_all_data_summary.json**: join keys, byAnalysis (4647997, 4647909) with ref/result/reporting row counts, refColumns, segmentationCandidates (portfolioIdentifier not found; assetClass used), segmentDimensionUsed=assetClass, groupBy.

---

## 2. No Input Payload Implications

- **analysisIds** and **inputPaths** came from the **callback** only.
- **current / prior** were **inferred from reportingDate** (latest = current, prior quarter = prior). **priorYear** = null because only two analyses.
- **Quarter labels** (e.g. "Q2 2025", "Q1 2025") came from **analysisDetails.reportingDate** per analysis.
- **analysis_metadata.json** was not used (no execution input dir in this run). To supply an `analyses` payload (e.g. tags, extra quarters, priorYear), the wrapper must either (1) include it in the callback response, or (2) place a file in the execution input so the model can load it (see MULTIQUARTER_SUPPORT.md and PAYLOAD_SAMPLES_README.md).

---

## 3. Remaining Gaps Toward the Report (PDF / REPORT_MAPPING)

### P1 – Quantitative (high impact) — **implemented**

| Gap | Status | Notes |
|-----|--------|--------|
| **Quarter-over-quarter and YoY deltas** | Done | quantitativeLossRatesBySegment.main rows now include **incrDecrQtoQ_lossRatePct**, **incrDecrYtoY_lossRatePct**, **incrDecrQtoQ_quantitativeReserve**, **incrDecrYtoY_quantitativeReserve** (null when prior quarter/prior year not in data). |
| **Unfunded trend full columns** | Done | unfundedTrend rows include **beginningReserve** (prior quarter’s requiredReserve), **provision** (null; can be wired to OBS provision from reporting later), **quarterChange** (requiredReserve − beginningReserve). |
| **netChargeOffsAnnual** | Done | **netChargeOffsAnnual** array: by segment and year, aggregated from netChargeOffsQuarterly. |
| **Segment display** | Done | **segment** normalized to **"Unallocated"** for nan/empty in segmentMethodology, quantitativeLossRatesBySegment.main, qualitativeReservesBySegment.main, unfundedBySegment, netChargeOffsQuarterly. |

### P2 – Structure / completeness

| Gap | v15 state | Target |
|-----|----------|--------|
| **CRE sub-segments** | creSubSegments = [] | Aggregate by assetSubClass1/assetSubClass2 for CRE-only; report labels (Gas Station, Industrial, Office, etc.) |
| **Parameters inventory** | {} | From analysisDetails (and metadata): scenario, model versions, weights, etc. |
| **Individual by sub-type and quarter** | Single row | DCF / Collateral Dependent / Other; by quarter and Q→Q/Y→Y if data available |

### P3 – Qualitative and metadata

| Gap | v15 state | Target |
|-----|----------|--------|
| **qualitativeFactorsMatrix** | Not in payload | Factors × segments applicability |
| **qualitativeAdjustmentByFactor** | Not in payload | By factor and quarter from adjustmentCategory/adjustmentReason |
| **peerRatios / hanmiSummaryMetrics** | {} | From metadata or external input |

---

## 4. Data Quality Notes (v15)

- **Segment dimension used:** **assetClass** (portfolioIdentifier not found in reference; see debug segmentationCandidates).
- **Empty / "nan" segments:** One segmentMethodology row with `segment: ""`; one quantitative main row and one unfundedBySegment row with `segment: "nan"`. Recommend normalizing to "Unallocated" or filtering.
- **Net charge-offs:** Single segment from instrumentReporting (portfolioIdentifier or similar); other sections use assetClass (CRE, Retail, SME). Segment names will differ until segment source is aligned or mapped.

---

## 5. Summary

- **Generated data:** v15 matches the current implementation: two quarters, date-inferred roles, segmentMethodology and collective/quant/qual/unfunded/individual/macro sections populated. quarterly_summary (6 sections) and debug summary are present.
- **No input payload:** Run is correct for callback-only; to use an `analyses` config file (e.g. tags, priorYear, more quarters), provide it via callback response or execution input.
- **Remaining work:** P1 = deltas (Q→Q, Y→Y), unfunded trend columns, netChargeOffsAnnual, segment cleanup; P2 = CRE sub-segments, parametersInventory, individual sub-type/quarter; P3 = qualitative factors matrix/adjustment, peer/hanmi metrics.

Reference: REPORT_MAPPING.md, QUANTITATIVE_GAP_AND_PRIORITY.md, MULTIQUARTER_SUPPORT.md.
