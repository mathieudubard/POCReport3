# Quantitative Data Points – Gap Analysis & Next-Work Priority

Compared to **REPORT_MAPPING.md** and the target Hanmi ACL Quarterly report (PDF), this doc summarizes what we have, what’s missing, and suggested priority.

---

## Current vs target (quantitative only)

| Section | Report target (REPORT_MAPPING) | Current JSON | Gap |
|--------|--------------------------------|--------------|-----|
| **reportMetadata** | asOfDate, analysisRoles (current, prior, priorYear, quarters) | ✓ current/prior/priorYear/quarters | `quartersAnalysisIds` empty; `priorYearAnalysisId` null (metadata/config) |
| **segmentMethodology** | Segment → Methodology | ✓ populated | Minor: one row with empty segment (""); consider "Unallocated" |
| **collectivelyEvaluatedByMethodology** | Methodology, AC, quant reserve, qual reserve, total, % | ✓ full | None for quantitative |
| **quantitativeLossRatesBySegment.main** | Balance & loss rate **by quarter** (Q4 '24, Q3 '25, Q4 '25), **Q→Q and Y→Y changes** | Segment, analysisId, label, AC, quantitativeReserve, lossRatePct for current/prior only | **Missing:** quarter labels (e.g. quarterLabel), **incrDecrQtoQ**, **incrDecrYtoY**; "nan" segment (data quality) |
| **quantitativeLossRatesBySegment.creSubSegments** | CRE sub-segments (e.g. Gas Station, Industrial, Office, Multifamily, …) | Empty `[]` | **Missing:** aggregate by assetSubClass1/assetSubClass2 for CRE-only; need ref with CRE + sub-class |
| **netChargeOffsQuarterly** | By segment **and by quarter** (Q4 2024 … Q4 2025) | One row per segment, single analysisId + netChargeOffs | **Missing:** multiple quarters per segment (e.g. quarters object or rows per quarter); **netChargeOffsAnnual** (by year) not present |
| **qualitativeReservesBySegment** | Same segment structure, qual reserve & rate | ✓ main | Optional: by-quarter view |
| **qualitativeFactorsMatrix** | Factors × segments applicability | Not in payload | **Missing** (qualitative section) |
| **qualitativeAdjustmentByFactor** | By factor: Q4_24, Q3_25, Q4_25, change Q→Q, Y→Y | Not in payload | **Missing** (instrumentResult adjustmentCategory/adjustmentReason) |
| **macroeconomicBaseline** | 4q back + 4q forward by variable | ✓ | None |
| **individualAnalysis** | By sub-type (DCF, Collateral Dependent, Other), by quarter, Q→Q/Y→Y | Single row "Individually Evaluated", no quarter breakdown | **Missing:** sub-type breakdown; by-quarter and deltas |
| **unfundedBySegment** | Segment, available credit, reserve | ✓ | "nan" segment (normalize to "Unallocated" or omit) |
| **unfundedTrend** | beginningReserve, requiredReserve, **provision**, totalUnfunded, **quarterChange** | analysisId, requiredReserve, totalUnfunded only | **Missing:** beginningReserve, provision, quarterChange |
| **parametersInventory** | From analysisDetails + metadata | `{}` | **Missing** (Exhibit 1) |
| **peerRatios** | ACL %, NPL %, etc. | `{}` | **Missing** (metadata/external) |
| **hanmiSummaryMetrics** | Hanmi summary | `{}` | **Missing** (metadata/external) |

---

## Prioritized next work (quantitative first)

### P1 – High impact, datamodel-ready

1. **Quarter labels and deltas for quantitative (and qualitative)**
   - Add `quarterLabel` (e.g. "Q4 2024", "Q3 2025", "Q4 2025") using analysisDetails.reportingDate or metadata.
   - Add **incrDecrQtoQ** and **incrDecrYtoY** (and same for reserves) to `quantitativeLossRatesBySegment.main` (and optionally qualitative). Requires mapping analysisId → quarter and prior quarter / prior year.

2. **Unfunded trend – full columns**
   - Add **beginningReserve** (prior quarter’s requiredReserve or from reporting), **provision** (from instrumentResult allowanceProvisionDelta or instrumentReporting for OBS), **quarterChange** (e.g. requiredReserve − beginningReserve + provision). Check datamodel for provision-by-OBS.

3. **netChargeOffsQuarterly by quarter**
   - Structure so each segment has values **per quarter** (e.g. `quarters: { "Q4 2024": x, "Q3 2025": y, "Q4 2025": z }` or one row per segment per quarter). Requires loading instrumentReporting for each analysisId in `quarters` (or current/prior) and mapping analysisId → quarter label.

4. **netChargeOffsAnnual**
   - New array: by segment and year (e.g. 2021–2025). Source: instrumentReporting aggregated by year (reportingDate or separate annual dataset). Depends on having multi-year reporting data.

### P2 – Report-complete, needs data or logic

5. **CRE sub-segments**
   - Populate `quantitativeLossRatesBySegment.creSubSegments`: filter ref where portfolio/segment = CRE (or instrumentSubType = Commercial real estate), then aggregate by **assetSubClass1** or **assetSubClass2** (Gas Station, Industrial, Office, etc.). If ref has no sub-class, leave [] and document.

6. **Parameters inventory**
   - Populate `parametersInventory` from **analysisDetails** (export/analysisidentifier={id}/analysisDetails.json): scenario, model versions, weights, etc. Map known keys to report labels; document which are from datamodel vs metadata.

7. **Individual by sub-type and quarter**
   - Split **individualAnalysis** by DCF / Collateral Dependent / Other (e.g. from ref or result riskClassification/custom field). Add by-quarter and Q→Q/Y→Y if metadata provides quarter mapping.

### P3 – Qualitative and metadata

8. **Qualitative factors matrix and by-factor adjustment**
   - **qualitativeFactorsMatrix**: which factors apply to which segments (from adjustmentCategory/adjustmentReason or client mapping).
   - **qualitativeAdjustmentByFactor**: amounts by factor and quarter from instrumentResult (adjustmentCategory, adjustmentReason, adjustmentValue), joined to segment. Requires non-zero adjustments in result.

9. **Segment display cleanup**
   - Normalize null/missing segment: show `"Unallocated"` (or omit) instead of `"nan"` or `""` in segmentMethodology, quantitativeLossRatesBySegment, unfundedBySegment.

10. **peerRatios / hanmiSummaryMetrics**
    - Filled from metadata or external input; document as such. Low priority for datamodel-only work.

---

## Summary

- **Already in good shape:** segmentMethodology, collectivelyEvaluatedByMethodology, main quantitative/qualitative by segment, macro baseline, individual (single row), unfunded by segment, unfunded trend (partial).
- **Next quantitative focus:** quarter labels + Q→Q/Y→Y on loss rates and reserves (P1.1), unfunded trend full columns (P1.2), net charge-offs by quarter and annual (P1.3–1.4), then CRE sub-segments and parameters inventory (P2), then qualitative factors and individual sub-type/quarter (P2–P3).

Use this order to close gaps from the PDF report while staying within current (and near-term) datamodel and metadata.
