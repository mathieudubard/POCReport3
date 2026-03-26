# Legacy layout: output/ parquet shards (reference only)

The live Hanmi model no longer downloads from this layout by default. Inputs are one CSV per category per analysis under export/ (see docs/INPUT_SOURCES.md).

This page preserves the previous S3 structure for Impairment Studio outputs and for anyone re-enabling a custom fork.

## Bucket root

All paths are under the tenant bucket (same as today).

## Per category (historical)

| Category | S3 prefix pattern |
|----------|-------------------|
| instrumentResult | output/instrumentResult/analysisidentifier={id}/scenarioidentifier=Summary/ — many *.parquet files (e.g. under adjusted=true/). |
| instrumentReporting | output/instrumentReporting/analysisidentifier={id}/ — parquet parts (flat or nested). |
| instrumentReference | output/instrumentReference/analysisidentifier={id}/ — parquet under portfolio partitions, etc. |
| analysisDetails | export/analysisidentifier={id}/analysisDetails.json (unchanged). |
| macroEconomicVariableInput | **Still the live path** (not legacy): `input/macroeconomicVariableInput/asofdate={YYYY-MM-DD}/scenarioidentifier=BASE/` — parquet; date from analysisDetails BASE scenario. Not supplied via export CSV. |

## Local download behavior (old code)

- List all *.parquet under each prefix (recursive), download each object.
- Preserve relative subpaths for instrumentResult / instrumentReference to avoid basename collisions.

## Model read behavior (still supported locally)

model.Model._load_parquet_for_analysis still falls back to loading all **/*.parquet under the category directory when {category}.csv is not present — useful for offline tests with parquet fixtures.
