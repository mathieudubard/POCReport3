# Analysis config: use analysisIds from callback (no metadata file)

The model **does not** read analysis metadata from any file (CSV or JSON). It uses **only the array of analysis IDs** provided by the **callback** (or run parameters).

## How it works

1. The **callback** returns **analysisIds** (and **inputPaths**) in `settings`. The first ID in the array is **not** assumed to be “latest” or “main”.
2. The model downloads **analysisDetails** for each analysis (from `export/analysisidentifier={id}/analysisDetails.json`).
3. It reads **reportingDate** from each and **automatically**:
   - Sorts analyses **chronologically (oldest first)** for multi-quarter tables
   - Sets **current** = analysis with the **latest** date
   - Sets **prior** = analysis in the **calendar quarter before** current
   - Sets **prior year** = analysis in the **same quarter, previous year** (if present)
   - Builds **quarter labels** (e.g. `Q2 2025`) from reportingDate

So you only need to supply **analysisIds** in the callback response; order does not matter. See **`payload_callback_response_sample.json`** for an example callback response (include `analysisIds` and `inputPaths` in `settings`).

## Callback response example

**`payload_callback_response_sample.json`** shows the `settings` object the callback returns: `analysisIds`, `inputPaths`, etc. Replace IDs and paths with your values.

## Legacy sample files

- **`analysis_metadata.json`**, **`analysis_metadata.csv`**, **`analysis_metadata_five_quarters.json`**: kept for reference only. The model **no longer** loads these; roles and quarters are derived from analysisDetails dates only.
