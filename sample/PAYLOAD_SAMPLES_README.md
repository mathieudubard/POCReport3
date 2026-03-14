# Sample payloads for analyses config

Use these to try the multi-quarter / analyses config.

## Upload a file (recommended)

**You provide the payload by uploading a file** into the execution input.

- **Where to upload:** Under the execution **input** directory. The model looks under `input/` (and subdirs). **CSV preferred** if the wrapper has issues with JSON: the model looks for **`analysis_metadata.csv`** first, then **`analysis_metadata.json`**.

### Option A: CSV (recommended when JSON is problematic)

1. **Filename:** `analysis_metadata.csv`
2. **Format:** Header row, then one row per analysis. Columns (case-insensitive): **analysisId** (required), **quarterLabel** (optional), **tags** (optional, comma-separated: `current`, `prior`, `priorYear`), **role** (optional, single value).

**Example – upload as `analysis_metadata.csv`:**

```csv
analysisId,quarterLabel,tags
4647909,Q1 2025,prior
4647997,Q2 2025,current
```

Use **`analysis_metadata.csv`** in this folder; replace IDs/labels and upload. Omit `tags`/`role` to let the model infer current/prior from analysisDetails dates.

### Option B: JSON

1. **Filename:** `analysis_metadata.json`
2. **File contents:** Valid JSON with an `analyses` array. No `settings` wrapper.

**Example – two quarters:**

```json
{
  "analyses": [
    { "analysisId": "4647909", "quarterLabel": "Q3 2025" },
    { "analysisId": "4647997", "quarterLabel": "Q4 2025" }
  ]
}
```

**Five quarters:** use **`analysis_metadata_five_quarters.json`**.

---

## Callback response (alternative)

If your run uses **settingsCallbackUrl**, the callback can return the payload in the response instead of a file. **`payload_callback_response_sample.json`** is an example: the `settings` object (including `analyses` and `inputPaths`) is what the callback returns. Replace IDs and paths with your values.

---

## File reference

| Use case     | CSV (preferred)     | JSON alternative        |
|-------------|---------------------|--------------------------|
| Two quarters| `analysis_metadata.csv` | `analysis_metadata.json` or contents above |
| Five quarters | Same CSV, add rows  | `analysis_metadata_five_quarters.json` |
