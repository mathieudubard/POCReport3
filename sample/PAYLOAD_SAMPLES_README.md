# Sample payloads for analyses config

Use these to try the multi-quarter / analyses config.

## Upload a file (recommended)

**You provide the payload by uploading a file** into the execution input.

1. **Filename:** `analysis_metadata.json`
2. **Where to upload:** Under the execution **input** directory (the same prefix/path used as run input). The model looks for this file under `input/` (and subdirs) when it runs.
3. **File contents:** Valid JSON with an `analyses` array. No `settings` wrapper.

**Example – two quarters (upload this as `analysis_metadata.json`):**

Use **`analysis_metadata.json`** in this folder: rename or copy it, then upload. Or use the contents of **`payload_analyses_sample.json`**’s `settings` block but **without** the `"settings"` wrapper – i.e. upload a file that contains only:

```json
{
  "analyses": [
    { "analysisId": "4647909", "quarterLabel": "Q3 2025" },
    { "analysisId": "4647997", "quarterLabel": "Q4 2025" }
  ]
}
```

Replace the analysis IDs with your real IDs. The model will normalize this and infer current/prior from analysisDetails dates if you don’t add tags.

**Five quarters:** Upload **`analysis_metadata_five_quarters.json`** (or copy its contents) as `analysis_metadata.json` for a 5-quarter trend.

---

## Callback response (alternative)

If your run uses **settingsCallbackUrl**, the callback can return the payload in the response instead of a file. **`payload_callback_response_sample.json`** is an example: the `settings` object (including `analyses` and `inputPaths`) is what the callback returns. Replace IDs and paths with your values.

---

## File reference

| Use case | File to upload as `analysis_metadata.json` |
|----------|--------------------------------------------|
| Two quarters | `analysis_metadata.json` or contents above |
| Five quarters | `analysis_metadata_five_quarters.json` |
