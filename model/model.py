from moodyscappy import Cappy
import gc
import glob
from . import iosession
from .cappy_log import (
    cappy_echo_info,
    log_cappy_jwt_unusable,
    log_cappy_tenant_infra_failure,
    looks_like_cappy_jwt_failure,
    looks_like_cappy_tenant_infra_failure,
    milestone_banner,
)
import json
import logging
from json import JSONDecodeError
import os
import re
import pandas as pd
import zipfile

DEFAULT_SETTINGS = ['inputFileName', 'outputFormat', 'outputPaths', 'inputPath', 'logPath']

# Macro variables and date range for Hanmi report only (REPORT_MAPPING.md §3: 4-quarter historical + 4-quarter forecast)
REPORT_MACRO_VARIABLE_NAMES = [
    "USA Unemployment Rate",
    "USA Unemployment Rate Monthly",
    "USA Real GDP Growth",
    "USA BBB Spread",
    "US Treasury 3 Year",
    "USA CRE Price Index Growth",
    "USA House Price Index FHFA",
    "USA Retail Sales and Food Services Growth",
]
REPORT_MACRO_QUARTERS_BACK = 4
REPORT_MACRO_QUARTERS_FORWARD = 4

# Segmentation dimensions from datamodel (instrumentReference): try in order; first found with non-null values wins.
# assetClass is first so reports match typical ACL exhibits (CRE / Retail / SME) when both portfolio and asset class exist.
# Each entry: (canonical_name, *flexible_variants for _resolve_column).
SEGMENT_DIMENSION_CANDIDATES = [
    ("assetClass", "assetclass", "Asset Class", "asset_class"),
    ("portfolioIdentifier", "portfolioidentifier", "Portfolio Identifier", "portfolio_identifier"),
    ("assetSubClass1", "assetsubclass1", "Asset Sub Class 1", "asset_sub_class_1"),
    ("assetSubClass2", "assetsubclass2", "Asset Sub Class 2", "asset_sub_class_2"),
    ("portfolioSubType1", "portfoliosubtype1", "Portfolio Sub Type 1", "portfolio_sub_type_1"),
    ("portfolioSubType2", "portfoliosubtype2", "Portfolio Sub Type 2", "portfolio_sub_type_2"),
    ("instrumentSubType", "instrumentsubtype", "Instrument Sub Type", "instrument_sub_type"),
    ("consumerProductCategory", "consumerproductcategory", "Consumer Product Category", "consumer_product_category"),
    ("productType", "producttype", "Product Type", "product_type"),
    ("usRegion", "usregion", "US Region", "us_region"),
    ("entityType", "entitytype", "Entity Type", "entity_type"),
    ("locationType", "locationtype", "Location Type", "location_type"),
    ("instrumentType", "instrumenttype", "Instrument Type", "instrument_type"),
]


def _parquet_col_key(name):
    """Normalize parquet field name for allowlist matching (case, spaces, underscores)."""
    return re.sub(r"[\s_]", "", str(name)).lower()


def _parquet_wanted_normalized(names):
    return frozenset(_parquet_col_key(n) for n in names)


def _instrument_reference_parquet_wanted_normalized():
    """Columns the Hanmi / quarterly reports may read from instrumentReference (see docs/DATAMODEL_COLUMNS.md)."""
    wanted = set(
        _parquet_wanted_normalized(
            [
                "instrumentIdentifier",
                "analysisIdentifier",
                "ascImpairmentEvaluation",
                "lossRateModelName",
                "pdModelName",
                "lgdModelName",
            ]
        )
    )
    for tup in SEGMENT_DIMENSION_CANDIDATES:
        for part in tup:
            wanted.add(_parquet_col_key(part))
    return frozenset(wanted)


# Normalized keys -> intersect with each file's schema so we do not load thousands of unused Impairment Studio columns.
REPORT_PARQUET_COLS_NORMALIZED = {
    "instrumentResult": _parquet_wanted_normalized(
        [
            "instrumentIdentifier",
            "scenarioIdentifier",
            "analysisIdentifier",
            "onBalanceSheetReserve",
            "offBalanceSheetReserve",
            "onBalanceSheetReserveAdjusted",
            "onBalanceSheetReserveUnadjusted",
            "lossAllowanceAdjusted",
            "lossAllowanceUnadjusted",
            "lossAllowanceDelta",
            "amortizedCost",
            "offBalanceSheetEADAmountLifetime",
            "allowanceProvisionDelta",
            "adjusted",
        ]
    ),
    "instrumentReporting": _parquet_wanted_normalized(
        [
            "instrumentIdentifier",
            "portfolioIdentifier",
            "netChargeOffAmount",
            "grossChargeOffAmount",
            "recoveryAmount",
            "allowanceProvision",
        ]
    ),
    "instrumentReference": _instrument_reference_parquet_wanted_normalized(),
    "macroEconomicVariableInput": _parquet_wanted_normalized(
        [
            "macroeconomicVariableName",
            "valueDate",
            "macroeconomicVariableValue",
        ]
    ),
}


class Model:
    """
    Main model class.

    :param credentials: Dictionary with valid javaScript web token or username and password
    :param proxy_credentials: Dictionary with valid javaScript web token or username and password
    :param model_run_parameters_path: S3 key or local path to model run parameters configuration file (e.g., modelRunParameter.json)
    :param local_mode: (Boolean) True if model_run_parameters_path is stored in an s3 bucket, else False if a file stored locally
    """
    def __init__(self, credentials, proxy_credentials, model_run_parameters_path, local_mode=False):
        # Create module's logger and session managers
        self.logger = logging.getLogger(__name__)
        self.logger.info(f'Running in local mode: {local_mode}')
        cappy_echo_info(
            self.logger,
            "[Cappy] Model: creating main session sso_url=%r tenant_url=%r",
            credentials.get("sso_url"),
            credentials.get("tenant_url"),
        )
        try:
            self.cap_session = Cappy(**credentials)
        except Exception as e:
            if looks_like_cappy_jwt_failure(e):
                log_cappy_jwt_unusable(self.logger, e, "main Cappy session")
            elif looks_like_cappy_tenant_infra_failure(e):
                log_cappy_tenant_infra_failure(self.logger, e, "main Cappy session")
            raise
        self.io_session = iosession.IOSession(self.cap_session, model_run_parameters_path, local_mode, credentials)
        self.model_run_parameters = self.io_session.model_run_parameters
        if proxy_credentials:
            cappy_echo_info(
                self.logger,
                "[Cappy] Model: creating proxy session sso_url=%r tenant_url=%r",
                proxy_credentials.get("sso_url"),
                proxy_credentials.get("tenant_url"),
            )
            try:
                self.proxy_cap_session = Cappy(**proxy_credentials, errors='log')
            except Exception as e:
                if looks_like_cappy_jwt_failure(e):
                    log_cappy_jwt_unusable(self.logger, e, "proxy Cappy session")
                elif looks_like_cappy_tenant_infra_failure(e):
                    log_cappy_tenant_infra_failure(self.logger, e, "proxy Cappy session")
                raise
        # When settings.returnReportsInResponse is true, run() sets this to {"reports": {filename: object, ...}} for API responses.
        self.report_response_payload = None
        self._parquet_load_cache = {}
        self._jwt = credentials.get("jwt")

    def _is_library_mode(self):
        """True when settings.libraryMode — importable runs: no S3 uploads, no zip, no log upload."""
        return bool(self.model_run_parameters.settings.get("libraryMode"))

    def run(self):
        print("[Model run] START")
        self._parquet_load_cache.clear()
        self.logger.info(f'Running model: {self.model_run_parameters.name}')

        print("[Model run] Step 1: List S3 folders")
        if self.model_run_parameters.use_per_analysis_s3_download():
            # Tenant CSV layout lives under bucket-root export/ (one file per category per analysis).
            self.io_session.list_and_print_s3_folders(
                prefix=self.io_session.EXPORT_BASE + "/",
                list_object_keys=False,
            )
        else:
            self.io_session.list_and_print_s3_folders()

        print("[Model run] Step 2: Get source input files from S3")
        self.io_session.getSourceInputFiles()
        print("[Model run] Step 2 done; Step 3+ hold loaded table data in memory (OOM/SIGKILL => increase worker RAM).")

        milestone_banner("building reports (in-memory)")
        print("[Model run] Step 3: Build quarterly summary report (current/prior from analysisIds, all doc sections)")
        self.build_quarterly_summary_report()

        print("[Model run] Step 3b: Build Hanmi ACL quarterly report (multi-section JSON)")
        self.build_hanmi_acl_quarterly_report()

        self._fetch_and_write_adjustment_details()

        if not self._is_library_mode():
            print("[Model run] Step 4: Create zip of all report output files")
            self.create_report_export_zip()

        self._build_report_response_payload()

        if not self._is_library_mode():
            print("[Model run] Step 5: Create local model run parameters")
            new_mrp = self.createLocalModelRunParameters()
            print(f"[Model run] Local MRP written to: {new_mrp}")

            print("[Model run] Step 6: Create file dicts and upload outputs to S3")
            all_files = self.io_session.createOutputFileDicts(new_mrp)
            keys_preview = list(all_files.keys())[:15]
            if len(all_files) > 15:
                keys_preview.append("...")
            print("[Model run] Uploading {} file(s): {}".format(len(all_files), keys_preview))
            if all_files:
                self.io_session.uploadFiles(all_files)
        else:
            print("[Model run] libraryMode: skipping zip, local MRP upload, and S3 output upload")
        print("[Model run] END")

    def sortOutInstrumentReferences(self, all_files):
        """Upload input instrumentReference to output destination if output instrumentReference isn't generated"""
        count = 0
        for dictionary in all_files:
            # print(dictionary)
            if 'instrumentReference' in dictionary:
                count += 1
        if count == 2:
            for dictionary in all_files:
                if 'instrumentReference' in dictionary and 'inputPath' in dictionary['instrumentReference']:
                    dictionary['inputInstrumentReference'] = dictionary['instrumentReference']
                    del dictionary['instrumentReference']
        return all_files

    def createLocalModelRunParameters(self):
        """Copy modelRunParameter.json, replacing input/output/log paths with local temp directories"""
        new_mrp = self.model_run_parameters.json.copy()
        new_mrp['settings'] = new_mrp.get('settings', {})
        # local_dir = iosession.IOSession.create_addtl_io_directories()
        new_mrp['settings'].update(self.io_session.local_directories)
        new_mrp_path = f'{self.io_session.local_temp_directory}/localModelRunParameters.json'
        with open(new_mrp_path, 'w') as f:
            json.dump(new_mrp, f)
        return new_mrp_path

    def _find_column(self, df, name):
        """Return column key from df that matches name case-insensitively, or None."""
        for c in df.columns:
            if c and str(c).strip().lower() == name.lower():
                return c
        return None

    def _normalize_col_name(self, s):
        """Lowercase, strip, remove spaces/underscores for flexible matching."""
        if not s or not isinstance(s, str):
            return ""
        return str(s).strip().lower().replace(" ", "").replace("_", "")

    def _find_column_flexible(self, df, *candidate_names):
        """Return column key from df that matches any of the candidate names (case-insensitive, spaces/underscores ignored)."""
        if df is None or df.empty:
            return None
        normalized_targets = {self._normalize_col_name(n): n for n in candidate_names if n}
        for c in df.columns:
            if not c:
                continue
            nc = self._normalize_col_name(c)
            if nc in normalized_targets:
                return c
            if nc and any(nc == self._normalize_col_name(t) for t in candidate_names):
                return c
        return None

    def _resolve_column(self, df, canonical_name, *flexible_variants):
        """
        Resolve column using datamodel canonical name first, then flexible variants.
        Use for all report-related lookups so parquet with lowercase/spaced names still match.
        Canonical names from datamodel/ImpairmentStudio-DataDictionary.csv (Attribute Name column).
        """
        if df is None or df.empty:
            return None
        c = self._find_column(df, canonical_name)
        if c is not None:
            return c
        if flexible_variants:
            return self._find_column_flexible(df, canonical_name, *flexible_variants)
        return None

    def _get_methodology_columns(self, df):
        """Return (lossRateModelName_col, pdModelName_col, lgdModelName_col) for ref; any may be None. Uses same resolution as report (datamodel + flexible)."""
        if df is None or df.empty:
            return None, None, None
        lr = self._resolve_column(df, "lossRateModelName", "lossratemodelname")
        pd = self._resolve_column(df, "pdModelName", "pdmodelname")
        lgd = self._resolve_column(df, "lgdModelName", "lgdmodelname")
        return lr, pd, lgd

    def _get_segment_column(self, df):
        """
        Return (canonical_name, resolved_col) for the first segmentation dimension present in df with at least one non-null value.
        Uses SEGMENT_DIMENSION_CANDIDATES (datamodel: portfolioIdentifier, assetClass, assetSubClass1, ...). Enables segmentMethodology/quantitative/qualitative from any available segment dimension.
        """
        if df is None or df.empty:
            return None, None
        for item in SEGMENT_DIMENSION_CANDIDATES:
            canonical = item[0]
            variants = item[1:] if len(item) > 1 else ()
            col = self._resolve_column(df, canonical, *variants)
            if col is not None and col in df.columns:
                non_null = df[col].notna() & (df[col].astype(str).str.strip() != "")
                if non_null.any():
                    return canonical, col
        return None, None

    def _get_next_segment_column_after(self, df, primary_canonical_name):
        """
        Next finer segmentation dimension after ``primary_canonical_name`` (e.g. assetClass after portfolio).
        Used so segment/methodology tables can show parent segment once and sub-segments per methodology row.
        """
        if df is None or df.empty or not primary_canonical_name:
            return None, None
        idx = None
        for i, item in enumerate(SEGMENT_DIMENSION_CANDIDATES):
            if item[0] == primary_canonical_name:
                idx = i
                break
        if idx is None:
            return None, None
        for j in range(idx + 1, len(SEGMENT_DIMENSION_CANDIDATES)):
            item = SEGMENT_DIMENSION_CANDIDATES[j]
            canonical = item[0]
            variants = item[1:] if len(item) > 1 else ()
            col = self._resolve_column(df, canonical, *variants)
            if col is None or col not in df.columns:
                continue
            non_null = df[col].notna() & (df[col].astype(str).str.strip() != "")
            if non_null.any():
                return canonical, col
        return None, None

    def _get_segmentation_candidates_for_debug(self, df):
        """Build a dict of segmentation candidate name -> {found, column, distinct, sample} for debug JSON."""
        if df is None or df.empty:
            return {}
        out = {}
        all_cols = list(df.columns)
        for item in SEGMENT_DIMENSION_CANDIDATES:
            canonical = item[0]
            variants = item[1:] if len(item) > 1 else ()
            col = self._resolve_column(df, canonical, *variants)
            if col is not None and col in df.columns:
                uniq = df[col].dropna().astype(str).str.strip()
                uniq = uniq[uniq != ""].unique().tolist()
                out[canonical] = {"found": True, "column": col, "distinct": len(uniq), "sample": uniq[:15]}
            else:
                out[canonical] = {"found": False}
        return out

    def _methodology_from_row(self, row, lr_col, pd_col, lgd_col):
        """First non-null of lossRate, pdModel, lgdModel for display/grouping to avoid nulls."""
        for col in (lr_col, pd_col, lgd_col):
            if col is not None and col in row.index:
                v = row.get(col)
                if pd.notna(v) and str(v).strip():
                    return str(v).strip()
        return ""

    def _safe_sum(self, df, col, default=0.0):
        """Sum of col in df; return default if col missing or empty."""
        if df is None or df.empty:
            return default
        c = self._find_column(df, col) if isinstance(col, str) else col
        if c is None or c not in df.columns:
            return default
        try:
            return float(df[c].sum())
        except (TypeError, ValueError):
            return default

    def _join_keys_and_log(self, left_df, right_df, left_name="left", right_name="right", analysis_id=None):
        """
        Return (left_on, right_on) for joining on instrumentIdentifier and optionally analysisIdentifier.
        Logs join keys and that both sides are for the same analysis.
        """
        id_left = self._find_column(left_df, "instrumentIdentifier")
        id_right = self._find_column(right_df, "instrumentIdentifier")
        an_left = self._find_column(left_df, "analysisIdentifier")
        an_right = self._find_column(right_df, "analysisIdentifier")
        left_on = []
        right_on = []
        if id_left and id_right:
            left_on.append(id_left)
            right_on.append(id_right)
        if an_left and an_right:
            left_on.append(an_left)
            right_on.append(an_right)
        if not left_on:
            print("[join] {} vs {}: no join keys (instrumentIdentifier L={} R={})".format(
                left_name, right_name, bool(id_left), bool(id_right)))
            return None, None
        return left_on, right_on

    def _filter_summary_scenario(self, df):
        """Filter instrumentResult to scenarioIdentifier = Summary (case-insensitive)."""
        if df is None or df.empty:
            return df
        col = self._find_column(df, "scenarioIdentifier")
        if col is None:
            return df
        return df[df[col].astype(str).str.strip().str.lower() == "summary"].copy()

    def _date_to_quarter(self, dt):
        """Return (year, quarter) for a datetime; quarter in 1..4."""
        if dt is None:
            return None
        try:
            q = (pd.Timestamp(dt).month - 1) // 3 + 1
            return (int(pd.Timestamp(dt).year), q)
        except Exception:
            return None

    def _normalize_segment_display(self, segment):
        """Return display string for segment; normalize nan/empty to 'Unallocated'."""
        if segment is None or (isinstance(segment, float) and pd.isna(segment)):
            return "Unallocated"
        s = str(segment).strip()
        if s == "" or s.lower() == "nan":
            return "Unallocated"
        return s

    def _instrument_result_adjusted_column(self, df):
        """Resolve instrumentResult ``adjusted`` (Boolean) column name, or None if missing."""
        if df is None or df.empty:
            return None
        return self._resolve_column(df, "adjusted", "Adjusted")

    def _resolve_instrument_result_adj_unadj_columns(self, df):
        """
        Prefer on-balance-sheet reserve adjusted/unadjusted; else datamodel lossAllowance adjusted/unadjusted.
        Export CSV/parquet may use either naming (flexible match). Returns (adj_col, unadj_col) or (None, None).
        """
        if df is None or df.empty:
            return None, None
        adj = self._resolve_column(
            df,
            "onBalanceSheetReserveAdjusted",
            "onbalancesheetreserveadjusted",
            "On Balance Sheet Reserve Adjusted",
            "on_balance_sheet_reserve_adjusted",
        )
        unadj = self._resolve_column(
            df,
            "onBalanceSheetReserveUnadjusted",
            "onbalancesheetreserveunadjusted",
            "On Balance Sheet Reserve Unadjusted",
            "on_balance_sheet_reserve_unadjusted",
        )
        if adj and unadj:
            return adj, unadj
        adj2 = self._resolve_column(
            df,
            "lossAllowanceAdjusted",
            "lossallowanceadjusted",
            "Loss Allowance Adjusted",
            "loss_allowance_adjusted",
        )
        unadj2 = self._resolve_column(
            df,
            "lossAllowanceUnadjusted",
            "lossallowanceunadjusted",
            "Loss Allowance Unadjusted",
            "loss_allowance_unadjusted",
        )
        if adj2 and unadj2:
            return adj2, unadj2
        return None, None

    def _resolve_unadjusted_reserve_column(self, df):
        """Unadjusted reserve level for quantitative loss-rate math (pair or unadjusted-only)."""
        _, u = self._resolve_instrument_result_adj_unadj_columns(df)
        if u:
            return u
        return self._resolve_column(
            df,
            "onBalanceSheetReserveUnadjusted",
            "lossAllowanceUnadjusted",
            "onbalancesheetreserveunadjusted",
            "lossallowanceunadjusted",
        )

    def _resolve_loss_allowance_delta_column(self, df):
        """
        Instrument Result **Allowance Amount** (datamodel: ``lossAllowanceDelta``): change in allowance,
        defined as the sum of on- and off-balance-sheet reserves. Preferred for quantitative vs qualitative
        splits by ``adjusted`` when present. See ImpairmentStudio-DataDictionary.csv (Instrument Result).
        """
        if df is None or df.empty:
            return None
        return self._resolve_column(
            df,
            "lossAllowanceDelta",
            "lossallowancedelta",
            "Loss Allowance Delta",
            "loss_allowance_delta",
        )

    def _mask_adjusted_true(self, df, col):
        """
        Mask for **qualitative** instrumentResult rows: ``adjusted`` must be the string ``TRUE``
        (case-insensitive, after strip), boolean True, or integer 1. **Everything else** is treated as
        **quantitative** — including ``FALSE``, null/NaN, empty, 0, or other strings.
        """
        if col is None or col not in df.columns:
            return pd.Series(False, index=df.index)
        s = df[col]
        if pd.api.types.is_bool_dtype(s):
            return s.fillna(False)
        if pd.api.types.is_integer_dtype(s):
            return s.fillna(0).astype(int).eq(1)
        if pd.api.types.is_float_dtype(s):
            return pd.to_numeric(s, errors="coerce").fillna(0).eq(1.0)
        str_s = s.astype(str).str.strip().str.upper()
        return (str_s == "TRUE") | (str_s == "1")

    def _parse_quarter_label(self, quarter_label):
        """Parse 'Q{n} {year}' -> (year, quarter_num). Returns None if unparseable."""
        if not quarter_label:
            return None
        m = re.match(r"Q(\d)\s+(\d{4})", str(quarter_label).strip(), re.IGNORECASE)
        if m:
            return (int(m.group(2)), int(m.group(1)))
        return None

    def _add_quantitative_deltas(self, main_rows):
        """
        Add incrDecrQtoQ and incrDecrYtoY (loss rate and reserve) to each row.
        Look up prior quarter and prior-year-same-quarter by (segment, year, q).
        """
        if not main_rows:
            return
        lookup = {}
        for row in main_rows:
            yq = self._parse_quarter_label(row.get("quarterLabel"))
            if yq:
                key = (row.get("segment"), yq[0], yq[1])
                lookup[key] = row
        for row in main_rows:
            yq = self._parse_quarter_label(row.get("quarterLabel"))
            if not yq:
                continue
            y, q = yq
            prior_q = (y, q - 1) if q > 1 else (y - 1, 4)
            prior_y = (y - 1, q)
            seg = row.get("segment")
            prior_q_row = lookup.get((seg, prior_q[0], prior_q[1]))
            prior_y_row = lookup.get((seg, prior_y[0], prior_y[1]))
            if prior_q_row is not None:
                cur_r = row.get("lossRatePct")
                pr_r = prior_q_row.get("lossRatePct")
                if cur_r is not None and pr_r is not None:
                    row["incrDecrQtoQ_lossRatePct"] = round(float(cur_r) - float(pr_r), 4)
                cur_res = row.get("quantitativeReserve")
                pr_res = prior_q_row.get("quantitativeReserve")
                if cur_res is not None and pr_res is not None:
                    row["incrDecrQtoQ_quantitativeReserve"] = round(float(cur_res) - float(pr_res), 2)
            else:
                row["incrDecrQtoQ_lossRatePct"] = None
                row["incrDecrQtoQ_quantitativeReserve"] = None
            if prior_y_row is not None:
                cur_r = row.get("lossRatePct")
                pr_r = prior_y_row.get("lossRatePct")
                if cur_r is not None and pr_r is not None:
                    row["incrDecrYtoY_lossRatePct"] = round(float(cur_r) - float(pr_r), 4)
                cur_res = row.get("quantitativeReserve")
                pr_res = prior_y_row.get("quantitativeReserve")
                if cur_res is not None and pr_res is not None:
                    row["incrDecrYtoY_quantitativeReserve"] = round(float(cur_res) - float(pr_res), 2)
            else:
                row["incrDecrYtoY_lossRatePct"] = None
                row["incrDecrYtoY_quantitativeReserve"] = None

    def _build_net_chargeoffs_annual(self, net_chargeoffs_quarterly):
        """Aggregate netChargeOffsQuarterly by segment and year. Returns list of { segment, year, netChargeOffs }."""
        by_seg_year = {}
        for row in net_chargeoffs_quarterly or []:
            seg = row.get("segment") or "Unallocated"
            yq = self._parse_quarter_label(row.get("quarterLabel"))
            if not yq:
                continue
            year = yq[0]
            key = (seg, year)
            by_seg_year[key] = by_seg_year.get(key, 0.0) + float(row.get("netChargeOffs") or 0)
        return [{"segment": seg, "year": year, "netChargeOffs": round(v, 2)} for (seg, year), v in sorted(by_seg_year.items())]

    def _resolve_analysis_roles_from_dates(self, report_dir, analysis_ids):
        """
        Use only the provided analysisIds array; scan analysisDetails for reportingDate and
        set current/prior/priorYear and chronological quarters automatically. First in array is
        not assumed to be latest – we sort by date. Writes to settings.analysisRoles and
        settings.quarterLabels. Returns (current_id, prior_id, prior_year_id, quarters_list).
        """
        if not report_dir or not analysis_ids:
            return None, None, None, []
        dated = []
        for aid in analysis_ids:
            dt = self._get_reporting_date_from_analysis_details(report_dir, aid)
            dated.append((str(aid), dt))
        # Keep all; use original order for tie-break when date is missing or tied
        order = {str(aid): i for i, aid in enumerate(analysis_ids)}
        # Sort by date ascending (oldest first); None dates last, ties by original order
        def sort_key(x):
            aid, dt = x
            if dt is None:
                return (1, 0, order.get(aid, 999))
            return (0, pd.Timestamp(dt).toordinal(), order.get(aid, 999))
        dated.sort(key=sort_key)
        # quarters_list = chronological (oldest first) for multi-quarter tables
        quarters_list = [aid for aid, _ in dated]
        # Current = latest date (last in sorted); prior = calendar quarter before; priorYear = same quarter prior year
        with_dates = [(aid, dt) for aid, dt in dated if dt is not None]
        if not with_dates:
            current_id = quarters_list[0] if quarters_list else None
            prior_id = prior_year_id = None
        else:
            current_id = with_dates[-1][0]
            current_dt = with_dates[-1][1]
            yq_current = self._date_to_quarter(current_dt)
            prior_yq = (yq_current[0], yq_current[1] - 1) if yq_current[1] > 1 else (yq_current[0] - 1, 4) if yq_current else None
            prior_year_yq = (yq_current[0] - 1, yq_current[1]) if yq_current else None
            prior_id = prior_year_id = None
            for aid, dt in reversed(with_dates[:-1]):
                yq = self._date_to_quarter(dt)
                if not yq:
                    continue
                if prior_id is None and yq == prior_yq:
                    prior_id = aid
                if prior_year_id is None and yq == prior_year_yq:
                    prior_year_id = aid
                if prior_id and prior_year_id:
                    break
        quarter_labels = {}
        for aid, dt in dated:
            if dt is not None:
                quarter_labels[str(aid)] = "Q{} {}".format((dt.month - 1) // 3 + 1, dt.year)
        io = self.io_session
        io.model_run_parameters.settings["analysisRoles"] = {
            "current": current_id,
            "prior": prior_id,
            "priorYear": prior_year_id,
            "quarters": quarters_list,
        }
        if quarter_labels:
            io.model_run_parameters.settings["quarterLabels"] = quarter_labels
        print("[hanmi_acl_report] roles from dates: current={}, prior={}, priorYear={}, quarters={} (chronological)".format(
            current_id, prior_id, prior_year_id, quarters_list))
        return current_id, prior_id, prior_year_id, quarters_list

    def _get_analysis_roles(self):
        """
        Return (current_id, prior_id, prior_year_id, quarters_list) from settings.analysisRoles
        if present, else from analysisIds: current=first, prior=second, prior_year=None, quarters=[].
        quarters is the ordered list of analysis IDs for multi-quarter tables.
        """
        io = self.io_session
        analysis_ids = io.model_run_parameters.settings.get("analysisIds", []) or []
        roles = io.model_run_parameters.settings.get("analysisRoles") or {}
        current_id = roles.get("current")
        prior_id = roles.get("prior")
        prior_year_id = roles.get("priorYear")
        quarters = roles["quarters"] if "quarters" in roles and isinstance(roles["quarters"], list) else []
        if not quarters and analysis_ids:
            quarters = [str(aid) for aid in analysis_ids]
        if (current_id is None or prior_id is None) and analysis_ids:
            current_id = current_id or (analysis_ids[0] if analysis_ids else None)
            prior_id = prior_id or (analysis_ids[1] if len(analysis_ids) >= 2 else None)
        return current_id, prior_id, prior_year_id, quarters

    def _get_quarter_label(self, report_dir, analysis_id):
        """Return display label for a quarter (e.g. 'Q4 2025'). Uses settings.quarterLabels if set, else reportingDate from analysisDetails."""
        labels = self.io_session.model_run_parameters.settings.get("quarterLabels") or {}
        aid_str = str(analysis_id)
        if aid_str in labels:
            return labels[aid_str]
        dt = self._get_reporting_date_from_analysis_details(report_dir, analysis_id)
        if dt is not None:
            return "Q{} {}".format((dt.month - 1) // 3 + 1, dt.year)
        return str(analysis_id)

    def _get_quarters_for_tables(self, report_dir, current_id, prior_id, quarters_list):
        """
        Return ordered list of (analysis_id, quarter_label) for multi-quarter tables.
        Uses quarters_list if non-empty; else [prior_id, current_id] when both exist, else [current_id].
        Each ID must be in settings.analysisIds so data can be loaded.
        """
        if quarters_list:
            ids = [str(aid) for aid in quarters_list]
        elif prior_id and current_id:
            ids = [str(prior_id), str(current_id)]
        elif current_id:
            ids = [str(current_id)]
        else:
            return []
        return [(aid, self._get_quarter_label(report_dir, aid)) for aid in ids]

    def _parquet_columns_to_read(self, file_path, category):
        """
        Return (columns, schema_col_count) for read_parquet(columns=...), or (None, count_or_None) to read all.
        Pruning avoids OOM when instrument parquets carry tens of thousands of schema columns.
        """
        try:
            import pyarrow.parquet as pq
        except ImportError:
            self.logger.warning("pyarrow unavailable; cannot prune parquet columns for %s", category)
            return None, None
        try:
            schema = pq.read_schema(file_path)
        except Exception as e:
            self.logger.debug("read_schema failed for %s: %s", file_path, e)
            return None, None
        n_all = len(schema.names)
        wanted = REPORT_PARQUET_COLS_NORMALIZED.get(category)
        if not wanted:
            return None, n_all
        names = list(schema.names)
        selected = [n for n in names if _parquet_col_key(n) in wanted]
        if not selected:
            self.logger.warning(
                "Parquet prune: 0 column overlap for %s file=%s (file has %d cols); reading full file",
                category,
                os.path.basename(file_path),
                n_all,
            )
            return None, n_all
        return selected, n_all

    def _csv_columns_to_read(self, file_path, category):
        """Return (columns, header_col_count) for read_csv(usecols=...), or (None, n) to read full file."""
        try:
            df0 = pd.read_csv(file_path, nrows=0)
        except Exception as e:
            self.logger.debug("CSV header read failed for %s: %s", file_path, e)
            return None, None
        names = list(df0.columns)
        n_all = len(names)
        wanted = REPORT_PARQUET_COLS_NORMALIZED.get(category)
        if not wanted:
            return None, n_all
        selected = [n for n in names if _parquet_col_key(n) in wanted]
        if not selected:
            self.logger.warning(
                "CSV prune: 0 column overlap for %s file=%s (file has %d cols); reading full file",
                category,
                os.path.basename(file_path),
                n_all,
            )
            return None, n_all
        return selected, n_all

    def _load_parquet_for_analysis(self, category, analysis_id, filter_summary=False):
        """Load category table for one analysis: prefer {category}.csv in the category dir, else all parquet under that dir."""
        cache_key = (category, str(analysis_id), bool(filter_summary))
        if cache_key in self._parquet_load_cache:
            return self._parquet_load_cache[cache_key]
        io = self.io_session
        input_paths = (io.local_directories.get("inputPaths") or {}).get(category) or []
        analysis_ids = io.model_run_parameters.settings.get("analysisIds", []) or []
        # Match by string so 4647997 and "4647997" both work (JSON/callback may use either)
        idx = None
        for i, aid in enumerate(analysis_ids):
            if str(aid) == str(analysis_id):
                idx = i
                break
        if idx is None or idx >= len(input_paths):
            self._parquet_load_cache[cache_key] = None
            return None
        load_dir = input_paths[idx]
        csv_path = os.path.join(load_dir, "{}.csv".format(category))
        if os.path.isfile(csv_path):
            try:
                cols, n_all = self._csv_columns_to_read(csv_path, category)
                if cols is not None and n_all is not None and len(cols) < n_all:
                    print(
                        "[_load_csv] column_prune category={} analysisId={} cols_read={}/{}".format(
                            category, analysis_id, len(cols), n_all
                        )
                    )
                df = pd.read_csv(csv_path, usecols=cols) if cols is not None else pd.read_csv(csv_path, low_memory=False)
                if filter_summary:
                    df = self._filter_summary_scenario(df)
                self._parquet_load_cache[cache_key] = df
                return df
            except Exception as e:
                self.logger.warning("Failed to read CSV %s: %s", csv_path, e)
                self._parquet_load_cache[cache_key] = None
                return None
        files = glob.glob(os.path.join(load_dir, "**", "*.parquet"), recursive=True)
        if not files:
            print("[_load_parquet] no parquet in {} (category={}, analysisId={})".format(load_dir, category, analysis_id))
            self._parquet_load_cache[cache_key] = None
            return None
        dfs = []
        pruned_files = 0
        total_schema_cols = 0
        total_read_cols = 0
        for f in files:
            try:
                cols, n_schema = self._parquet_columns_to_read(f, category)
                if cols is not None and n_schema is not None and len(cols) < n_schema:
                    pruned_files += 1
                    total_schema_cols += n_schema
                    total_read_cols += len(cols)
                df_part = pd.read_parquet(f, columns=cols) if cols is not None else pd.read_parquet(f)
                dfs.append(df_part)
            except Exception as e:
                self.logger.warning("Failed to read %s: %s", f, e)
        if not dfs:
            self._parquet_load_cache[cache_key] = None
            return None
        if pruned_files:
            print(
                "[_load_parquet] column_prune category={} analysisId={} files_pruned={}/{} "
                "approx_schema_cols={} cols_read={}".format(
                    category,
                    analysis_id,
                    pruned_files,
                    len(files),
                    total_schema_cols,
                    total_read_cols,
                )
            )
        out = pd.concat(dfs, ignore_index=True)
        if filter_summary:
            out = self._filter_summary_scenario(out)
        self._parquet_load_cache[cache_key] = out
        return out

    def _get_reporting_date_from_analysis_details(self, report_dir, analysis_id):
        """Return reportingDate from report_dir/analysisDetails_{id}.json as datetime or None."""
        if not report_dir or not analysis_id:
            return None
        path = os.path.join(report_dir, "analysisDetails_{}.json".format(analysis_id))
        if not os.path.isfile(path):
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (OSError, json.JSONDecodeError):
            return None
        d = data.get("reportingDate")
        if not d:
            return None
        try:
            return pd.to_datetime(d)
        except Exception:
            return None

    def _get_analysis_display_meta_from_details(self, report_dir, analysis_id):
        """
        Read analysisDetails_{id}.json for human-facing labels: analysis ``name`` and ``reportingDate``.
        Used for adjustment sections (prefer name over raw analysis id in reports).
        """
        if not report_dir or not analysis_id:
            return {}
        path = os.path.join(report_dir, "analysisDetails_{}.json".format(analysis_id))
        if not os.path.isfile(path):
            return {}
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (OSError, JSONDecodeError):
            return {}
        name = data.get("name")
        rd = data.get("reportingDate")
        rd_str = None
        if rd is not None:
            try:
                rd_str = pd.to_datetime(rd).strftime("%Y-%m-%d")
            except Exception:
                s = str(rd).strip()
                rd_str = s if s else None
        an = None
        if name is not None:
            an = str(name).strip() or None
        return {"analysisName": an, "reportingDate": rd_str}

    def _normalize_iso_date_str(self, value):
        """Return YYYY-MM-DD for analysisDetails dates, or None."""
        if value is None:
            return None
        try:
            return pd.to_datetime(value).strftime("%Y-%m-%d")
        except Exception:
            s = str(value).strip()
            return s if s else None

    def _load_scenarios_from_analysis_details(self, report_dir, analysis_id):
        """Return list of scenario dicts from analysisDetails ``scenarios`` array."""
        if not report_dir or not analysis_id:
            return []
        path = os.path.join(report_dir, "analysisDetails_{}.json".format(analysis_id))
        if not os.path.isfile(path):
            return []
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (OSError, JSONDecodeError):
            return []
        out = []
        for s in (data.get("scenarios") or []):
            if not isinstance(s, dict):
                continue
            w = s.get("weight")
            try:
                w = float(w) if w is not None and str(w).strip() != "" else None
            except (TypeError, ValueError):
                w = None
            out.append({
                "name": s.get("name"),
                "asOfDate": self._normalize_iso_date_str(s.get("asOfDate")),
                "weight": w,
                "scenarioType": s.get("scenarioType"),
            })
        return out

    def _build_assumptions_and_parameters(self, report_dir, main_analysis_id, prior_analysis_id):
        """
        Section 1.1.2: scenario lists from analysisDetails for main (current) vs prior analysis.
        """
        if not main_analysis_id:
            return {
                "mainAnalysis": None,
                "priorAnalysis": None,
                "comparison": {"sameScenarioSet": None, "note": "No current analysis id."},
            }
        main = {
            "analysisId": str(main_analysis_id),
            **{k: v for k, v in self._get_analysis_display_meta_from_details(report_dir, main_analysis_id).items()},
            "quarterLabel": self._get_quarter_label(report_dir, main_analysis_id),
            "scenarios": self._load_scenarios_from_analysis_details(report_dir, main_analysis_id),
        }
        prior = None
        if prior_analysis_id:
            prior = {
                "analysisId": str(prior_analysis_id),
                **{k: v for k, v in self._get_analysis_display_meta_from_details(report_dir, prior_analysis_id).items()},
                "quarterLabel": self._get_quarter_label(report_dir, prior_analysis_id),
                "scenarios": self._load_scenarios_from_analysis_details(report_dir, prior_analysis_id),
            }
        names_main = [str(s.get("name")).strip() for s in (main.get("scenarios") or []) if s.get("name")]
        names_prior = [str(s.get("name")).strip() for s in ((prior or {}).get("scenarios") or []) if s.get("name")]
        set_m = {n.lower() for n in names_main}
        set_p = {n.lower() for n in names_prior}
        only_main = sorted({n for n in names_main if n.lower() not in set_p}, key=str.lower)
        only_prior = sorted({n for n in names_prior if n.lower() not in set_m}, key=str.lower)
        in_both = sorted({n for n in names_main if n.lower() in set_p}, key=str.lower)
        same_set = (set_m == set_p) if prior else None
        return {
            "mainAnalysis": main,
            "priorAnalysis": prior,
            "comparison": {
                "sameScenarioSet": same_set,
                "scenarioNamesMain": names_main,
                "scenarioNamesPrior": names_prior,
                "onlyInMain": only_main,
                "onlyInPrior": only_prior,
                "inBoth": in_both,
            },
        }

    def _filter_macro_for_report(self, df_macro, report_dir, current_id):
        """
        Filter macro DataFrame to only variables and valueDates needed for the report.
        Variables: REPORT_MACRO_VARIABLE_NAMES. Dates: reportingDate ± REPORT_MACRO_QUARTERS_BACK/FORWARD.
        """
        if df_macro is None or df_macro.empty:
            return df_macro
        name_col = self._find_column(df_macro, "macroeconomicVariableName")
        date_col = self._find_column(df_macro, "valueDate")
        if not name_col:
            return df_macro
        # Filter to report variables only (case-insensitive match to allowed names)
        allowed = {s.strip().lower() for s in REPORT_MACRO_VARIABLE_NAMES}
        mask_name = df_macro[name_col].astype(str).str.strip().str.lower().isin(allowed)
        out = df_macro.loc[mask_name].copy()
        if out.empty:
            print("[hanmi_acl_report] macro: filtered to report variables only -> 0 rows (allowed: {})".format(len(REPORT_MACRO_VARIABLE_NAMES)))
            return out
        if date_col:
            center = self._get_reporting_date_from_analysis_details(report_dir, current_id)
            if center is not None:
                start = center - pd.DateOffset(months=3 * REPORT_MACRO_QUARTERS_BACK)
                end = center + pd.DateOffset(months=3 * REPORT_MACRO_QUARTERS_FORWARD)
                try:
                    out["_valueDate_dt"] = pd.to_datetime(out[date_col], errors="coerce")
                    out = out[out["_valueDate_dt"].notna() & (out["_valueDate_dt"] >= start) & (out["_valueDate_dt"] <= end)]
                    out = out.drop(columns=["_valueDate_dt"], errors="ignore")
                except Exception:
                    pass
        print("[hanmi_acl_report] macro: filtered to {} report variables, date range {}q back / {}q forward -> {} rows".format(
            len(REPORT_MACRO_VARIABLE_NAMES), REPORT_MACRO_QUARTERS_BACK, REPORT_MACRO_QUARTERS_FORWARD, len(out)))
        return out

    def _get_macro_base_as_of_date(self, report_dir, analysis_id):
        """BASE scenario asOfDate (YYYY-MM-DD) from analysisDetails, for macro input path alignment."""
        for s in self._load_scenarios_from_analysis_details(report_dir, analysis_id):
            if str(s.get("name") or "").strip().upper() == "BASE":
                return s.get("asOfDate")
        return None

    def _build_macroeconomic_baseline_wide(self, flat_rows, quarter_labels_ordered):
        """
        One numeric value per (variableName, quarterLabel): latest valueDate in that quarter's rows wins.
        quarter_labels_ordered preserves chronological column order from the report run.
        """
        out = {"quarterLabels": list(quarter_labels_ordered or []), "rows": []}
        if not flat_rows or not quarter_labels_ordered:
            return out
        ql_set = set(quarter_labels_ordered)
        best = {}  # (variableName, quarterLabel) -> (pd.Timestamp, value)
        for r in flat_rows:
            vn = (r.get("variableName") or "").strip()
            ql = r.get("quarterLabel")
            if not vn or ql not in ql_set:
                continue
            vd = r.get("valueDate")
            val = r.get("value")
            try:
                vdt = pd.to_datetime(vd, errors="coerce")
            except Exception:
                vdt = pd.NaT
            key = (vn, ql)
            cur = best.get(key)
            if cur is None:
                best[key] = (vdt, val)
            else:
                cur_dt, cur_val = cur
                if pd.notna(vdt) and (pd.isna(cur_dt) or vdt > cur_dt):
                    best[key] = (vdt, val)
                elif pd.isna(vdt) and pd.isna(cur_dt) and cur_val is None and val is not None:
                    best[key] = (vdt, val)
        var_names = sorted({(r.get("variableName") or "").strip() for r in flat_rows if (r.get("variableName") or "").strip()})
        rows_out = []
        for vn in var_names:
            vbq = {}
            for ql in quarter_labels_ordered:
                t = best.get((vn, ql))
                vbq[ql] = t[1] if t is not None else None
            rows_out.append({"variableName": vn, "valuesByQuarter": vbq})
        out["rows"] = rows_out
        return out

    def _normalize_macro_value_date_key(self, value_date):
        """Stable key for pivoting macro rows (ISO date string when parseable)."""
        if value_date is None:
            return None
        try:
            dt = pd.to_datetime(value_date, errors="coerce")
            if pd.notna(dt):
                return dt.strftime("%Y-%m-%d")
        except Exception:
            pass
        s = str(value_date).strip()
        return s if s else None

    def _build_macroeconomic_baseline_matrix(self, flat_rows, quarter_labels_ordered):
        """
        Per MEV: two-dimensional grid — valueDate (rows) × quarter / BASE as-of date (columns).
        Each cell is the macroeconomicVariableValue for that (variable, quarterLabel, valueDate).
        """
        out = {"quarterLabels": list(quarter_labels_ordered or []), "variables": []}
        if not flat_rows or not quarter_labels_ordered:
            return out
        ql_order = list(quarter_labels_ordered)
        ql_set = set(ql_order)
        asof_by_quarter = {}
        for r in flat_rows:
            ql = r.get("quarterLabel")
            if ql in ql_set and ql not in asof_by_quarter and r.get("macroBaseAsOfDate"):
                asof_by_quarter[ql] = r.get("macroBaseAsOfDate")
        var_names = sorted({(r.get("variableName") or "").strip() for r in flat_rows if (r.get("variableName") or "").strip()})
        for vn in var_names:
            cells = {}  # (vd_key, ql) -> value
            for r in flat_rows:
                if (r.get("variableName") or "").strip() != vn:
                    continue
                ql = r.get("quarterLabel")
                if ql not in ql_set:
                    continue
                vd_key = self._normalize_macro_value_date_key(r.get("valueDate"))
                if not vd_key:
                    continue
                cells[(vd_key, ql)] = r.get("value")
            vd_keys = list({k[0] for k in cells.keys()})

            def _macro_matrix_vd_sort_key(x):
                d = pd.to_datetime(x, errors="coerce")
                return d if pd.notna(d) else pd.Timestamp(1900, 1, 1)

            vd_keys.sort(key=_macro_matrix_vd_sort_key)
            rows_out = []
            for vd_key in vd_keys:
                vbq = {}
                for ql in ql_order:
                    vbq[ql] = cells.get((vd_key, ql))
                rows_out.append({"valueDate": vd_key, "valuesByQuarter": vbq})
            columns_meta = [{"quarterLabel": ql, "macroBaseAsOfDate": asof_by_quarter.get(ql)} for ql in ql_order]
            out["variables"].append({
                "variableName": vn,
                "columns": columns_meta,
                "rows": rows_out,
            })
        return out

    def build_quarterly_summary_report(self):
        """
        Build structured JSON for Allowance for Credit Losses – Quarterly Summary per sample/useCase.txt.
        First analysisId = current period, second = prior; ignore others. Uses instrumentResult,
        instrumentReporting, and instrumentReference from output/ (same analysisIdentifier pattern).
        Datamodel: datamodel/ImpairmentStudio-DataDictionary.csv.
        """
        io = self.io_session
        if not io.model_run_parameters.use_per_analysis_s3_download():
            print("[Model run] Step 3: Skipped (no per-analysis S3 input mode).")
            return
        report_dir = io.local_directories.get("outputPaths", {}).get("report")
        if not report_dir:
            print("[Model run] Step 3: Skipped (no report output path).")
            self.logger.warning("No output path for 'report'.")
            return
        analysis_ids = io.model_run_parameters.settings.get("analysisIds", []) or []
        if not analysis_ids:
            print("[Model run] Step 3: Skipped (no analysisIds).")
            self.logger.warning("No analysisIds; skipping quarterly summary report.")
            return
        # Resolve current/prior/priorYear and chronological quarters from analysisDetails (used by both reports)
        self._resolve_analysis_roles_from_dates(report_dir, analysis_ids)
        current_id, prior_id, prior_year_id, quarters_list = self._get_analysis_roles()
        df_current = self._load_parquet_for_analysis("instrumentResult", current_id, filter_summary=True)
        df_prior = self._load_parquet_for_analysis("instrumentResult", prior_id, filter_summary=True) if prior_id else None
        df_reporting_current = self._load_parquet_for_analysis("instrumentReporting", current_id)
        df_ref_current = self._load_parquet_for_analysis("instrumentReference", current_id)
        df_ref_prior = self._load_parquet_for_analysis("instrumentReference", prior_id) if prior_id else None
        print(
            "[quarterly_summary] roles current={}, prior={}, priorYear={}; "
            "rows result_current={} result_prior={} reporting_current={} ref_current={} ref_prior={}".format(
                current_id,
                prior_id,
                prior_year_id,
                len(df_current) if df_current is not None else 0,
                len(df_prior) if df_prior is not None else 0,
                len(df_reporting_current) if df_reporting_current is not None else 0,
                len(df_ref_current) if df_ref_current is not None else 0,
                len(df_ref_prior) if df_ref_prior is not None else 0,
            )
        )

        # --- Section 1: Changes to ACL (report contract: beginningACL, chargeOffs, recoveries, provision, endingACL)
        prior_reserves = self._safe_sum(df_prior, "onBalanceSheetReserve") + self._safe_sum(df_prior, "offBalanceSheetReserve") if df_prior is not None else 0.0
        current_reserves = self._safe_sum(df_current, "onBalanceSheetReserve") + self._safe_sum(df_current, "offBalanceSheetReserve") if df_current is not None else 0.0
        section1 = {
            "type": "changesToAcl",
            "beginningACL": prior_reserves,
            "chargeOffs": self._safe_sum(df_reporting_current, "grossChargeOffAmount"),
            "recoveries": self._safe_sum(df_reporting_current, "recoveryAmount"),
            "provision": self._safe_sum(df_reporting_current, "allowanceProvision"),
            "endingACL": current_reserves,
        }

        # --- Sections 2 & 3: Join instrumentResult with instrumentReference on instrumentIdentifier for ascImpairmentEvaluation
        df_current_with_ref = df_current
        if df_ref_current is not None and not df_ref_current.empty and df_current is not None and not df_current.empty:
            id_col_res = self._find_column(df_current, "instrumentIdentifier")
            id_col_ref = self._find_column(df_ref_current, "instrumentIdentifier")
            asc_ref = self._resolve_column(df_ref_current, "ascImpairmentEvaluation", "ascimpairmentevaluation")
            if id_col_res and id_col_ref and asc_ref:
                ref_sub = df_ref_current[[id_col_ref, asc_ref]].drop_duplicates()
                df_current_with_ref = df_current.merge(ref_sub, left_on=id_col_res, right_on=id_col_ref, how="left")
        _df_for_asc = pd.DataFrame() if df_current_with_ref is None or df_current_with_ref.empty else df_current_with_ref
        asc_col = self._resolve_column(_df_for_asc, "ascImpairmentEvaluation", "ascimpairmentevaluation")
        if asc_col and df_current_with_ref is not None and not df_current_with_ref.empty:
            collective = df_current_with_ref[df_current_with_ref[asc_col].astype(str).str.strip().str.lower().str.contains("collective", na=False)]
            adj_col, unadj_col = self._resolve_instrument_result_adj_unadj_columns(collective)
            delta_col = self._resolve_loss_allowance_delta_column(collective)
            exp_col = self._find_column(collective, "amortizedCost")
            collectively_exposure = self._safe_sum(collective, "amortizedCost") if exp_col else 0.0
            total_coll = self._safe_sum(collective, adj_col) if adj_col else 0.0
            adj_flag_col = self._instrument_result_adjusted_column(collective)
            if adj_flag_col and adj_flag_col in collective.columns:
                m_true = self._mask_adjusted_true(collective, adj_flag_col)
                g_false = collective[~m_true]
                g_true = collective[m_true]
                if delta_col and delta_col in collective.columns:
                    dc = pd.to_numeric(collective[delta_col], errors="coerce").fillna(0.0)
                    quant = float(dc.loc[g_false.index].sum())
                    qual = float(dc.loc[g_true.index].sum())
                    total_coll = float(dc.sum())
                else:
                    quant = self._safe_sum(g_false, unadj_col) if unadj_col else 0.0
                    qual = (
                        (g_true[adj_col].sum() - g_true[unadj_col].sum())
                        if (adj_col and unadj_col and not g_true.empty)
                        else 0.0
                    )
                    if not total_coll and (quant or qual):
                        total_coll = float(quant) + float(qual)
            else:
                qual = (adj_col and unadj_col) and (collective[adj_col] - collective[unadj_col]).sum() or 0.0
                quant = self._safe_sum(collective, unadj_col) if unadj_col else 0.0
                if not total_coll:
                    total_coll = (quant + qual)
        else:
            collectively_exposure = 0.0
            quant = 0.0
            qual = 0.0
            total_coll = 0.0
        section2 = {
            "type": "collectivelyEvaluated",
            "collectivelyEvaluatedExposure": collectively_exposure,
            "quantitativeReserveAmount": quant,
            "qualitativeReserveAmount": qual,
            "totalCollectiveReserveAmount": total_coll,
            "quantitativeReserveToExposure": (quant / collectively_exposure) if collectively_exposure else None,
            "qualitativeReserveToExposure": (qual / collectively_exposure) if collectively_exposure else None,
            "totalCollectiveReserveToExposure": (total_coll / collectively_exposure) if collectively_exposure else None,
        }

        # --- Section 3: Individually Evaluated
        if asc_col and df_current_with_ref is not None and not df_current_with_ref.empty:
            individual = df_current_with_ref[df_current_with_ref[asc_col].astype(str).str.strip().str.lower().str.contains("individual", na=False)]
            individually_exposure = self._safe_sum(individual, "amortizedCost")
            ind_adj, _ = self._resolve_instrument_result_adj_unadj_columns(individual)
            individual_reserve = (
                (self._safe_sum(individual, ind_adj) if ind_adj else 0.0)
                or self._safe_sum(individual, "onBalanceSheetReserve")
            )
        else:
            individually_exposure = 0.0
            individual_reserve = 0.0
        section3 = {
            "type": "individuallyEvaluated",
            "individuallyEvaluatedExposure": individually_exposure,
            "individualReserveAmount": individual_reserve,
            "individualReserveToExposure": (individual_reserve / individually_exposure) if individually_exposure else None,
        }

        # --- Section 4: Total Reserve (datamodel: amortizedCost, onBalanceSheetReserve + offBalanceSheetReserve)
        total_exposure = self._safe_sum(df_current, "amortizedCost")
        total_reserves = current_reserves
        section4 = {
            "type": "totalReserveRequirements",
            "totalExposure": total_exposure,
            "totalReserves": total_reserves,
            "totalReservesToTotalExposure": (total_reserves / total_exposure) if total_exposure else None,
        }

        # --- Section 5: Unfunded (datamodel: offBalanceSheetEADAmountLifetime, offBalanceSheetReserve)
        unfunded_exposure = self._safe_sum(df_current, "offBalanceSheetEADAmountLifetime")
        unfunded_reserve = self._safe_sum(df_current, "offBalanceSheetReserve")
        section5 = {
            "type": "unfundedCommitmentLiabilities",
            "unfundedExposure": unfunded_exposure,
            "unfundedReserveAmount": unfunded_reserve,
            "totalUnfundedExposure": unfunded_exposure,
        }

        # --- Section 6: QTD Credit Loss Expense (datamodel: allowanceProvisionDelta)
        provision_delta = self._safe_sum(df_current, "allowanceProvisionDelta")
        section6 = {
            "type": "qtdCreditLossExpense",
            "provisionExpense": provision_delta,
            "unfundedProvisionExpense": 0.0,
            "totalQtdCreditLossExpense": provision_delta,
        }

        out = {
            "currentAnalysisId": current_id,
            "priorAnalysisId": prior_id,
            "sections": [
                section1,
                section2,
                section3,
                section4,
                section5,
                section6,
            ],
        }
        out_path = os.path.join(report_dir, "quarterly_summary_report.json")
        os.makedirs(report_dir, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(out, f, indent=2)
        print("[quarterly_summary] Wrote structured report -> {} (sections: 6)".format(out_path))
        gc.collect()

    def build_hanmi_acl_quarterly_report(self):
        """
        Build Hanmi ACL Quarterly Analysis report as structured JSON (docs/REPORT_MAPPING.md).
        Uses instrumentResult (Summary), instrumentReporting, instrumentReference, and optional macroEconomicVariableInput (Baseline).
        """
        io = self.io_session
        if not io.model_run_parameters.use_per_analysis_s3_download():
            print("[Model run] Step 3b: Skipped (no per-analysis S3 input mode).")
            return
        report_dir = io.local_directories.get("outputPaths", {}).get("report")
        if not report_dir:
            self.logger.warning("No output path for 'report'; skipping Hanmi ACL report.")
            return
        analysis_ids = io.model_run_parameters.settings.get("analysisIds", []) or []
        if not analysis_ids:
            self.logger.warning("No analysisIds; skipping Hanmi ACL report.")
            return
        # Roles and quarters are set from dates in Step 3 (build_quarterly_summary_report); use them here
        current_id, prior_id, prior_year_id, quarters_list = self._get_analysis_roles()
        if not quarters_list:
            self._resolve_analysis_roles_from_dates(report_dir, analysis_ids)
            current_id, prior_id, prior_year_id, quarters_list = self._get_analysis_roles()
        if not quarters_list:
            quarters_list = [str(aid) for aid in analysis_ids]
            current_id = current_id or (analysis_ids[0] if analysis_ids else None)
            prior_id = prior_id or (analysis_ids[1] if len(analysis_ids) >= 2 else None)
        print("[hanmi_acl_report] roles: current={}, prior={}, priorYear={}, quarters={}".format(
            current_id, prior_id, prior_year_id, quarters_list))

        df_current = self._load_parquet_for_analysis("instrumentResult", current_id, filter_summary=True)
        df_prior = self._load_parquet_for_analysis("instrumentResult", prior_id, filter_summary=True) if prior_id else None
        df_reporting_current = self._load_parquet_for_analysis("instrumentReporting", current_id)
        df_ref_current = self._load_parquet_for_analysis("instrumentReference", current_id)
        df_ref_prior = self._load_parquet_for_analysis("instrumentReference", prior_id) if prior_id else None
        print("[hanmi_acl_report] data loaded: result(current={}, prior={}), reporting={}, ref={}".format(
            len(df_current) if df_current is not None else 0, len(df_prior) if df_prior is not None else 0,
            len(df_reporting_current) if df_reporting_current is not None else 0,
            len(df_ref_current) if df_ref_current is not None else 0))
        if df_current is None or df_current.empty:
            print("[hanmi_acl_report] result current EMPTY -> segment/collective/quant/qual/individual/unfunded will be empty")

        report_metadata = {
            "reportTitle": "Allowance for Credit Losses – Quarterly Analysis and Supplemental Exhibits",
            "currentAnalysisId": current_id,
            "priorAnalysisId": prior_id,
            "priorYearAnalysisId": prior_year_id,
            "quartersAnalysisIds": quarters_list,
        }

        segment_methodology = []
        segment_dim_name, segment_col = None, None
        if df_ref_current is not None and not df_ref_current.empty:
            segment_dim_name, segment_col = self._get_segment_column(df_ref_current)
            sub_dim_name, sub_col = self._get_next_segment_column_after(df_ref_current, segment_dim_name)
            lr_col, pd_col, lgd_col = self._get_methodology_columns(df_ref_current)
            model_cols = [c for c in (lr_col, pd_col, lgd_col) if c is not None]
            if segment_col:
                dup_cols = [segment_col] + ([sub_col] if sub_col else []) + model_cols
                dup = df_ref_current[dup_cols].drop_duplicates()
                for _, row in dup.iterrows():
                    parent_seg = self._normalize_segment_display(row.get(segment_col))
                    sub_seg = self._normalize_segment_display(row.get(sub_col)) if sub_col else None
                    meth = self._methodology_from_row(row, lr_col, pd_col, lgd_col)
                    row_out = {
                        "parentSegment": parent_seg,
                        "subSegment": sub_seg,
                        "segment": sub_seg if sub_seg else parent_seg,
                        "methodology": meth,
                    }
                    segment_methodology.append(row_out)
                print(
                    "[hanmi_acl_report] segmentMethodology: dimension={}, subDimension={}, ref rows={}, distinct={}, output rows={}".format(
                        segment_dim_name or segment_col,
                        sub_dim_name or (sub_col if sub_col else ""),
                        len(df_ref_current),
                        len(dup),
                        len(segment_methodology),
                    )
                )
            else:
                all_cols = list(df_ref_current.columns)
                print("[hanmi_acl_report] segmentMethodology: SKIP - no segment dimension found (ref_cols={})".format(all_cols[:30]))
        else:
            print("[hanmi_acl_report] segmentMethodology: SKIP - df_ref_current empty or None")

        collectively_by_methodology = []
        if df_current is not None and not df_current.empty and df_ref_current is not None and not df_ref_current.empty:
            id_res = self._find_column(df_current, "instrumentIdentifier")
            id_ref = self._find_column(df_ref_current, "instrumentIdentifier")
            asc_col = self._resolve_column(df_ref_current, "ascImpairmentEvaluation", "ascimpairmentevaluation")
            lr_col, pd_col, lgd_col = self._get_methodology_columns(df_ref_current)
            model_cols = [c for c in (lr_col, pd_col, lgd_col) if c is not None]
            left_on, right_on = self._join_keys_and_log(df_current, df_ref_current, "result", "ref", current_id)
            if id_res and id_ref and asc_col and left_on is not None:
                ref_sub = df_ref_current[right_on + [asc_col] + model_cols].drop_duplicates() if model_cols else df_ref_current[right_on + [asc_col]].drop_duplicates()
                merged = df_current.merge(ref_sub, left_on=left_on, right_on=right_on, how="left")
                collective = merged[merged[asc_col].astype(str).str.strip().str.lower().str.contains("collective", na=False)]
                # Per-row methodology: first non-null of lossRate, PD, LGD so report avoids nulls
                collective = collective.copy()
                collective["_methodology"] = collective.apply(lambda r: self._methodology_from_row(r, lr_col, pd_col, lgd_col), axis=1)
                if not collective.empty:
                    adj_col, unadj_col = self._resolve_instrument_result_adj_unadj_columns(collective)
                    delta_col = self._resolve_loss_allowance_delta_column(collective)
                    if not delta_col and (not adj_col or not unadj_col):
                        print(
                            "[hanmi_acl_report] collectivelyByMethodology: no lossAllowanceDelta and no adj/unadj "
                            "reserve columns — split will be zero"
                        )
                    ac_col = self._find_column(collective, "amortizedCost")
                    adj_flag_col = self._instrument_result_adjusted_column(collective)
                    for methodology, grp in collective.groupby("_methodology", dropna=False):
                        ac = self._safe_sum(grp, "amortizedCost") if ac_col else 0.0
                        total = self._safe_sum(grp, adj_col) if adj_col else 0.0
                        if adj_flag_col and adj_flag_col in collective.columns:
                            m_true = self._mask_adjusted_true(grp, adj_flag_col)
                            g_false = grp[~m_true]
                            g_true = grp[m_true]
                            if delta_col and delta_col in grp.columns:
                                dc = pd.to_numeric(grp[delta_col], errors="coerce").fillna(0.0)
                                quant = float(dc.loc[g_false.index].sum())
                                qual = float(dc.loc[g_true.index].sum())
                                total = float(dc.sum())
                            else:
                                quant = self._safe_sum(g_false, unadj_col) if unadj_col else 0.0
                                qual = (
                                    (g_true[adj_col].sum() - g_true[unadj_col].sum())
                                    if (adj_col and unadj_col and not g_true.empty)
                                    else 0.0
                                )
                                if not total and (quant or qual):
                                    total = float(quant) + float(qual)
                        else:
                            quant = self._safe_sum(grp, unadj_col) if unadj_col else 0.0
                            qual = (grp[adj_col].sum() - grp[unadj_col].sum()) if (adj_col and unadj_col) else 0.0
                            if not total:
                                total = quant + qual
                        collectively_by_methodology.append({
                            "methodology": str(methodology) if methodology is not None else "",
                            "amortizedCost": round(ac, 2),
                            "quantitativeReserve": round(quant, 2),
                            "qualitativeReserve": round(qual, 2),
                            "totalReserve": round(total, 2),
                            "pctOfEvaluationType": None,
                        })
                    total_coll_reserve = sum(r["totalReserve"] for r in collectively_by_methodology)
                    if total_coll_reserve:
                        for r in collectively_by_methodology:
                            r["pctOfEvaluationType"] = round(100.0 * r["totalReserve"] / total_coll_reserve, 2)
                    if collectively_by_methodology:
                        tot_ac = sum(r["amortizedCost"] for r in collectively_by_methodology)
                        tot_q = sum(r["quantitativeReserve"] for r in collectively_by_methodology)
                        tot_qual = sum(r["qualitativeReserve"] for r in collectively_by_methodology)
                        tot_res = sum(r["totalReserve"] for r in collectively_by_methodology)
                        collectively_by_methodology.append({
                            "methodology": "Total pooled",
                            "amortizedCost": round(tot_ac, 2),
                            "quantitativeReserve": round(tot_q, 2),
                            "qualitativeReserve": round(tot_qual, 2),
                            "totalReserve": round(tot_res, 2),
                            "pctOfEvaluationType": 100.0,
                            "rowType": "total",
                        })
            else:
                print("[hanmi_acl_report] collectivelyByMethodology: SKIP - id_res={}, id_ref={}, asc_col={}".format(
                    bool(id_res), bool(id_ref), bool(asc_col)))
        else:
            print("[hanmi_acl_report] collectivelyByMethodology: SKIP - result empty or ref empty (result={}, ref={})".format(
                len(df_current) if df_current is not None else 0, len(df_ref_current) if df_ref_current is not None else 0))

        quantitative_by_segment = {"main": [], "creSubSegments": []}
        quarters_for_tables = self._get_quarters_for_tables(report_dir, current_id, prior_id, quarters_list)
        quarter_labels_chrono = []
        _seen_ql = set()
        for _aid, _ql in quarters_for_tables:
            if _ql and _ql not in _seen_ql:
                _seen_ql.add(_ql)
                quarter_labels_chrono.append(_ql)

        macroeconomic_baseline = []
        for analysis_id, quarter_label in quarters_for_tables:
            if not analysis_id:
                continue
            df_m = self._load_parquet_for_analysis("macroEconomicVariableInput", analysis_id)
            if df_m is None or df_m.empty:
                continue
            df_m = self._filter_macro_for_report(df_m, report_dir, analysis_id)
            if df_m is None or df_m.empty:
                continue
            asof = self._get_macro_base_as_of_date(report_dir, analysis_id)
            name_col = self._find_column(df_m, "macroeconomicVariableName")
            date_col = self._find_column(df_m, "valueDate")
            value_col = self._find_column(df_m, "macroeconomicVariableValue")
            if not name_col or not value_col:
                continue
            for _, row in df_m.iterrows():
                macroeconomic_baseline.append({
                    "variableName": str(row[name_col]) if pd.notna(row.get(name_col)) else "",
                    "valueDate": str(row[date_col]) if date_col and pd.notna(row.get(date_col)) else None,
                    "value": round(float(row[value_col]), 6) if pd.notna(row.get(value_col)) else None,
                    "quarterLabel": quarter_label,
                    "analysisId": str(analysis_id),
                    "macroBaseAsOfDate": asof,
                })
        macroeconomic_baseline_wide = self._build_macroeconomic_baseline_wide(macroeconomic_baseline, quarter_labels_chrono)
        macroeconomic_baseline_matrix = self._build_macroeconomic_baseline_matrix(macroeconomic_baseline, quarter_labels_chrono)
        print("[hanmi_acl_report] macro: baseline rows={}, wide columns={}, matrix vars={}".format(
            len(macroeconomic_baseline),
            len(macroeconomic_baseline_wide.get("quarterLabels") or []),
            len(macroeconomic_baseline_matrix.get("variables") or []),
        ))

        for analysis_id, quarter_label in quarters_for_tables:
            if not analysis_id:
                continue
            df_res = self._load_parquet_for_analysis("instrumentResult", analysis_id, filter_summary=True)
            df_ref = self._load_parquet_for_analysis("instrumentReference", analysis_id) if analysis_id else None
            if df_res is None or df_res.empty or df_ref is None or df_ref.empty:
                print("[hanmi_acl_report] quantitativeLossRates: analysisId={} SKIP - result={}, ref={}".format(
                    analysis_id, len(df_res) if df_res is not None else 0, len(df_ref) if df_ref is not None else 0))
                continue
            id_res = self._find_column(df_res, "instrumentIdentifier")
            id_ref = self._find_column(df_ref, "instrumentIdentifier")
            seg_dim, seg_col = self._get_segment_column(df_ref)
            if not id_res or not id_ref or not seg_col:
                print("[hanmi_acl_report] quantitativeLossRates: analysisId={} SKIP - id_res={}, id_ref={}, segment_col={}".format(
                    analysis_id, bool(id_res), bool(id_ref), bool(seg_col)))
                continue
            sub_dim, sub_col = self._get_next_segment_column_after(df_ref, seg_dim)
            left_on, right_on = self._join_keys_and_log(df_res, df_ref, "result", "ref", analysis_id)
            if left_on is None:
                continue
            ref_cols = right_on + [seg_col] + ([sub_col] if sub_col else [])
            ref_sub = df_ref[ref_cols].drop_duplicates()
            merged = df_res.merge(ref_sub, left_on=left_on, right_on=right_on, how="left")
            ac_col = self._find_column(merged, "amortizedCost")
            quant_col = self._resolve_unadjusted_reserve_column(merged)
            delta_col = self._resolve_loss_allowance_delta_column(merged)
            adj_flag_col = self._instrument_result_adjusted_column(merged)
            group_keys = [seg_col, sub_col] if sub_col else [seg_col]

            def _append_quant_row(parent_val, sub_val, grp):
                ac = self._safe_sum(grp, "amortizedCost") if ac_col else 0.0
                if adj_flag_col and adj_flag_col in merged.columns:
                    if delta_col and delta_col in grp.columns:
                        g_false = grp[~self._mask_adjusted_true(grp, adj_flag_col)]
                        quant = float(pd.to_numeric(g_false[delta_col], errors="coerce").fillna(0.0).sum())
                    else:
                        g_false = grp[~self._mask_adjusted_true(grp, adj_flag_col)]
                        quant = self._safe_sum(g_false, quant_col) if quant_col else 0.0
                else:
                    if delta_col and delta_col in grp.columns:
                        quant = float(pd.to_numeric(grp[delta_col], errors="coerce").fillna(0.0).sum())
                    else:
                        quant = self._safe_sum(grp, quant_col) if quant_col else 0.0
                rate = (quant / ac * 100.0) if ac else None
                parent_seg = self._normalize_segment_display(parent_val)
                if sub_col:
                    sub_seg = self._normalize_segment_display(sub_val)
                    seg_key = "{} — {}".format(parent_seg, sub_seg)
                    row_out = {
                        "parentSegment": parent_seg,
                        "subSegment": sub_seg,
                        "segment": seg_key,
                        "analysisId": analysis_id,
                        "quarterLabel": quarter_label,
                        "amortizedCost": round(ac, 2),
                        "quantitativeReserve": round(quant, 2),
                        "lossRatePct": round(rate, 4) if rate is not None else None,
                    }
                else:
                    row_out = {
                        "segment": parent_seg,
                        "analysisId": analysis_id,
                        "quarterLabel": quarter_label,
                        "amortizedCost": round(ac, 2),
                        "quantitativeReserve": round(quant, 2),
                        "lossRatePct": round(rate, 4) if rate is not None else None,
                    }
                quantitative_by_segment["main"].append(row_out)

            if sub_col:
                for (parent_val, sub_val), grp in merged.groupby(group_keys, dropna=False):
                    _append_quant_row(parent_val, sub_val, grp)
            else:
                for portfolio, grp in merged.groupby(seg_col, dropna=False):
                    _append_quant_row(portfolio, None, grp)
        # P1: Add Q→Q and Y→Y deltas for loss rate and reserve
        self._add_quantitative_deltas(quantitative_by_segment["main"])

        net_chargeoffs_quarterly = []
        for aid, qlabel in quarters_for_tables:
            if not aid:
                continue
            df_rep = self._load_parquet_for_analysis("instrumentReporting", aid)
            if df_rep is None or df_rep.empty:
                continue
            net_col = self._find_column(df_rep, "netChargeOffAmount")
            gco_col = self._find_column(df_rep, "grossChargeOffAmount")
            rec_col = self._find_column(df_rep, "recoveryAmount")
            port_col = self._resolve_column(df_rep, "portfolioIdentifier", "portfolioidentifier", "Portfolio Identifier", "portfolio_identifier")
            if port_col:
                for portfolio, grp in df_rep.groupby(port_col, dropna=False):
                    net = grp[net_col].sum() if net_col else ((grp[gco_col].sum() - grp[rec_col].sum()) if (gco_col and rec_col) else 0.0)
                    net_chargeoffs_quarterly.append({
                        "segment": self._normalize_segment_display(portfolio),
                        "analysisId": aid,
                        "quarterLabel": qlabel,
                        "netChargeOffs": round(float(net), 2),
                    })
            else:
                net = self._safe_sum(df_rep, "netChargeOffAmount") or (
                    self._safe_sum(df_rep, "grossChargeOffAmount") - self._safe_sum(df_rep, "recoveryAmount")
                )
                net_chargeoffs_quarterly.append({"segment": "Total", "analysisId": aid, "quarterLabel": qlabel, "netChargeOffs": round(float(net), 2)})
        # P1: netChargeOffsAnnual by segment and year
        net_chargeoffs_annual = self._build_net_chargeoffs_annual(net_chargeoffs_quarterly)

        qualitative_by_segment = {"main": []}
        for analysis_id, quarter_label in quarters_for_tables:
            if not analysis_id:
                continue
            df_res = self._load_parquet_for_analysis("instrumentResult", analysis_id, filter_summary=True)
            df_ref = self._load_parquet_for_analysis("instrumentReference", analysis_id) if analysis_id else None
            if df_res is None or df_res.empty or df_ref is None or df_ref.empty:
                continue
            id_res = self._find_column(df_res, "instrumentIdentifier")
            id_ref = self._find_column(df_ref, "instrumentIdentifier")
            seg_dim, seg_col = self._get_segment_column(df_ref)
            if not id_res or not id_ref or not seg_col:
                print("[hanmi_acl_report] qualitativeReserves: SKIP analysisId={} id_res={}, id_ref={}, segment_col={}".format(
                    analysis_id, bool(id_res), bool(id_ref), bool(seg_col)))
                continue
            left_on, right_on = self._join_keys_and_log(df_res, df_ref, "result", "ref", analysis_id)
            if left_on is None:
                continue
            ref_sub = df_ref[right_on + [seg_col]].drop_duplicates()
            merged = df_res.merge(ref_sub, left_on=left_on, right_on=right_on, how="left")
            adj_col, unadj_col = self._resolve_instrument_result_adj_unadj_columns(merged)
            delta_col = self._resolve_loss_allowance_delta_column(merged)
            ac_col = self._find_column(merged, "amortizedCost")
            adj_flag_col = self._instrument_result_adjusted_column(merged)
            for portfolio, grp in merged.groupby(seg_col, dropna=False):
                ac = self._safe_sum(grp, "amortizedCost") if ac_col else 0.0
                if adj_flag_col and adj_flag_col in merged.columns:
                    if delta_col and delta_col in grp.columns:
                        g_true = grp[self._mask_adjusted_true(grp, adj_flag_col)]
                        qual = float(pd.to_numeric(g_true[delta_col], errors="coerce").fillna(0.0).sum())
                    else:
                        g_true = grp[self._mask_adjusted_true(grp, adj_flag_col)]
                        qual = (
                            (g_true[adj_col].sum() - g_true[unadj_col].sum())
                            if (adj_col and unadj_col and not g_true.empty)
                            else 0.0
                        )
                else:
                    qual = (grp[adj_col].sum() - grp[unadj_col].sum()) if (adj_col and unadj_col) else 0.0
                rate = (qual / ac * 100.0) if ac else None
                qualitative_by_segment["main"].append({
                    "segment": self._normalize_segment_display(portfolio),
                    "analysisId": analysis_id,
                    "quarterLabel": quarter_label,
                    "qualitativeReserve": round(qual, 2),
                    "qualitativeRatePct": round(rate, 4) if rate is not None else None,
                })

        individual_analysis = []
        if df_current is not None and df_ref_current is not None and not df_ref_current.empty:
            id_res = self._find_column(df_current, "instrumentIdentifier")
            id_ref = self._find_column(df_ref_current, "instrumentIdentifier")
            asc_col = self._resolve_column(df_ref_current, "ascImpairmentEvaluation", "ascimpairmentevaluation")
            left_on, right_on = self._join_keys_and_log(df_current, df_ref_current, "result", "ref", current_id)
            if id_res and id_ref and asc_col and left_on is not None:
                ref_sub = df_ref_current[right_on + [asc_col]].drop_duplicates()
                merged = df_current.merge(ref_sub, left_on=left_on, right_on=right_on, how="left")
                individual = merged[merged[asc_col].astype(str).str.strip().str.lower().str.contains("individual", na=False)]
                if not individual.empty:
                    ac = self._safe_sum(individual, "amortizedCost")
                    ind_adj, _ = self._resolve_instrument_result_adj_unadj_columns(individual)
                    res = (
                        (self._safe_sum(individual, ind_adj) if ind_adj else 0.0)
                        or self._safe_sum(individual, "onBalanceSheetReserve")
                    )
                    individual_analysis.append({
                        "evaluationType": "Individually Evaluated",
                        "amortizedCost": round(ac, 2),
                        "specificReserve": round(res, 2),
                        "pctOfType": round(100.0 * res / ac, 2) if ac else None,
                    })
            else:
                print("[hanmi_acl_report] individualAnalysis: SKIP - id_res={}, id_ref={}, asc_col={}".format(
                    bool(id_res), bool(id_ref), bool(asc_col)))
        else:
            print("[hanmi_acl_report] individualAnalysis: SKIP - result or ref empty")

        unfunded_by_segment = []
        if df_current is not None and not df_current.empty and df_ref_current is not None:
            id_res = self._find_column(df_current, "instrumentIdentifier")
            id_ref = self._find_column(df_ref_current, "instrumentIdentifier")
            ead_col = self._find_column(df_current, "offBalanceSheetEADAmountLifetime")
            res_col = self._find_column(df_current, "offBalanceSheetReserve")
            print("[hanmi_acl_report] unfundedBySegment: ead_col={}, res_col={}, id_res={}, segment_col={}".format(
                bool(ead_col), bool(res_col), bool(id_res), bool(segment_col)))
            left_on, right_on = self._join_keys_and_log(df_current, df_ref_current, "result", "ref", current_id)
            if id_res and id_ref and segment_col and (ead_col or res_col) and left_on is not None:
                ref_sub = df_ref_current[right_on + [segment_col]].drop_duplicates()
                merged = df_current.merge(ref_sub, left_on=left_on, right_on=right_on, how="left")
                for portfolio, grp in merged.groupby(segment_col, dropna=False):
                    ead = self._safe_sum(grp, "offBalanceSheetEADAmountLifetime") if ead_col else 0.0
                    res = self._safe_sum(grp, "offBalanceSheetReserve") if res_col else 0.0
                    if ead or res:
                        unfunded_by_segment.append({
                            "segment": self._normalize_segment_display(portfolio),
                            "availableCredit": round(ead, 2),
                            "reserve": round(res, 2),
                        })
            else:
                print("[hanmi_acl_report] unfundedBySegment: SKIP - missing required columns")
        else:
            print("[hanmi_acl_report] unfundedBySegment: SKIP - result or ref empty")

        unfunded_trend = []
        for aid, qlabel in quarters_for_tables:
            if not aid:
                continue
            df_res = self._load_parquet_for_analysis("instrumentResult", aid, filter_summary=True)
            if df_res is None:
                print("[hanmi_acl_report] unfundedTrend: analysisId={} SKIP - result None".format(aid))
                continue
            obr_col = self._find_column(df_res, "offBalanceSheetReserve")
            ead_col = self._find_column(df_res, "offBalanceSheetEADAmountLifetime")
            obr = self._safe_sum(df_res, "offBalanceSheetReserve")
            ead = self._safe_sum(df_res, "offBalanceSheetEADAmountLifetime")
            row = {"analysisId": aid, "quarterLabel": qlabel, "requiredReserve": round(obr, 2), "totalUnfunded": round(ead, 2)}
            unfunded_trend.append(row)
        # P1: beginningReserve (prior quarter's requiredReserve), provision, quarterChange
        for i, row in enumerate(unfunded_trend):
            row["beginningReserve"] = round(unfunded_trend[i - 1]["requiredReserve"], 2) if i > 0 else None
            # Provision for unfunded: from instrumentReporting if available (e.g. allowanceChangeDueTo* OBS); else None
            row["provision"] = None
            if row["beginningReserve"] is not None:
                row["quarterChange"] = round(float(row["requiredReserve"]) - float(row["beginningReserve"]), 2)
            else:
                row["quarterChange"] = None

        assumptions_and_parameters = self._build_assumptions_and_parameters(report_dir, current_id, prior_id)

        out = {
            "reportMetadata": report_metadata,
            "assumptionsAndParameters": assumptions_and_parameters,
            "segmentMethodology": segment_methodology,
            "collectivelyEvaluatedByMethodology": collectively_by_methodology,
            "quantitativeLossRatesBySegment": quantitative_by_segment,
            "netChargeOffsQuarterly": net_chargeoffs_quarterly,
            "netChargeOffsAnnual": net_chargeoffs_annual,
            "qualitativeReservesBySegment": qualitative_by_segment,
            "macroeconomicBaseline": macroeconomic_baseline,
            "macroeconomicBaselineWide": macroeconomic_baseline_wide,
            "macroeconomicBaselineMatrix": macroeconomic_baseline_matrix,
            "individualAnalysis": individual_analysis,
            "unfundedBySegment": unfunded_by_segment,
            "unfundedTrend": unfunded_trend,
            "parametersInventory": {},
            "peerRatios": {},
            "hanmiSummaryMetrics": {},
        }
        out_path = os.path.join(report_dir, "hanmi_acl_quarterly_report.json")
        os.makedirs(report_dir, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(out, f, indent=2)
        print("[hanmi_acl_report] sections: segMethod={}, collective={}, quantSegs={}, netCO={}, qualSegs={}, macro={}, individual={}, unfundedSeg={}, unfundedTrend={}".format(
            len(segment_methodology), len(collectively_by_methodology), len(quantitative_by_segment.get("main", [])),
            len(net_chargeoffs_quarterly), len(qualitative_by_segment.get("main", [])), len(macroeconomic_baseline),
            len(individual_analysis), len(unfunded_by_segment), len(unfunded_trend)))
        print("[hanmi_acl_report] Wrote -> {}".format(out_path))

        # Debug: all-data summary (ref-centered outer join, group by dimensions, aggregate metrics)
        self._build_debug_all_data_summary(report_dir, analysis_ids)

    def _build_debug_all_data_summary(self, report_dir, analysis_ids):
        """
        Build a debug JSON: ref-centered left join with result and reporting (same analysisId),
        group by key dimensions, aggregate key metrics. Written to report_dir/debug_all_data_summary.json.
        """
        if self._is_library_mode():
            print("[debug_all_data] skipped (libraryMode — saves memory on API/interactive runs)")
            return
        if not report_dir or not analysis_ids:
            return
        join_keys_used = []
        by_analysis = {}
        for aid in analysis_ids:
            df_ref = self._load_parquet_for_analysis("instrumentReference", aid)
            df_res = self._load_parquet_for_analysis("instrumentResult", aid, filter_summary=True)
            df_rep = self._load_parquet_for_analysis("instrumentReporting", aid)
            ref_rows = len(df_ref) if df_ref is not None else 0
            res_rows = len(df_res) if df_res is not None else 0
            rep_rows = len(df_rep) if df_rep is not None else 0
            if df_ref is None or df_ref.empty:
                by_analysis[str(aid)] = {"refRows": 0, "resultRows": res_rows, "reportingRows": rep_rows, "mergedRows": 0, "groupedSummary": [], "refColumns": [], "segmentationCandidates": {}, "error": "ref empty"}
                continue
            # Join keys: instrumentIdentifier (+ analysisIdentifier if in both)
            left_on, right_on = self._join_keys_and_log(
                df_ref, df_res if (df_res is not None and not df_res.empty) else df_ref.head(0), "ref", "result", aid
            )
            if left_on is None:
                by_analysis[str(aid)] = {"refRows": ref_rows, "resultRows": res_rows, "reportingRows": rep_rows, "mergedRows": 0, "groupedSummary": [], "refColumns": list(df_ref.columns), "segmentationCandidates": self._get_segmentation_candidates_for_debug(df_ref), "error": "no join keys"}
                continue
            if not join_keys_used:
                join_keys_used = ["instrumentIdentifier"] + (["analysisIdentifier"] if len(left_on) > 1 else [])
            # Ref-centered: ref left-join result, then left-join reporting
            merged = df_ref.copy()
            if df_res is not None and not df_res.empty:
                merged = df_ref.merge(df_res, left_on=left_on, right_on=right_on, how="left", suffixes=("", "_result"))
            merged_rows = len(merged)
            if df_rep is not None and not df_rep.empty:
                left_rep, right_rep = self._join_keys_and_log(merged, df_rep, "ref+result", "reporting", aid)
                if left_rep is not None:
                    merged = merged.merge(df_rep, left_on=left_rep, right_on=right_rep, how="left", suffixes=("", "_rep"))
                    merged_rows = len(merged)
            # Group by key dimensions (from ref): prefer segment dimension then methodology columns (same as report)
            seg_dim, seg_col = self._get_segment_column(df_ref)
            asc_col = self._resolve_column(merged, "ascImpairmentEvaluation", "ascimpairmentevaluation")
            lr_col = self._resolve_column(merged, "lossRateModelName", "lossratemodelname")
            pd_col = self._resolve_column(merged, "pdModelName", "pdmodelname")
            lgd_col = self._resolve_column(merged, "lgdModelName", "lgdmodelname")
            group_cols = [c for c in [seg_col, asc_col, lr_col, pd_col, lgd_col] if c is not None and c in merged.columns]
            if not group_cols:
                group_cols = [c for c in merged.columns if merged[c].nunique() < 100][:3]
            agg_map = {}
            for metric, col in [("amortizedCost", "amortizedCost"), ("onBalanceSheetReserveAdjusted", "onBalanceSheetReserveAdjusted"), ("onBalanceSheetReserveUnadjusted", "onBalanceSheetReserveUnadjusted"), ("netChargeOffAmount", "netChargeOffAmount"), ("offBalanceSheetReserve", "offBalanceSheetReserve"), ("offBalanceSheetEADAmountLifetime", "offBalanceSheetEADAmountLifetime")]:
                c = self._find_column(merged, col)
                if c is not None:
                    agg_map[metric] = c
            if not agg_map:
                num_cols = merged.select_dtypes(include=["number"]).columns.tolist()
                agg_map = {c: c for c in num_cols[:5]} if num_cols else {}
            try:
                g = merged.groupby(group_cols, dropna=False)
                agg_df = g.agg({v: "sum" for v in agg_map.values()}).reset_index()
                agg_df.columns = [str(x) for x in agg_df.columns]
                summary = agg_df.to_dict(orient="records")
                for r in summary[:50]:
                    for k, v in list(r.items()):
                        try:
                            if pd.notna(v) and isinstance(v, (int, float)):
                                r[k] = round(float(v), 2)
                        except (TypeError, ValueError):
                            pass
                by_analysis[str(aid)] = {"refRows": ref_rows, "resultRows": res_rows, "reportingRows": rep_rows, "mergedRows": merged_rows, "refColumns": list(df_ref.columns), "segmentationCandidates": self._get_segmentation_candidates_for_debug(df_ref), "segmentDimensionUsed": seg_dim or (seg_col if seg_col else None), "groupBy": group_cols, "groupedSummary": summary[:50], "groupedSummaryTotalRows": len(summary)}
            except Exception as e:
                by_analysis[str(aid)] = {"refRows": ref_rows, "resultRows": res_rows, "reportingRows": rep_rows, "mergedRows": merged_rows, "refColumns": list(df_ref.columns), "segmentationCandidates": self._get_segmentation_candidates_for_debug(df_ref), "groupedSummary": [], "error": str(e)}
        out = {"joinKeysUsed": join_keys_used, "byAnalysis": by_analysis}
        debug_path = os.path.join(report_dir, "debug_all_data_summary.json")
        os.makedirs(report_dir, exist_ok=True)
        with open(debug_path, "w", encoding="utf-8") as f:
            json.dump(out, f, indent=2, default=str)
        print("[debug_all_data] built for {} analyses -> {}".format(len(by_analysis), debug_path))

    def _fetch_and_write_adjustment_details(self):
        """
        For **each** analysis in ``settings.analysisIds``, GET Impairment Studio adjustment details and write
        ``adjustment_details.json`` into the report dir (included in report_response_payload when enabled).

        Output shape: ``{ currentAnalysisId, analysisIds, adjustments, byAnalysisId, analysisSummaries }``.
        ``analysisSummaries`` lists each analysis with ``analysisName`` and ``reportingDate`` from
        ``analysisDetails_{id}.json`` (for report labeling). Legacy consumers that expect a top-level
        JSON array still work if they read ``adjustments``.

        API: ``{IMPAIRMENT_STUDIO_API_BASE}/adjustment/1.0/analyses/{id}/adjustmentdetails`` (Bearer JWT).
        Opt out: ``settings.fetchAdjustmentDetails`` = false.
        """
        if not self.model_run_parameters.settings.get("fetchAdjustmentDetails", True):
            print("[Model run] adjustment details: skipped (settings.fetchAdjustmentDetails=false)")
            return
        if not self._jwt:
            print("[Model run] adjustment details: skipped (no JWT)")
            return
        report_dir = self.io_session.local_directories.get("outputPaths", {}).get("report")
        if not report_dir:
            return
        analysis_ids = self.model_run_parameters.settings.get("analysisIds") or []
        self._resolve_analysis_roles_from_dates(report_dir, analysis_ids)
        current_id, _, _, _ = self._get_analysis_roles()
        if not current_id:
            print("[Model run] adjustment details: skipped (no current analysis id)")
            return
        from .adjustment_api import fetch_adjustment_details_json

        by_analysis_id = {}
        any_ok = False
        for aid in analysis_ids:
            print(
                "[Model run] adjustment details: GET analysisId={} (Bearer JWT)".format(aid)
            )
            data = fetch_adjustment_details_json(self._jwt, aid)
            if data is not None:
                by_analysis_id[str(aid)] = data
                any_ok = True
            else:
                by_analysis_id[str(aid)] = None

        out_path = os.path.join(report_dir, "adjustment_details.json")
        os.makedirs(report_dir, exist_ok=True)
        if not any_ok:
            err_doc = {
                "_error": "adjustment_details_unavailable",
                "currentAnalysisId": str(current_id),
                "analysisIds": [str(a) for a in analysis_ids],
                "adjustments": [],
                "byAnalysisId": {k: [] for k in by_analysis_id},
                "analysisSummaries": [],
            }
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(err_doc, f, indent=2)
            print("[Model run] adjustment details: wrote failure stub -> {}".format(out_path))
            return

        cur_list = by_analysis_id.get(str(current_id))
        if cur_list is None:
            cur_list = []
        analysis_summaries = []
        for aid in analysis_ids:
            meta = self._get_analysis_display_meta_from_details(report_dir, aid)
            ql = self._get_quarter_label(report_dir, aid)
            analysis_summaries.append({
                "analysisId": str(aid),
                "analysisName": (meta.get("analysisName") or ql or "").strip() or None,
                "reportingDate": meta.get("reportingDate"),
                "quarterLabel": ql,
            })
        out_doc = {
            "currentAnalysisId": str(current_id),
            "analysisIds": [str(a) for a in analysis_ids],
            "adjustments": cur_list if isinstance(cur_list, list) else [],
            "byAnalysisId": {k: (v if isinstance(v, list) else []) for k, v in by_analysis_id.items()},
            "analysisSummaries": analysis_summaries,
        }
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(out_doc, f, indent=2, default=str)
        n_cur = len(out_doc["adjustments"])
        n_total = sum(len(v) for v in out_doc["byAnalysisId"].values() if isinstance(v, list))
        print(
            "[Model run] adjustment details: wrote current={} rows, {} total across analyses -> {}".format(
                n_cur, n_total, out_path
            )
        )

    def _build_report_response_payload(self):
        """
        If settings.returnReportsInResponse is true, load every *.json in the report output dir into
        self.report_response_payload for HTTP handlers (Interactive /v1/run) to return as the response body.
        Excludes embedding binary zip; JSON parse failures are stored as {"_parseError": "..."}.
        """
        if not self.model_run_parameters.settings.get("returnReportsInResponse"):
            self.report_response_payload = None
            return
        report_dir = self.io_session.local_directories.get("outputPaths", {}).get("report")
        reports = Model.collect_json_reports_from_directory(report_dir)
        self.report_response_payload = {"reports": reports}
        print("[Model run] report_response_payload: {} JSON file(s)".format(len(reports)))
        if report_dir and os.path.isdir(report_dir):
            for fname in sorted(reports.keys()):
                fp = os.path.join(report_dir, fname)
                if os.path.isfile(fp):
                    print(
                        "[Model run] report_response_payload file on disk: {} size_bytes={}".format(
                            fname,
                            os.path.getsize(fp),
                        )
                    )
                err = reports[fname] if isinstance(reports.get(fname), dict) else None
                if isinstance(err, dict) and err.get("_parseError"):
                    print(
                        "[Model run] report_response_payload parse error: {} -> {}".format(
                            fname,
                            err.get("_parseError"),
                        )
                    )

    @staticmethod
    def collect_json_reports_from_directory(report_dir):
        """Load all top-level *.json files from report_dir into {filename: parsed JSON}. Public for tests."""
        if not report_dir or not os.path.isdir(report_dir):
            return {}
        out = {}
        for fname in sorted(os.listdir(report_dir)):
            if not fname.endswith(".json"):
                continue
            path = os.path.join(report_dir, fname)
            if not os.path.isfile(path):
                continue
            try:
                with open(path, "r", encoding="utf-8") as f:
                    out[fname] = json.load(f)
            except (OSError, UnicodeDecodeError) as e:
                out[fname] = {"_parseError": str(e), "_file": fname}
            except JSONDecodeError as e:
                out[fname] = {"_parseError": str(e), "_file": fname}
        return out

    def create_report_export_zip(self):
        """Create report_export.zip in the report output dir containing all current files there (aggregate JSONs + analysisDetails)."""
        io = self.io_session
        report_dir = io.local_directories.get("outputPaths", {}).get("report")
        if not report_dir or not os.path.isdir(report_dir):
            print("[Model run] Step 4: Skipped (report dir not found: {})".format(report_dir))
            self.logger.warning("Report output dir not found; skipping zip.")
            return
        zip_path = os.path.join(report_dir, "report_export.zip")
        try:
            added = []
            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
                for f in sorted(os.listdir(report_dir)):
                    if f == "report_export.zip":
                        continue
                    full = os.path.join(report_dir, f)
                    if os.path.isfile(full):
                        zf.write(full, arcname=f)
                        added.append(f)
            print("[Model run] Step 4: zip created ({} files)".format(len(added)))
        except Exception as e:
            self.logger.warning("Failed to create report zip: {}".format(e))

    def cleanUp(self, log_file=None, keep_temp=False):
        """Delete temp directories and upload logfile and batch id file"""
        if not keep_temp:
            self.io_session.deleteTempDirectories()
        if log_file and not self._is_library_mode():
            self.io_session.uploadFiles({'log': log_file})
