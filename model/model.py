from moodyscappy import Cappy
import glob
import iosession
import json
import logging
import os
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
        self.cap_session = Cappy(**credentials)
        self.io_session = iosession.IOSession(self.cap_session, model_run_parameters_path, local_mode, credentials)
        self.model_run_parameters = self.io_session.model_run_parameters
        if proxy_credentials:
            self.proxy_cap_session = Cappy(**proxy_credentials, errors='log')

    def run(self):
        print("[Model run] START")
        self.logger.info(f'Running model: {self.model_run_parameters.name}')

        print("[Model run] Step 1: List S3 folders")
        self.io_session.list_and_print_s3_folders()

        print("[Model run] Step 2: Get source input files from S3")
        self.io_session.getSourceInputFiles()

        print("[Model run] Step 3: Build quarterly summary report (current/prior from analysisIds, all doc sections)")
        self.build_quarterly_summary_report()

        print("[Model run] Step 3b: Build Hanmi ACL quarterly report (multi-section JSON)")
        self.build_hanmi_acl_quarterly_report()

        print("[Model run] Step 4: Create zip of all report output files")
        self.create_report_export_zip()

        print("[Model run] Step 5: Create local model run parameters")
        new_mrp = self.createLocalModelRunParameters()
        print(f"[Model run] Local MRP written to: {new_mrp}")

        print("[Model run] Step 6: Create file dicts and upload outputs to S3")
        all_files = self.io_session.createOutputFileDicts(new_mrp)
        print("[Model run] Uploading {} output file(s): {}".format(len(all_files), list(all_files.keys())))
        if all_files:
            self.io_session.uploadFiles(all_files)
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
            print("[join] {} vs {}: no join keys (instrumentIdentifier left={}, right={})".format(
                left_name, right_name, bool(id_left), bool(id_right)))
            return None, None
        print("[join] {} (rows={}) vs {} (rows={}) for same analysisId={}: join on {} -> left_on={}, right_on={}".format(
            left_name, len(left_df), right_name, len(right_df), analysis_id,
            ["instrumentIdentifier"] + (["analysisIdentifier"] if an_left and an_right else []),
            left_on, right_on))
        return left_on, right_on

    def _filter_summary_scenario(self, df):
        """Filter instrumentResult to scenarioIdentifier = Summary (case-insensitive)."""
        if df is None or df.empty:
            return df
        col = self._find_column(df, "scenarioIdentifier")
        if col is None:
            return df
        return df[df[col].astype(str).str.strip().str.lower() == "summary"].copy()

    def _get_analysis_roles(self):
        """
        Return (current_id, prior_id, prior_year_id, quarters_list) from settings.analysisRoles
        if present, else from analysisIds: current=first, prior=second, prior_year=None, quarters=[].
        quarters is the ordered list of analysis IDs to use for multi-quarter tables (e.g. Q4 '24, Q3 '25, Q4 '25).
        """
        io = self.io_session
        analysis_ids = io.model_run_parameters.settings.get("analysisIds", []) or []
        roles = io.model_run_parameters.settings.get("analysisRoles") or {}
        current_id = roles["current"] if "current" in roles else (analysis_ids[0] if analysis_ids else None)
        prior_id = roles["prior"] if "prior" in roles else (analysis_ids[1] if len(analysis_ids) >= 2 else None)
        prior_year_id = roles.get("priorYear")
        quarters = roles["quarters"] if "quarters" in roles and isinstance(roles["quarters"], list) else []
        return current_id, prior_id, prior_year_id, quarters

    def _load_parquet_for_analysis(self, category, analysis_id, filter_summary=False):
        """Load all parquet under that category dir for one analysis_id. If filter_summary, filter to scenarioIdentifier=Summary."""
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
            return None
        load_dir = input_paths[idx]
        files = glob.glob(os.path.join(load_dir, "**", "*.parquet"), recursive=True)
        if not files:
            print("[_load_parquet] no parquet files in {} (category={}, analysisId={})".format(load_dir, category, analysis_id))
            return None
        dfs = []
        for f in files:
            try:
                dfs.append(pd.read_parquet(f))
            except Exception as e:
                self.logger.warning("Failed to read %s: %s", f, e)
        if not dfs:
            return None
        out = pd.concat(dfs, ignore_index=True)
        if filter_summary:
            out = self._filter_summary_scenario(out)
        print("[_load_parquet] {} analysisId={}: {} files -> {} rows".format(
            category, analysis_id, len(files), len(out)))
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

    def build_quarterly_summary_report(self):
        """
        Build structured JSON for Allowance for Credit Losses – Quarterly Summary per sample/useCase.txt.
        First analysisId = current period, second = prior; ignore others. Uses instrumentResult,
        instrumentReporting, and instrumentReference from output/ (same analysisIdentifier pattern).
        Datamodel: datamodel/ImpairmentStudio-DataDictionary.csv.
        """
        io = self.io_session
        if not io.model_run_parameters.callBack:
            print("[Model run] Step 3: Skipped (callBack is False).")
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
        current_id, prior_id, prior_year_id, quarters_list = self._get_analysis_roles()
        print("[quarterly_summary] current analysisId={}, prior analysisId={}, priorYear={}, quarters count={}".format(
            current_id, prior_id, prior_year_id, len(quarters_list)))

        df_current = self._load_parquet_for_analysis("instrumentResult", current_id, filter_summary=True)
        df_prior = self._load_parquet_for_analysis("instrumentResult", prior_id, filter_summary=True) if prior_id else None
        df_reporting_current = self._load_parquet_for_analysis("instrumentReporting", current_id)
        df_ref_current = self._load_parquet_for_analysis("instrumentReference", current_id)
        df_ref_prior = self._load_parquet_for_analysis("instrumentReference", prior_id) if prior_id else None
        print("[quarterly_summary] data loaded: result current={}, prior={}, reporting={}, ref current={}, prior={}".format(
            len(df_current) if df_current is not None else 0, len(df_prior) if df_prior is not None else 0,
            len(df_reporting_current) if df_reporting_current is not None else 0,
            len(df_ref_current) if df_ref_current is not None else 0, len(df_ref_prior) if df_ref_prior is not None else 0))

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
            asc_ref = self._find_column(df_ref_current, "ascImpairmentEvaluation")
            if id_col_res and id_col_ref and asc_ref:
                ref_sub = df_ref_current[[id_col_ref, asc_ref]].drop_duplicates()
                df_current_with_ref = df_current.merge(ref_sub, left_on=id_col_res, right_on=id_col_ref, how="left")
        _df_for_asc = pd.DataFrame() if df_current_with_ref is None or df_current_with_ref.empty else df_current_with_ref
        asc_col = self._find_column(_df_for_asc, "ascImpairmentEvaluation")
        if asc_col and df_current_with_ref is not None and not df_current_with_ref.empty:
            collective = df_current_with_ref[df_current_with_ref[asc_col].astype(str).str.strip().str.lower().str.contains("collective", na=False)]
            adj_col = self._find_column(collective, "onBalanceSheetReserveAdjusted")
            unadj_col = self._find_column(collective, "onBalanceSheetReserveUnadjusted")
            qual = (adj_col and unadj_col) and (collective[adj_col] - collective[unadj_col]).sum() or 0.0
            quant = self._safe_sum(collective, "onBalanceSheetReserveUnadjusted")
            exp_col = self._find_column(collective, "amortizedCost")
            collectively_exposure = self._safe_sum(collective, "amortizedCost") if exp_col else 0.0
            total_coll = self._safe_sum(collective, "onBalanceSheetReserveAdjusted") or (quant + qual)
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
            individual_reserve = self._safe_sum(individual, "onBalanceSheetReserveAdjusted") or self._safe_sum(individual, "onBalanceSheetReserve")
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

    def build_hanmi_acl_quarterly_report(self):
        """
        Build Hanmi ACL Quarterly Analysis report as structured JSON (docs/REPORT_MAPPING.md).
        Uses instrumentResult (Summary), instrumentReporting, instrumentReference, and optional macroEconomicVariableInput (Baseline).
        """
        io = self.io_session
        if not io.model_run_parameters.callBack:
            print("[Model run] Step 3b: Skipped (callBack is False).")
            return
        report_dir = io.local_directories.get("outputPaths", {}).get("report")
        if not report_dir:
            self.logger.warning("No output path for 'report'; skipping Hanmi ACL report.")
            return
        analysis_ids = io.model_run_parameters.settings.get("analysisIds", []) or []
        if not analysis_ids:
            self.logger.warning("No analysisIds; skipping Hanmi ACL report.")
            return
        current_id, prior_id, prior_year_id, quarters_list = self._get_analysis_roles()
        print("[hanmi_acl_report] roles: current={}, prior={}, priorYear={}, quarters={}".format(
            current_id, prior_id, prior_year_id, quarters_list))

        df_current = self._load_parquet_for_analysis("instrumentResult", current_id, filter_summary=True)
        df_prior = self._load_parquet_for_analysis("instrumentResult", prior_id, filter_summary=True) if prior_id else None
        df_reporting_current = self._load_parquet_for_analysis("instrumentReporting", current_id)
        df_ref_current = self._load_parquet_for_analysis("instrumentReference", current_id)
        df_ref_prior = self._load_parquet_for_analysis("instrumentReference", prior_id) if prior_id else None
        df_macro = self._load_parquet_for_analysis("macroEconomicVariableInput", current_id) if current_id else None
        if df_macro is not None and not df_macro.empty:
            df_macro = self._filter_macro_for_report(df_macro, report_dir, current_id)
        print("[hanmi_acl_report] data loaded: result current={}, prior={}, reporting={}, ref={}, macro={}".format(
            len(df_current) if df_current is not None else 0, len(df_prior) if df_prior is not None else 0,
            len(df_reporting_current) if df_reporting_current is not None else 0,
            len(df_ref_current) if df_ref_current is not None else 0, len(df_macro) if df_macro is not None else 0))
        # Debug: key columns and sample values to explain empty sections
        if df_current is None or df_current.empty:
            print("[hanmi_acl_report] result current: EMPTY or None -> segment/collective/quant/qual/individual/unfunded will be empty")
        else:
            id_c = self._find_column(df_current, "instrumentIdentifier")
            print("[hanmi_acl_report] result current: rows={}, cols={}, instrumentId={}".format(
                len(df_current), list(df_current.columns)[:15], "ok" if id_c else "MISSING"))
        if df_ref_current is not None and not df_ref_current.empty:
            port_c = self._find_column(df_ref_current, "portfolioIdentifier")
            asc_c = self._find_column(df_ref_current, "ascImpairmentEvaluation")
            model_c = self._find_column(df_ref_current, "lossRateModelName") or self._find_column(df_ref_current, "pdModelName")
            uniq_asc = df_ref_current[asc_c].dropna().astype(str).unique().tolist()[:5] if asc_c else []
            uniq_port = df_ref_current[port_c].dropna().astype(str).unique().tolist()[:5] if port_c else []
            print("[hanmi_acl_report] ref current: portfolioIdentifier={}, ascImpairmentEvaluation={}, modelCol={}, sample asc={}, sample port={}".format(
                "ok" if port_c else "MISSING", "ok" if asc_c else "MISSING", "ok" if model_c else "MISSING", uniq_asc, uniq_port))

        report_metadata = {
            "reportTitle": "Allowance for Credit Losses – Quarterly Analysis and Supplemental Exhibits",
            "currentAnalysisId": current_id,
            "priorAnalysisId": prior_id,
            "priorYearAnalysisId": prior_year_id,
            "quartersAnalysisIds": quarters_list,
        }

        segment_methodology = []
        if df_ref_current is not None and not df_ref_current.empty:
            port_col = self._find_column(df_ref_current, "portfolioIdentifier")
            model_col = self._find_column(df_ref_current, "lossRateModelName") or self._find_column(df_ref_current, "pdModelName")
            if port_col:
                dup = df_ref_current[[port_col] + ([model_col] if model_col else [])].drop_duplicates()
                for _, row in dup.iterrows():
                    seg = str(row[port_col]) if pd.notna(row[port_col]) else ""
                    meth = str(row[model_col]) if model_col and pd.notna(row.get(model_col)) else ""
                    segment_methodology.append({"segment": seg, "methodology": meth})
                print("[hanmi_acl_report] segmentMethodology: ref rows={}, distinct (port+model)={}, output rows={}".format(
                    len(df_ref_current), len(dup), len(segment_methodology)))
            else:
                all_cols = list(df_ref_current.columns)
                # Log columns that might be segment/portfolio (case-insensitive)
                port_like = [c for c in all_cols if c and "port" in str(c).lower()]
                print("[hanmi_acl_report] segmentMethodology: SKIP - portfolioIdentifier not found in ref (port-like cols: {}, total cols: {})".format(
                    port_like[:10] if port_like else "none", len(all_cols)))
        else:
            print("[hanmi_acl_report] segmentMethodology: SKIP - df_ref_current empty or None")

        collectively_by_methodology = []
        if df_current is not None and not df_current.empty and df_ref_current is not None and not df_ref_current.empty:
            id_res = self._find_column(df_current, "instrumentIdentifier")
            id_ref = self._find_column(df_ref_current, "instrumentIdentifier")
            asc_col = self._find_column(df_ref_current, "ascImpairmentEvaluation")
            model_col = self._find_column(df_ref_current, "lossRateModelName") or self._find_column(df_ref_current, "pdModelName")
            left_on, right_on = self._join_keys_and_log(df_current, df_ref_current, "result", "ref", current_id)
            if id_res and id_ref and asc_col and left_on is not None:
                ref_sub = df_ref_current[right_on + [asc_col] + ([model_col] if model_col else [])].drop_duplicates()
                merged = df_current.merge(ref_sub, left_on=left_on, right_on=right_on, how="left")
                collective = merged[merged[asc_col].astype(str).str.strip().str.lower().str.contains("collective", na=False)]
                print("[hanmi_acl_report] collectivelyByMethodology: result={}, ref={}, merged={}, after collective filter={}, model_col={}".format(
                    len(df_current), len(df_ref_current), len(merged), len(collective), "ok" if model_col else "MISSING"))
                if not collective.empty and model_col:
                    adj_col = self._find_column(collective, "onBalanceSheetReserveAdjusted")
                    unadj_col = self._find_column(collective, "onBalanceSheetReserveUnadjusted")
                    ac_col = self._find_column(collective, "amortizedCost")
                    for methodology, grp in collective.groupby(model_col, dropna=False):
                        ac = self._safe_sum(grp, "amortizedCost") if ac_col else 0.0
                        quant = self._safe_sum(grp, "onBalanceSheetReserveUnadjusted") if unadj_col else 0.0
                        qual = (grp[adj_col].sum() - grp[unadj_col].sum()) if (adj_col and unadj_col) else 0.0
                        total = self._safe_sum(grp, "onBalanceSheetReserveAdjusted") if adj_col else (quant + qual)
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
            else:
                print("[hanmi_acl_report] collectivelyByMethodology: SKIP - id_res={}, id_ref={}, asc_col={}".format(
                    bool(id_res), bool(id_ref), bool(asc_col)))
        else:
            print("[hanmi_acl_report] collectivelyByMethodology: SKIP - result empty or ref empty (result={}, ref={})".format(
                len(df_current) if df_current is not None else 0, len(df_ref_current) if df_ref_current is not None else 0))

        quantitative_by_segment = {"main": [], "creSubSegments": []}
        for analysis_id, label in [(current_id, "current"), (prior_id, "prior")]:
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
            port_ref = self._find_column(df_ref, "portfolioIdentifier")
            if not id_res or not id_ref or not port_ref:
                print("[hanmi_acl_report] quantitativeLossRates: analysisId={} SKIP - id_res={}, id_ref={}, port_ref={}".format(
                    analysis_id, bool(id_res), bool(id_ref), bool(port_ref)))
                continue
            left_on, right_on = self._join_keys_and_log(df_res, df_ref, "result", "ref", analysis_id)
            if left_on is None:
                continue
            ref_sub = df_ref[right_on + [port_ref]].drop_duplicates()
            merged = df_res.merge(ref_sub, left_on=left_on, right_on=right_on, how="left")
            print("[hanmi_acl_report] quantitativeLossRates: analysisId={} result={}, ref={}, merged={}, groups={}".format(
                analysis_id, len(df_res), len(df_ref), len(merged), merged[port_ref].nunique() if port_ref in merged.columns else 0))
            ac_col = self._find_column(merged, "amortizedCost")
            quant_col = self._find_column(merged, "onBalanceSheetReserveUnadjusted")
            for portfolio, grp in merged.groupby(port_ref, dropna=False):
                ac = self._safe_sum(grp, "amortizedCost") if ac_col else 0.0
                quant = self._safe_sum(grp, "onBalanceSheetReserveUnadjusted") if quant_col else 0.0
                rate = (quant / ac * 100.0) if ac else None
                quantitative_by_segment["main"].append({
                    "segment": str(portfolio) if portfolio is not None else "",
                    "analysisId": analysis_id,
                    "label": label,
                    "amortizedCost": round(ac, 2),
                    "quantitativeReserve": round(quant, 2),
                    "lossRatePct": round(rate, 4) if rate is not None else None,
                })

        net_chargeoffs_quarterly = []
        if df_reporting_current is not None and not df_reporting_current.empty:
            net_col = self._find_column(df_reporting_current, "netChargeOffAmount")
            gco_col = self._find_column(df_reporting_current, "grossChargeOffAmount")
            rec_col = self._find_column(df_reporting_current, "recoveryAmount")
            port_col = self._find_column(df_reporting_current, "portfolioIdentifier")
            if port_col:
                for portfolio, grp in df_reporting_current.groupby(port_col, dropna=False):
                    net = grp[net_col].sum() if net_col else ((grp[gco_col].sum() - grp[rec_col].sum()) if (gco_col and rec_col) else 0.0)
                    net_chargeoffs_quarterly.append({
                        "segment": str(portfolio) if portfolio is not None else "",
                        "analysisId": current_id,
                        "netChargeOffs": round(float(net), 2),
                    })
            else:
                net = self._safe_sum(df_reporting_current, "netChargeOffAmount") or (
                    self._safe_sum(df_reporting_current, "grossChargeOffAmount") - self._safe_sum(df_reporting_current, "recoveryAmount")
                )
                net_chargeoffs_quarterly.append({"segment": "Total", "analysisId": current_id, "netChargeOffs": round(float(net), 2)})

        qualitative_by_segment = {"main": []}
        if df_current is not None and not df_current.empty and df_ref_current is not None:
            id_res = self._find_column(df_current, "instrumentIdentifier")
            id_ref = self._find_column(df_ref_current, "instrumentIdentifier")
            port_ref = self._find_column(df_ref_current, "portfolioIdentifier")
            if id_res and id_ref and port_ref:
                ref_sub = df_ref_current[[id_ref, port_ref]].drop_duplicates()
                merged = df_current.merge(ref_sub, left_on=id_res, right_on=id_ref, how="left")
                adj_col = self._find_column(merged, "onBalanceSheetReserveAdjusted")
                unadj_col = self._find_column(merged, "onBalanceSheetReserveUnadjusted")
                ac_col = self._find_column(merged, "amortizedCost")
                print("[hanmi_acl_report] qualitativeReserves: merged={}, adj_col={}, unadj_col={}".format(
                    len(merged), bool(adj_col), bool(unadj_col)))
                for portfolio, grp in merged.groupby(port_ref, dropna=False):
                    qual = (grp[adj_col].sum() - grp[unadj_col].sum()) if (adj_col and unadj_col) else 0.0
                    ac = self._safe_sum(grp, "amortizedCost") if ac_col else 0.0
                    rate = (qual / ac * 100.0) if ac else None
                    qualitative_by_segment["main"].append({
                        "segment": str(portfolio) if portfolio is not None else "",
                        "qualitativeReserve": round(qual, 2),
                        "qualitativeRatePct": round(rate, 4) if rate is not None else None,
                    })
            else:
                print("[hanmi_acl_report] qualitativeReserves: SKIP - id_res={}, id_ref={}, port_ref={}".format(
                    bool(id_res), bool(id_ref), bool(port_ref)))
        else:
            print("[hanmi_acl_report] qualitativeReserves: SKIP - result or ref empty")

        macroeconomic_baseline = []
        if df_macro is not None and not df_macro.empty:
            name_col = self._find_column(df_macro, "macroeconomicVariableName")
            date_col = self._find_column(df_macro, "valueDate")
            value_col = self._find_column(df_macro, "macroeconomicVariableValue")
            if name_col and value_col:
                for _, row in df_macro.iterrows():
                    macroeconomic_baseline.append({
                        "variableName": str(row[name_col]) if pd.notna(row.get(name_col)) else "",
                        "valueDate": str(row[date_col]) if date_col and pd.notna(row.get(date_col)) else None,
                        "value": round(float(row[value_col]), 6) if pd.notna(row.get(value_col)) else None,
                    })

        individual_analysis = []
        if df_current is not None and df_ref_current is not None and not df_ref_current.empty:
            id_res = self._find_column(df_current, "instrumentIdentifier")
            id_ref = self._find_column(df_ref_current, "instrumentIdentifier")
            asc_col = self._find_column(df_ref_current, "ascImpairmentEvaluation")
            left_on, right_on = self._join_keys_and_log(df_current, df_ref_current, "result", "ref", current_id)
            if id_res and id_ref and asc_col and left_on is not None:
                ref_sub = df_ref_current[right_on + [asc_col]].drop_duplicates()
                merged = df_current.merge(ref_sub, left_on=left_on, right_on=right_on, how="left")
                individual = merged[merged[asc_col].astype(str).str.strip().str.lower().str.contains("individual", na=False)]
                print("[hanmi_acl_report] individualAnalysis: merged={}, after individual filter={}".format(len(merged), len(individual)))
                if not individual.empty:
                    ac = self._safe_sum(individual, "amortizedCost")
                    res = self._safe_sum(individual, "onBalanceSheetReserveAdjusted") or self._safe_sum(individual, "onBalanceSheetReserve")
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
            port_ref = self._find_column(df_ref_current, "portfolioIdentifier")
            ead_col = self._find_column(df_current, "offBalanceSheetEADAmountLifetime")
            res_col = self._find_column(df_current, "offBalanceSheetReserve")
            print("[hanmi_acl_report] unfundedBySegment: ead_col={}, res_col={}, id_res={}, port_ref={}".format(
                bool(ead_col), bool(res_col), bool(id_res), bool(port_ref)))
            left_on, right_on = self._join_keys_and_log(df_current, df_ref_current, "result", "ref", current_id)
            if id_res and id_ref and port_ref and (ead_col or res_col) and left_on is not None:
                ref_sub = df_ref_current[right_on + [port_ref]].drop_duplicates()
                merged = df_current.merge(ref_sub, left_on=left_on, right_on=right_on, how="left")
                for portfolio, grp in merged.groupby(port_ref, dropna=False):
                    ead = self._safe_sum(grp, "offBalanceSheetEADAmountLifetime") if ead_col else 0.0
                    res = self._safe_sum(grp, "offBalanceSheetReserve") if res_col else 0.0
                    if ead or res:
                        unfunded_by_segment.append({
                            "segment": str(portfolio) if portfolio is not None else "",
                            "availableCredit": round(ead, 2),
                            "reserve": round(res, 2),
                        })
            else:
                print("[hanmi_acl_report] unfundedBySegment: SKIP - missing required columns")
        else:
            print("[hanmi_acl_report] unfundedBySegment: SKIP - result or ref empty")

        unfunded_trend = []
        for aid in [prior_id, current_id]:
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
            print("[hanmi_acl_report] unfundedTrend: analysisId={} result rows={}, obr_col={}, ead_col={}, obr={}, ead={}".format(
                aid, len(df_res), bool(obr_col), bool(ead_col), obr, ead))
            unfunded_trend.append({"analysisId": aid, "requiredReserve": round(obr, 2), "totalUnfunded": round(ead, 2)})

        out = {
            "reportMetadata": report_metadata,
            "segmentMethodology": segment_methodology,
            "collectivelyEvaluatedByMethodology": collectively_by_methodology,
            "quantitativeLossRatesBySegment": quantitative_by_segment,
            "netChargeOffsQuarterly": net_chargeoffs_quarterly,
            "qualitativeReservesBySegment": qualitative_by_segment,
            "macroeconomicBaseline": macroeconomic_baseline,
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
        print("[hanmi_acl_report] sections: segmentMethodology={}, collectivelyByMethodology={}, quantitativeSegments={}, netChargeOffs={}, qualitativeSegments={}, macroVars={}, individual={}, unfundedSegments={}, unfundedTrend={}".format(
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
        if not report_dir or not analysis_ids:
            return
        join_keys_used = []
        by_analysis = {}
        for aid in analysis_ids:
            print("[debug_all_data] loading for same analysisId={}".format(aid))
            df_ref = self._load_parquet_for_analysis("instrumentReference", aid)
            df_res = self._load_parquet_for_analysis("instrumentResult", aid, filter_summary=True)
            df_rep = self._load_parquet_for_analysis("instrumentReporting", aid)
            ref_rows = len(df_ref) if df_ref is not None else 0
            res_rows = len(df_res) if df_res is not None else 0
            rep_rows = len(df_rep) if df_rep is not None else 0
            print("[debug_all_data] analysisId={}: ref={}, result={}, reporting={} (all same analysis)".format(aid, ref_rows, res_rows, rep_rows))
            if df_ref is None or df_ref.empty:
                by_analysis[str(aid)] = {"refRows": 0, "resultRows": res_rows, "reportingRows": rep_rows, "mergedRows": 0, "groupedSummary": [], "error": "ref empty"}
                continue
            # Join keys: instrumentIdentifier (+ analysisIdentifier if in both)
            left_on, right_on = self._join_keys_and_log(
                df_ref, df_res if (df_res is not None and not df_res.empty) else df_ref.head(0), "ref", "result", aid
            )
            if left_on is None:
                by_analysis[str(aid)] = {"refRows": ref_rows, "resultRows": res_rows, "reportingRows": rep_rows, "mergedRows": 0, "groupedSummary": [], "error": "no join keys"}
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
            # Group by key dimensions (from ref)
            port_col = self._find_column(merged, "portfolioIdentifier")
            asc_col = self._find_column(merged, "ascImpairmentEvaluation")
            model_col = self._find_column(merged, "lossRateModelName") or self._find_column(merged, "pdModelName")
            group_cols = [c for c in [port_col, asc_col, model_col] if c is not None and c in merged.columns]
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
                by_analysis[str(aid)] = {"refRows": ref_rows, "resultRows": res_rows, "reportingRows": rep_rows, "mergedRows": merged_rows, "groupBy": group_cols, "groupedSummary": summary[:50], "groupedSummaryTotalRows": len(summary)}
            except Exception as e:
                by_analysis[str(aid)] = {"refRows": ref_rows, "resultRows": res_rows, "reportingRows": rep_rows, "mergedRows": merged_rows, "groupedSummary": [], "error": str(e)}
        out = {"joinKeysUsed": join_keys_used, "byAnalysis": by_analysis}
        debug_path = os.path.join(report_dir, "debug_all_data_summary.json")
        os.makedirs(report_dir, exist_ok=True)
        with open(debug_path, "w", encoding="utf-8") as f:
            json.dump(out, f, indent=2, default=str)
        print("[debug_all_data] Wrote -> {} ({} analyses)".format(debug_path, len(by_analysis)))

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
            print("[Model run] Step 4: Created {} with {} file(s): {}".format(zip_path, len(added), added))
        except Exception as e:
            self.logger.warning("Failed to create report zip: {}".format(e))

    def cleanUp(self, log_file=None, keep_temp=False):
        """Delete temp directories and upload logfile and batch id file"""
        if not keep_temp:
            self.io_session.deleteTempDirectories()
        if log_file:
            self.io_session.uploadFiles({'log': log_file})
