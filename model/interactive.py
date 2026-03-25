"""
Lean entry point for library-style use: JWT + analysis IDs → download inputs from S3 via Cappy, build reports, return JSON.

Uses ``settings.libraryMode`` so :meth:`model.model.Model.run` skips zip creation and all S3 uploads (outputs and log).
Parquet and ``analysisDetails`` still use bucket-root layouts in ``iosession`` (``output/``, ``export/``, ``input/``).

Placeholder ``inputPath`` / ``outputPaths`` / ``logPath`` values exist only so ``ModelRunParameters`` / local temp dirs
initialize; they are not used as upload targets in library mode.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import tempfile
from typing import Any, Dict, List, Optional, Sequence, Union

import pandas as pd

# Repo root on sys.path (same pattern as model/run.py)
_MODEL_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_MODEL_DIR)
import sys

if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

logger = logging.getLogger(__name__)

AnalysisId = Union[str, int]


def build_interactive_mrp(
    analysis_ids: Sequence[AnalysisId],
    *,
    run_name: str = "interactive_run",
    analyses: Optional[List[Dict[str, Any]]] = None,
    settings_patch: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Build a minimal modelRunParameter-shaped dict for live S3 per-analysis download (no callback).

    ``analyses`` optional: same shape as settings.analyses (analysisId, quarterLabel, tags / role).
    When provided, :meth:`IOSession.normalize_analyses_to_settings` is applied.
    """
    settings: Dict[str, Any] = {
        "inputPath": "_library/input",
        "outputPaths": {"report": "_library/report"},
        "logPath": "_library/log",
        "runDate": pd.Timestamp.utcnow().strftime("%Y-%m-%d"),
        "reportingDate": pd.Timestamp.utcnow().strftime("%Y-%m-%d"),
        "scenarios": [],
        "liveS3InputsByAnalysisId": True,
        "returnReportsInResponse": True,
        "libraryMode": True,
    }
    if analyses is not None:
        from model.iosession import IOSession

        settings["analyses"] = analyses
        IOSession.normalize_analyses_to_settings(settings)
    if not settings.get("analysisIds"):
        ids = [str(a) for a in analysis_ids if a is not None and str(a).strip() != ""]
        if not ids:
            raise ValueError("analysis_ids (or settings.analyses) must define at least one analysis id")
        settings["analysisIds"] = ids
    if settings_patch:
        settings.update(settings_patch)
    settings["libraryMode"] = True

    return {
        "name": run_name,
        "datasets": {
            "modelFactors": [],
            "inputData": [],
            "outputData": [{"category": "report", "attributes": []}],
            "supportingData": [],
            "settings": [],
        },
        "settings": settings,
    }


def interactive_run(
    jwt: str,
    analysis_ids: Sequence[AnalysisId],
    *,
    run_name: Optional[str] = None,
    analyses: Optional[List[Dict[str, Any]]] = None,
    settings_patch: Optional[Dict[str, Any]] = None,
    configure_logging: bool = True,
    load_host_config: bool = True,
    keep_temp: bool = False,
) -> Dict[str, Any]:
    """
    Run the report pipeline: S3 reads via Cappy (JWT), local report JSON only — **no S3 uploads** (``libraryMode``).

    Returns **one JSON-serializable dict** with exactly two top-level keys:

    - ``quarterly_summary_report``: parsed body of ``quarterly_summary_report.json`` (required).
    - ``hanmi_acl_quarterly_report``: parsed body of ``hanmi_acl_quarterly_report.json``, or ``null`` if missing / parse error.

    :param jwt: Bearer token for Cappy (bucket and S3 client come from Cappy context).
    :param analysis_ids: Analysis identifiers (strings or ints).
    :param run_name: ``modelRunParameter.name`` / logging (default ``interactive_run``).
    :param analyses: Optional rich analyses array (quarter labels, roles); merged via ``normalize_analyses_to_settings``.
    :param settings_patch: Merged into ``settings`` after defaults. ``libraryMode`` remains forced on.
    :param configure_logging: If True, call ``config.configureLogger()`` after loading host config.
    :param load_host_config: If True (default), load ``config/local.ini`` into ``os.environ`` (``MOODYS_SSO_URL``, etc.).
           **Required for Cappy** unless those variables are already set in the process environment. Set False only when
           the host injects SSO/tenant URLs without using ``local.ini``.
    :param keep_temp: Retain IOSession temp dirs on success (debug).
    :raises RuntimeError: if the model run fails or the quarterly summary JSON is missing / failed to parse.
    """
    if not jwt or not str(jwt).strip():
        raise ValueError("jwt is required")

    from config import config
    from model.run import run_model_batch

    if load_host_config:
        config.processConfigurations(None, None, True)
    if configure_logging:
        config.configureLogger()

    mrp = build_interactive_mrp(
        analysis_ids,
        run_name=run_name or "interactive_run",
        analyses=analyses,
        settings_patch=settings_patch,
    )

    tmp = tempfile.mkdtemp(prefix="hanmi_interactive_")
    mrp_path = os.path.join(tmp, "modelRunParameter.json")
    try:
        with open(mrp_path, "w", encoding="utf-8") as f:
            json.dump(mrp, f, indent=2)

        ns = argparse.Namespace(
            jwt=str(jwt).strip(),
            unpw=[None, None],
            local=os.path.abspath(mrp_path).replace("\\", "/"),
            s3=None,
            proxyjwt=None,
            proxyunpw=[None, None],
            keeptemp=keep_temp,
        )
        exit_code, model = run_model_batch(ns, return_model=True)
        if exit_code != 0:
            raise RuntimeError("interactive_run failed (model exited with errors); see logs above")

        payload = getattr(model, "report_response_payload", None)
        reports = (payload or {}).get("reports") if isinstance(payload, dict) else None
        if not isinstance(reports, dict):
            reports = {}

        qkey = "quarterly_summary_report.json"
        quarterly = reports.get(qkey)
        if isinstance(quarterly, dict) and quarterly.get("_parseError"):
            raise RuntimeError(
                "quarterly_summary_report.json parse error: {}".format(quarterly.get("_parseError"))
            )
        if quarterly is None:
            raise RuntimeError("quarterly_summary_report.json was not produced")

        hkey = "hanmi_acl_quarterly_report.json"
        hanmi = reports.get(hkey)
        if isinstance(hanmi, dict) and hanmi.get("_parseError"):
            hanmi = None

        return {
            "quarterly_summary_report": quarterly,
            "hanmi_acl_quarterly_report": hanmi,
        }
    finally:
        if not keep_temp and os.path.isdir(tmp):
            shutil.rmtree(tmp, ignore_errors=True)


__all__ = ["build_interactive_mrp", "interactive_run"]
