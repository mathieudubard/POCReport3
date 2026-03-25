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


def _apply_host_env_config(config_module) -> None:
    """
    Load ``config/local.ini`` into ``os.environ`` using a path **anchored to this repo** (next to ``model/``).

    On some hosts (e.g. Domino), ``import config`` can resolve to a different package than this project, so
    ``config.config.ENV_CONFIGURATION_FILE`` may not point at our ``local.ini`` — Cappy then sees no ``MOODYS_SSO_URL``
    and fails with ``None/auth/certs``. Tenant infra URL is resolved per run from JWT claims or ``MOODYS_TENANT_URL``
    (see ``model/jwt_tenant.py``); it is not a single fixed QA endpoint.

    Also mirrors ``GLOBAL_SSO_API_SERVICE_URL`` → ``MOODYS_SSO_URL`` when the platform sets the former only.

    If still unset, sets ``MOODYS_SSO_URL`` to the QA default and logs **CRITICAL** lines plus ``print`` when that
    fallback is used.
    """
    from config import config as project_config

    explicit_ini = os.path.join(_REPO_ROOT, "config", "local.ini")
    if os.path.isfile(explicit_ini):
        config_module.processConfigurations(
            optional_config=explicit_ini, optional_additions=None, overwrite_existing=True
        )
        logger.info("Loaded host env from %s", explicit_ini)
    else:
        config_module.processConfigurations(None, None, True)
        logger.info("Loaded host env via config package default path (no file at %s)", explicit_ini)

    # moodyscappy expects MOODYS_SSO_URL; platforms often inject GLOBAL_SSO_API_SERVICE_URL only (see runner/config.py).
    if not (os.environ.get("MOODYS_SSO_URL") or "").strip() and (
        os.environ.get("GLOBAL_SSO_API_SERVICE_URL") or ""
    ).strip():
        os.environ["MOODYS_SSO_URL"] = os.environ["GLOBAL_SSO_API_SERVICE_URL"].strip()
        logger.info("Set MOODYS_SSO_URL from GLOBAL_SSO_API_SERVICE_URL")

    if not (os.environ.get("MOODYS_SSO_URL") or "").strip():
        os.environ["MOODYS_SSO_URL"] = project_config.resolve_sso_url_for_cappy()
        if os.environ["MOODYS_SSO_URL"] == project_config.FALLBACK_QA_MOODYS_SSO_URL:
            _log_last_resort_qa_sso_fallback(project_config.FALLBACK_QA_MOODYS_SSO_URL)

    # Do not set MOODYS_TENANT_URL here: that would run before run_model_batch and skip JWT/iss resolution.
    # QA tenant backstop is in config.resolve_tenant_url_for_cappy() after env + JWT.


def _log_last_resort_qa_sso_fallback(url: str) -> None:
    """Loud diagnostics when QA SSO URL is applied because no other source set MOODYS_SSO_URL."""
    banner = "=" * 76
    lines = [
        banner,
        "LAST-RESORT FALLBACK: MOODYS_SSO_URL was unset after config + GLOBAL_SSO mirror.",
        "Cappy will use QA SSO base (same as config/local.ini default):",
        "  " + url,
        "This is NOT appropriate for production unless you intend QA SSO.",
        "Set MOODYS_SSO_URL or GLOBAL_SSO_API_SERVICE_URL in the environment, or ship config/local.ini.",
        banner,
    ]
    for line in lines:
        logger.critical(line)
    logger.warning(
        "MOODYS_SSO_URL=%s (QA last-resort fallback). See CRITICAL lines above.",
        os.environ.get("MOODYS_SSO_URL"),
    )
    # Domino / WSGI often surface print reliably
    print("\n".join(["[interactive_run] " + ln for ln in lines]), flush=True)


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
    :raises ValueError: if ``jwt`` is missing or not a compact JWT (three segments); avoids opaque jose errors.
    """
    if not jwt or not str(jwt).strip():
        raise ValueError("jwt is required")

    from model.jwt_normalize import normalize_bearer_jwt, validate_compact_jwt_three_segments

    jwt_norm = normalize_bearer_jwt(jwt)
    validate_compact_jwt_three_segments(jwt_norm)

    from config import config
    from model.run import run_model_batch

    if load_host_config:
        _apply_host_env_config(config)
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
            jwt=jwt_norm,
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
