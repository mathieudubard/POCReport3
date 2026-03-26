"""
HTTP client for Impairment Studio **Adjustment** API (qualitative / manual adjustments metadata).

Swagger (QA): ``/adjustment/docs/swagger-ui/`` — operation *downloadAdjustmentDetailsByAnalysisID*.

**Authentication:** each request sends the same access token as Managed Batch / Cappy::

    Authorization: Bearer <JWT>

No separate API key — the JWT must be accepted by the Impairment Studio API for your tenant/environment.

Base URL: ``IMPAIRMENT_STUDIO_API_BASE`` (defaults to QA).
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, List, Optional

import requests

logger = logging.getLogger(__name__)

DEFAULT_IMPAIRMENT_STUDIO_API_BASE = "https://qa-api.impairmentstudio.moodysanalytics.net"


def impairment_studio_api_base() -> str:
    return (os.environ.get("IMPAIRMENT_STUDIO_API_BASE") or DEFAULT_IMPAIRMENT_STUDIO_API_BASE).strip().rstrip("/")


def adjustment_details_url(analysis_id: Any) -> str:
    """Full URL: ``.../adjustment/1.0/analyses/{id}/adjustmentdetails``."""
    return "{}/adjustment/1.0/analyses/{}/adjustmentdetails".format(impairment_studio_api_base(), analysis_id)


def fetch_adjustment_details_json(jwt: str, analysis_id: Any, timeout: float = 120.0) -> Optional[List[Any]]:
    """
    GET adjustment details for one analysis (main / latest quarter in product terms).

    Returns a list of adjustment objects, ``[]`` on 404, or ``None`` on auth/network/parse failure.

    The JWT is sent only in the ``Authorization`` header (never logged).
    """
    if not jwt or not str(jwt).strip():
        logger.info("[adjustment API] skip: no JWT")
        return None
    url = adjustment_details_url(analysis_id)
    headers = {
        "Authorization": "Bearer {}".format(jwt.strip()),
        "Accept": "application/json",
    }
    try:
        r = requests.get(url, headers=headers, timeout=timeout)
        if r.status_code == 404:
            logger.warning("[adjustment API] 404 %s — using empty list", url)
            return []
        if r.status_code == 401:
            logger.warning(
                "[adjustment API] 401 Unauthorized %s — Bearer JWT rejected; use same token as Cappy / refresh SSO token",
                url,
            )
            return None
        if r.status_code == 403:
            logger.warning(
                "[adjustment API] 403 Forbidden %s — token valid but not allowed for this analysis or API",
                url,
            )
            return None
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return [data]
        logger.warning("[adjustment API] unexpected JSON type from %s", url)
        return None
    except requests.HTTPError as e:
        logger.warning("[adjustment API] HTTP error %s: %s", url, e)
        return None
    except requests.RequestException as e:
        logger.warning("[adjustment API] request failed %s: %s", url, e)
        return None
    except (ValueError, json.JSONDecodeError) as e:
        logger.warning("[adjustment API] invalid JSON from %s: %s", url, e)
        return None
