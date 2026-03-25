"""
Resolve Moodys **tenant infra base URL** (Rafa ``.../infra/...`` style) for Cappy.

Tokens often **do not** include a literal infra URL. Moodys Rafa JWTs typically expose:

- Optional URL claims (tried first) — see ``TENANT_INFRA_URL_CLAIM_KEYS``.
- ``iss`` (OAuth issuer), e.g. ``https://qa-auth.rafa.moodysanalytics.net/oauth2/token`` — we map that
  host to the matching **environment** infra base (same tiers as ``config/model-conf-*.ini``).
  Per-tenant identity is still ``orgsiteid`` / ``organization`` / etc.; Cappy resolves tenant using
  the infra API + token.

Override anytime with ``MOODYS_TENANT_URL`` (batch / Domino env).

Unverified decode is only for reading claims; signature validation is done by moodyscappy Cappy.
"""

from __future__ import annotations

import base64
import json
import logging
import os
from typing import Any, Dict, Optional
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# Claim names / dotted paths to try when ``MOODYS_TENANT_URL_CLAIM_KEY`` is unset.
# Extend this tuple for your IdP; override entirely with MOODYS_TENANT_URL_CLAIM_KEY (single key or path).
TENANT_INFRA_URL_CLAIM_KEYS = (
    "tenantInfraUrl",
    "tenant_infra_url",
    "moodysTenantInfraUrl",
    "rafaInfraUrl",
    "rafa_infra_url",
    "infraUrl",
    "infra_url",
)

# OAuth ``iss`` host -> Rafa tenant infra API base (must match config/model-conf-*.ini).
_ISSUER_HOST_TO_INFRA_BASE: Dict[str, str] = {
    "qa-auth.rafa.moodysanalytics.net": "https://qa-api.rafa.moodysanalytics.net/infra/1.0/",
    "ci-auth.rafa.moodysanalytics.net": "https://ci-api.rafa.moodysanalytics.net/infra/1.0/",
    "ea-auth.rafa.moodysanalytics.com": "https://ea-api.rafa.moodysanalytics.com/infra/1.0/",
    # Production: issuer host may vary; align with model-conf-prd.ini when known.
    "auth.rafa.moodysanalytics.com": "https://api.rafa.moodysanalytics.com/infra/1.0/",
}


def tenant_infra_url_from_issuer(iss: Any) -> Optional[str]:
    """
    Map JWT ``iss`` URL host to the Rafa infra base for that environment.

    Example: ``https://qa-auth.rafa.moodysanalytics.net/oauth2/token`` ->
    ``https://qa-api.rafa.moodysanalytics.net/infra/1.0/``
    """
    if not iss or not isinstance(iss, str):
        return None
    try:
        host = (urlparse(iss.strip()).hostname or "").lower()
    except Exception:
        return None
    if not host:
        return None
    base = _ISSUER_HOST_TO_INFRA_BASE.get(host)
    if base:
        return base
    logger.debug("No infra base mapping for iss host %r (add to _ISSUER_HOST_TO_INFRA_BASE)", host)
    return None


def decode_jwt_payload_unverified(token: str) -> Dict[str, Any]:
    """Return JWT payload JSON (no signature verification)."""
    token = (token or "").strip()
    if not token:
        raise ValueError("JWT is empty")
    parts = token.split(".")
    if len(parts) != 3:
        raise ValueError("JWT must have three dot-separated segments")
    payload_b64 = parts[1]
    pad = 4 - len(payload_b64) % 4
    if pad != 4:
        payload_b64 += "=" * pad
    try:
        raw = base64.urlsafe_b64decode(payload_b64)
        return json.loads(raw.decode("utf-8"))
    except (ValueError, json.JSONDecodeError) as e:
        raise ValueError(f"Invalid JWT payload: {e}") from e


def _get_by_path(claims: Any, path: str) -> Any:
    cur: Any = claims
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return None
        cur = cur[part]
    return cur


def _as_infra_url(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        s = value.strip()
        if s.startswith("http://") or s.startswith("https://"):
            return s
        return None
    if isinstance(value, dict):
        for inner in ("url", "infraUrl", "tenantInfraUrl", "href"):
            u = _as_infra_url(value.get(inner))
            if u:
                return u
    return None


def tenant_infra_url_from_claims_with_source(claims: Dict[str, Any]) -> tuple[Optional[str], str]:
    """
    Extract tenant infra base URL and a short provenance string for logging.

    Returns ``(url, source)`` where ``source`` is like ``claim:tenantInfraUrl`` or ``iss:qa-auth.rafa...``.
    """
    override = (os.environ.get("MOODYS_TENANT_URL_CLAIM_KEY") or "").strip()
    keys_to_try: tuple
    if override:
        keys_to_try = (override,)
    else:
        keys_to_try = TENANT_INFRA_URL_CLAIM_KEYS

    for key in keys_to_try:
        val = _get_by_path(claims, key)
        url = _as_infra_url(val)
        if url:
            logger.debug("[Cappy] tenant_url from JWT claim %r -> %r", key, url)
            return url, f"claim:{key}"

    iss = claims.get("iss")
    url = tenant_infra_url_from_issuer(iss)
    if url:
        try:
            host = (urlparse(str(iss).strip()).hostname or "") or "?"
        except Exception:
            host = "?"
        logger.debug("[Cappy] tenant_url from JWT iss host %r -> %r", host, url)
        return url, f"iss:{host}"

    logger.debug(
        "No tenant infra URL from claims (tried keys: %s) or iss mapping",
        keys_to_try,
    )
    return None, "none"


def tenant_infra_url_from_claims(claims: Dict[str, Any]) -> Optional[str]:
    """
    Extract tenant infra base URL from decoded JWT claims.

    Order: explicit URL claim (see ``MOODYS_TENANT_URL_CLAIM_KEY`` or defaults), then ``iss`` host mapping.
    """
    url, _ = tenant_infra_url_from_claims_with_source(claims)
    return url


def normalize_infra_base_url(url: str) -> str:
    """Strip and ensure a trailing slash (matches config/local.ini style)."""
    s = (url or "").strip()
    if not s:
        return s
    return s if s.endswith("/") else s + "/"
