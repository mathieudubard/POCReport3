#!/usr/bin/env python3
"""
Local diagnostic: print every Hanmi step for SSO / tenant URL resolution and (if installed)
introspect moodyscappy Cappy. Does not run the full Model or touch S3.

Usage (from repo root):
  python scripts/diagnose_cappy_tenant_urls.py
  python scripts/diagnose_cappy_tenant_urls.py --jwt-file path/to/token.txt
  python scripts/diagnose_cappy_tenant_urls.py --clear-env   # ignore MOODYS_* in environment

Hanmi code does not build HTTP paths to "categories" - Cappy does, using tenant_url + path suffixes.
This script shows typical urljoin patterns inferred from common Cappy behavior and stack traces.
"""

from __future__ import annotations

import argparse
import base64
import inspect
import json
import os
import sys
from urllib.parse import urljoin

# Repo root = parent of scripts/
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_SCRIPT_DIR)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)


def _banner(title: str) -> None:
    line = "=" * min(76, max(len(title) + 4, 40))
    print(f"\n{line}\n  {title}\n{line}", flush=True)


def _step(n: int, msg: str) -> None:
    print(f"\n[step {n}] {msg}", flush=True)


def _make_sample_jwt_qa_rafa() -> str:
    """JWT body matching QA Rafa iss (no signature verification needed for our resolver)."""
    payload = {
        "iss": "https://qa-auth.rafa.moodysanalytics.net/oauth2/token",
        "sub": "mathieu.dubard@banking.com",
        "organization": "0014000000NXtS8",
    }
    header = {"alg": "none", "typ": "JWT"}
    h = base64.urlsafe_b64encode(json.dumps(header, separators=(",", ":")).encode()).decode().rstrip("=")
    p = base64.urlsafe_b64encode(json.dumps(payload, separators=(",", ":")).encode()).decode().rstrip("=")
    return f"{h}.{p}.diagnostic-signature"


def _print_urljoin_examples(tenant_base: str, sso_base: str) -> None:
    """Illustrate how bases are usually combined (Cappy uses similar patterns internally)."""
    _banner("Hypothetical full URLs (urljoin - verify in moodyscappy/cappy.py)")
    tb = tenant_base.rstrip("/") + "/"
    sb = sso_base.rstrip("/") + "/"
    print("tenant_url is typically the Rafa **infra** API root, e.g. .../infra/1.0/", flush=True)
    for suffix in ("resources", "tenant", "tenants"):
        print(f"  urljoin(tenant_url, {suffix!r}) -> {urljoin(tb, suffix)}", flush=True)
    print("\nsso_url is the SSO API root (JWKS / certs):", flush=True)
    for suffix in ("auth/certs", "oauth2/token"):
        print(f"  urljoin(sso_url, {suffix!r}) -> {urljoin(sb, suffix)}", flush=True)
    print(
        "\nS3 paths for parquet (output/..., export/...) come from cap_session.context + boto3 - "
        "not from tenant_url string concatenation in Hanmi code (see model/iosession.py).",
        flush=True,
    )


def _introspect_cappy() -> None:
    _banner("moodyscappy.Cappy (if installed)")
    try:
        from moodyscappy import Cappy
    except ImportError as e:
        print(f"  Not installed: {e}", flush=True)
        print("  Install the same env as Domino, or read site-packages/moodyscappy/cappy.py on the remote host.", flush=True)
        return
    try:
        sig = inspect.signature(Cappy.__init__)
        print(f"  Cappy.__init__{sig}", flush=True)
        for name in sorted(sig.parameters):
            if name == "self":
                continue
            print(f"    param: {name}", flush=True)
    except Exception as ex:
        print(f"  signature: {ex}", flush=True)
    try:
        import moodyscappy.cappy as cmod

        path = inspect.getfile(cmod)
        print(f"  module file: {path}", flush=True)
        print("  Search this file for: tenant_url, sso_url, urljoin, resources, _get_tenant", flush=True)
    except Exception as ex:
        print(f"  could not locate module file: {ex}", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Diagnose Cappy SSO/tenant URL resolution (Hanmi + optional Cappy)")
    parser.add_argument("--jwt-file", metavar="PATH", help="Read JWT string from file (strip whitespace)")
    parser.add_argument("--clear-env", action="store_true", help="Clear MOODYS_SSO_URL, MOODYS_TENANT_URL, GLOBAL_* for this process")
    args = parser.parse_args()

    if args.clear_env:
        for k in list(os.environ.keys()):
            if k.startswith("MOODYS_") or k.startswith("GLOBAL_SSO") or k == "MOODYS_TENANT_URL_CLAIM_KEY":
                del os.environ[k]
        _step(0, "Cleared MOODYS_* / GLOBAL_SSO* in this process (--clear-env)")

    jwt_in = None
    if args.jwt_file:
        with open(args.jwt_file, encoding="utf-8") as f:
            jwt_in = f.read().strip()
    else:
        jwt_in = _make_sample_jwt_qa_rafa()
        _step(0, "Using built-in sample QA Rafa JWT (use --jwt-file for a real token)")

    from config import config as cfg

    # Avoid duplicate stdout: resolve_* uses _cappy_echo_info (log+print). Keep logger only here.
    def _echo_log_only_info(fmt, *args):
        text = fmt % args if args else fmt
        cfg._log.info(text)

    def _echo_log_only_error(fmt, *args):
        text = fmt % args if args else fmt
        cfg._log.error(text)

    cfg._cappy_echo_info = _echo_log_only_info
    cfg._cappy_echo_error = _echo_log_only_error

    from model.jwt_tenant import (
        decode_jwt_payload_unverified,
        tenant_infra_url_from_claims_with_source,
        tenant_infra_url_from_issuer,
    )

    _banner("Environment (relevant keys)")
    for k in sorted(os.environ.keys()):
        if "MOODYS" in k or "GLOBAL_SSO" in k or "PROXY_TOKEN" in k:
            print(f"  {k}={os.environ[k]!r}", flush=True)
    if not any("MOODYS" in k for k in os.environ):
        print("  (no MOODYS_* in env - resolution will use JWT / fallbacks)", flush=True)

    _banner("JWT payload (unverified decode)")
    claims = decode_jwt_payload_unverified(jwt_in)
    print(json.dumps(claims, indent=2), flush=True)

    _step(1, "tenant_infra_url_from_issuer(claims['iss'])")
    iss = claims.get("iss")
    print(f"  iss = {iss!r}", flush=True)
    print(f"  -> {tenant_infra_url_from_issuer(iss)!r}", flush=True)

    _step(2, "tenant_infra_url_from_claims_with_source(claims)")
    url, source = tenant_infra_url_from_claims_with_source(claims)
    print(f"  source = {source!r}", flush=True)
    print(f"  url    = {url!r}", flush=True)

    _step(3, "config.resolve_sso_url_for_cappy()")
    sso = cfg.resolve_sso_url_for_cappy()
    print(f"  sso_url = {sso!r}", flush=True)

    _step(4, "config.resolve_tenant_url_for_cappy(jwt=...)")
    tenant = cfg.resolve_tenant_url_for_cappy(jwt=jwt_in)
    print(f"  tenant_url = {tenant!r}", flush=True)

    _step(5, "Credentials dict as in model/run.py (secrets redacted)")
    creds = {
        "jwt": "<redacted>" if jwt_in else None,
        "username": None,
        "password": None,
        "sso_url": sso,
        "tenant_url": tenant,
    }
    print(json.dumps({k: v for k, v in creds.items() if k != "jwt"}, indent=2))
    print('  "jwt": "<present>"' if jwt_in else '  "jwt": null')

    _print_urljoin_examples(tenant, sso)
    _introspect_cappy()

    print("\nDone.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
