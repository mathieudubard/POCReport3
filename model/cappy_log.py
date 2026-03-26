"""
Echo Cappy diagnostics to **stdout** and the logger so bare runtimes (Domino, WSGI) show URLs
without tuning ``logging.ini`` levels or handlers.
"""

from __future__ import annotations

import logging
from typing import Any


def cappy_echo_info(logger: logging.Logger, fmt: str, *args: Any) -> None:
    text = fmt % args if args else fmt
    logger.info(text)
    print(text, flush=True)


def cappy_echo_warning(logger: logging.Logger, fmt: str, *args: Any) -> None:
    text = fmt % args if args else fmt
    logger.warning(text)
    print(text, flush=True)


def cappy_echo_error(logger: logging.Logger, fmt: str, *args: Any) -> None:
    text = fmt % args if args else fmt
    logger.error(text)
    print(text, flush=True)


def milestone_banner(title: str) -> None:
    """One-line stdout marker for major phases (Domino / plain logs). Keep usage sparse."""
    label = (title or "").strip()
    if not label:
        return
    print("", flush=True)
    print("******* {} *******".format(label), flush=True)
    print("", flush=True)


def looks_like_cappy_jwt_failure(exc: BaseException) -> bool:
    """True when Cappy/python-jose rejected the JWT (signature, format, etc.)."""
    name = type(exc).__name__
    msg = str(exc).lower()
    if name in ("JWTError", "JWSError", "JWSSignatureError", "JWSAlgorithmError"):
        return True
    if "signature verification failed" in msg:
        return True
    if "jwt" in name.lower() and ("signature" in msg or "verification" in msg or "invalid" in msg):
        return True
    return False


def log_cappy_jwt_unusable(logger: logging.Logger, exc: BaseException, context: str) -> None:
    """
    Loud stdout + logger when Cappy cannot validate the JWT. Never log the raw token.
    """
    ctx = (context or "Cappy session").strip() or "Cappy session"
    lines = [
        "",
        "=" * 72,
        "  CAPPY / JWT — TOKEN NOT ACCEPTED",
        "  (%s)" % ctx,
        "=" * 72,
        "  Cappy could not validate the JWT (python-jose). This is not a Hanmi report-model bug.",
        "",
        "  If you see 'Signature verification failed':",
        "    - Paste the token on one line (no line breaks inside xxx.yyy.zzz).",
        "    - If you use Bearer, pass the token through normalize_bearer_jwt / -j correctly.",
        "    - Use a fresh access token from the same SSO environment as tenant_url (e.g. QA).",
        "    - Wrong token type (not the access JWT Cappy expects) also fails here.",
        "",
        "  Underlying error: %s" % (exc,),
        "=" * 72,
        "",
    ]
    text = "\n".join(lines)
    logger.error(text)
    print(text, flush=True)
