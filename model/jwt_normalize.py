"""
Normalize JWT strings from HTTP layers (``Bearer ...``, JSON quotes) and validate compact JWT shape
before moodyscappy / python-jose parse (clear errors vs ``Not enough segments``).
"""

from __future__ import annotations


def normalize_bearer_jwt(raw: object) -> str:
    """
    Strip whitespace, optional surrounding quotes, and a leading ``Bearer `` / ``bearer `` prefix.
    """
    s = str(raw).strip()
    if len(s) >= 2 and s[0] == s[-1] and s[0] in "\"'":
        s = s[1:-1].strip()
    if s.lower().startswith("bearer "):
        s = s[7:].strip()
    return s


def validate_compact_jwt_three_segments(token: str) -> None:
    """
    Require a standard compact JWT (``header.payload.signature``). Cappy uses jose, which fails
    with ``JWTError: Not enough segments`` when this shape is wrong.
    """
    if not token:
        raise ValueError("jwt is empty after normalization (strip 'Bearer ' and pass the access token only)")
    parts = token.split(".")
    if len(parts) != 3:
        raise ValueError(
            "jwt must be a compact JWT with exactly three dot-separated segments "
            "(header.payload.signature). Got %d segment(s). "
            "Do not pass JSON, user id, or a truncated token; pass only the raw access token "
            "(optionally prefixed with 'Bearer ')." % len(parts)
        )
    if not all(p.strip() for p in parts):
        raise ValueError("jwt has an empty segment; token may be truncated or malformed")
