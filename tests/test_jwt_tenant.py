"""Tests for JWT tenant infra URL helpers (no moodyscappy)."""

import base64
import json
import os
import sys
import unittest
from unittest.mock import patch

_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from model.jwt_tenant import (
    decode_jwt_payload_unverified,
    normalize_infra_base_url,
    tenant_infra_url_from_claims,
    tenant_infra_url_from_issuer,
)


def _make_jwt(payload: dict) -> str:
    header = base64.urlsafe_b64encode(b'{"alg":"none"}').decode().rstrip("=")
    body = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip("=")
    return f"{header}.{body}.x"


class TestJwtTenant(unittest.TestCase):
    def test_decode_roundtrip(self):
        payload = {"sub": "u", "tenantInfraUrl": "https://tenant-a.example.com/infra/1.0/"}
        tok = _make_jwt(payload)
        self.assertEqual(decode_jwt_payload_unverified(tok), payload)

    def test_tenant_infra_url_from_claims_default_key(self):
        url = "https://tenant-a.example.com/infra/1.0/"
        claims = {"tenantInfraUrl": url}
        self.assertEqual(tenant_infra_url_from_claims(claims), url)

    def test_tenant_infra_url_from_iss_qa_auth_rafa(self):
        """Real Moodys token shape: no infra URL claim; iss points at OAuth host."""
        iss = "https://qa-auth.rafa.moodysanalytics.net/oauth2/token"
        self.assertEqual(
            tenant_infra_url_from_issuer(iss),
            "https://qa-api.rafa.moodysanalytics.net/infra/1.0/",
        )
        self.assertEqual(
            tenant_infra_url_from_claims({"iss": iss, "sub": "user@banking.com"}),
            "https://qa-api.rafa.moodysanalytics.net/infra/1.0/",
        )

    def test_tenant_infra_url_dict_value_with_inner_url(self):
        url = "https://x.example.com/infra/1.0/"
        claims = {"infraUrl": {"url": url}}
        self.assertEqual(tenant_infra_url_from_claims(claims), url)

    def test_tenant_infra_url_dotted_claim_path(self):
        url = "https://dotted.example.com/infra/1.0/"
        claims = {"moodys": {"tenantInfraUrl": url}}
        with patch.dict(os.environ, {"MOODYS_TENANT_URL_CLAIM_KEY": "moodys.tenantInfraUrl"}):
            self.assertEqual(tenant_infra_url_from_claims(claims), url)

    def test_normalize_trailing_slash(self):
        self.assertEqual(normalize_infra_base_url("https://x/a"), "https://x/a/")

    def test_resolve_tenant_url_for_cappy_env_wins(self):
        from config import config

        with patch.dict(os.environ, {"MOODYS_TENANT_URL": "https://env-only.example/infra/1.0/"}):
            jwt_tok = _make_jwt({"tenantInfraUrl": "https://from-jwt.example/infra/1.0/"})
            out = config.resolve_tenant_url_for_cappy(jwt=jwt_tok)
            self.assertTrue(out.startswith("https://env-only.example"))

    def test_resolve_tenant_url_for_cappy_from_jwt(self):
        from config import config

        with patch.dict(os.environ, {"MOODYS_TENANT_URL": ""}):
            jwt_tok = _make_jwt({"tenantInfraUrl": "https://jwt-tenant.example.com/infra/1.0/"})
            out = config.resolve_tenant_url_for_cappy(jwt=jwt_tok)
            self.assertEqual(out, "https://jwt-tenant.example.com/infra/1.0/")

    def test_resolve_tenant_url_for_cappy_missing_raises(self):
        from config import config

        with patch.dict(os.environ, {"MOODYS_TENANT_URL": ""}):
            jwt_tok = _make_jwt({"sub": "only"})
            with self.assertRaises(ValueError):
                config.resolve_tenant_url_for_cappy(jwt=jwt_tok)


if __name__ == "__main__":
    unittest.main()
