"""Unit tests for adjustment API URL helper (no HTTP)."""

import os
import sys
import unittest

_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from model.adjustment_api import adjustment_details_url, impairment_studio_api_base


class TestAdjustmentApiUrl(unittest.TestCase):
    def test_default_base_and_path(self):
        self.assertIn("qa-api.impairmentstudio.moodysanalytics.net", impairment_studio_api_base())
        u = adjustment_details_url(4648731)
        self.assertIn("/adjustment/1.0/analyses/4648731/adjustmentdetails", u)
        self.assertTrue(u.startswith("https://"))

    def test_env_override_base(self):
        os.environ["IMPAIRMENT_STUDIO_API_BASE"] = "https://example.com"
        try:
            self.assertEqual(
                adjustment_details_url(1),
                "https://example.com/adjustment/1.0/analyses/1/adjustmentdetails",
            )
        finally:
            del os.environ["IMPAIRMENT_STUDIO_API_BASE"]


if __name__ == "__main__":
    unittest.main()
