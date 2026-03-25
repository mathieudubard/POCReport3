"""Tests for model.interactive (no Cappy/S3)."""

import os
import sys
import unittest
from unittest.mock import MagicMock

_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

sys.modules.setdefault("boto3", MagicMock())

from model.interactive import build_interactive_mrp
from model.iosession import ModelRunParameters


class TestInteractiveMrp(unittest.TestCase):
    def test_build_interactive_mrp_requires_ids(self):
        with self.assertRaises(ValueError):
            build_interactive_mrp([])

    def test_build_interactive_mrp_loads_in_model_run_parameters(self):
        mrp = build_interactive_mrp(["4647909", "4647997"], run_name="t")
        obj = ModelRunParameters(mrp, "/fake/localModelRunParameters.json", {"jwt": "x"})
        self.assertTrue(obj.use_per_analysis_s3_download())
        self.assertEqual(obj.settings.get("analysisIds"), ["4647909", "4647997"])
        self.assertTrue(obj.settings.get("liveS3InputsByAnalysisId"))
        self.assertTrue(obj.settings.get("returnReportsInResponse"))
        self.assertTrue(obj.settings.get("libraryMode"))

    def test_analyses_normalized(self):
        mrp = build_interactive_mrp(
            [],
            analyses=[
                {"analysisId": "1", "tags": ["current"]},
                {"analysisId": "2", "tags": ["prior"]},
            ],
        )
        obj = ModelRunParameters(mrp, "/fake/mrp.json", {"jwt": "x"})
        self.assertEqual(obj.settings.get("analysisIds"), ["1", "2"])
        roles = obj.settings.get("analysisRoles") or {}
        self.assertEqual(roles.get("current"), "1")
        self.assertEqual(roles.get("prior"), "2")


if __name__ == "__main__":
    unittest.main()
