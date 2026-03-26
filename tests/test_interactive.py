"""Tests for model.interactive (no Cappy/S3)."""

import os
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

sys.modules.setdefault("boto3", MagicMock())

from model.interactive import build_interactive_mrp
from model.iosession import IOSession, ModelRunParameters


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
        self.assertTrue(obj.settings.get("exportCsvInputs"))
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


class TestTenantS3WithLocalMrp(unittest.TestCase):
    """Library/interactive: local_mode True but bucket inputs still use S3 when liveS3InputsByAnalysisId is set."""

    def _io_stub(self, local_mode: bool, mrp_dict: dict):
        mrp = ModelRunParameters(mrp_dict, "/tmp/mrp.json", {"jwt": "x"})
        io = IOSession.__new__(IOSession)
        io.logger = MagicMock()
        io.local_mode = local_mode
        io.model_run_parameters = mrp
        io.cap_session = MagicMock()
        io.cap_session.context = {"s3_bucket": "tenant-bucket"}
        return io

    def test_get_s3_object_keys_skipped_when_local_without_live_flag(self):
        mrp = {
            "name": "t",
            "datasets": {
                "modelFactors": [],
                "inputData": {},
                "outputData": {},
                "supportingData": {},
                "settings": [],
            },
            "settings": {
                "inputPath": "apps/x/input",
                "outputPaths": {"report": "apps/x/out"},
                "logPath": "apps/x/log",
                "runDate": "2025-01-01",
                "reportingDate": "2025-01-01",
                "analysisIds": ["1"],
                "liveS3InputsByAnalysisId": False,
            },
        }
        io = self._io_stub(True, mrp)
        self.assertEqual(io._get_s3_object_keys("output/"), [])

    def test_get_s3_object_keys_uses_s3_when_local_with_live_flag(self):
        io = self._io_stub(True, build_interactive_mrp(["4647997"]))
        mock_client = MagicMock()
        paginator = MagicMock()
        mock_client.get_paginator.return_value = paginator
        paginator.paginate.return_value = [
            {"Contents": [{"Key": "output/instrumentResult/analysisidentifier=4647997/scenarioidentifier=Summary/a.parquet"}]}
        ]
        io._cached_s3_client = None
        io.cap_session.init_s3_client.return_value = mock_client
        keys = io._get_s3_object_keys("output/instrumentResult/analysisidentifier=4647997/scenarioidentifier=Summary/")
        self.assertEqual(len(keys), 1)
        self.assertTrue(keys[0].endswith(".parquet"))
        mock_client.get_paginator.assert_called_once_with("list_objects_v2")

    def test_download_file_uses_boto3_when_tenant_live(self):
        io = self._io_stub(True, build_interactive_mrp(["4647997"]))
        io._cached_s3_client = None
        mock_client = MagicMock()
        io.cap_session.init_s3_client.return_value = mock_client
        with patch.dict(os.environ, {"HANMI_S3_DOWNLOAD_VIA_BOTO3": "1"}, clear=False):
            with tempfile.TemporaryDirectory() as td:
                path = os.path.join(td, "sub", "a.parquet")
                io._downloadFile("output/x/a.parquet", path)
        mock_client.download_file.assert_called_once_with("tenant-bucket", "output/x/a.parquet", path)
        io.cap_session.s3_download_file.assert_not_called()

    def test_download_file_skips_boto3_when_env_off(self):
        io = self._io_stub(True, build_interactive_mrp(["4647997"]))
        io._cached_s3_client = None
        mock_client = MagicMock()
        io.cap_session.init_s3_client.return_value = mock_client
        with patch.dict(os.environ, {"HANMI_S3_DOWNLOAD_VIA_BOTO3": "0"}, clear=False):
            with tempfile.TemporaryDirectory() as td:
                path = os.path.join(td, "b.parquet")
                io._downloadFile("output/x/b.parquet", path)
        mock_client.download_file.assert_not_called()
        io.cap_session.s3_download_file.assert_called_once_with("output/x/b.parquet", path)


if __name__ == "__main__":
    unittest.main()
