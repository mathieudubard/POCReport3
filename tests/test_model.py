"""
Unit tests for model-report-generator: Model class, run flow, and report building.
Uses mocks for Cappy and IOSession so tests run without S3 or moodyscappy.
Run from project root: python -m pytest tests/ -v
"""
import json
import os
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_model_dir = os.path.join(_project_root, "model")
for _p in (_model_dir, _project_root):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# Load model.model with moodyscappy/boto3 mocked, then model package (so model.run can import Model)
import importlib.util
_mocks = {"moodyscappy": MagicMock(), "boto3": MagicMock()}  # keep for TestRunEntrypoint
with patch.dict("sys.modules", _mocks):
    _spec = importlib.util.spec_from_file_location("model.model", os.path.join(_model_dir, "model.py"))
    model_module = importlib.util.module_from_spec(_spec)
    sys.modules["model.model"] = model_module
    _spec.loader.exec_module(model_module)
    import model  # noqa: F401 - loads package; __init__.py does "from model.model import Model"


def _make_mock_io_session(local_directories=None, call_back=True, analysis_ids=None):
    """Build a mock IOSession with minimal attributes needed for report building."""
    mock_io = MagicMock()
    mock_io.local_directories = local_directories or {
        "inputPaths": {
            "instrumentResult": [],
            "instrumentReporting": [],
            "instrumentReference": [],
        },
        "outputPaths": {"report": None},
    }
    mock_mrp = MagicMock()
    mock_mrp.callBack = call_back
    mock_mrp.settings = {"analysisIds": analysis_ids or [4647997, 4647909]}
    mock_io.model_run_parameters = mock_mrp
    mock_io.local_temp_directory = tempfile.mkdtemp()
    return mock_io


class TestModelHelpers(unittest.TestCase):
    """Tests for Model helper methods (_find_column, _safe_sum, _filter_summary_scenario)."""

    def setUp(self):
        mock_io = _make_mock_io_session()
        with patch.object(model_module, "Cappy", MagicMock()):
            with patch.object(model_module.iosession, "IOSession", MagicMock(return_value=mock_io)):
                self.model = model_module.Model(
                    {"jwt": "x"}, {}, "/fake/mrp.json", local_mode=True
                )

    def test_find_column_case_insensitive(self):
        df = pd.DataFrame({"Foo": [1], "ScenarioIdentifier": ["Summary"], "bar": [3]})
        self.assertEqual(self.model._find_column(df, "scenarioidentifier"), "ScenarioIdentifier")
        self.assertEqual(self.model._find_column(df, "foo"), "Foo")
        self.assertIsNone(self.model._find_column(df, "missing"))

    def test_safe_sum_present(self):
        df = pd.DataFrame({"a": [1.0, 2.0, 3.0]})
        self.assertEqual(self.model._safe_sum(df, "a"), 6.0)

    def test_safe_sum_missing_column(self):
        df = pd.DataFrame({"a": [1, 2]})
        self.assertEqual(self.model._safe_sum(df, "b"), 0.0)

    def test_safe_sum_none_df(self):
        self.assertEqual(self.model._safe_sum(None, "a"), 0.0)

    def test_filter_summary_scenario(self):
        df = pd.DataFrame({
            "scenarioIdentifier": ["Summary", "Base", "summary", "OTHER"],
            "v": [1, 2, 3, 4],
        })
        out = self.model._filter_summary_scenario(df)
        self.assertEqual(len(out), 2)
        self.assertTrue(out["scenarioIdentifier"].str.lower().eq("summary").all())

    def test_get_analysis_roles_default(self):
        """Without analysisRoles, current=first analysisId, prior=second, priorYear=None, quarters=[]."""
        self.model.io_session.model_run_parameters.settings["analysisIds"] = [100, 200]
        self.model.io_session.model_run_parameters.settings.pop("analysisRoles", None)
        cur, pri, py, quarters = self.model._get_analysis_roles()
        self.assertEqual(cur, 100)
        self.assertEqual(pri, 200)
        self.assertIsNone(py)
        self.assertEqual(quarters, [])

    def test_get_analysis_roles_from_metadata(self):
        """With analysisRoles, use current/prior/priorYear/quarters from it."""
        self.model.io_session.model_run_parameters.settings["analysisIds"] = [100, 200, 300]
        self.model.io_session.model_run_parameters.settings["analysisRoles"] = {
            "current": 300,
            "prior": 200,
            "priorYear": 100,
            "quarters": [100, 200, 300],
        }
        cur, pri, py, quarters = self.model._get_analysis_roles()
        self.assertEqual(cur, 300)
        self.assertEqual(pri, 200)
        self.assertEqual(py, 100)
        self.assertEqual(quarters, [100, 200, 300])

    def test_get_analysis_roles_quarters_only(self):
        """quarters is used for multi-quarter tables; other keys optional."""
        self.model.io_session.model_run_parameters.settings["analysisIds"] = [1, 2, 3]
        self.model.io_session.model_run_parameters.settings["analysisRoles"] = {"quarters": [1, 2, 3]}
        cur, pri, py, quarters = self.model._get_analysis_roles()
        self.assertEqual(cur, 1)
        self.assertEqual(pri, 2)
        self.assertIsNone(py)
        self.assertEqual(quarters, [1, 2, 3])


class TestBuildQuarterlySummaryReport(unittest.TestCase):
    """Tests for build_quarterly_summary_report with mocked IO and temp dirs."""

    def setUp(self):
        self.report_dir = tempfile.mkdtemp()
        mock_io = _make_mock_io_session(
            local_directories={
                "inputPaths": {
                    "instrumentResult": [],
                    "instrumentReporting": [],
                    "instrumentReference": [],
                },
                "outputPaths": {"report": self.report_dir},
            },
            call_back=True,
            analysis_ids=["curr", "prior"],
        )
        mock_io.local_directories["outputPaths"]["report"] = self.report_dir
        with patch.object(model_module, "Cappy", MagicMock()):
            with patch.object(model_module.iosession, "IOSession", MagicMock(return_value=mock_io)):
                self.model = model_module.Model(
                    {"jwt": "x"}, {}, "/fake/mrp.json", local_mode=True
                )

    def test_skips_when_no_callback(self):
        self.model.io_session.model_run_parameters.callBack = False
        self.model.build_quarterly_summary_report()
        self.assertFalse(os.path.exists(os.path.join(self.report_dir, "quarterly_summary_report.json")))

    def test_skips_when_no_analysis_ids(self):
        self.model.io_session.model_run_parameters.settings["analysisIds"] = []
        self.model.build_quarterly_summary_report()
        self.assertFalse(os.path.exists(os.path.join(self.report_dir, "quarterly_summary_report.json")))

    def test_writes_report_when_callback_and_ids(self):
        # No input data: report still written with zeros/empty sections
        self.model.build_quarterly_summary_report()
        path = os.path.join(self.report_dir, "quarterly_summary_report.json")
        self.assertTrue(os.path.isfile(path), path)
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        self.assertEqual(data["currentAnalysisId"], "curr")
        self.assertEqual(data["priorAnalysisId"], "prior")
        self.assertEqual(len(data["sections"]), 6)
        self.assertEqual(data["sections"][0]["type"], "changesToAcl")


class TestBuildHanmiAclQuarterlyReport(unittest.TestCase):
    """Tests for build_hanmi_acl_quarterly_report with mocked IO."""

    def setUp(self):
        self.report_dir = tempfile.mkdtemp()
        mock_io = _make_mock_io_session(
            local_directories={
                "inputPaths": {
                    "instrumentResult": [],
                    "instrumentReporting": [],
                    "instrumentReference": [],
                    "macroEconomicVariableInput": [],
                },
                "outputPaths": {"report": self.report_dir},
            },
            call_back=True,
            analysis_ids=["c1", "p1"],
        )
        mock_io.local_directories["outputPaths"]["report"] = self.report_dir
        with patch.object(model_module, "Cappy", MagicMock()):
            with patch.object(model_module.iosession, "IOSession", MagicMock(return_value=mock_io)):
                self.model = model_module.Model(
                    {"jwt": "x"}, {}, "/fake/mrp.json", local_mode=True
                )

    def test_skips_when_no_callback(self):
        self.model.io_session.model_run_parameters.callBack = False
        self.model.build_hanmi_acl_quarterly_report()
        self.assertFalse(os.path.exists(os.path.join(self.report_dir, "hanmi_acl_quarterly_report.json")))

    def test_skips_when_no_analysis_ids(self):
        self.model.io_session.model_run_parameters.settings["analysisIds"] = []
        self.model.build_hanmi_acl_quarterly_report()
        self.assertFalse(os.path.exists(os.path.join(self.report_dir, "hanmi_acl_quarterly_report.json")))

    def test_writes_report_with_expected_structure(self):
        self.model.build_hanmi_acl_quarterly_report()
        path = os.path.join(self.report_dir, "hanmi_acl_quarterly_report.json")
        self.assertTrue(os.path.isfile(path), path)
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        self.assertIn("reportMetadata", data)
        self.assertIn("segmentMethodology", data)
        self.assertIn("collectivelyEvaluatedByMethodology", data)
        self.assertIn("quantitativeLossRatesBySegment", data)
        self.assertIn("netChargeOffsQuarterly", data)
        self.assertIn("qualitativeReservesBySegment", data)
        self.assertIn("macroeconomicBaseline", data)
        self.assertIn("individualAnalysis", data)
        self.assertIn("unfundedBySegment", data)
        self.assertIn("unfundedTrend", data)
        self.assertEqual(data["reportMetadata"]["currentAnalysisId"], "c1")
        self.assertEqual(data["reportMetadata"]["priorAnalysisId"], "p1")

    def test_collectively_by_methodology_with_mock_data(self):
        """With mock result + ref (collective, methodology), section is populated."""
        ref = pd.DataFrame({
            "instrumentIdentifier": ["i1", "i2"],
            "ascImpairmentEvaluation": ["Collectively Evaluated", "Collectively Evaluated"],
            "lossRateModelName": ["CRE Loss Rate Model", "CRE Loss Rate Model"],
        })
        res = pd.DataFrame({
            "instrumentIdentifier": ["i1", "i2"],
            "amortizedCost": [1000.0, 2000.0],
            "onBalanceSheetReserveAdjusted": [15.0, 25.0],
            "onBalanceSheetReserveUnadjusted": [10.0, 20.0],
        })
        def load_mock(category, analysis_id, filter_summary=False):
            if category == "instrumentResult" and analysis_id == "c1":
                return res.copy()
            if category == "instrumentReference" and analysis_id == "c1":
                return ref.copy()
            return None
        self.model._load_parquet_for_analysis = load_mock
        self.model.build_hanmi_acl_quarterly_report()
        path = os.path.join(self.report_dir, "hanmi_acl_quarterly_report.json")
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        coll = data["collectivelyEvaluatedByMethodology"]
        self.assertEqual(len(coll), 1)
        self.assertEqual(coll[0]["methodology"], "CRE Loss Rate Model")
        self.assertEqual(coll[0]["amortizedCost"], 3000.0)
        self.assertEqual(coll[0]["quantitativeReserve"], 30.0)
        self.assertEqual(coll[0]["qualitativeReserve"], 10.0)
        self.assertEqual(coll[0]["totalReserve"], 40.0)

    def test_macroeconomic_baseline_when_macro_data_loaded(self):
        """When macro parquet is loaded, macroeconomicBaseline is populated."""
        macro_df = pd.DataFrame({
            "macroeconomicVariableName": ["USA Unemployment Rate", "USA Real GDP Growth"],
            "valueDate": ["2025-12-31", "2025-12-31"],
            "macroeconomicVariableValue": [4.1, 1.5],
        })
        def load_mock(category, analysis_id, filter_summary=False):
            if category == "macroEconomicVariableInput" and analysis_id == "c1":
                return macro_df.copy()
            return None
        self.model._load_parquet_for_analysis = load_mock
        self.model.build_hanmi_acl_quarterly_report()
        path = os.path.join(self.report_dir, "hanmi_acl_quarterly_report.json")
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        macro = data["macroeconomicBaseline"]
        self.assertEqual(len(macro), 2)
        self.assertEqual(macro[0]["variableName"], "USA Unemployment Rate")
        self.assertEqual(macro[0]["value"], 4.1)


class TestCreateReportExportZip(unittest.TestCase):
    """Tests for create_report_export_zip."""

    def setUp(self):
        self.report_dir = tempfile.mkdtemp()
        mock_io = MagicMock()
        mock_io.local_directories = {"outputPaths": {"report": self.report_dir}}
        with patch.object(model_module, "Cappy", MagicMock()):
            with patch.object(model_module.iosession, "IOSession", MagicMock(return_value=mock_io)):
                self.model = model_module.Model(
                    {"jwt": "x"}, {}, "/fake/mrp.json", local_mode=True
                )

    def test_creates_zip_with_files_in_report_dir(self):
        with open(os.path.join(self.report_dir, "dummy.json"), "w") as f:
            f.write("{}")
        with open(os.path.join(self.report_dir, "hanmi_acl_quarterly_report.json"), "w") as f:
            f.write("{}")
        self.model.create_report_export_zip()
        zip_path = os.path.join(self.report_dir, "report_export.zip")
        self.assertTrue(os.path.isfile(zip_path))
        import zipfile
        with zipfile.ZipFile(zip_path, "r") as z:
            names = z.namelist()
        self.assertIn("hanmi_acl_quarterly_report.json", names)
        self.assertIn("dummy.json", names)
        self.assertNotIn("report_export.zip", names)


class TestRunEntrypoint(unittest.TestCase):
    """Tests for run.py entry and argument parsing (need mocks when importing run)."""

    def test_parse_args_requires_mode_and_credentials(self):
        with patch.dict("sys.modules", _mocks):
            from model.run import _parseInputArguments
            with patch("sys.argv", ["run.py"]):
                with self.assertRaises(SystemExit):
                    _parseInputArguments()

    def test_parse_args_local_and_jwt(self):
        with patch.dict("sys.modules", _mocks):
            from model.run import _parseInputArguments
            with patch("sys.argv", ["run.py", "-L", "/path", "-j", "token"]):
                args = _parseInputArguments()
        self.assertTrue(args.local)
        self.assertEqual(args.jwt, "token")
        self.assertFalse(args.s3)


if __name__ == "__main__":
    unittest.main()
