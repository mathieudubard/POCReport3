"""
Batch entrypoint smoke tests: the CLI path must import ``model`` as the **package**, not ``model/model.py``,
when the process cwd / sys.path[0] is the ``model/`` directory (Domino / ``python run.py`` from ``model/``).

Unit tests elsewhere only parse argparse; they do not execute ``run.py`` as the main script.
"""

import os
import subprocess
import sys
import unittest


_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class TestRunBatchEntrypoint(unittest.TestCase):
    def test_run_py_help_exits_zero_from_project_root(self):
        run_script = os.path.join(_project_root, "model", "run.py")
        r = subprocess.run(
            [sys.executable, run_script, "--help"],
            cwd=_project_root,
            capture_output=True,
            text=True,
        )
        self.assertEqual(r.returncode, 0, msg=r.stderr or r.stdout)

    def test_run_py_help_exits_zero_when_cwd_is_model_subdir(self):
        """
        Reproduces hosts where ``sys.path[0]`` is the ``model/`` folder so a bare ``import model``
        would load ``model.py`` unless ``run.py`` prepends the project root first.
        """
        model_dir = os.path.join(_project_root, "model")
        run_script = os.path.join(model_dir, "run.py")
        r = subprocess.run(
            [sys.executable, run_script, "--help"],
            cwd=model_dir,
            capture_output=True,
            text=True,
        )
        self.assertEqual(r.returncode, 0, msg=r.stderr or r.stdout)


if __name__ == "__main__":
    unittest.main()
