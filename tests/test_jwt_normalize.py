"""Tests for model.jwt_normalize."""

import os
import sys
import unittest

_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from model.jwt_normalize import normalize_bearer_jwt, validate_compact_jwt_three_segments


class TestJwtNormalize(unittest.TestCase):
    def test_normalize_bearer(self):
        tok = "a.b.c"
        self.assertEqual(normalize_bearer_jwt("  Bearer a.b.c  "), "a.b.c")
        self.assertEqual(normalize_bearer_jwt("bearer a.b.c"), "a.b.c")
        self.assertEqual(normalize_bearer_jwt('"a.b.c"'), "a.b.c")
        self.assertEqual(normalize_bearer_jwt("'a.b.c'"), "a.b.c")

    def test_validate_three_segments_ok(self):
        validate_compact_jwt_three_segments("eyJhbGciOiJIUzI1NiJ9.e30.sig")

    def test_validate_wrong_segment_count(self):
        with self.assertRaises(ValueError) as ctx:
            validate_compact_jwt_three_segments("only-one")
        self.assertIn("three", str(ctx.exception).lower())
        with self.assertRaises(ValueError):
            validate_compact_jwt_three_segments("a.b")


if __name__ == "__main__":
    unittest.main()
