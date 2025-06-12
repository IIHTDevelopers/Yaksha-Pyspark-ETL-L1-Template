import unittest
import ast
import inspect
import sys
import os

# Add parent dir to sys.path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from tests.TestUtils import TestUtils
import etl  # This should point to your ETL script

class TestETLNoCollectUsage(unittest.TestCase):
    def setUp(self):
        self.test_obj = TestUtils()

    def test_collect(self):
        try:
            # Get source and parse AST
            source = inspect.getsource(etl)
            tree = ast.parse(source)

            # Look for .collect() usage
            uses_collect = any(
                isinstance(node, ast.Attribute) and node.attr == "collect"
                for node in ast.walk(tree)
            )
            
            result = not uses_collect
            self.test_obj.yakshaAssert("TestNoCollectUsedInETL", result, "functional")
            print("TestNoCollectUsedInETL =", "Passed" if result else "Failed")

        except Exception as e:
            self.test_obj.yakshaAssert("TestNoCollectUsedInETL", False, "functional")
            print(f"TestNoCollectUsedInETL = Failed | Exception: {e}")
