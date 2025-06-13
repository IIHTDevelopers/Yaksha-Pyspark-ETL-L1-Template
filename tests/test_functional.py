import unittest
import sys
import os
import ast
import inspect

# Adjust path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from tests.TestUtils import TestUtils
import etl  # Your ETL module (must contain main function)

class TestETLImplementation(unittest.TestCase):
    def setUp(self):
        self.test_obj = TestUtils()

    def get_ast_tree(self, func):
        """Helper: Get AST of a function"""
        source = inspect.getsource(func)
        return ast.parse(source)

    def test_no_collect_used(self):
        try:
            tree = self.get_ast_tree(etl.main)
            collect_calls = [node for node in ast.walk(tree)
                             if isinstance(node, ast.Attribute) and node.attr == "collect"]
            result = len(collect_calls) == 0

            self.test_obj.yakshaAssert("TestNoCollectUsed", result, "functional")
            print("TestNoCollectUsed =", "Passed" if result else "Failed")
        except Exception as e:
            self.test_obj.yakshaAssert("TestNoCollectUsed", False, "functional")
            print(f"TestNoCollectUsed = Failed | Exception: {e}")

    def test_uses_col_function(self):
        try:
            tree = self.get_ast_tree(etl.main)
            col_used = any(
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "col"
                for node in ast.walk(tree)
            )

            self.test_obj.yakshaAssert("TestUsesColFunction", col_used, "functional")
            print("TestUsesColFunction =", "Passed" if col_used else "Failed")
        except Exception as e:
            self.test_obj.yakshaAssert("TestUsesColFunction", False, "functional")
            print(f"TestUsesColFunction = Failed | Exception: {e}")

    def test_uses_filter_method(self):
        try:
            tree = self.get_ast_tree(etl.main)
            filter_used = any(
                isinstance(node, ast.Attribute) and node.attr == "filter"
                for node in ast.walk(tree)
            )

            self.test_obj.yakshaAssert("TestUsesFilterMethod", filter_used, "functional")
            print("TestUsesFilterMethod =", "Passed" if filter_used else "Failed")
        except Exception as e:
            self.test_obj.yakshaAssert("TestUsesFilterMethod", False, "functional")
            print(f"TestUsesFilterMethod = Failed | Exception: {e}")
