import unittest
import shutil
import os
from pyspark.sql import SparkSession
from py4j.protocol import Py4JJavaError
import sys

# Add parent directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from tests.TestUtils import TestUtils
import etl_job  # This is the buggy ETL with .collect()

class TestETLBuggyVersion(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.test_obj = TestUtils()
        cls.spark = SparkSession.builder \
            .appName("ETLBuggyTest") \
            .master("local[2]") \
            .getOrCreate()

        os.makedirs("input", exist_ok=True)
        cls.input_path = "input/test_data.csv"
        cls.output_path = "output"

        # Create input CSV simulating large structure (not large file size)
        with open(cls.input_path, "w") as f:
            f.write("id,amount\n")
            for i in range(10000):
                f.write(f"{i},{100 + (i % 2000)}\n")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        shutil.rmtree("input", ignore_errors=True)
        shutil.rmtree("output", ignore_errors=True)

    def test_etl_buggy_collect_causes_risk(self):
        try:
            # Run the ETL job (which uses collect)
            etl_job.main()

            # Check if output exists and has data
            output_exists = os.path.exists(self.output_path)
            output_has_data = False

            if output_exists:
                result_df = self.spark.read.parquet(self.output_path)
                output_has_data = result_df.count() > 0

            result = output_exists and output_has_data
            self.test_obj.yakshaAssert("TestETLBuggyCollectUsed", result, "functional")
            print("TestETLBuggyCollectUsed =", "Passed" if result else "Failed")

        except Py4JJavaError as e:
            # Catch driver memory crash / Java heap issues
            self.test_obj.yakshaAssert("TestETLBuggyCollectUsed", False, "functional")
            print(f"TestETLBuggyCollectUsed = Failed | Java error: {str(e)}")

        except Exception as e:
            self.test_obj.yakshaAssert("TestETLBuggyCollectUsed", False, "functional")
            print(f"TestETLBuggyCollectUsed = Failed | Exception: {str(e)}")
