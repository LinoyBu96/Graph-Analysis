import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, LongType, IntegerType
import sys
import os
import logging

# Add the src directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))
from utils import find_common_neighbors, load_csv_to_df

MAX_N = 100

# Define unit test base class
class PySparkTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        logging.basicConfig(level=logging.CRITICAL)  # Output only critical messages in testing environment
        cls.spark = SparkSession.builder.appName("Sample PySpark ETL").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

# Define unit test
class TestCommonNeighbors(PySparkTestCase):
    def setUp(self):
        self.base_test_dir = os.path.join("tests", "data")
        # self.test_dirs = [os.path.join(base_test_dir, d) for d in os.listdir(base_test_dir) if os.path.isdir(os.path.join(base_test_dir, d))]
        
    def load_expected_result(self, spark_session, path: str):
        schema = StructType([
            StructField("node1", IntegerType(), True),
            StructField("node2", IntegerType(), True),
            StructField("common", LongType(), False)
        ])
        df = self.spark.read.csv(path, schema=schema, header=True)
        return df

    def load_test_data(self, test_dir):
        """ Load test data from a specified directory containing input and expected.csv """
        input_path = os.path.join(test_dir, 'input')
        expected_directed_path = os.path.join(test_dir, 'expected_directed.csv')
        expected_undirected_path = os.path.join(test_dir, 'expected_undirected.csv')
        
        input_df = load_csv_to_df(self.spark, input_path)
        expected_directed_df = self.load_expected_result(self.spark, expected_directed_path)
        expected_undirected_df = self.load_expected_result(self.spark, expected_undirected_path)
        
        return input_df, expected_directed_df, expected_undirected_df
    
    def run_sub_tests(self, test_dir):
        test_name = os.path.basename(test_dir)
        input_df, expected_directed_df, expected_undirected_df = self.load_test_data(test_dir)

        with self.subTest(test_name + "_directed"):
            actual_directed_df = find_common_neighbors(input_df, MAX_N, False)
            self.assertTrue(self.areDataFramesEqual(actual_directed_df, expected_directed_df), f"{test_name}_directed test failed")

        with self.subTest(test_name + "_undirected"):
            actual_undirected_df = find_common_neighbors(input_df, MAX_N, True)
            self.assertTrue(self.areDataFramesEqual(actual_undirected_df, expected_undirected_df), f"{test_name}_undirected test failed")

    def test_default(self):
        test_dir = os.path.join(self.base_test_dir, "test_default")
        self.run_sub_tests(test_dir)

    def test_default_null(self):
        test_dir = os.path.join(self.base_test_dir, "test_default_null")
        self.run_sub_tests(test_dir)

    def test_no_common_neighbors(self):
        test_dir = os.path.join(self.base_test_dir, "test_no_common_neighbors")
        self.run_sub_tests(test_dir)

    def test_empty_dir(self):
        test_dir = os.path.join(self.base_test_dir, "test_empty_dir\input")
        with self.assertRaises(FileNotFoundError) as context:
            load_csv_to_df(self.spark, test_dir)
        
        # Check if the error message is correct
        self.assertIn("No CSV files found in the directory", str(context.exception))

    def test_missing_dir(self):
        test_dir = os.path.join(self.base_test_dir, "test_missing_dir\input")
        with self.assertRaises(FileNotFoundError) as context:
            load_csv_to_df(self.spark, test_dir)
        
        # Check if the error message is correct
        self.assertIn(f"The specified directory does not exist: {test_dir}.", str(context.exception))
    
    def areDataFramesEqual(self, df1, df2, tol=0):
        """ Check if two dataframes are equal, allowing for a tolerance in numeric columns """
        if len(df1.schema.fields) != len(df2.schema.fields):
            print("Schemas do not match in number of fields.")
            return False

        for f1, f2 in zip(df1.schema.fields, df2.schema.fields):
            if f1.name != f2.name or f1.dataType != f2.dataType:
                print("Schemas do not match:")
                print(f"Field {f1.name} in actual results is of type {f1.dataType}, while in expected results it is of type {f2.dataType}")
                return False
        
        # Check data contents, focusing on differences that impact the comparison
        df1_not_in_df2 = df1.subtract(df2)
        df2_not_in_df1 = df2.subtract(df1)
        
        if df1_not_in_df2.count() != 0 or df2_not_in_df1.count() != 0:
            print("Data does not match:")
            print("Rows in actual results not in expected results:")
            df1_not_in_df2.show(truncate=False)
            print("Rows in expected results not in actual results:")
            df2_not_in_df1.show(truncate=False)
            return False

        return True



if __name__ == '__main__':
    unittest.main()