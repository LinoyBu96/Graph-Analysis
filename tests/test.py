import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, LongType, IntegerType
import sys
import os
import logging
from pyspark.sql.functions import DataFrame
from typing import Tuple

# Add the src directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))
from utils import find_common_neighbors, load_csv_to_df

MAX_N = 100

# Define unit test base class
class PySparkTestCase(unittest.TestCase):
    """
    Base test case class for PySpark tests, setting up and tearing down Spark sessions.

    Attributes:
        spark (SparkSession): A Spark session for tests to use.
    """
    
    @classmethod
    def setUpClass(cls):
        """
        Set up the class by initializing a Spark session with logging.
        """
        logging.basicConfig(level=logging.CRITICAL)  # Output only critical messages in testing environment
        cls.spark = SparkSession.builder.appName("Sample PySpark ETL").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        """
        Tear down the class by stopping the Spark session.
        """
        cls.spark.stop()

# Define unit test
class TestCommonNeighbors(PySparkTestCase):
    """
    Tests for verifying the functionality of finding common neighbors in a graph dataset.
    """
    def setUp(self):
        """
        Set the base directory for test data.
        """
        self.base_test_dir = os.path.join("tests", "data")

    def load_expected_result(self, path: str) -> DataFrame:
        """
        Load the expected results from a CSV file into a DataFrame with a predefined schema.

        Args:
            path (str): The file path to the CSV containing expected results.

        Returns:
            DataFrame: A DataFrame containing the loaded data.
        """
        schema = StructType([
            StructField("node1", IntegerType(), True),
            StructField("node2", IntegerType(), True),
            StructField("common", LongType(), False)
        ])
        df = self.spark.read.csv(path, schema=schema, header=True)
        return df

    def load_test_data(self, test_dir:str) -> Tuple[DataFrame, DataFrame, DataFrame]:
        """
        Load test data from a specified directory containing input () and expected results
        for directed and undirected graphs (csv files).

        Args:
            test_dir (str): The directory path where the test data is stored.

        Returns:
            tuple[DataFrame, DataFrame, DataFrame]: A tuple containing three DataFrames:
                - The first DataFrame contains the input data loaded from the 'input' directory.
                - The second DataFrame contains the expected results for directed graph analysis.
                - The third DataFrame contains the expected results for undirected graph analysis.
        """
        
        input_path = os.path.join(test_dir, 'input')
        expected_directed_path = os.path.join(test_dir, 'expected_directed.csv')
        expected_undirected_path = os.path.join(test_dir, 'expected_undirected.csv')
        
        input_df = load_csv_to_df(self.spark, input_path)
        expected_directed_df = self.load_expected_result(expected_directed_path)
        expected_undirected_df = self.load_expected_result(expected_undirected_path)
        
        return input_df, expected_directed_df, expected_undirected_df
    
    def run_sub_tests(self, test_dir):
        test_name = os.path.basename(test_dir)
        input_df, expected_directed_df, expected_undirected_df = self.load_test_data(test_dir)
        tests = [
            ("_directed", False, expected_directed_df),
            ("_undirected", True, expected_undirected_df)
        ]

        # Loop through each test configuration
        for suffix, is_undirected, expected_df in tests:
            full_test_name = test_name + suffix
            with self.subTest(full_test_name):
                # Run the find_common_neighbors function with the current configuration
                actual_df = find_common_neighbors(input_df, MAX_N, is_undirected)
                # Assert that the actual DataFrame matches the expected DataFrame
                self.assertTrue(self.areDataFramesEqual(actual_df, expected_df, full_test_name), f"{full_test_name} test failed")

    def test_default(self):
        test_dir = os.path.join(self.base_test_dir, "test_default")
        self.run_sub_tests(test_dir)

    def test_default_null(self):
        test_dir = os.path.join(self.base_test_dir, "test_default_null")
        self.run_sub_tests(test_dir)

    def test_no_common_neighbors(self):
        test_dir = os.path.join(self.base_test_dir, "test_no_common_neighbors")
        self.run_sub_tests(test_dir)
        
    def test_no_common_neighbors(self):
        test_dir = os.path.join(self.base_test_dir, "test_larger_data")
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
    
    def areDataFramesEqual(self, df1, df2, test_name):
        """
        Check if two DataFrames are equal, allowing for a tolerance in numeric columns.

        Args:
            df1 (DataFrame): First DataFrame to compare.
            df2 (DataFrame): Second DataFrame to compare.
            test_name (str): Name of the test for reporting results.
        """
        if not self._schemas_match(df1, df2, test_name):
            return False

        if not self._data_match(df1, df2, test_name):
            return False

        self._print_test_result(test_name, "PASSED")
        return True
    
    def _schemas_match(self, df1, df2, test_name):
        """Check if the schemas of two DataFrames match."""
        if len(df1.schema.fields) != len(df2.schema.fields):
            self._print_failure(test_name, "Schemas do not match in number of fields.")
            return False

        for f1, f2 in zip(df1.schema.fields, df2.schema.fields):
            if f1.name != f2.name or f1.dataType != f2.dataType:
                self._print_failure(test_name, "Schemas do not match.")
                print(f"Field '{f1.name}' in actual results is of type {f1.dataType},")
                print(f"while in expected results it is of type {f2.dataType}.")
                return False
        return True
    
    def _data_match(self, df1, df2, test_name):
        """Check if the data of two DataFrames match."""
        df1_not_in_df2 = df1.subtract(df2)
        df2_not_in_df1 = df2.subtract(df1)
        
        if df1_not_in_df2.count() != 0 or df2_not_in_df1.count() != 0:
            self._print_failure(test_name, "Data does not match.")
            print("Rows in actual results not in expected results:")
            df1_not_in_df2.show(truncate=False)
            print("Rows in expected results not in actual results:")
            df2_not_in_df1.show(truncate=False)
            return False
        return True
            
    def _print_failure(self, test_name, reason):
        """Print the failure message for a test."""
        print(f"========== {test_name} ==========")
        print(f"Status: FAILED")
        print(f"Reason: {reason}")
        print(f"=================================")

    def _print_test_result(self, test_name, result):
        """Print the result of a test."""
        print(f"========== {test_name} ==========")
        print(f"Status: {result}")
        print(f"=================================")
        print("\n")
    

if __name__ == '__main__':
    unittest.main()