import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, LongType, IntegerType
import sys
import os

# Add the src directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))
from utils import find_common_neighbors, load_csv_to_df

MAX_N = 100

# Define unit test base class
class PySparkTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("Sample PySpark ETL").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

# Define unit test
class TestCommonNeighbors(PySparkTestCase):

    def load_expected_result(self, spark_session, path: str):
        schema = StructType([
            StructField("node1", IntegerType(), True),
            StructField("node2", IntegerType(), True),
            StructField("common", LongType(), False)
        ])
        df = self.spark.read.csv(path, schema=schema, header=True)
        return df

    def load_test_data(self, test_dir):
        """ Load test data from a specified directory containing input.csv and expected.csv """
        input_path = os.path.join(test_dir, 'input.csv')
        expected_directed_path = os.path.join(test_dir, 'expected_directed.csv')
        expected_undirected_path = os.path.join(test_dir, 'expected_undirected.csv')
        
        input_df = load_csv_to_df(self.spark, input_path)
        expected_directed_df = self.load_expected_result(self.spark, expected_directed_path)
        expected_undirected_df = self.load_expected_result(self.spark, expected_undirected_path)
        
        return input_df, expected_directed_df, expected_undirected_df
    
    def test_cases(self):
        test_dirs = [os.path.join("tests", "data", d) for d in os.listdir(os.path.join("tests", "data")) if os.path.isdir(os.path.join("tests", "data", d))]
        for test_dir in test_dirs:
            input_df, expected_directed_df, expected_undirected_df = self.load_test_data(test_dir)
            with self.subTest(test_dir=test_dir + "_directed"):
                # Directed
                actual_directed_df = find_common_neighbors(input_df, MAX_N, False)
                self.assertTrue(self.areDataFramesEqual(actual_directed_df, expected_directed_df), "Directed test failed")

            with self.subTest(test_dir=test_dir + "_undirected"):
                # Undirected, reusing input_df loaded above
                actual_undirected_df = find_common_neighbors(input_df, MAX_N, True)
                self.assertTrue(self.areDataFramesEqual(actual_undirected_df, expected_undirected_df), "Undirected test failed")

    
    def areDataFramesEqual(self, df1, df2, tol=0):
        """ Check if two dataframes are equal, allowing for a tolerance in numeric columns """
        if len(df1.schema.fields) != len(df2.schema.fields):
            print("Schemas do not match in number of fields:")
            return False

        for f1, f2 in zip(df1.schema.fields, df2.schema.fields):
            if f1.name != f2.name or f1.dataType != f2.dataType:
                print("Schemas do not match:")
                print(f"Field {f1.name} in df1 is of type {f1.dataType}, while in df2 it is of type {f2.dataType}")
                return False

        if df1.subtract(df2).count() != 0 or df2.subtract(df1).count() != 0:
            print("Data does not match:")
            print("Rows in df1 not in df2:", df1.subtract(df2).show())
            print("Rows in df2 not in df1:", df2.subtract(df1).show())
            return False

        return True



if __name__ == '__main__':
    unittest.main()