import argparse
from pyspark.sql import SparkSession
from utils import load_csv_to_df, find_common_neighbors
import os

JAVA_HOME = "C:\Program Files\Java\jdk-22"

os.environ["JAVA_HOME"] = JAVA_HOME

def parse_args() -> None:
    parser = argparse.ArgumentParser(description='Graph Analysis to find common neighbors.')
    parser.add_argument('-n', type=int, default=3, help='Number of top node pairs to retrieve')
    parser.add_argument('--input', help='Input path to the CSV files containing the graph data')
    parser.add_argument('--output', choices=['show', 'save'], help='Output mode: "show" on console or "save" to CSV file')
    parser.add_argument('--output_path', help='Path to save the results if output mode is "save"')
    return parser.parse_args()

def main(session_name: str="Graph Analysis") -> None:
    args = parse_args()
    spark = SparkSession.builder \
        .appName(session_name) \
        .getOrCreate()

    df = load_csv_to_df(spark, args.input)
    top_pairs = find_common_neighbors(df, args.n)

    if args.output == 'show':
        top_pairs.show()
    elif args.output == 'save':
        top_pairs.write.csv(args.output_path, header=True, mode='overwrite')  # TODO: mode
    spark.stop()

if __name__ == "__main__":
    main()
    # TODO: Add prints for logs?