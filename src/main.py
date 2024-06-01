import argparse
from pyspark.sql import SparkSession
from utils import load_csv_to_df, find_common_neighbors, handle_output
import sys
import os


def parse_args() -> None:
    parser = argparse.ArgumentParser(description='Graph Analysis to find common neighbors.')
    parser.add_argument('-n', type=int, required=True, help='Number of top node pairs to retrieve')  # Require the number of node pairs
    parser.add_argument('--input', required=True, help='Input path to the CSV files containing the graph data')  # Require input path
    parser.add_argument('--output', choices=['show', 'save'], required=True, help='Output mode: "show" on console or "save" to CSV file')  # Require output mode
    parser.add_argument('--output_path', help='Path to save the results if output mode is "save"')
    args = parser.parse_args()

    if args.output == 'save' and not args.output_path:
        parser.error("--output 'save' requires --output_path to be specified.")

    return args

def validate_output_path(path):
    if not os.path.exists(os.path.dirname(path)):
        raise ValueError(f"Output directory does not exist: {os.path.dirname(path)}")

def run_analysis(n, input_path, output_mode, output_path=None):
    # TODO: Try with hadoop
    spark = SparkSession.builder \
        .appName("Graph Analysis") \
        .getOrCreate()

    df = load_csv_to_df(spark, input_path)
    top_pairs = find_common_neighbors(df, n)  #TODO: Duplicated edge
    handle_output(top_pairs, output_mode, output_path)

def main() -> None:
    args = parse_args()
    if args.output == 'save' and args.output_path:
        validate_output_path(args.output_path)
    run_analysis(args.n, args.input, args.output, args.output_path)

if __name__ == "__main__":
    print("start")
    main()
    # TODO: Add prints for logs?