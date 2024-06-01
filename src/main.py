import argparse
from utils import load_csv_to_df, find_common_neighbors, handle_output, create_spark_session
import sys
import os
from datetime import datetime
import logging
from logging_config import setup_logging

# Constants
DEFAULT_OUTPUT_DIR = 'output'
OUTPUT_FILE_TEMPLATE = 'output_{current_time}.csv'
FILENAME_DATE_FORMAT = "%Y%m%d_%H%M%S"
SAVE = "save"

sys.argv = ['ipykernel_launcher.py', '-n', '20', '--input', 'input/data.csv', '--output', 'save', '--undirected']

def setup_default_output_path(args, current_time):
    if not os.path.exists(DEFAULT_OUTPUT_DIR):
        os.makedirs(DEFAULT_OUTPUT_DIR)
    output_filename = OUTPUT_FILE_TEMPLATE.format(current_time=current_time)
    args.output_path = os.path.join(DEFAULT_OUTPUT_DIR, output_filename)

def parse_args(current_time) -> None:
    parser = argparse.ArgumentParser(description='Graph Analysis to find common neighbors.')
    parser.add_argument('-n', type=int, required=True, help='Number of top node pairs to retrieve')  # Require the number of node pairs
    parser.add_argument('--input', required=True, help='Input path to the CSV files containing the graph data')  # Require input path
    parser.add_argument('--output', choices=['show', 'save'], required=True, help='Output mode: "show" on console or "save" to CSV file')  # Require output mode
    parser.add_argument('--output_path', help='Path to save the results if output mode is "save"')
    parser.add_argument('--undirected', action='store_true', help='Treat the graph as undirected')
    args = parser.parse_args()

    if args.output == SAVE and not args.output_path:
        setup_default_output_path(args, current_time)
    return args

def run_analysis(spark, n, input_path, output_mode, output_path, is_undirected):
    df = load_csv_to_df(spark, input_path)
    top_pairs = find_common_neighbors(df, n, is_undirected)  #TODO: Duplicated edge
    handle_output(top_pairs, output_mode, output_path)

def main() -> None:
    # Define `current_time` at the start of main to ensure consistent timestamps across log and output files generated during this run.
    current_time = datetime.now().strftime(FILENAME_DATE_FORMAT)
    setup_logging(current_time)
    logging.info("Starting the application")

    try:
        args = parse_args(current_time)
        logging.info(f"Running analysis with settings: {args}")
        spark = create_spark_session()
        run_analysis(spark, args.n, args.input, args.output, args.output_path, args.undirected)
        logging.info("Analysis completed")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise
    finally:
        logging.info("Shutting down the application")
        spark.stop()

if __name__ == "__main__":
    main()