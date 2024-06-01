import argparse
from utils import load_csv_to_df, find_common_neighbors, handle_output, create_spark_session
import sys
import os
from datetime import datetime
import logging
from logging_config import setup_logging
from pyspark.sql import SparkSession

# Constants
DEFAULT_OUTPUT_DIR = 'output'
OUTPUT_FILE_TEMPLATE = 'output_{current_time}.csv'
FILENAME_DATE_FORMAT = "%Y%m%d_%H%M%S"
SAVE = "save"

sys.argv = ['ipykernel_launcher.py', '-n', '20', '--input', 'tests/data/test_empty_dir/input', '--output_mode', 'show']

def setup_default_output_path(args: argparse.Namespace, current_time: str) -> None:
    """
    Sets up the default path for saving output csv file if not specified.
    Ensures the output directory exists and if not, creates it. Then, sets the output
    path in the args namespace based on the current time stamp.

    Args:
        args (Namespace): The argument namespace from argparse containing the command line arguments.
        current_time (str): The current time formatted as a string used to generate the output file name.

        args (_type_): _description_
        current_time (_type_): _description_
    """
    if not os.path.exists(DEFAULT_OUTPUT_DIR):
        os.makedirs(DEFAULT_OUTPUT_DIR)
    output_filename = OUTPUT_FILE_TEMPLATE.format(current_time=current_time)
    args.output_path = os.path.join(DEFAULT_OUTPUT_DIR, output_filename)

def parse_args(current_time: str) -> argparse.Namespace:
    """
    Parses command line arguments needed. Sets up default paths for output if necessary.

    Args:
        current_time (str): Current time formatted as a string, used to define default output paths.

    Returns:
        argparse.Namespace: Namespace containing all the arguments with values.
    """
    parser = argparse.ArgumentParser(description='Graph Analysis to find common neighbors.')
    parser.add_argument('-n', type=int, required=True, help='Number of top node pairs to retrieve')  # Require the number of node pairs
    parser.add_argument('--input', required=True, help='Input path to the CSV files containing the graph data')  # TODO: change to dir
    parser.add_argument('--output_mode', choices=['show', 'save'], required=True, help='Output mode: "show" on console or "save" to CSV file')  # Require output mode
    parser.add_argument('--output_path', help='Path to save the results if output mode is "save"')
    parser.add_argument('--undirected', action='store_true', help='Treat the graph as undirected')
    args = parser.parse_args()

    if args.output_mode == SAVE and not args.output_path:
        setup_default_output_path(args, current_time)
    return args

def run_analysis(spark: SparkSession, n: int, input_path: str, output_mode: str, output_path: str, is_undirected: bool) -> None:
    """
    Runs the graph analysis to find common neighbors and handles output based on specified mode.

    Args:
        spark (SparkSession): A Spark session object.
        n (int): Number of top pairs to retrieve.
        input_path (str): The path to the CSV files directory containing the graph data.
        output_mode (str): Mode of output, either 'show' or 'save'.
        output_path (str): Path where the output should be saved, for save mode.
        is_undirected (bool): Flag indicating whether the graph should be treated as undirected.
    """
    df = load_csv_to_df(spark, input_path)
    top_pairs = find_common_neighbors(df, n, is_undirected)  #TODO: Duplicated edge
    handle_output(top_pairs, output_mode, output_path)

def main():
    """
    Main function for running the graph analysis application.
    Sets up logging, parses arguments, creates a Spark session, and runs the analysis.
    Handles any exceptions that occur during the analysis process and ensures proper shutdown of resources.
    """
    # Define `current_time` at the start of main to ensure consistent timestamps across log and output files generated during this run.
    current_time = datetime.now().strftime(FILENAME_DATE_FORMAT)
    setup_logging(current_time)
    logging.info("Starting the application")

    try:
        args = parse_args(current_time)
        logging.info(f"Running analysis with settings: {args}")
        spark = create_spark_session()
        run_analysis(spark, args.n, args.input, args.output_mode, args.output_path, args.undirected)
        logging.info("Analysis completed")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise
    finally:
        logging.info("Shutting down the application")
        spark.stop()

if __name__ == "__main__":
    main()