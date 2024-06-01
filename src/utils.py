from pyspark.sql.functions import col, DataFrame
from pyspark.sql import SparkSession
import logging
import os
from glob import glob


def create_spark_session() -> SparkSession:
    """
    Create and return a Spark session.
    
    Returns:
        SparkSession: Configured Spark session.
    """
    # TODO: Try with hadoop
    return SparkSession.builder \
        .appName("Graph Analysis") \
        .getOrCreate()


def check_for_csv_files(spark_session: SparkSession, path: str):
    """
    Check if the given directory exists and contains any CSV files.
    This function uses the Java VM from SparkSession to access the Hadoop FileSystem to check the 
    existence of the directory and to list its contents to check for CSV files.
    
    Args:
        spark_session (SparkSession): The Spark session object.
        path (str): The path to the directory to check.

    Raises:
        FileNotFoundError: In case the directory does not exist or contains no CSV files.
    """
    # Access Java VM and Hadoop Configuration
    jvm = spark_session._jvm
    jsc = spark_session._jsc
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(jsc.hadoopConfiguration())
    hadoop_path = jvm.org.apache.hadoop.fs.Path(path)

    # Check if the directory exists
    if not fs.exists(hadoop_path):
        logging.error(f"The specified directory does not exist: {path}.")
        raise FileNotFoundError(error_message)

    # Check if there are any CSV files in the directory
    status = fs.listStatus(hadoop_path)  # Get list of entries in the directory
    csv_files = [file.getPath().getName() for file in status if file.getPath().getName().endswith('.csv')]

    if not csv_files:
        error_message = f"No CSV files found in the directory: {path}."
        logging.error(error_message)
        raise FileNotFoundError(error_message)


def load_csv_to_df(spark_session: SparkSession, path: str) -> DataFrame:
    """
    Load CSV data from a given directory into a DataFrame.
    First checks if the directory exits and contains any CSV file.

    Args:
        spark_session (SparkSession): The Spark session used for this app.
        path (str): Path to the directory containing the CSV files.

    Returns:
        DataFrame: A Dataframe containing the data loaded from the CSV files.
    """
    logging.info(f"Attempting to load CSV data from directory: {path}.")

    # TODO: define specific schema?

    # Check if the directory exists
    # if not os.path.exists(path):
    #     error_message = f"The specified directory does not exist: {path}"
    #     logging.error(error_message)
    #     raise FileNotFoundError(error_message)
    
    # # Check if the directory contains any CSV files
    # csv_files = glob(os.path.join(path, '*.csv'))
    # if not csv_files:
    #     error_message = f"No CSV files found in the directory: {path}"
    #     logging.error(error_message)
    #     raise FileNotFoundError(error_message)
    
    check_for_csv_files(spark_session, path)

    df = spark_session.read.csv(path, header=True, inferSchema=True)
    logging.info(f"Loaded DataFrame with {df.count()} records from {path}.")
    return df


def add_reverse_edges(df: DataFrame):
    """
    Add reverse edges to a undirected graph represented by a DataFrame to make an directed.

    Args:
        df (DataFrame): A DataFrame representing edges of a graph.

    Returns:
        DataFrame: A new DataFrame with reverse edges added, representing an undirected graph as directed graph.
    """
    # Add reverse edges to make the graph undirected.
    logging.info("Adding reverse edges to DataFrame to ensure undirected graph representation.")
    reverse_df = df.select(
        col("dst").alias("src"),
        col("src").alias("dst")
    )
    undirected_df = df.union(reverse_df).distinct()
    logging.info(f"DataFrame now has {undirected_df.count()} total edges after adding reverse edges.")
    return undirected_df


def find_common_neighbors(df: DataFrame, n: int, is_undirected: bool):
    """ 
    Find the top n pairs of nodes with the highest number of common neighbors in a graph.
    First converts an undirected graph to directed graph if needed.
    

    Args:
        df (DataFrame): The Dataframe representing the graph.
        n (int): The number of top pairs to retrieve.
        is_undirected (bool): Flag indicating if the graph is undirected.

    Returns:
        DataFrame: A DataFrame containing the top 'n' pairs of nodes with their count of common neighbors.
    """
    logging.info("Calculating common neighbors.")
    if is_undirected:
        df = add_reverse_edges(df)

    # Finding common neighbors
    pairs_df = df.alias('g1') \
        .join(df.alias('g2'), 
              (col('g1.dst') == col('g2.dst')) & (col('g1.src') < col('g2.src'))) \
        .select(
            col('g1.src').alias('node1'),
            col('g2.src').alias('node2')
        )

    common_neighbors_df = pairs_df.groupBy('node1', 'node2').count() \
        .withColumnRenamed('count', 'common') \
        .orderBy(col('common').desc()) \
        .limit(n)
    
    logging.info(f"Found top {n} pairs of nodes with the most common neighbors.")
    return common_neighbors_df
    

def handle_output(df, output_mode, output_path=None):
    """
    Handle the output of DataFrame based on the specified mode.
    Supports displaying the DataFrame to the console or saving it to a CSV file.
    If saving, the output path must be provided, otherwise raises a ValueError.

    Args:
        df (DataFrame): The DataFrame to output.
        output_mode (str): The mode of output; either 'show' or 'save'.
        output_path (str, optional): The path where the Dataframe should be saved if the mode is 'save'.
    """
    logging.info(f"Handling output, mode: {output_mode}.")
    if output_mode == 'show':
        logging.info("Displaying DataFrame.")
        df.show()
    elif output_mode == 'save':
        if output_path is not None:
            logging.info(f"Saving DataFrame to {output_path}.")
            df.write.csv(output_path, header=True, mode='overwrite')
        else:
            logging.error("Output path must be provided for save mode.")
            raise ValueError("Output path must be provided for save mode.")