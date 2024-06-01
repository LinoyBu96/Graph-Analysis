from pyspark.sql.functions import col, DataFrame
from pyspark.sql import SparkSession
import logging

def create_spark_session():
    """
    Create and return a Spark session.
    
    Returns:
        SparkSession: Configured Spark session.
    """
    # TODO: Try with hadoop
    return SparkSession.builder \
        .appName("Graph Analysis") \
        .getOrCreate()

def load_csv_to_df(spark_session, path: str) -> DataFrame:
    logging.info(f"Loading CSV from path: {path}")
    # TODO: define specific schema?
    df = spark_session.read.csv(path, header=True, inferSchema=True)
    logging.info(f"Loaded DataFrame with {df.count()} records from {path}")
    return df

def add_reverse_edges(df):
    """Add reverse edges to make the graph undirected."""
    logging.info("Adding reverse edges to DataFrame to ensure undirected graph representation.")
    reverse_df = df.select(
        col("dst").alias("src"),
        col("src").alias("dst")
    )
    undirected_df = df.union(reverse_df).distinct()
    logging.info(f"DataFrame now has {undirected_df.count()} total edges after adding reverse edges.")
    return undirected_df

def find_common_neighbors(df, n, is_undirected):
    """Find the top n pairs of nodes with the highest number of common neighbors in an undirected graph."""
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
    logging.info(f"Handling output, mode: {output_mode}")
    if output_mode == 'show':
        logging.info("Displaying DataFrame")
        df.show()
    elif output_mode == 'save':
        if output_path is not None:
            logging.info(f"Saving DataFrame to {output_path}")
            df.write.csv(output_path, header=True, mode='overwrite')
        else:
            logging.error("Output path must be provided for save mode.")
            raise ValueError("Output path must be provided for save mode.")