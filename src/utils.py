import os
from pyspark.sql.functions import col, DataFrame

def load_csv_to_df(spark_session, path: str) -> DataFrame:
    # Read CSV files into DataFrame
    return spark_session.read.csv(path, header=True, inferSchema=True)

def find_common_neighbors(df, n):
    pairs_df = df.alias('g1') \
        .join(df.alias('g2'),
                (col('g1.dst') == col('g2.dst')) & (col('g1.src') < col('g2.src'))) \
        .select(
            col('g1.src').alias('node1'),
            col('g2.src').alias('node2')
    )
    common_neighbors_df = pairs_df.groupBy('node1', 'node2').count() \
        .withColumnRenamed('count', 'common')
    return common_neighbors_df.orderBy(col('common').desc()).limit(n)
    