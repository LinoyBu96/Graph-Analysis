from pyspark.sql.functions import col, DataFrame
import yaml

def load_config(file_path):
    with open(file_path, 'r') as file:
        config = yaml.safe_load(file)
    return config


def load_csv_to_df(spark_session, path: str) -> DataFrame:
    # TODO: define specific schema?
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
    
def handle_output(df, output_mode, output_path=None):
    if output_mode == 'show':
        df.show()
    elif output_mode == 'save':
        if output_path is not None:
            df.write.csv(output_path, header=True, mode='overwrite')
        else:
            raise ValueError("Output path must be provided for save mode.")