from pyspark.sql import DataFrame
from pyspark.sql import SparkSession


def read_csv_data(spark: SparkSession, file_path: str) -> DataFrame:
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    # df.show(5)
    df.printSchema()
    return df
