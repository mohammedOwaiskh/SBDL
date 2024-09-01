from pyspark.sql import SparkSession

from lib.configloader import get_spark_conf


def get_spark_session(env: str) -> SparkSession:

    config = get_spark_conf(env)

    if env == "LOCAL":
        config.set(
            "spark.driver.extraJavaOptions",
            "-Dlog4j.configuration=file:src/resources/log4j.properties",
        )
        return (
            SparkSession.builder.config(conf=config)
            .master("local[2]")
            .enableHiveSupport()
            .getOrCreate()
        )
    else:
        return (
            SparkSession.builder.config(conf=config).enableHiveSupport().getOrCreate()
        )
