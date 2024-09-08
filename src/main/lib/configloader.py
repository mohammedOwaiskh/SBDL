import configparser
from pyspark import SparkConf


def get_config(env):
    config = configparser.ConfigParser()
    config.read("src/resources/conf/sbdl.conf")
    conf = {}
    for key, val in config.items(env):
        conf[key] = val
    return conf


def get_spark_conf(env):
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("src/resources/conf/spark.conf")
    print(config)

    # if config.get(env) is not None:
    # return "HI"
    # print("HI")

    # for key, val in config.items(env) :
    # spark_conf.set(key, val)
    return spark_conf


def get_data_filter(env, data_filter):
    conf = get_config(env)
    return "true" if conf[data_filter] == "" else conf[data_filter]
