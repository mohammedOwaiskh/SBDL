import configparser
from pyspark import SparkConf


def get_config(env: str) -> dict:
    """Retrieve configuration settings for a specified environment.

    This function reads configuration values from a specified configuration file and returns them as a dictionary.
    It allows users to access environment-specific settings easily.

    Args:
        env (str): The name of the environment for which to retrieve configuration settings.

    Returns:
        dict: A dictionary containing the configuration settings for the specified environment.

    Raises:
        configparser.NoSectionError: If the specified environment does not exist in the configuration file.

    Examples:
        >>> get_config("LOCAL")
        {'key1': 'value1', 'key2': 'value2'}
    """
    config: configparser.ConfigParser = configparser.ConfigParser()
    config.read("src/resources/conf/sbdl.conf")
    conf: dict = dict(config.items(env))
    return conf


def get_spark_conf(env: str) -> SparkConf:
    """
      Load and return Spark configuration settings for a specified environment.

      This function reads configuration values from a designated environment section in a
      configuration file and applies them to a SparkConf object, enabling the setup of a
      Spark session with the appropriate settings.

    Args:
          env (str): The name of the environment section to read from the configuration file.

      Returns:
          SparkConf: A SparkConf object populated with the configuration settings for the specified environment.

      Raises:
          FileNotFoundError: If the configuration file cannot be found.
          KeyError: If the specified environment section does not exist in the configuration file.

      Examples:
          spark_conf = get_spark_conf("QA")
    """
    spark_conf: SparkConf = SparkConf()
    config: configparser.ConfigParser = configparser.ConfigParser()
    config.read("src/resources/conf/spark.conf")

    for key, val in config.items(env):
        spark_conf.set(key, val)
    return spark_conf


def get_data_filter(env: str, data_filter: str) -> str:
    """Retrieve a specific data filter value for a given environment.

    This function fetches the configuration settings for the specified environment and returns the value associated
    with the provided data filter key. If the value is an empty string, it returns "true" as a string.

    Args:
        env (str): The name of the environment for which to retrieve the data filter.
        data_filter (str): The key for the specific data filter to retrieve.

    Returns:
        str: The value of the specified data filter, or "true" if the value is an empty string.

    Raises:
        KeyError: If the data_filter key does not exist in the configuration for the specified environment.

    Examples:
        >>> get_data_filter("LOCAL", "filter_key")
        "value"  # or "true" if the value is an empty string
    """

    conf = get_config(env)
    return "true" if conf[data_filter] == "" else conf[data_filter]
