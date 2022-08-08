import pytest
from pyspark import SparkConf
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session():
    """
    We use a fixture to instantiate
    the spark session object which we will
    use for all our test cases.

    Scope is important to be set to "session"
    so that the object remains alive for the whole
    test execution session.

    """
    config = _get_local_spark_config()
    spark = (
        SparkSession.builder.config(conf=config)
        .appName("spark_unit_testing")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    yield spark
    spark.stop()


def _get_local_spark_config() -> SparkConf:
    CONFIG = [
        ("spark.default.parallelism", 1),
        ("spark.executor.cores", 1),
        ("spark.executor.instances", 1),
        ("spark.sql.shuffle.partitions", 1),
    ]

    return SparkConf().setAll(CONFIG)
