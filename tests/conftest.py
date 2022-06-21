import pytest
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
    spark = SparkSession.builder.master("local[*]").appName("unit_test").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR") 
    # We can also set number of partitions to 1, and
    # the number of executors to 1 for cosnuming less
    # resources.
    yield spark
    spark.stop()
