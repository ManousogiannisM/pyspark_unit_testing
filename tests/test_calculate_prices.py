import pytest
from ny_taxi_prices.calculate_prices import get_price_and_distance_per_vendor, calculate_price_group_per_vendor
from chispa.dataframe_comparer import assert_approx_df_equality, assert_df_equality
from pandas.testing import assert_frame_equal
from pyspark_test import assert_pyspark_df_equal
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Different ways of defining a sample Dataframe

@pytest.fixture(scope="module") 
def nyc_taxi_input_data_anti_pattern(spark_session):
    """
    This is a fixture created to demonstrate 
    how you can create a spark dataframe for testing purposes.
    It is NOT the recommended way, since the column types are 
    note defined. Please avoid this.

    PS - You do not need a fixture to create a test dataframe,
    unless you need to use it in multiple test cases.
    
    """
    return (spark_session.createDataFrame(
        [
            ('Vendor_A', 10.0, 5.0),
            ('Vendor_B', 15.0, 5.0),
            ('Vendor_C', 20.0, 5.0),
            ('Vendor_A', 8.0, 4.0),
            ('Vendor_A', 20.0, 10.0),
            ('Vendor_C', 40.0, 10.0),
            ("Vendor_C",float("NaN"),None)
        ],
        ['vendor_id', 'total_amount', 'trip_distance']
    ))



@pytest.fixture(scope="module")
def nyc_taxi_input_data_schema_with_struct_type(spark_session):
    """
    This is how we can create a test dataframe 
    with predefined schema types. ie. vendor_id 
    is a non-nullable string, total_amount is a
    nullable double etc.
    
    """

    schema = (StructType([ 
        StructField("vendor_id",StringType(),False), 
        StructField("total_amount",DoubleType(),True), 
        StructField("trip_distance",DoubleType(),True), 
    ]))

    return (spark_session.createDataFrame(
        [
            ('Vendor_A', 10.0, 5.0),
            ('Vendor_B', 15.0, 5.0),
            ('Vendor_C', 20.0, 5.0),
            ('Vendor_A', 8.0, 4.0),
            ('Vendor_A', 20.0, 10.0),
            ('Vendor_C', 40.0, 10.0),
            ("Vendor_C",float("NaN"),None)
        ],
        schema=schema
    ))


@pytest.fixture(scope="session")
def nyc_taxi_input_data_schema_as_string(spark_session):
    """
    Define the dataframe schema as a string, without using StructType, 
    and StructField. This makes it easier to create and requires less
    code, and is easier to read.

    """
    
    return (spark_session.createDataFrame(
        [
            ('Vendor_A', 10.0, 5.0),
            ('Vendor_B', 15.0, 5.0),
            ('Vendor_C', 20.0, 5.0),
            ('Vendor_A', 8.0, 4.0),
            ('Vendor_A', 20.0, 10.0),
            ('Vendor_C', 40.0, 10.0),
            #("Vendor_C",float("NaN"),None)
        ],
        schema = "vendor_id: string, total_amount: double, trip_distance: double"
    ))



def test_get_price_and_distance_per_vendor(spark_session, nyc_taxi_input_data_schema_as_string):
    """
    This is a test case example that demonstrates different ways of comparing 
    two Spark Dataframes. The expected Dataframe and the actual Dataframe that
    is returned by your tested function. The following methods are used:

    - collect():will fail if the 'double' type columns are not exactly the same. 
                (i.e 16.66666 != 16.665)

    - subtract(): same as above

    - pyspark-test: does not support approximate equality either.

    - pandas: We convert the (small) spark dfs to pandas dfs.
              Pandas testing supports approximate matching.
              Disanvantages: - can not compare schemas 100% 
                            (nullable, no nullable etc).
                             - NaN and None values are handled
                              differently in Spark. In Pandas
                              they are the same.
    
    - chispa: - approximate spark df comparison is supported
              - schema comparison is supported 100%. 
                (including nullable, non-nullable)
              - very nice error messages pop up when
                a test fails. 

    """

    expected_sdf = (spark_session.createDataFrame(
        [
            ('Vendor_A', 12.676, 6.333),
            ('Vendor_B', 15.0, 5.0),
            ('Vendor_C', 30.0, 7.5),
            #(None,None,None)
        ],
        schema = "vendor_id: string, average_amount: double, average_distance: double "
        #['vendor_id', 'average_amount', 'average_distance']
    ))

    actual_sdf = get_price_and_distance_per_vendor(nyc_taxi_input_data_schema_as_string)

    # collect: 
    #assert actual_sdf.schema == expected_sdf.schema
    #assert sorted(actual_sdf.collect()) == sorted(expected_sdf.collect())

    # subtract: 
    #assert actual_sdf.subtract(expected_sdf).count() == 0

    # pyspark-test: 
    #assert_pyspark_df_equal(actual_sdf,expected_sdf)

    # pandas: 
    #assert_frame_equal(actual_sdf.toPandas(),expected_sdf.toPandas(),check_less_precise=1)

    # chispa: 
    assert_approx_df_equality(actual_sdf,expected_sdf,precision=0.1)
    #assert_df_equality(actual_sdf,expected_sdf)


