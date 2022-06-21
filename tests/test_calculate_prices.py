import pytest
from ny_taxi_prices.calculate_prices import get_price_and_distance_per_vendor, calculate_price_group_per_vendor
from chispa.dataframe_comparer import assert_approx_df_equality, assert_df_equality
from pandas.testing import assert_frame_equal
from pyspark_test import assert_pyspark_df_equal
import os


@pytest.fixture(scope="session")
def nyc_taxi_input_data(spark_session):
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
        schema = "vendor_id: string, total_amount: double, trip_distance: double"
        #['vendor_id', 'total_amount', 'trip_distance']
    ))


# testing with collect()
# testing with subtract()
# testing with pandas dataframe
# testing with chispa
def test_get_price_and_distance_per_vendor(spark_session, nyc_taxi_input_data):
    expected_sdf = (spark_session.createDataFrame(
        [
            ('Vendor_A', 12.676, 6.333),
            ('Vendor_B', 15.0, 5.0),
            ('Vendor_C', 30.0, 7.5),
            #(None,None,None)
        ],
        schema = "vendor_id: string, average_amount: double, average_distance:double "
        #['vendor_id', 'average_amount', 'average_distance']
    ))

    actual_sdf = get_price_and_distance_per_vendor(nyc_taxi_input_data)
    #chispa
    assert_approx_df_equality(actual_sdf,expected_sdf,precision=0.1, ignore_nullable=True)
    #assert_df_equality(actual_sdf,expected_sdf)

    # collect
    #assert actual_sdf.schema == expected_sdf.schema
    #assert sorted(actual_sdf.collect()) == sorted(expected_sdf.collect())

    # subtract
    #assert actual_sdf.subtract(expected_sdf).count() == 0

    #pandas
    #assert_frame_equal(actual_sdf.toPandas(),expected_sdf.toPandas(),check_less_precise=1)

    #pyspark-test: No support for approx equality
    #assert_pyspark_df_equal(actual_sdf,expected_sdf)
