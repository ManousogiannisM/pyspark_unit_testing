from email import header
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql import DataFrame as SparkDataFrame



def extract_data(spark, config):
    """Load raw data from specified path given in config."""
    return (
        spark.read.csv(config.get("input_data_path"),header=True)
    )


def get_price_and_distance_per_vendor(sdf:SparkDataFrame)-> SparkDataFrame:
    """Groups NYC taxi data per vendor id"""


    return (
            sdf.groupBy("vendor_id")
            .agg(F.avg("total_amount").alias("average_amount"), 
                F.avg("trip_distance").alias("average_distance"))

    )


def calculate_price_group_per_vendor(sdf: SparkDataFrame)->SparkDataFrame:
    """Calculates the price per km for each vendor"""

    return (
        sdf.withColumn("price_per_km",F.col("average_amount")/F.col("average_distance"))
            .withColumn("price_group",F.when(F.col("price_per_km")<F.lit(4),"cheap").otherwise("expensive"))

    )


def load_data(config, df):
    """Saves the output data to the desired location as csv."""
    df.write.mode("overwrite").csv(config.get("output_data_path"), header=True)


def run_ny_taxi_prices_job(spark, config):
    """Run spark job to calculate heatwaves in NL."""
    load_data(
        config,
        calculate_price_group_per_vendor(get_price_and_distance_per_vendor(extract_data(spark, config))),
    )