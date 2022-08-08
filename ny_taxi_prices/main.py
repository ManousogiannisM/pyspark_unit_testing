import os
import sys
import json
import importlib
import argparse
from pyspark.sql import SparkSession
from ny_taxi_prices.calculate_prices import run_ny_taxi_prices_job


def _parse_arguments():
    """Parse arguments provided by spark-submit."""
    parser = argparse.ArgumentParser()
    return parser.parse_args()


def main():
    """Main function excecuted by spark-submit command."""
    args = _parse_arguments()
    config_dir = (
        os.path.dirname(os.path.realpath(__file__+"/.."))
        + os.sep
        + "config"
        + os.sep
        + f"config_nyc_taxi_prices.json"
    )

    with open(config_dir, "r") as config_file:
        config = json.load(config_file)

    spark = SparkSession.builder.appName(config.get("app_name")).getOrCreate()

# Input data sample:
#| vendor_id | trip_distance |   total_amount  |
#|:----------|--------------:|----------------:|
#| CSH       |            20 |             100 |
#| CRD       |            30 |             200 |
#| CSH       |            25 |             150 |
    run_ny_taxi_prices_job(spark,config=config)


if __name__ == "__main__":
    main()