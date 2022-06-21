# pyspark_unit_testing

Draft repository to demonstrate how we can perform unit testing in a Pyspark application.

Please check the following file in the tests/ module:

- `conftest.py` shows how we can instantiate a spark session object which is going to be used by every single unit test.
- `test_calculate_prices.py` demonstrates two things: a) what are the different ways to create a sample Spark Dataframe for unit testing.
- b) What are the different options/tools you can use to compare two Spark Dataframes (ie. expected_output vs actual_output)