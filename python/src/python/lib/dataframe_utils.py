import logging

from pyspark.sql import DataFrame


def collect_print_dataframe(df: DataFrame):
    logging.info("calling util function")
    for row in df.collect():
        print(row)
