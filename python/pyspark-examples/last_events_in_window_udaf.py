import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("EventStreamProcessing") \
    .getOrCreate()

topic_name = "TestTopic"

schema = StructType([
    StructField("name", StringType(), True),
    StructField("value1", IntegerType(), True),
    StructField("value2", LongType(), True),
])

bootstrap_url = "xxx"
truststore_location = "xxx"
truststore_password = "xxx"
keystore_location = "xxx"
keystore_password = "xxx"
key_password = "xxx"

source = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", bootstrap_url)
    .option("subscribe", topic_name)
    .option("startingOffsets", "latest")
    .option("kafka.security.protocol", "SSL")
    .option("includeHeaders", True)

    .option("kafka.ssl.truststore.location", truststore_location)
    .option("kafka.ssl.truststore.password", truststore_password)
    .option("kafka.ssl.keystore.location", keystore_location)
    .option("kafka.ssl.keystore.password", keystore_password)
    .option("kafka.ssl.key.password", key_password)
    # .option("startingTimestamp", int(time.time()) - 5 * 60)
    .load()
)

df = (
    source
    .select(from_json(col("value").cast("string"), schema).alias("value"), col("partition"), col("offset"),
            col("timestamp"))
    .selectExpr("timestamp", "partition", "offset", "value.*")
    .selectExpr("timestamp", "name", "value1")
)


# The following code is not working because of the following error:
# org.apache.spark.sql.AnalysisException: Streaming aggregation doesn't support group aggregate pandas UDF
#
# @pandas_udf(DoubleType(), PandasUDFType.GROUPED_AGG)
# def max_order_data_udaf(order_column: pd.Series, data_column: pd.Series) -> float:
#     # Create a DataFrame with the order and data columns
#     df = pd.DataFrame({'order': order_column, 'data': data_column})
#     # Find the row with the maximum order
#     max_order_row = df.loc[df['order'].idxmax()]
#     # Return the data value from the row with the maximum order
#     return max_order_row['data']


class MaxOrderDataUDAF:
    def __init__(self):
        self.max_order = None
        self.max_data = None

    def add(self, order, data):
        # Update the state with the data for the maximum order seen so far
        if (self.max_order is None) or (order > self.max_order):
            self.max_order, self.max_data = order, data

    def merge(self, other):
        # Merge logic for combining two UDAFs
        if (other.max_order is not None) and ((self.max_order is None) or (other.max_order > self.max_order)):
            self.max_order, self.max_data = other.max_order, other.max_data

    def finish(self):
        # Final result logic
        return self.max_data


# Define the schema of the UDAF's result
result_schema = StructType([
    StructField("max_data", DoubleType())
])


@udf(result_schema)
def max_order_data_udaf(orders, datas):
    aggregator = MaxOrderDataUDAF()
    for order, data in zip(orders, datas):
        aggregator.add(order, data)
    return Row(max_data=aggregator.finish())


last_events = (
    df.groupBy(
        df["name"],
        window("timestamp", "30 seconds", "10 seconds")
    )
    .agg(
        max_order_data_udaf(df["timestamp"], df["value1"]).alias("value")
        max_by()
    )
)

(
    last_events
    .writeStream
    .outputMode('update')
    .foreachBatch(lambda micro_batch, _: micro_batch.show())
    .start()
)
