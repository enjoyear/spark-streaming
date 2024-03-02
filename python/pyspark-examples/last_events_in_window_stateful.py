from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
from typing import Tuple, Iterator
import pandas as pd

spark = SparkSession.builder \
    .appName("EventStreamProcessing") \
    .getOrCreate()

topic_name = "TestTopic"

schema = StructType([
    StructField("name", StringType(), True),
    StructField("value", IntegerType(), True)
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
    .selectExpr("timestamp", "name", "value")
)

mapSchema = StructType([
    StructField("name", StringType(), True),
    StructField("value1", IntegerType(), True),
    StructField("value2", LongType(), True),
])

temp_table_name = "last_events_in_window"
spark.createDataFrame(spark.sparkContext.emptyRDD(), mapSchema).createOrReplaceTempView(temp_table_name)
lcsWindow = Window.partitionBy("name", window("timestamp", "30 seconds")).orderBy(desc("timestamp"))


def func(
        key: Tuple[str], pdfs: Iterator[pd.DataFrame], state: GroupState
) -> Iterator[pd.DataFrame]:
    if state.hasTimedOut:
        (word,) = key
        (count,) = state.get
        state.remove()
        yield pd.DataFrame({"session": [word], "count": [count]})
    else:
        # Aggregate the number of words.
        count = sum(map(lambda pdf: len(pdf), pdfs))
        if state.exists:
            (old_count,) = state.get
            count += old_count
        state.update((count,))
        # Set the timeout as 10 seconds.
        state.setTimeoutDuration(10000)
        yield pd.DataFrame()


output_schema = "name STRING, count LONG"
state_schema = "value1 INTEGER, value2 LONG"

last_events = (
    df.groupBy(
        df["name"],
        window("timestamp", "30 seconds", "10 seconds")
    )
    .agg(
        last()
    )
    .applyInPandasWithState(
        func,
        output_schema,
        state_schema,
        "append",
        GroupStateTimeout.ProcessingTimeTimeout,
    )
)

query = last_events.writeStream.foreachBatch(lambda micro_batch, _: micro_batch.show()).start()
