from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import time

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
    .selectExpr("timestamp", "partition", "offset", "name", "value")
)

mapSchema = StructType([
    StructField("name", StringType(), True),
    StructField("value", IntegerType(), True),
])

temp_table_name = "last_events_in_window"
checkpoint_table_name = "users.chen_guo.last_events_in_window"
spark.createDataFrame([], mapSchema).createOrReplaceTempView(temp_table_name)

# lcsWindow = Window.partitionBy("name", window("timestamp", "30 seconds")).orderBy(desc("timestamp"))
lcsWindow = Window.partitionBy("name").orderBy(desc("timestamp"))


def keep_in_memory_map(micro_batch: DataFrame, batch_id: int) -> None:
    print("Showing incoming micro_batch for batch_id: ", batch_id)
    micro_batch.show()

    if batch_id % 10 == 0:
        print("Persisting last_events_in_window for batch_id: ", batch_id)
        micro_batch.sparkSession.table(temp_table_name).write.mode("overwrite").saveAsTable(checkpoint_table_name)
        previous_map = micro_batch.sparkSession.table(checkpoint_table_name).alias("previous_map")
    else:
        previous_map = micro_batch.sparkSession.table(temp_table_name).alias("previous_map")

    # Pick the latest change for a name
    updates = (
        micro_batch
        .withColumn("rn", row_number().over(lcsWindow))
        .where("rn = 1")
        .select("name", "value")
        .alias("updates")
    )

    last_events_by_name = (
        previous_map
        .join(updates, col('previous_map.name') == col('updates.name'), "full_outer")
        .select(
            coalesce(col('updates.name'), col('previous_map.name')).alias("name"),
            coalesce(col('updates.value'), col('previous_map.value')).alias("value"),
        )
        .cache()
    )

    last_events_by_name.count()
    print("Latest map at batch_id: ", batch_id)
    last_events_by_name.show()
    micro_batch.sparkSession.table(temp_table_name).unpersist()
    last_events_by_name.createOrReplaceTempView(temp_table_name)


query = (
    df
    .writeStream
    .outputMode('update')
    .queryName("in-memory-map")
    .trigger(processingTime="10 seconds")
    .foreachBatch(lambda micro_batch, batch_id: keep_in_memory_map(micro_batch, batch_id))
    .start()
)

query.awaitTermination()
