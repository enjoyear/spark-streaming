package com.chen.guo

import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, StreamingQuery, Trigger}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

case class MyState(timeBucket: String, var uniqueUsers: Set[String])

object StreamCountDistinct {
  private val columnUserId = "user_id"
  private val columnEventTimestamp = "_etl_cc"

  val schema: StructType = StructType(Array(
    StructField("name", StringType, true),
    StructField(columnUserId, StringType, true),
  ))

  private val columnStartTime: String = "start_time"
  private val aggWindowSize: Int = 10 // 10 seconds

  private val spark = SparkSession
    .builder
    .master("local[4]")
    .appName("StreamCountDistinct")
    .getOrCreate()

  private val kafkaSource: Dataset[Row] = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "abc.xyz.c8.kafka.us-east-1.amazonaws.com:9094")
    .option("subscribe", "topic")
    .option("startingOffsets", "latest")
    .option("kafka.security.protocol", "SSL")
    .option("kafka.ssl.truststore.location", "/path/to/truststore.jks")
    .option("kafka.ssl.truststore.password", "password")
    .option("kafka.ssl.keystore.location", "/path/to/keystore.jks")
    .option("kafka.ssl.keystore.password", "password")
    .option("kafka.ssl.key.password", "password")
    .load()
    .select(
      from_json(col("value").cast("string"), schema).alias("value"),
      col("partition"),
      col("offset"),
      col("timestamp")
    )
    .selectExpr("value.*", s"timestamp as ${columnEventTimestamp}")
    .selectExpr(
      columnEventTimestamp,
      // calculate the start time of the window
      s"from_unixtime(floor(unix_timestamp($columnEventTimestamp)/$aggWindowSize) * $aggWindowSize) as $columnStartTime",
      columnUserId
    )

  import spark.implicits._
  // implicit val myStateEncoder: Encoder[MyState] = Encoders.product[MyState]

  val kvpDf: KeyValueGroupedDataset[String, Row] = kafkaSource.groupByKey(_.getAs[String](columnStartTime))
  val query: StreamingQuery = kvpDf
    .mapGroupsWithState(
      timeoutConf = GroupStateTimeout.NoTimeout
    )(arbitraryStatefulProcessing)
    .writeStream
    .trigger(Trigger.ProcessingTime("3 seconds"))
    .queryName("test")
    .outputMode("update")
    .option("checkpointLocation", f"/path/to/checkpoints")
    .option("truncate", false)
    .format("console")
    .start()

  query.awaitTermination()

  private def arbitraryStatefulProcessing(timeBucket: String, inputs: Iterator[Row], oldState: GroupState[MyState]): MyState = {
    val uu = inputs.map(r => r.getAs[String](columnUserId)).toSet

    if (oldState.hasTimedOut) {
      println("State has timed out")
    }

    val state: MyState = {
      if (oldState.exists) {
        MyState(timeBucket, oldState.get.uniqueUsers ++ uu)
      } else {
        MyState(timeBucket, uu)
      }
    }
    oldState.update(state)

    state
  }
}
