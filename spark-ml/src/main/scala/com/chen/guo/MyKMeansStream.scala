package com.chen.guo


import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.Timestamp

object MyKMeansStream extends App {
  val spark = SparkSession
    .builder
    .master("local[2]")
    .appName("MyKMeans")
    .getOrCreate()
  val sc = spark.sparkContext

  val cdcSchema: StructType = StructType(Array(
    StructField("oc", StringType),
    StructField("ns", StringType),
    StructField("pk", StringType),
    StructField("va", StringType)
  ))

  val keySchema = StructType(Array(
    StructField("_id", StringType)
  ))

  val payloadSchema = StructType(Array(
    StructField("created_at", TimestampType),
    StructField("type", StringType),
  ))

  val topicName = "local-test"

  val df = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", topicName)
    .load()

  val query: StreamingQuery = df
    .select(from_json(col("value").cast("string"), cdcSchema).as("payload"))
    .select(col("payload.oc").as("oc"),
      from_json(col("payload.pk"), keySchema).as("pk"),
      from_json(col("payload.va"), payloadSchema).as("va"))
    .selectExpr("oc", "pk.*", "va.*")
    .where("oc != 'D'")

    .writeStream
    .queryName("kafka-ingest")
    .outputMode(OutputMode.Update())
    .option("numRows", 5) //show more than 20 rows by default
    .option("truncate", value = false) //To show the full column content
    .trigger(Trigger.ProcessingTime("3 seconds"))
    .foreachBatch((microBatchDF: DataFrame, batchId: Long) => processBatch(microBatchDF, batchId))
    .start()

  //    .option("checkpointLocation", "dbfs:/tmp/chen-guo/checkpoints/test2")  // It always starts from /dbfs for some reason
  //    .format("delta").table("chen_guo.test2")


  query.awaitTermination()

  def processBatch(microBatchDF: DataFrame, batchId: Long): Unit = {
    if (microBatchDF.isEmpty) {
      return
    }

    val minMaxBounds = microBatchDF.agg(
      min("created_at").as("min_created_at"),
      max("created_at").as("max_created_at")
    ).collect()

    val minTs = minMaxBounds(0).getAs[Timestamp]("min_created_at")
    val maxTs = minMaxBounds(0).getAs[Timestamp]("max_created_at")

    println(s"Min max bounds for current micro-batch is [$minTs,$maxTs] or [${minTs.getTime},${maxTs.getTime}] in epoch long milliseconds")
    microBatchDF.select(microBatchDF("created_at").cast(DoubleType)).show()

    val featureSchema: StructType = StructType(Array(
      StructField("features", ArrayType(DoubleType, false), nullable = false)
    ))
    val timeDF = spark.createDataFrame(microBatchDF.select(
      array(microBatchDF("created_at").cast(DoubleType).multiply(1000).minus(lit(minTs.getTime)))
    ).rdd, featureSchema)

    timeDF.cache()
    timeDF.show

    val kmeans = new KMeans().setK(3).setSeed(1L)
    val model: KMeansModel = kmeans.fit(timeDF)

    val predictions: DataFrame = model.transform(timeDF)
    val ranges = predictions.select(predictions("features")(0).as("ts"), predictions("prediction"))
      .groupBy("prediction")
      .agg(
        min("ts").as("rangeMin").plus(lit(minTs.getTime)).divide(1000.0).cast(TimestampType).as("clusterLeftBound"),
        max("ts").as("rangeMax").plus(lit(minTs.getTime)).divide(1000.0).cast(TimestampType).as("clusterRightBound")
      )

    ranges.orderBy("clusterLeftBound").show(truncate = false)
    timeDF.unpersist()
  }
}