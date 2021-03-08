package com.chen.guo

import java.sql.Timestamp

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode


/**
  * Ref:
  * https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#stream-stream-joins
  *
  * How to run:
  * Run a Netcat as a data server for impressions table: nc -lk 9998
  * Then enter rows like below
  * 2021-03-07 07:07:02, 12, printer
  * 2021-03-07 06:27:02, 12, drink
  *
  * Run a Netcat as a data server for clicks table: nc -lk 9999
  * Then enter rows like below
  * 2021-03-07 07:32:02, 12, Jim
  * 2021-03-07 07:32:02, 18, Jim
  * 2021-03-07 06:32:02, 12, Jason
  * 2021-03-07 06:32:02, 12, John
  * 2021-03-07 08:02:02, 12, Jane
  *
  */
object SStreamSocket_StreamStreamJoin extends App {

  val spark = SparkSession
    .builder
    .master("local[4]")
    .appName("StructuredNetworkJoin")
    .getOrCreate()

  // https://kontext.tech/column/spark/457/tutorial-turn-off-info-logs-in-spark
  // info log is too much
  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  val impressions: DataFrame = spark.readStream
    //Socket source should be used only for testing as this does not provide end-to-end fault-tolerance guarantees.
    .format("socket")
    .option("host", "localhost")
    .option("port", 9998)
    .load()


  val clicks: DataFrame = spark.readStream
    //Socket source should be used only for testing as this does not provide end-to-end fault-tolerance guarantees.
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()

  //Apply watermarks on event-time columns
  val impressionsWithWatermark: Dataset[Impression] = impressions.as[String]
    .map(Impression(_))
    .withWatermark("impressionTime", "2 hours")

  val clicksWithWatermark: Dataset[Click] = clicks.as[String]
    .map(Click(_)).as[Click]
    .withWatermark("clickTime", "3 hours")

  // Join with event-time constraints
  val joined = impressionsWithWatermark.join(
    clicksWithWatermark,
    expr(
      """
      clickAdId = impressionAdId AND
      clickTime >= impressionTime AND
      clickTime <= impressionTime + interval 1 hour
      """)
  )

  // Start running the query that prints the running counts to the console
  val query = joined.writeStream
    .outputMode(OutputMode.Append()) // Join between two streaming DataFrames/Datasets is only supported in Append output mode
    .option("numRows", 100) //show more than 20 rows by default
    .option("truncate", value = false) //To show the full column content
    .format("console")
    .start()

  query.awaitTermination()
}

case class Impression(impressionTime: Timestamp, impressionAdId: Int, name: String)

object Impression {
  def apply(row: String): Impression = {
    val parts = row.split(",")
    Impression(Timestamp.valueOf(parts(0).trim()), Integer.valueOf(parts(1).trim()), parts(2).trim())
  }
}

case class Click(clickTime: Timestamp, clickAdId: Int, name: String)

object Click {
  def apply(row: String): Click = {
    val parts = row.split(",")
    Click(Timestamp.valueOf(parts(0).trim()), Integer.valueOf(parts(1).trim()), parts(2).trim())
  }
}