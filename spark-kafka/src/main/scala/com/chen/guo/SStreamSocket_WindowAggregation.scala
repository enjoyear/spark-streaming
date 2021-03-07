package com.chen.guo

import java.sql.Timestamp

import org.apache.spark.sql.functions.window
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}

/**
  * Ref:
  * https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#window-operations-on-event-time
  *
  * How to run:
  * Run a Netcat as a data server by using
  * nc -lk 9999
  *
  * Then enter rows like below. Adjust based on [[Locals.slidingWindow]].
  * You can even have event time in the future
  * 2021-03-07 07:57:02, a
  * 2021-03-07 07:51:32, a b
  */
object SStreamSocket_Window extends App {

  val spark = SparkSession
    .builder
    .master("local[2]")
    .appName("StructuredNetworkWordCount")
    .getOrCreate()

  // https://kontext.tech/column/spark/457/tutorial-turn-off-info-logs-in-spark
  // info log is too much
  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  val lines: DataFrame = spark.readStream
    //Socket source should be used only for testing as this does not provide end-to-end fault-tolerance guarantees.
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()

  // Split the lines into words
  val words: Dataset[TimestampedWord] = lines.as[String].flatMap(TimestampedWords(_))

  // Group the data by window and word and compute the count of each group
  val windowedCounts = words

    /**
      * In doc:
      * the watermark set as (max event time - daleyThreshold) at the beginning of every trigger
      *
      * Understanding this statement: "the guarantee is strict only in one direction. Data delayed by more than 2 hours
      * is not guaranteed to be dropped; it may or may not get aggregated. More delayed is the data, less likely is the
      * engine going to process it." at https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#semantic-guarantees-of-aggregation-with-watermarking
      *
      * In reality:
      * Actual watermark could be calculated as {the oldest window for max event} - daleyThreshold instead of the
      * {max event time} - daleyThreshold
      *
      * Experiment
      * if window size is 1 min, sliding size is 20 seconds
      * First event: 2021-03-07 07:57:02, a    => Three windows created:
      * {2021-03-07 07:56:20, 2021-03-07 07:57:20}
      * {2021-03-07 07:56:40, 2021-03-07 07:57:40}
      * {2021-03-07 07:57:00, 2021-03-07 07:58:00}
      *
      * Set the mode to UPDATE
      * Watermark should be calculated as {2021-03-07 07:57:02} - 5min = {2021-03-07 07:52:02}
      * Actual watermark seems to be {the oldest window for max event} - 5 min instead of the {max event time} - 5min,
      * which is 07:56:20 - 5min = 07:51:20
      *
      * 2021-03-07 07:51:19, b   <- Will be discarded
      * 2021-03-07 07:51:20, b   <- Will be accepted
      * 2021-03-07 07:57:22, a   <- The oldest window becomes {2021-03-07 07:56:40, 2021-03-07 07:57:40} - 5min,
      * which is 07:56:40 - 5min = 07:51:40
      *
      * 2021-03-07 07:51:20, b   <- Will be discarded this time as this window has been closed
      * 2021-03-07 07:51:39, b   <- Will be discarded this time as this window has been closed
      * 2021-03-07 07:51:40, b   <- Will be accepted as in the oldest acceptable window
      *
      *
      * if the mode is set to COMPLETE: it requires all aggregate data to be preserved, hence cannot use watermarking
      * to drop intermediate state.
      * if the mode is set to APPEND: only closed windows are output
      *
      */
    .withWatermark("eventTime", "5 minutes")
    .groupBy(Locals.slidingWindow, $"word")
    .count()


  // Start running the query that prints the running counts to the console
  val query = windowedCounts.writeStream
    //.outputMode(OutputMode.Complete()) //Note that: windows won't be dropped in Complete mode
    .outputMode(OutputMode.Update())
    //.outputMode(OutputMode.Append()) // In the append mode, only the windows that are closed will be output
    .option("numRows", 100) //show more than 20 rows by default
    .option("truncate", value = false) //To show the full column content
    .format("console")
    .start()

  query.awaitTermination()
}

object Locals {
  /**
    * Ref:
    * https://jaceklaskowski.gitbooks.io/spark-structured-streaming/content/spark-sql-streaming-window.html
    *
    * For example: if window size is 2 minute, which slides every 30 seconds
    * For a event time 2021-03-07 07:57:02, the following 4 windows will be created
    * {2021-03-07 07:55:30, 2021-03-07 07:57:30}
    * {2021-03-07 07:56:00, 2021-03-07 07:58:00}
    * {2021-03-07 07:56:30, 2021-03-07 07:58:30}
    * {2021-03-07 07:57:00, 2021-03-07 07:59:00}
    */
  private val windowSize = "1 minutes"
  // Note that if the sliding size is too small, there will be a lot of check at each triggering
  // so the generated code will be very long. There will be an issue like below
  // https://stackoverflow.com/questions/50891509/apache-spark-codegen-stage-grows-beyond-64-kb
  private val slidingSize = "20 seconds"
  val slidingWindow: Column = window(new Column("eventTime"), windowSize, slidingSize)
}

/**
  * Ref:
  * https://databricks.com/blog/2020/07/22/a-comprehensive-look-at-dates-and-timestamps-in-apache-spark-3-0.html
  */
case class TimestampedWord(eventTime: Timestamp, word: String)

object TimestampedWords {
  def apply(row: String): List[TimestampedWord] = {
    val parts = row.split(",")
    val strings: Array[String] = parts(1).trim().split(" ")
    strings.map(x => TimestampedWord(Timestamp.valueOf(parts(0).trim()), x)).toList
  }
}