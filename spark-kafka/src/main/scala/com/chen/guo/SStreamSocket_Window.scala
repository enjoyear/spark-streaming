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
  * Then enter rows like below. Adjust based on [[Locals.slidingWindow]]
  * 2021-03-06 22:57:02, a b c
  * 2021-03-06 22:58:02, a b
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

  val schema = new StructType()
    .add("eventTime", TimestampType)
    .add("words", StringType)

  val lines: DataFrame = spark.readStream
    //Socket source should be used only for testing as this does not provide end-to-end fault-tolerance guarantees.
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()

  // Split the lines into words
  val words: Dataset[TimestampedWord] = lines.as[String].flatMap(TimestampedWords(_))

  // Group the data by window and word and compute the count of each group
  val windowedCounts = words.groupBy(Locals.slidingWindow, $"word").count()


  // Start running the query that prints the running counts to the console
  val query = windowedCounts.writeStream
    .outputMode(OutputMode.Complete())
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
    * Count words within 2 minute windows, sliding every 30 seconds
    * e.g. 12:00 - :12:30, :12:30 - :13:00, :13:00 - :13:30, etc
    */
  private val windowSize = "2 minutes"
  // Note that if the sliding size is too small, there will be a lot of check at each triggering
  // so the generated code will be very long. There will be an issue like below
  // https://stackoverflow.com/questions/50891509/apache-spark-codegen-stage-grows-beyond-64-kb
  private val slidingSize = "30 seconds"
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