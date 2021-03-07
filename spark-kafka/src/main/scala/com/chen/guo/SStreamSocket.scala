package com.chen.guo

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Ref:
  * https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
  * https://dzone.com/articles/spark-structured-streaming-using-java
  * https://dzone.com/articles/spark-streaming-vs-structured-streaming#:~:text=Spark%20Streaming%20works%20on%20something%20we%20call%20a%20micro%20batch.&text=In%20Structured%20Streaming%2C%20there%20is,into%20the%20unbounded%20result%20table.
  *
  * 1. use the Dataset/DataFrame API executed on the same optimized Spark SQL engine
  * 2. ensures end-to-end exactly-once fault-tolerance guarantees through checkpointing and Write-Ahead Logs
  * 3. queries are processed using a micro-batch processing engine achieving end-to-end latencies as low as 100 milliseconds
  *    - since Spark 2.3, we have introduced a new low-latency processing mode called Continuous Processing,
  * which can achieve end-to-end latencies as low as 1 millisecond with at-least-once guarantees
  * 4. can handle event-time and late data
  *
  * How to run:
  * Run a Netcat as a data server by using
  * nc -lk 9999
  */
object SStreamSocket extends App {

  val spark = SparkSession
    .builder
    .master("local[2]")
    .appName("StructuredNetworkWordCount")
    .getOrCreate()

  // https://kontext.tech/column/spark/457/tutorial-turn-off-info-logs-in-spark
  // info log is too much
  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  // Create DataFrame representing the stream of input lines from connection to localhost:9999
  // Since Spark 2.0, DataFrames and Datasets can represent static, bounded data, as well as streaming, unbounded data.
  // Streaming DataFrames can be created through the DataStreamReader interface
  val lines: DataFrame = spark.readStream
    //Socket source should be used only for testing as this does not provide end-to-end fault-tolerance guarantees.
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()

  // Split the lines into words
  val words: Dataset[String] = lines.as[String].flatMap(_.split(" "))

  // Generate running word count
  val wordCounts: DataFrame = words.groupBy("value").count()


  // Start running the query that prints the running counts to the console
  val query = wordCounts.writeStream

    /**
      * Complete Mode - The entire updated Result Table will be written to the external storage.
      * It is up to the storage connector to decide how to handle writing of the entire table.
      *
      * Append Mode - Only the new rows appended in the Result Table since the last trigger will be written to the external storage.
      * This is applicable only on the queries where existing rows in the Result Table are not expected to change.
      *
      * Update Mode - Only the rows that were updated in the Result Table since the last trigger will be written to the external storage
      * (available since Spark 2.1.1). Note that this is different from the Complete Mode in that this mode only outputs
      * the rows that have changed since the last trigger. If the query doesn’t contain aggregations, it will be
      * equivalent to Append mode.
      *
      */
    //.outputMode("complete")
    //.outputMode("append") // Append output mode not supported when there are streaming aggregations on streaming DataFrames/DataSets without watermark;
    .outputMode("update")
    .format("console")
    .start()

  query.awaitTermination()
}

/**
  * Notes:
  * Structured Streaming does not materialize the entire table.
  * It reads the latest available data from the streaming data source, processes it incrementally to update the result,
  * and then discards the source data. It only keeps around the minimal intermediate state data as required to update
  * the result (e.g. intermediate counts in the earlier example).
  *
  * This model is significantly different from many other stream processing engines. Many streaming systems require
  * the user to maintain running aggregations themselves, thus having to reason about fault-tolerance, and data
  * consistency (at-least-once, or at-most-once, or exactly-once). In this model, Spark is responsible for updating
  * the Result Table when there is new data, thus relieving the users from reasoning about it.
  *
  */
