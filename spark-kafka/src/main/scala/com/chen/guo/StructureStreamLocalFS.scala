package com.chen.guo

import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Run a Netcat as a data server by using
  * nc -lk 9999
  */
object StructureStreamLocalFS extends App {

  val spark = SparkSession
    .builder()
    .appName("StreamingLocalJsons")
    .master("local[2]")
    .getOrCreate()

  // https://kontext.tech/column/spark/457/tutorial-turn-off-info-logs-in-spark
  // info log is too much
  spark.sparkContext.setLogLevel("WARN")

  val schema = new StructType()
    .add("name", StringType)
    .add("age", IntegerType)

  /**
    * This generates streaming DataFrames that are untyped, meaning that the schema of the DataFrame is not checked at
    * compile time, only checked at runtime when the query is submitted. Some operations like map, flatMap, etc. need
    * the type to be known at compile time. To do those, you can convert these untyped streaming DataFrames to typed
    * streaming Datasets using the same methods as static DataFrame.
    */
  val csvDF: DataFrame = spark
    .readStream

    /**
      * More options can be found here:
      * https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#input-sources
      */
    .option("sep", ";")
    .schema(schema)

    /**
      * Reads files written in a directory as a stream of data. Files will be processed in the order of file modification
      * time. If latestFirst is set, order will be reversed.
      *
      * Note that the files must be atomically placed in the given directory, which in most file systems, can be
      * achieved by file move operations.
      */
    // Equivalent to format("csv").load("/path/to/directory")
    .csv("file:///Users/chguo/repos/enjoyear/spark-streaming/spark-kafka/src/main/resources/SampleCSV/*")


  val query = csvDF.writeStream
    .outputMode("append")
    .format("console")
    .start()

  query.awaitTermination()
}
