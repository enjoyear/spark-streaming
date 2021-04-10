package com.chen.guo

import com.chen.guo.constant.Constant
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Schema inference:
  * For ad-hoc use cases, you can reenable schema inference by setting spark.sql.streaming.schemaInference to true.
  *
  * Partition discovery:
  * Supported when subdirectories that are named /key=value/ are present and listing will automatically recurse into
  * these directories. If these columns appear in the user-provided schema, they will be filled in by Spark based on the
  * path of the file being read. The directories that make up the partitioning scheme must be present when the query
  * starts and must remain static. For example, it is okay to add /data/year=2016/ when /data/year=2015/ was present,
  * but it is invalid to change the partitioning column (i.e. by creating the directory /data/date=2016-04-17/)
  *
  * How to run:
  * Add more files like a.txt into resources/SampleCSV/...
  */
object SStreamLocalFS extends App {

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
    .option("sep", ",")
    .schema(schema)

    /**
      * Reads files written in a directory as a stream of data. Files will be processed in the order of file modification
      * time. If latestFirst is set, order will be reversed.
      *
      * Note that the files must be atomically placed in the given directory, which in most file systems, can be
      * achieved by file move operations.
      */
    // Equivalent to format("csv").load("/path/to/directory")
    .csv(Constant.CSVSampleFiles)


  val query = csvDF.writeStream
    .outputMode("append")
    .format("console")
    .start()

  query.awaitTermination()
}
