package com.chen.guo

import org.apache.spark.sql.types.{StringType, StructType}
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

  val schema = new StructType()
    .add("id", StringType)
    .add("name", StringType)

  val rawRecords: DataFrame = spark.readStream
    .schema(schema)

    /**
      * Reads files written in a directory as a stream of data. Files will be processed in the order of file modification
      * time. If latestFirst is set, order will be reversed.
      *
      * Note that the files must be atomically placed in the given directory, which in most file systems, can be
      * achieved by file move operations.
      */
    .json("file:///Users/chguo/repos/enjoyear/spark-streaming/spark-kafka/src/main/resources/SampleJson/*")


  rawRecords.foreach(println(_))

}
