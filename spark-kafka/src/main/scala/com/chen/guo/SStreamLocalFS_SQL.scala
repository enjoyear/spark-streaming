package com.chen.guo

import com.chen.guo.constant.Constant
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Ref:
  * https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#basic-operations---selection-projection-aggregation
  *
  * An example that registers a streaming DataFrame/Dataset as a temporary view and then apply SQL commands on it.
  * How to run:
  * Add more files like a.txt into resources/SampleCSV/...
  */
object SStreamLocalFS_SQL extends App {

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

  val csvDF: DataFrame = spark
    .readStream
    .option("sep", ";")
    .schema(schema)
    .csv(Constant.CSVSampleFiles)

  csvDF.createOrReplaceTempView("updates")

  val query = spark
    .sql("select count(*) from updates") // returns another streaming DF
    .writeStream
    .outputMode("complete")
    .format("console")
    .start()

  query.awaitTermination()
}
