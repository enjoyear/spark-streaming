package com.chen.guo

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

/**
 * No RDDs supported in Connect mode. If you need RDD/SparkContext, run classic Spark (e.g., spark-submit),
 * or run your notebook on a classic Spark kernel.
 *
 * Logging level: since SparkContext is hidden, set log level on the server side (e.g., log4j config in the Spark
 * Connect server image) or via server Spark conf at startup.
 */
object WordCountSparkConnectIDE extends App {
  val spark =
    //.appName("WordCount") spark.app.name configuration is not supported in Connect mode.
    SparkSession.builder.remote("sc://localhost:15002").getOrCreate()

  import spark.implicits._

  // In Spark Connect, anything like .flatMap(_.split...) or .filter(_.nonEmpty) becomes a ScalaUDF
  // that must exist on the server.
  // Option 1: avoid closures
  // .select(explode(split(col("value"), "\\s+")).as("word"))
  //      .filter(length(col("word")) > 0)
  // Option 2: package and add artifact
  spark.addArtifact("/Users/chenguo/src/chen/spark-streaming/word-count-sc/build/libs/word-count-sc.jar")

  val words: DataFrame =
    spark.read
      .textFile("file:///etc/passwd") // paths resolved on the Spark server
      .flatMap(_.split("\\s+")) // Dataset[String]
      .filter(_.nonEmpty)
      .toDF("word")

  val result =
    words.groupBy("word")
      .agg(count(lit(1)).as("count"))
      .orderBy(desc("count"))
      .limit(100)

  result.collect().foreach(println)
  spark.stop()
}

