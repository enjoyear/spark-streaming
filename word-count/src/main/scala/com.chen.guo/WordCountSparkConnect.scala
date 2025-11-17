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
object WordCountSparkConnect extends App {
  val argumentList: Array[String] = args
  argumentList.foreach(arg => println(s"Received argument: $arg"))

  val spark = SparkSession
    .builder
    .remote("sc://all-purpose1-svc.spark-operator.svc.cluster.local:15002")
    // .appName("WordCount") spark.app.name configuration is not supported in Connect mode.
    .getOrCreate()

  import spark.implicits._

  val inputPath = "file:///etc/passwd" // paths resolved on the Spark server

  val words: DataFrame =
    spark.read
      .textFile(inputPath)          // Dataset[String]
      .flatMap(_.split("\\s+"))     // Dataset[String]
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

