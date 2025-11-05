package com.chen.guo

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object WordCountEMR extends App {
  val spark = SparkSession
    .builder
    // .master("local[*]")
    .appName("WordCount")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  // val inputPath = "s3a://chen-guo-test/test/input/word-count.txt"
  val inputPath = "file:///etc/passwd"

  val data: RDD[String] = spark.sparkContext.textFile(inputPath)
  //Print out the RDD to verify that file can be loaded correctly
  data.foreach(println(_))
  val splitdata = data.flatMap(line => line.split(" "));
  // The type of this map data is RDD[(String, Int)]

  import spark.implicits._

  val df: DataFrame = splitdata.map(word => (word, 1)).toDF("word", "count")
  df.groupBy("word")
    .agg(sum("count").as("count"))
    .orderBy(desc("count"))
    .limit(100)
    .collect()
    .foreach(println)
}

