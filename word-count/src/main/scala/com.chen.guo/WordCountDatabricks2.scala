package com.chen.guo

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{desc, sum}

import java.io.File

object WordCountDatabricks2 {
  val outputPath = "/tmp/chenguo/test2"

  def main(args: Array[String]): Unit = {
    println("Job starts-v2")

    for (arg <- args) {
      println(s"Got arg: ${arg}")
    }
    val testDataBucket = args(0)
    val testDataKey = args(1)
    println(s"Bucket name: ${testDataBucket}")
    println(s"S3 key name: ${testDataKey}")
    println(s"Home directory is ${System.getenv("HOME")}")
    listLocalFs("/")
    listLocalFs("/home")
    listLocalFs("/usr")
    listLocalFs("/dbfs")
    listLocalFs("/mnt")
    listLocalFs("/databricks")

    val spark = SparkSession
      .builder
      .appName("WordCount")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val df = spark.table("chen_guo.diamonds")
    df.write.mode("overwrite").json(outputPath)
    println(s"Overwrite output ${outputPath}")


    //Word Count Example
    val logData: RDD[String] = spark.sparkContext.textFile(s"s3a://$testDataBucket/$testDataKey")
    val splitdata = logData.flatMap(line => line.split(" "));

    import spark.implicits._

    val logDf: DataFrame = splitdata.map(word => (word, 1)).toDF("word", "count")
    logDf.groupBy("word")
      .agg(sum("count").as("count"))
      .orderBy(desc("count"))
      .limit(5)
      .collect()
      .foreach(println(_))
  }

  private def listLocalFs(pathToInspect: String): Unit = {
    val d = new File(pathToInspect)
    println(s"Listing files on the local file system for ${pathToInspect}")
    d.listFiles.foreach(println(_))
  }
}

