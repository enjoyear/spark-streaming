package com.chen.guo

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{desc, sum}
import org.slf4j.LoggerFactory

object WordCountDatabricks {
  val logger = LoggerFactory.getLogger(this.getClass)
  val outputPath = "/tmp/chenguo/test2"

  def main(args: Array[String]): Unit = {
    logger.info("Job starts")

    for (arg <- args) {
      logger.info(s"Received arg: ${arg}")
    }

    val spark = SparkSession
      .builder
      .appName("WordCount")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val df = spark.table("chen_guo.diamonds")
    df.write.mode("overwrite").json(outputPath)
    logger.info(s"Overwrite output ${outputPath}")


    //Word Count Example
    val logData: RDD[String] = spark.sparkContext.textFile("s3a://cb-databricks-logs-development/0326-210651-davit416/driver/log4j-active.log")
    val splitdata = logData.flatMap(line => line.split(" "));

    import spark.implicits._

    val logDf: DataFrame = splitdata.map(word => (word, 1)).toDF("word", "count")
    logDf.groupBy("word")
      .agg(sum("count").as("count"))
      .orderBy(desc("count"))
      .limit(5)
      .collect()
      .foreach(x => logger.info(x.toString()))
  }
}

