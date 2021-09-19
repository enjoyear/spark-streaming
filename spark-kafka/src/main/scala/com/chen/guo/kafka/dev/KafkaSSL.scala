package com.chen.guo.kafka.dev

import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.{Dataset, SparkSession}
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils

object KafkaSSL extends App {
  val spark = SparkSession
    .builder
    .master("local[2]")
    .appName("KafkaSSL")
    .getOrCreate()

  // https://kontext.tech/column/spark/457/tutorial-turn-off-info-logs-in-spark
  // info log is too much
  spark.sparkContext.setLogLevel("WARN")

  for (arg <- args) {
    println(s"Got arg: ${arg.substring(0, 2)}")
  }
  val broker = args(0)
  val trustStoreLocation = args(1) //e.g.  /tmp/chenguo/truststore.jks
  val keyStoreLocation = args(2) //e.g.  /tmp/chenguo/keystore.jks
  val keyStorePassword = args(3)
  val keyPassword = args(4)
  val topicName = args(5)

  // import java.io.File
  //  new File("/").listFiles.foreach(println(_))

  /**
    * dbutils.fs.cp("dbfs:/FileStore/kafkaCredentials/truststore.jks", "/tmp/chenguo/truststore.jks")         -> save to S3 /tmp/chenguo
    * dbutils.fs.cp("dbfs:/FileStore/kafkaCredentials/truststore.jks", "dbfs:/tmp/chenguo/truststore.jks")    -> save to S3 /tmp/chenguo, same as above
    * dbutils.fs.cp("dbfs:/FileStore/kafkaCredentials/truststore.jks", "/dbfs/tmp/chenguo/truststore.jks")    -> save to S3 /dbfs/tmp/chenguo
    * dbutils.fs.cp("dbfs:/FileStore/kafkaCredentials/truststore.jks", "file:///tmp/chenguo/truststore.jks")  -> save to driver local fs
    *
    *
    * .option(s"kafka.ssl.truststore.location", "/dbfs/tmp/chenguo/truststore.jks")           -> read file from S3 /tmp/chenguo
    * .option(s"kafka.ssl.truststore.location", "dbfs:/tmp/chenguo/truststore.jks")           -> NoSuchFileException: dbfs:/tmp/chenguo/truststore.jks
    * .option(s"kafka.ssl.truststore.location", "/tmp/chenguo/truststore.jks")                -> NoSuchFileException: /tmp/chenguo/truststore.jks
    * .option(s"kafka.ssl.truststore.location", "file:///tmp/chenguo/truststore.jks")         -> NoSuchFileException: file:/tmp/chenguo/truststore.jks
    */
  //  dbutils.fs.cp("dbfs:/FileStore/kafkaCredentials/truststore.jks", trustStoreLocation)
  //  dbutils.fs.cp("dbfs:/FileStore/kafkaCredentials/keystore.jks", keyStoreLocation)

  import spark.implicits._

  println(s"Constructing the spark readStream ...")
  val df = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", broker)
    .option("kafka.security.protocol", "SSL")
    .option("kafka.ssl.truststore.location", s"/dbfs$trustStoreLocation")
    .option("kafka.ssl.truststore.password", "changeit")
    .option("kafka.ssl.keystore.location", s"/dbfs$keyStoreLocation")
    .option("kafka.ssl.keystore.password", keyStorePassword)
    .option("kafka.ssl.key.password", keyPassword)
    .option("subscribe", topicName)
    //start consuming from the earliest. By default it will be the latest, which is to discard all history
    //.option("startingOffsets", "earliest")
    //change to {"$topicName":{"0":1}} if you want to read from offset 1
    //.option("startingOffsets", s"""{"$topicName":{"0":-2}}""")
    //.option("endingOffsets", "latest")  //ending offset cannot be set in streaming queries
    .load()

  val kafkaDF: Dataset[(String, String)] = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .as[(String, String)]

  val query: StreamingQuery = kafkaDF.writeStream
    .queryName("kafka-ingest")
    .outputMode(OutputMode.Append())
    .option("numRows", 100) //show more than 20 rows by default
    .option("truncate", value = false) //To show the full column content
    .trigger(Trigger.ProcessingTime("3 seconds"))
    .format("console")
    .start()

  query.awaitTermination()
}
