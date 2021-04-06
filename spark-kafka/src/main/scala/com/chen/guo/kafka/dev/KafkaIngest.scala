package com.chen.guo.kafka.dev

import org.apache.spark.sql.streaming._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object KafkaIngest {
  val appName = "KafkaIngest"
  val queryNamePrefix = "kfkingest"
  val consumerGroup = "dp-spkkfk-ingest"

  val topicName = "data-analytics-service-events"
  val topicPartition = 30
  val kafkaSourceExampleTopic: Array[String] = Array(consumerGroup, topicName, s"${OffsetStringGenerator.fromEarliest(topicName, topicPartition)}")
  val outputPathRoot = s"dbfs:/tmp/chenguo/kafka/${topicName}"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[2]")
      .appName(appName)
      .config("spark.sql.streaming.kafka.useDeprecatedOffsetFetching", false)
      .getOrCreate()

    val df: DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("groupIdPrefix", kafkaSourceExampleTopic(0))
      .option("subscribe", kafkaSourceExampleTopic(1))
      .option("startingOffsets", kafkaSourceExampleTopic(2))
      .option("includeHeaders", "true")
      .load()


    val kafkaDF = df

    val kafkaIngestWriter: DataStreamWriter[Row] = kafkaDF.writeStream
      .queryName(getQueryName(kafkaSourceExampleTopic(1)))
      .format("json")
      .option("path", getOutputPath(outputPathRoot, "ingested", getQueryName(kafkaSourceExampleTopic(1))))
      .option("checkpointLocation", getOutputPath(outputPathRoot, "checkpoint", getQueryName(kafkaSourceExampleTopic(1))))
      .outputMode(OutputMode.Append())
      .trigger(Trigger.ProcessingTime("30 seconds"))

    //"kafka-ingest" won't be active unless started
    println(s"Active queries: ${spark.streams.active.map(x => s"${x.name}(${x.id})").mkString(",")}")
    val qKafkaIngest: StreamingQuery = kafkaIngestWriter.start()
    //"kafka-ingest" will be included
    println(s"Active queries: ${spark.streams.active.map(x => s"${x.name}(${x.id})").mkString(",")}")

    addShutdownHook(spark)

    Thread.sleep(2000)
    qKafkaIngest.explain(true)

    spark.streams.active.foreach(x => x.awaitTermination())

    println(s"Exiting structured streaming application ${appName} now...")
  }

  private def getQueryName(topicNames: String): String = s"${queryNamePrefix}-${topicNames}"

  private def getOutputPath(rootDir: String, pathType: String, queryName: String): String = s"${rootDir}/${pathType}/${queryName}"

  /**
    * To stop
    * ps -ef | grep KafkaSourceFileSink | grep -v grep | awk '{print $2}' | xargs kill -s SIGTERM
    */
  def addShutdownHook(spark: SparkSession): Unit = {
    /**
      * Be careful, don't use ()=>{}
      * https://stackoverflow.com/questions/26944515/scala-shutdown-hooks-never-running
      * https://stackoverflow.com/questions/4543228/whats-the-difference-between-and-unit
      */
    sys.addShutdownHook({
      // TODO: What are the impacts if kill directly without stopping the queries?
      // For example, the application or cluster is terminated directly
      println("Got kill signal. Stopping the Spark Context directly without stopping the queries.")
      //      spark.streams.active.foreach(x => {
      //        println(s"Stopping the query ${x.name} ${x.id}")
      //        x.stop() // will fail because "Cannot call methods on a stopped SparkContext."
      //      })
      println(s"Currently active queries count: ${spark.streams.active.length}")
    })
  }
}
