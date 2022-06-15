package com.chen.guo.kafka

import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * To produce events using ./kafka-console-producer.sh, run:
  * ./kafka-console-producer.sh --topic TestTopic --bootstrap-server b-1.cdc.abcdef.c1.kafka.us-east-1.amazonaws.com:9096 \
  * --producer.config /tmp/kafka-local-credentials/client-sasl.properties
  *
  * The content of "client-sasl.properties" is:
  * security.protocol=SASL_SSL
  * sasl.mechanism=SCRAM-SHA-512
  * ssl.truststore.location=/tmp/kafka-local-credentials/kc/truststore.jks
  * ssl.truststore.password=changeit
  * sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="kafka_connect" password="12345";
  *
  */
object KafkaSASL extends App {

  val spark = SparkSession
    .builder
    .master("local[2]")
    .appName("KafkaIntegration")
    .getOrCreate()

  // https://kontext.tech/column/spark/457/tutorial-turn-off-info-logs-in-spark
  // info log is too much
  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  val topicName = "TestTopic"

  val df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "b-1.cdc.abcdef.c1.kafka.us-east-1.amazonaws.com:9096")
    .option("subscribe", topicName)
    .option("kafka.ssl.truststore.location", "/tmp/kafka-local-credentials/kc/truststore.jks")
    .option("kafka.ssl.truststore.password", "changeit")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
    .option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"kafka_connect\" password=\"12345\";")
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
