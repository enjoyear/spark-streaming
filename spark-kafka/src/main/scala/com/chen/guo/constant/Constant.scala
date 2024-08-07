package com.chen.guo.constant

object Constant {
  val homeDir: String = System.getProperty("user.home")
  val CheckpointLocation = s"file://${homeDir}/Downloads/kafka-ingest/checkpoint"
  val OutputPath = s"file://${homeDir}/Downloads/kafka-ingest/ingested"
  val CSVSampleFiles = s"file://${homeDir}/repo/enjoyear/spark-streaming/spark-kafka/src/main/resources/SampleCSV/*"
  val TextSampleFiles = s"file://${homeDir}/repo/enjoyear/spark-streaming/spark-kafka/src/main/resources/dummy.txt"
}

object GlobalKafkaSettings {
  val BROKER_URL = "xxx"
  val TRUST_STORE_LOCATION = "xxx"
  val TRUST_STORE_PASSWORD = "xxx"
  val KEY_STORE_LOCATION = "xxx"
  val KEY_STORE_PASSWORD = "xxx"
  val KEY_PASSWORD = "xxx"
}