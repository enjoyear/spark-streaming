package com.chen.guo.constant

object Constant {
  val CheckpointLocation = s"file:///Users/chguo/Downloads/kafka-ingest/checkpoint"
  val OutputPath = s"file:///Users/chguo/Downloads/kafka-ingest/ingested"
  val CSVSampleFiles = s"file://${System.getProperty("user.home")}/repo/enjoyear/spark-streaming/spark-kafka/src/main/resources/SampleCSV/*"
}
