package com.chen.guo.kafka.dev

object OffsetStringGenerator {
  /**
    * Generate the offset string for a single Kafka topic
    *
    * @param topicName     the kafka topic name
    * @param numPartitions number of partitions for this topic(TODO: this metadata can be retrieved)
    *                      Offset number should start from 0(inclusive) to numPartitions(exclusive)
    * @return offset string e.g. {"example-topic":{"0":-2,"2":-2}}
    */
  def fromEarliest(topicName: String, numPartitions: Int): String = {
    generateOffsetString(topicName, numPartitions, -2)
  }

  /**
    * Generate the offset string for a single Kafka topic
    *
    * @param topicName     the kafka topic name
    * @param numPartitions number of partitions for this topic(TODO: this metadata can be retrieved)
    *                      Offset number should start from 0(inclusive) to numPartitions(exclusive)
    * @return offset string e.g. {"example-topic":{"0":-1,"2":-1}}
    */
  def fromLatest(topicName: String, numPartitions: Int): String = {
    generateOffsetString(topicName, numPartitions, -1)
  }

  /**
    * Generate the offset string for a single Kafka topic with a fixed offset
    *
    * An example of the generated offset string can be like
    * {"example-topic":{"0":-2,"1":-2,"2":-2},"quickstart-events":{"0": 123}}
    *
    * @param topicName     the kafka topic name
    * @param numPartitions number of partitions for this topic(TODO: this metadata can be retrieved)
    *                      Offset number should start from 0(inclusive) to numPartitions(exclusive)
    * @param offset        use -2 for the earliest offset, -1 for the latest offset, and any non-negative number for other
    *                      offsets
    * @return offset string
    */
  private def generateOffsetString(topicName: String, numPartitions: Int, offset: Int): String = {
    assert(offset >= -2, "Only -1 and -2 are supported for negative offsets")
    val partitionOffsets = Range(0, numPartitions).map(x => s""""${x}":${offset}""").mkString("{", ",", "}")
    s"""{"$topicName":$partitionOffsets}"""
  }
}


object test extends App {
  println(OffsetStringGenerator.fromEarliest("topic", 20))
}