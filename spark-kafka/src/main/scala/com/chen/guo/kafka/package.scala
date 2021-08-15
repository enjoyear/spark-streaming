package com.chen.guo

import org.apache.spark.sql.streaming.{DataStreamReader, DataStreamWriter}

import java.io.FileInputStream
import java.util.Properties

package object kafka {
  val propertyFile = s"${System.getenv("HOME")}/Downloads/ssl/open-source-connect.properties"
  val properties = new Properties()
  val in = new FileInputStream(propertyFile)
  try {
    properties.load(in)
  } finally {
    in.close()
  }

  implicit class KafkaStreamReaderMTls(streamReader: DataStreamReader) {
    def withMTlsOptions(): DataStreamReader = {
      streamReader
        .option(s"kafka.security.protocol", properties.getProperty("security.protocol"))
        .option(s"kafka.ssl.truststore.location", properties.getProperty("ssl.truststore.location"))
        .option(s"kafka.ssl.truststore.password", properties.getProperty("ssl.truststore.password"))
        .option(s"kafka.ssl.keystore.location", properties.getProperty("ssl.keystore.location"))
        .option(s"kafka.ssl.keystore.password", properties.getProperty("ssl.keystore.password"))
        .option(s"kafka.ssl.key.password", properties.getProperty("ssl.key.password"))
    }
  }

  implicit class KafkaStreamWriterMTLS[T](streamWriter: DataStreamWriter[T]) {
    def withMTlsOptions(): DataStreamWriter[T] = {
      streamWriter
        .option(s"kafka.security.protocol", properties.getProperty("security.protocol"))
        .option(s"kafka.ssl.truststore.location", properties.getProperty("ssl.truststore.location"))
        .option(s"kafka.ssl.truststore.password", properties.getProperty("ssl.truststore.password"))
        .option(s"kafka.ssl.keystore.location", properties.getProperty("ssl.keystore.location"))
        .option(s"kafka.ssl.keystore.password", properties.getProperty("ssl.keystore.password"))
        .option(s"kafka.ssl.key.password", properties.getProperty("ssl.key.password"))
    }
  }

}
