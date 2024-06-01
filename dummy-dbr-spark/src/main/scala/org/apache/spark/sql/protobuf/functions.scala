package org.apache.spark.sql.protobuf

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.Column

/**
  * Dummy API to be compatible with Databricks protobuf integration.
  * Reference : https://stackoverflow.com/questions/71069226/unable-to-find-databricks-spark-sql-avro-shaded-jars-in-any-public-maven-reposit
  */
object functions {
  @Experimental
  def from_protobuf(data: Column, options: java.util.Map[String, String])
  : Column = {
    new Column("dummy")
  }


  @Experimental
  def from_protobuf(
                     data: Column,
                     messageName: String,
                     descFilePath: String,
                     options: java.util.Map[String, String]): Column = {
    new Column("dummy")
  }

  @Experimental
  def to_protobuf(data: Column, options: java.util.Map[String, String])
  : Column = {
    new Column("dummy")
  }
}
