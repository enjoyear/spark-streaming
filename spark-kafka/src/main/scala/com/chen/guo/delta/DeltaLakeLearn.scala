package com.chen.guo.delta

import io.delta.tables._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Find tutorial here: https://docs.delta.io/latest/quick-start.html#quickstart
  * NOTE that delta-core 0.8.0 isn't compatible with spark 3.1.1
  *
  */
object DeltaLakeLearn extends App {
  val spark = SparkSession
    .builder()
    .appName("DeltaLakeLearn")
    .master("local[2]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()

  // https://kontext.tech/column/spark/457/tutorial-turn-off-info-logs-in-spark
  // info log is too much
  spark.sparkContext.setLogLevel("WARN")

  val data = spark.range(0, 5)
  private val outputPath = "/tmp/spark/delta-table"

  data.write.format("delta").mode("overwrite").save(outputPath)

  val df = spark.read.format("delta").load(outputPath)
  df.show()

  val deltaTable: DeltaTable = DeltaTable.forPath(outputPath)

  // Update every even value by adding 100 to it
  deltaTable.update(
    condition = expr("id % 2 == 0"),
    set = Map("id" -> expr("id + 100")))

  // Delete every even value
  deltaTable.delete(condition = expr("id % 2 == 0"))

  // Upsert (merge) new data
  val newData = spark.range(0, 20).toDF

  deltaTable.as("oldData")
    .merge(
      newData.as("newData"),
      "oldData.id = newData.id")
    .whenMatched
    .update(Map("id" -> col("newData.id")))
    .whenNotMatched
    .insert(Map("id" -> col("newData.id")))
    .execute()

  deltaTable.toDF.show()
}
