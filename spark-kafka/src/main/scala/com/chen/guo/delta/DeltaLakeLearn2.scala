package com.chen.guo.delta

import io.delta.tables._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

import java.io.File
import java.lang
import scala.reflect.io.Directory

/**
  * Find tutorial here: https://docs.delta.io/latest/quick-start.html#quickstart
  * NOTE that delta-core 0.8.0 isn't compatible with spark 3.1.1
  *
  */
object DeltaLakeLearn2 extends App {
  val spark = SparkSession
    .builder()
    .appName("DeltaLakeLearn")
    .master("local[2]")

    /**
      * NOTE!!!
      * Delta Lake supports creating two types of tables—tables defined in the metastore and tables defined by path.
      * To work with metastore-defined tables, you must enable integration with Apache Spark DataSourceV2 and
      * Catalog APIs by setting configurations when you create a new SparkSession.
      * https://docs.delta.io/latest/delta-batch.html#create-a-table
      * https://docs.delta.io/latest/delta-batch.html#configure-sparksession
      */
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()

  // https://kontext.tech/column/spark/457/tutorial-turn-off-info-logs-in-spark
  // info log is too much
  spark.sparkContext.setLogLevel("WARN")
  cleanUpDBPath("test_db")
  val data: Dataset[lang.Long] = spark.range(0, 5)
  data.write.format("delta").saveAsTable("test_db.deleteme")

  def cleanUpDBPath(dbName: String): Unit = {
    // The database isn't recognized for some reason when the Spark starts even if the DB folder exists on disk
    // Create the DB again to register it in the delta catalog
    spark.sql(s"create database if not exists $dbName")

    val dbMetadata = spark.sessionState.catalog.getDatabaseMetadata(dbName)
    val dbPath = dbMetadata.locationUri.toString
    val directory = new Directory(new File(dbPath))
    // Clean up the DB folder before each one
    directory.deleteRecursively()
  }
}
