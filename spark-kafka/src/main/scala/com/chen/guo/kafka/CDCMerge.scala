package com.chen.guo.kafka

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.{col, desc, from_json, row_number}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.io.File
import scala.reflect.io.Directory

/**
  * Send dummy data
  * echo '1:{"oc": "U","ns": "chen_guo.cdc_target1","pk":{"_id": 1},"va": {"value": "v1"}}' | kcat -P -b localhost:9092 -t cdc-topic -K:
  * echo '3:{"oc": "I","ns": "chen_guo.cdc_target1","pk":{"_id": 3},"va": {"value": "v0"}}' | kcat -P -b localhost:9092 -t cdc-topic -K:
  * echo '1:{"oc": "U","ns": "chen_guo.cdc_target1","pk":{"_id": 1},"va": {"value": "v2"}}' | kcat -P -b localhost:9092 -t cdc-topic -K:
  * echo '2:{"oc": "D","ns": "chen_guo.cdc_target1","pk":{"_id": 2}}' | kcat -P -b localhost:9092 -t cdc-topic -K:
  *
  * echo '"uuid1":{"oc": "U","ns": "chen_guo.cdc_target2","pk":{"uuid": "uuid1"},"va":{"version":"version 1","num":11}}' | kcat -P -b localhost:9092 -t cdc-topic -K:
  * echo '"uuid4":{"oc": "I","ns": "chen_guo.cdc_target2","pk":{"uuid": "uuid4"},"va":{"version":"version 0","num":44}}' | kcat -P -b localhost:9092 -t cdc-topic -K:
  * echo '"uuid3":{"oc": "D","ns": "chen_guo.cdc_target2","pk":{"uuid": "uuid3"}}' | kcat -P -b localhost:9092 -t cdc-topic -K:
  * echo '"uuid1":{"oc": "U","ns": "chen_guo.cdc_target2","pk":{"uuid": "uuid1"},"va":{"version":"version 2","num":12}}' | kcat -P -b localhost:9092 -t cdc-topic -K:
  * echo '"uuid2":{"oc": "U","ns": "chen_guo.cdc_target2","pk":{"uuid": "uuid2"},"va":{"version":"version 1","num":22}}' | kcat -P -b localhost:9092 -t cdc-topic -K:
  */
object CDCMerge extends App with WithSparkSession {
  val databaseName: String = "chen_guo"
  // region Table1 Definitions

  val table1 = "cdc_target1"
  val table1KeySchema: StructType = StructType(Array(
    StructField("_id", IntegerType, nullable = true)
  ))
  val table1ValSchema: StructType = StructType(Array(
    StructField("value", StringType, nullable = true)
  ))
  val table1Schema: StructType = StructType(table1KeySchema.fields ++ table1ValSchema.fields)

  // endregion

  // region Table2 Definitions

  val table2 = "cdc_target2"
  val table2KeySchema: StructType = StructType(Array(
    StructField("uuid", StringType, nullable = true)
  ))
  val table2ValSchema: StructType = StructType(Array(
    StructField("version", StringType, nullable = true),
    StructField("num", IntegerType, nullable = true)
  ))
  val table2Schema: StructType = StructType(table2KeySchema.fields ++ table2ValSchema.fields)

  // endregion

  val cdcTargets: Map[String, (StructType, StructType)] = Map(
    table1 -> (table1KeySchema, table1ValSchema),
    table2 -> (table2KeySchema, table2ValSchema)
  )

  private val COL_METADATA_TS = "_etl_ts"
  private val COL_KEY = "key"
  private val COL_PAYLOAD_NS = "ns"
  private val COL_PAYLOAD_OC = "oc"
  private val COL_PAYLOAD_KEY = "pk"
  private val COL_PAYLOAD_VAL = "va"

  val partitionWindow: WindowSpec = Window.partitionBy(COL_PAYLOAD_NS, COL_KEY).orderBy(desc(COL_METADATA_TS))


  cleanUpDBPath(databaseName)
  createTargetTable1Data()
  createTargetTable2Data()

  val df: DataFrame = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("groupIdPrefix", "CDCProcessor")
    // ./kafka-topics.sh --create --partitions 2 --replication-factor 1 --topic cdc-topic --bootstrap-server localhost:9092
    .option("subscribe", "cdc-topic")
    .option("startingOffsets", "latest")
    .load()

  // val tumblingWindow: Column = window(col("metadataTimestamp"), "10 seconds").as("window")
  val query: StreamingQuery = df
    .select(
      col(COL_KEY).cast("string"),
      from_json(col("value").cast("string"), cdcSchema).as("payload"),
      col("timestamp").as(COL_METADATA_TS)
    )
    .select(
      col(s"payload.$COL_PAYLOAD_NS"),
      col(COL_KEY),
      col(s"payload.$COL_PAYLOAD_OC"),
      col(s"payload.$COL_PAYLOAD_KEY"),
      col(s"payload.$COL_PAYLOAD_VAL"),
      col(COL_METADATA_TS),
    )
    .writeStream
    .queryName("cdcIngest")
    .option("mergeSchema", "true")
    .outputMode(OutputMode.Update())
    .trigger(Trigger.ProcessingTime("5 seconds"))
    // https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-sinks
    .foreachBatch((microBatchDF: DataFrame, batchId: Long) => mergeIntoTargetTable(microBatchDF, batchId))
    .start()

  // query.explain
  query.awaitTermination()


  private def mergeIntoTargetTable(microBatchDF: DataFrame, batchId: Long): Unit = {
    import spark.implicits._

    val deduped: DataFrame = microBatchDF
      .withColumn("rn", row_number().over(partitionWindow))
      .where($"rn" === 1)
      .drop("rn")
      .cache()

    for (cdcTarget <- cdcTargets) {
      val targetTable = cdcTarget._1
      val keySchema: StructType = cdcTarget._2._1
      val valSchema: StructType = cdcTarget._2._2

      println(s"Target table $databaseName.$targetTable before Batch $batchId")
      spark.table(s"$databaseName.$targetTable").show

      val streamUpdates: DataFrame = deduped
        .where(col(COL_PAYLOAD_NS) === s"$databaseName.$targetTable")
        .select(
          col(COL_PAYLOAD_OC),
          from_json(col(COL_PAYLOAD_KEY), keySchema).as(COL_PAYLOAD_KEY),
          from_json(col(COL_PAYLOAD_VAL), valSchema).as(COL_PAYLOAD_VAL),
          col(COL_METADATA_TS)
        )
        .selectExpr(
          COL_PAYLOAD_OC,
          s"$COL_PAYLOAD_KEY.*",
          s"$COL_PAYLOAD_VAL.*",
          COL_METADATA_TS
        )

      streamUpdates.show()

      val streamView: String = "streamUpdates"
      streamUpdates.createOrReplaceTempView(streamView)

      val targetTableAlias = "t"
      val sourceStreamAlias = "s"
      val mergeCondition = keySchema.fields
        .map(x => s"$targetTableAlias.${x.name} = $sourceStreamAlias.${x.name}")
        .mkString(" AND ")

      print("Condition is " + mergeCondition)
      streamUpdates.sparkSession.sql(
        s"""
          MERGE INTO $databaseName.$targetTable $targetTableAlias
          USING $streamView $sourceStreamAlias
          ON $mergeCondition
          WHEN MATCHED and $sourceStreamAlias.oc = "D" THEN DELETE
          WHEN MATCHED and $sourceStreamAlias.oc != "D" THEN UPDATE SET *
          WHEN NOT MATCHED THEN INSERT *
        """)

      println(s"Target table $databaseName.$targetTable after Batch $batchId")
      spark.table(s"$databaseName.$targetTable").show
    }

    deduped.unpersist()
  }

  private def createTargetTable1Data(): Unit = {
    val data = Seq(
      Row(1, "v0"),
      Row(2, "v0")
    )
    val rdd: RDD[Row] = sc.parallelize(data)
    val df = spark.createDataFrame(rdd, table1Schema)
    df.write.format("delta").saveAsTable(s"$databaseName.$table1")
  }

  private def createTargetTable2Data(): Unit = {
    val data = Seq(
      Row("uuid1", "version 0", 1),
      Row("uuid2", "version 0", 2),
      Row("uuid3", "version 0", 3),
    )
    val rdd: RDD[Row] = sc.parallelize(data)
    val df = spark.createDataFrame(rdd, table2Schema)
    df.write.format("delta").saveAsTable(s"$databaseName.$table2")
  }
}

trait WithSparkSession {
  val spark: SparkSession = SparkSession.builder()
    .master("local[4]")
    .appName("cdc")
    //.enableHiveSupport() // Try to create a DELTA table instead of turning this on
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  /**
    * |-- key: binary (nullable = true)
    * |-- value: binary (nullable = true)
    * |-- topic: string (nullable = true)
    * |-- partition: integer (nullable = true)
    * |-- offset: long (nullable = true)
    * |-- timestamp: timestamp (nullable = true)
    * |-- timestampType: integer (nullable = true)
    */
  val kafkaSchema: StructType = StructType(Array(
    StructField("key", BinaryType, nullable = true),
    StructField("value", BinaryType, nullable = true),
    StructField("topic", StringType, nullable = true),
    StructField("partition", IntegerType, nullable = true),
    StructField("offset", LongType, nullable = true),
    StructField("timestamp", TimestampType, nullable = true),
    StructField("timestampType", IntegerType, nullable = true),
  ))

  val cdcSchema: StructType = StructType(Array(
    StructField("oc", StringType),
    StructField("ns", StringType),
    StructField("pk", StringType),
    StructField("va", StringType),
  ))

  /**
    * A utility function that can be used to clean up the local DB directory before your tests start
    *
    * @param dbName the database to be cleaned up
    */
  def cleanUpDBPath(dbName: String): Unit = {
    // The database isn't recognized for some reason when the Spark starts even if the DB folder exists on disk
    // Create the DB again to register it in the delta catalog
    spark.sql(s"create database if not exists $dbName")

    val directory = new Directory(getDBPath(dbName))
    // Clean up the DB folder before the test starts
    directory.deleteRecursively()
  }

  /**
    * @param dbName a Spark test DB name
    */
  def getDBPath(dbName: String): File = {
    val dbMetadata = spark.sessionState.catalog.getDatabaseMetadata(dbName)
    new File(dbMetadata.locationUri)
  }

  /**
    * @param dbName    a Spark test DB name
    * @param tableName a Spark test table name in the DB
    */
  def getTablePath(dbName: String, tableName: String): File = {
    val tableMetadata = spark.sessionState.catalog.getTableMetadata(new TableIdentifier(tableName, Some(dbName)))
    new File(tableMetadata.location)
  }
}