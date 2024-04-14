package com.chen.guo.streamprocessing

import com.datadog.api.client.v2.api.MetricsApi
import com.datadog.api.client.v2.model.{MetricIntakeType, MetricPayload, MetricPoint, MetricSeries}
import com.datadog.api.client.{ApiClient, ApiException}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.{util => jutil}
import scala.collection.JavaConverters._

object DistinctCountAgg extends App {

  private val secrets = new jutil.HashMap[String, String]()
  secrets.put("apiKeyAuth", "abc")
  secrets.put("appKeyAuth", "xyz")

  private val defaultClient: ApiClient = ApiClient.getDefaultApiClient()
  defaultClient.configureApiKeys(secrets)
  private val metricsApi = new MetricsApi(defaultClient)

  val columnUserId = "user_id"
  val columnEventTimestamp = "_etl_cc"
  val schema = StructType(Array(
    StructField("name", StringType, true),
    StructField(columnUserId, StringType, true),
    StructField("created_at", TimestampType, true),
    StructField("params", StringType, true),
  ))

  val columnStartTime = "start_time"
  val columnEndTime = "end_time"
  val columnUserType = "user_type"
  val columnWindowSize = "aggregation_window_seconds"
  val columnDistinctUsers = "distinct_users"
  val columnDistinctUsersCount = "distinct_users_count"

  val withTestUsersSuffix = "_WITH_TEST"
  val excludeTestUsersFilter = "NOT (user_id LIKE ANY ('9999%','8888%'))"
  val name_filter1 = """NOT (name LIKE ANY ('%email\_%','admin\_%'))"""
  val name_filter2 = "NOT (name IN ('event1','event2'))"


  def andSqlFilters(filters: List[String]): String = {
    filters match {
      case Nil => "1 = 1"
      case singleFilter :: Nil => singleFilter
      case _ => filters.map(filter => s"($filter)").mkString(" AND ")
    }
  }

  val userType1 = "JANUS_IDENTITY"
  val userType1Test = userType1 + withTestUsersSuffix
  val user1Filter = "name ='http_request'"
  val user1ExcludeTestFilter = andSqlFilters(List(user1Filter, excludeTestUsersFilter))

  val userType2 = "TRANSACT"
  val userType2Test = userType2 + withTestUsersSuffix
  val user2Filter = """ name LIKE any ('%order%','%trade') or params LIKE '%"transact_id"%' """
  val user2ExcludeTestFilter = andSqlFilters(List(user2Filter, excludeTestUsersFilter))

  val userType3 = "TRANSFER"
  val userType3Test = userType3 + withTestUsersSuffix
  val user3Filter = """ (name LIKE ANY ('%transfer%') OR params LIKE ANY ('%"transfer_id"%')) """
  val user3ExcludeTestFilter = andSqlFilters(List(user3Filter, excludeTestUsersFilter))

  val userTypeAll = "ALL"
  val userTypeAllLoadtest = userTypeAll + withTestUsersSuffix
  val allUsersFilter = "1 = 1"
  val allUsersExcludeLoadTestFilter = andSqlFilters(List(allUsersFilter, excludeTestUsersFilter))

  val topic_name = "api-events"
  val window_10s = 10
  val window_60s = 60

  val cpv = 1
  val checkpointPrefix = s"/Volumes/volumes/examples/DistinctCountAgg/$cpv"
  val spark = SparkSession
    .builder
    .master("local[2]")
    .appName("DistinctCountAgg")
    .getOrCreate()

  val kfksource = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "msk.kafka.us-west-2.amazonaws.com:9094")
    .option("subscribe", topic_name)
    .option("startingOffsets", "latest")
    .option("kafka.security.protocol", "SSL")
    .option("kafka.ssl.truststore.location", "/path/to/truststore.jks")
    .option("kafka.ssl.truststore.password", "password")
    .option("kafka.ssl.keystore.location", "/path/to/keystore.jks")
    .option("kafka.ssl.keystore.password", "password")
    .option("kafka.ssl.key.password", "password")
    .load()
    .select(
      from_json(col("value").cast("string"), schema).alias("value"),
      col("partition"),
      col("offset"),
      col("timestamp")
    )
    .selectExpr("value.*", s"timestamp as ${columnEventTimestamp}")
    .filter(name_filter1)
    .filter(name_filter2)

  val streamWriter = kfksource.writeStream
    //.trigger(Trigger.ProcessingTime(trigger_10s))
    .queryName("test")
    .outputMode("update")
    // checkpointDir for streaming state files
    .option("checkpointLocation", s"$checkpointPrefix/writeStream")
    .option("truncate", false)
    .foreachBatch((microBatchDF: DataFrame, batchId: Long) => doAggregation(microBatchDF, batchId))

  // should only set the checkpointDir after creating the streamWriter
  // otherwise, it will fail due to "Cannot access the UC Volume path from this location. Path was ..."
  // spark.sparkContext.setCheckpointDir(s"$checkpointPrefix/batchState") // checkpointDir for batch state files
  val query = streamWriter.start()

  print(query.id)
  print(query.runId)
  query.awaitTermination()


  val unclosedWindowState = "unclosedWindowState"
  private val stateSchema: StructType = StructType(
    Array(
      StructField(columnStartTime, StringType),
      StructField(columnUserType, StringType),
      StructField(columnWindowSize, IntegerType),
      StructField(columnDistinctUsers, ArrayType(StringType))
    ))
  spark.createDataFrame(spark.sparkContext.emptyRDD[Row], stateSchema)
    .createOrReplaceTempView(unclosedWindowState)


  /**
    * Aggregate the microBatchDF based on the starting time of a window whose size is defined by aggregationWindowSize.
    * TODO: we can add watermark logic to drop the late arrivals
    */
  def createAggregatedDataFrame(microBatchDF: DataFrame,
                                userType: String,
                                userTypeFilter: String,
                                aggregationWindowSize: Int): DataFrame =
    microBatchDF
      .where(userTypeFilter)
      .selectExpr(
        s"from_unixtime(floor(unix_timestamp($columnEventTimestamp)/$aggregationWindowSize) * $aggregationWindowSize) as $columnStartTime",
        columnUserId
      )
      .groupBy(columnStartTime)
      .agg(collect_set(columnUserId).as(columnDistinctUsers))
      .withColumn(columnUserType, lit(userType))
      .withColumn(columnWindowSize, lit(aggregationWindowSize))

  def getLogTs(batchId: Long): String =
    s"$batchId at ${LocalDateTime.now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))}:"

  def sendMetrics(data: Array[Row], batchId: Long): Unit = {
    if (data.isEmpty) {
      return
    }

    val parser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var minTs = Long.MaxValue
    val metrics: jutil.List[MetricSeries] = data.map(r => {
      val startTime = r.getAs[String](columnStartTime) // 2024-04-12 02:46:30
      val userType = r.getAs[String](columnUserType) // type 1
      val windowSize = r.getAs[Integer](columnWindowSize) // 10
      val distinctUserCount = r.getAs[Integer](columnDistinctUsersCount)
      // println(s"[$startTime ~${windowSize}s): $userType -> $distinctUserCount")
      val endTs = parser.parse(startTime).getTime() + windowSize * 1000
      val metricName = s"sss.distinct.count.${windowSize}s"
      minTs = math.min(minTs, endTs)
      new MetricSeries()
        .metric(metricName)
        .`type`(MetricIntakeType.GAUGE)
        .points(jutil.Collections.singletonList(
          new MetricPoint()
            .timestamp(endTs / 1000)
            .value(distinctUserCount.toDouble))
        ).tags(jutil.Arrays.asList(s"user_type:$userType"))
    }).toList.asJava

    val payload = new MetricPayload().series(metrics)

    try {
      println(s"${getLogTs(batchId)} Sending metrics with latency ${(System.currentTimeMillis() - minTs) / 1000} s")
      metricsApi.submitMetrics(payload)
    } catch {
      case e: ApiException =>
        println("Exception when calling MetricsApi.submitMetrics")
        e.printStackTrace()
    }
  }

  def doAggregation(microBatchDF: DataFrame, batchId: Long): Unit = {
    microBatchDF.cache()
    var newState: DataFrame = null
    var unclosedWindows: DataFrame = null // a map to the latest window start time for each group

    try {
      val janus10sDf = createAggregatedDataFrame(microBatchDF, userType1, user1ExcludeTestFilter, window_10s)
      val janus10sDfLt = createAggregatedDataFrame(microBatchDF, userType1Test, user1Filter, window_10s)
      val janus60sDf = createAggregatedDataFrame(microBatchDF, userType1, user1ExcludeTestFilter, window_60s)
      val janus60sDfLt = createAggregatedDataFrame(microBatchDF, userType1Test, user1Filter, window_60s)

      val order10sDf = createAggregatedDataFrame(microBatchDF, userType2, user2ExcludeTestFilter, window_10s)
      val order10sDfLt = createAggregatedDataFrame(microBatchDF, userType2Test, user2Filter, window_10s)
      val order60sDf = createAggregatedDataFrame(microBatchDF, userType2, user2ExcludeTestFilter, window_60s)
      val order60sDfLt = createAggregatedDataFrame(microBatchDF, userType2Test, user2Filter, window_60s)

      val transfer10sDf = createAggregatedDataFrame(microBatchDF, userType3, user3ExcludeTestFilter, window_10s)
      val transfer10sDfLt = createAggregatedDataFrame(microBatchDF, userType3Test, user3Filter, window_10s)
      val transfer60sDf = createAggregatedDataFrame(microBatchDF, userType3, user3ExcludeTestFilter, window_60s)
      val transfer60sDfLt = createAggregatedDataFrame(microBatchDF, userType3Test, user3Filter, window_60s)

      val all10sDf = createAggregatedDataFrame(microBatchDF, userTypeAll, allUsersExcludeLoadTestFilter, window_10s)
      val all10sDfLt = createAggregatedDataFrame(microBatchDF, userTypeAllLoadtest, allUsersFilter, window_10s)
      val all60sDf = createAggregatedDataFrame(microBatchDF, userTypeAll, allUsersExcludeLoadTestFilter, window_60s)
      val all60sDfLt = createAggregatedDataFrame(microBatchDF, userTypeAllLoadtest, allUsersFilter, window_60s)

      val aggregatedDfAlias = "allAggregated"
      val allAggregated = janus10sDf.unionAll(janus10sDfLt)
        .unionAll(janus60sDf).unionAll(janus60sDfLt)
        .unionAll(order10sDf).unionAll(order10sDfLt)
        .unionAll(order60sDf).unionAll(order60sDfLt)
        .unionAll(transfer10sDf).unionAll(transfer10sDfLt)
        .unionAll(transfer60sDf).unionAll(transfer60sDfLt)
        .unionAll(all10sDf).unionAll(all10sDfLt)
        .unionAll(all60sDf).unionAll(all60sDfLt)
        .alias(aggregatedDfAlias)

      val lastState = microBatchDF.sparkSession.table(unclosedWindowState)

      // update the old state with the new data
      newState = allAggregated
        .join(lastState, Seq(columnStartTime, columnUserType, columnWindowSize), "full_outer")
        .selectExpr(
          s"coalesce($aggregatedDfAlias.$columnStartTime, $unclosedWindowState.$columnStartTime) as $columnStartTime",
          s"coalesce($aggregatedDfAlias.$columnUserType, $unclosedWindowState.$columnUserType) as $columnUserType",
          s"coalesce($aggregatedDfAlias.$columnWindowSize, $unclosedWindowState.$columnWindowSize) as $columnWindowSize",
          s"""
            array_union(
              ifnull($aggregatedDfAlias.$columnDistinctUsers,array()),
              ifnull($unclosedWindowState.$columnDistinctUsers,array())
            ) as $columnDistinctUsers
           """,
        ).cache()

      // the latest window start time indicates the unclosed window
      unclosedWindows = newState
        .groupBy(columnUserType, columnWindowSize)
        .agg(max(columnStartTime).as(columnStartTime))
        .cache()

      // derive the metrics based on the new state
      val allMetrics = newState
        .selectExpr(
          columnStartTime,
          columnUserType,
          columnWindowSize,
          s"size($columnDistinctUsers) as $columnDistinctUsersCount"
        )

      // identify metrics for closed windows
      val closedWindows = allMetrics
        .join(unclosedWindows, Seq(columnStartTime, columnUserType, columnWindowSize), "left_anti")
        .select(
          allMetrics(columnStartTime),
          allMetrics(columnUserType),
          allMetrics(columnWindowSize),
          allMetrics(columnDistinctUsersCount),
        )

      sendMetrics(closedWindows.collect(), batchId)

      val stateToKeep = newState
        .join(unclosedWindows, Seq(columnStartTime, columnUserType, columnWindowSize), "inner")
        .select(
          newState(columnStartTime),
          newState(columnUserType),
          newState(columnWindowSize),
          newState(columnDistinctUsers),
        )

      //      if (batchId % 2 == 1) {
      //        println(s"${getLogTs(batchId)} persisting state.")
      //        //        microBatchDF.sparkSession
      //        //          // The column order and schema must exactly match
      //        //          .createDataFrame(stateToKeep.collectAsList(), stateSchema)
      //        //          .createOrReplaceTempView(unclosedWindowState)
      //        stateToKeep.write.format("memory").mode("overwrite").saveAsTable("chen_guo.state_persist")
      //        microBatchDF.sparkSession.table("chen_guo.state_persist").createOrReplaceTempView(unclosedWindowState)
      //        println(s"${getLogTs(batchId)} loaded back state.")
      //      } else {
      //        stateToKeep.createOrReplaceTempView(unclosedWindowState)
      //      }

      if (batchId % 2 == 1) {
        println(s"${getLogTs(batchId)} before checkpoint.")
        stateToKeep.localCheckpoint().createOrReplaceTempView(unclosedWindowState)
        println(s"${getLogTs(batchId)} after checkpoint.")
      }
    } finally {
      microBatchDF.unpersist()
      if (newState != null) newState.unpersist()
      if (unclosedWindows != null) unclosedWindows.unpersist()
    }
  }
}
