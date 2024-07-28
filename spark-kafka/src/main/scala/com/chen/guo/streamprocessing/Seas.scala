package com.chen.guo.streamprocessing

import com.chen.guo.constant.GlobalKafkaSettings._
import com.chen.guo.streamprocessing.Seas.{State, SumByBucket}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset}
import scala.collection.mutable.ListBuffer

object SeasUtil extends Serializable {
  /**
    * @param column      the base column for deriving the bucket start timestamp based on the bucketWidth
    * @param bucketWidth the width of the time bucket in seconds. For example,
    *                    * use 3600 seconds to bucket events by hour for aggregation
    *                    * use 60 seconds to bucket events by minute for aggregation
    * @return a timestamp column indicating the start timestamp of the time bucket whose width is defined by bucketWidth
    */
  def getBucketStartTimestampColumn(column: Column, bucketWidth: Int): Column =
    from_unixtime(floor(unix_timestamp(column) / bucketWidth) * bucketWidth).cast("timestamp")

  /**
    * @param timestamp   the timestamp to convert
    * @param bucketWidth the width of the time bucket in seconds
    * @return the start timestamp of the time bucket for a timestamp based on bucketWidth
    */
  def getBucketStartFromTimestamp(timestamp: Timestamp, bucketWidth: Int): Timestamp =
    getBucketStartFromEpoch(timestamp.getTime, bucketWidth)

  /**
    * @return the start timestamp of the time bucket for an epoch long value based on bucketWidth
    */
  def getBucketStartFromEpoch(epoch: Long, bucketWidth: Int): Timestamp = {
    if (epoch < 20000000000L) {
      throw new IllegalArgumentException("Please use milliseconds for this function!")
    }
    if (bucketWidth > 86400) {
      throw new IllegalArgumentException("You can get unexpected results if the bucketWidth is larger than a day!")
    }

    val bucketStart: Long = epoch / 1000 / bucketWidth * bucketWidth
    new Timestamp(bucketStart * 1000)
  }
}

/**
  * The input event from Kafka which triggers the state update
  *
  * @param id          the primary key
  * @param timestamp   the timestamp of an incoming event which will be used for watermarking and bucketing
  * @param bucketStart Same as {@link SumByBucket.bucketStart}
  * @param value       the value of the incoming event to be aggregated
  */
case class StateUpdateKafkaEvent(id: String, timestamp: Timestamp, bucketStart: Timestamp, value: Long)

/**
  * The event to be emitted after the state update caused by an incoming {@link StateUpdateKafkaEvent}.
  * It carries the state information when the update happens.
  *
  * @param id         : the primary key
  * @param timestamp  : the {@link StateUpdateKafkaEvent.timestamp} of the incoming {@link StateUpdateKafkaEvent}
  *                   that triggered the state update
  * @param runningSum : the running sum of all values in the sliding window for the primary key in current state
  */
case class OutputEvent(id: String, timestamp: Timestamp, runningSum: Long)

/**
  * @param windowSize  the width of the sliding window measured in seconds.
  *                    The sliding window is defined as the bucketStart for the timestamp range
  *                    (current_processing_event_timestamp - windowSize, current_processing_event_timestamp].
  *                    Old state out of the sliding window will be dropped.
  * @param bucketWidth the width of a time bucket in seconds
  */
class StateUpdater(val windowSize: Long, val bucketWidth: Int) extends Serializable {
  /**
    * Emit one {@link OutputEvent} to indicate current state and running sum for each input {@link StateUpdateKafkaEvent}
    *
    * @param id       the primary key
    * @param events   the incoming {@link StateUpdateKafkaEvent}s for the primary key that will trigger the state update.
    *                 The events will be firstly sorted by {@link StateUpdateKafkaEvent.timestamp} in ascending order,
    *                 then be used to update the current state one by one.
    * @param oldState the old state for the primary key
    * @return an iterator of {@link OutputEvent}s
    */
  def updateStateWithEvents(id: String, events: Iterator[StateUpdateKafkaEvent], oldState: GroupState[State])
  : Iterator[OutputEvent] = {
    if (events.isEmpty) {
      // this can happen
      return Iterator.empty
    }
    val (newState, iterator) = processEvents(id, events, oldState.getOption)
    oldState.update(newState)
    iterator
  }

  /**
    * Process the incoming {@link StateUpdateKafkaEvent}s one by one and update the state accordingly
    */
  def processEvents(id: String, events: Iterator[StateUpdateKafkaEvent], currentStateOption: Option[State]):
  (State, Iterator[OutputEvent]) = {
    val currentState: State = currentStateOption match {
      case Some(state) => state
      case None => State(id, ListBuffer.empty[SumByBucket], 0L) // initialize an empty state for the new key
    }

    val outputEvents = ListBuffer[OutputEvent]()
    // the currentState.bucketSums is in a sorted order by the time bucket, and should always be maintained in that way
    val newBucketSums: ListBuffer[SumByBucket] = currentState.bucketSums
    var newRunningSum: Long = currentState.runningSum
    // Sort the incoming events based on the bucketStart in ascending order and iterate through them.
    // Note that late arrival events will be processed in the order of their timestamps even if they are
    // sent out of order!
    for (event <- events.toArray.sortWith((x, y) => x.timestamp.before(y.timestamp))) {
      // runningSumForFutureBuckets keeps track of the bucketSums which happen after the late arrival events
      var runningSumForFutureBuckets = 0L

      if (newBucketSums.isEmpty) {
        // If see an event for the ID for the first time, initialize a new bucket
        newBucketSums += SumByBucket(event.bucketStart, event.value)
        newRunningSum = event.value
      } else {
        // Step1: calculate the cut-off timestamp, and drop old state out of the sliding window
        val cutOffTimeEpoch = event.timestamp.getTime / 1000 - windowSize
        val cutOffTimeInclusive = SeasUtil.getBucketStartFromEpoch(cutOffTimeEpoch * 1000, bucketWidth)
        while (newBucketSums.nonEmpty && (newBucketSums.head.bucketStart.compareTo(cutOffTimeInclusive) <= 0)) {
          val dropped = newBucketSums.remove(0)
          newRunningSum -= dropped.bucketSum
        }

        // Step2: update the state with the new event
        newBucketSums.lastOption match {
          case Some(lastBucket) =>
            if (lastBucket.bucketStart.compareTo(event.bucketStart) < 0) {
              // append a new bucket at the end at constant time
              newBucketSums += SumByBucket(event.bucketStart, event.value)
              newRunningSum += event.value
            } else {
              // iterate through the newBucketSums in reverse order to find the right bucket to update or insert
              var done = false
              val bucketIterator = newBucketSums.reverseIterator
              var insertIndex = newBucketSums.length

              while (bucketIterator.hasNext && !done) {
                val currentBucket = bucketIterator.next()
                val order = event.bucketStart.compareTo(currentBucket.bucketStart)
                if (order == 0) {
                  // the event belongs to current bucket: update the existing bucket in place
                  currentBucket.bucketSum += event.value
                  newRunningSum += event.value
                  done = true
                } else if (order > 0) {
                  // the event needs to be inserted to the right of current bucket
                  /**
                    * ListBuffer gives O(1) time complexity for removing the head, and add to the tail
                    * https://docs.scala-lang.org/overviews/collections-2.13/concrete-mutable-collection-classes.html#list-buffers
                    * But it takes O(n) to find the right place to insert a new bucket.
                    *
                    * The good thing is that insertion in the middle(most likely towards the end of the list) will not
                    * happen when there is no late arrival events. Or, we set a low or 0 watermark threshold to drop
                    * most late arrival events to reduce such operations.
                    *
                    * If frequent insertions cannot be avoided, we need to switch the implementation to use the Vector
                    * class, which provides O(log32\N) time complexity for most operations: add/remove head,
                    * add/remove tail, slice and concatenate. Alternatively, a customized doubly-linked list
                    * implementation will achieve the best performance for APIs frequently used in this code.
                    */
                  newBucketSums.insert(insertIndex, SumByBucket(event.bucketStart, event.value))
                  newRunningSum += event.value
                  done = true
                } else {
                  // the event is earlier than current bucket
                  runningSumForFutureBuckets += currentBucket.bucketSum
                }
                insertIndex -= 1
              }

              if (!done) {
                assert(insertIndex == 0, "we must be at the head of the ListBuffer if the new event is not used!")
                newBucketSums.prepend(SumByBucket(event.bucketStart, event.value))
                newRunningSum += event.value
              }
            }
          case None =>
            // all states have been dropped, initialize a new bucket
            newBucketSums += SumByBucket(event.bucketStart, event.value)
            newRunningSum = event.value
        }
      }
      // emit an output event for each incoming event
      outputEvents += OutputEvent(id, event.timestamp, newRunningSum - runningSumForFutureBuckets)
    }

    val newState = State(id, newBucketSums, newRunningSum)
    (newState, outputEvents.iterator)
  }
}

/**
  * A Spark streaming job that reads events from Kafka, sum up all values from events in a sliding window whose size
  * is determined by {@link Seas.slidingWindowSize}. The running sum update is emitted for each input event.
  * 1. The job first loads the initial state from the bootstrap table.
  * - A state-load cutoff timestamp will be derived such that only states before this cutoff timestamp will be loaded
  * in a batch way
  * 2. Then it reads events from Kafka, and updates the state based on each incoming event.
  * - Only events after the state-load cutoff timestamp will be processed
  * - The state dropping out of the sliding window will be discarded, which is defined as the bucket start timestamp
  * for the range (current_processing_event_timestamp - windowSize, current_processing_event_timestamp].
  * - An output event will be emitted for each incoming one
  */
object Seas {
  val bootstrapTableName = "users.chen_guo.seas_sample"
  val slidingWindowSize = 3600
  val bucketWidth = 1800
  val colTimestamp = "timestamp" // the timestamp column used for watermarking and time bucketing
  val colBucketStart = "bucketStart" // a derived column from "colTimestamp" to represent the time bucket start timestamp
  val columnId = "id" // the primary key column
  val colValue = "value" // the column whose running sum will be calculated
  /**
    * Go back 1 minute, which is more than enough, from current high watermark of the bootstrap table to ensure
    * data completeness. Then use this timestamp to calculate the bootstrap cut-off timestamp by deriving its
    * time bucket start timestamp.
    */
  val bootstrapBackoffSeconds = 60
  val sourceTopicName = "test_topic-1"
  val spark = SparkSession.builder.appName("emit-sliding-window-aggregation-upon-new-event").getOrCreate()

  def readKafkaTopic(topicName: String): DataFrame = {
    spark
      .readStream
      .format("kafka")
      .option("subscribe", topicName)
      .option("kafka.bootstrap.servers", BROKER_URL)
      .option("kafka.security.protocol", "SSL")
      .option("kafka.ssl.truststore.location", TRUST_STORE_LOCATION)
      .option("kafka.ssl.truststore.password", TRUST_STORE_PASSWORD)
      .option("kafka.ssl.keystore.location", KEY_STORE_LOCATION)
      .option("kafka.ssl.keystore.password", KEY_STORE_PASSWORD)
      .option("kafka.ssl.key.password", KEY_PASSWORD)
      .option("startingOffsets", "latest")
      .load()
  }

  val schema: StructType = StructType(Array(
    StructField(columnId, StringType),
    StructField(colTimestamp, TimestampType),
    StructField(colValue, IntegerType)
  ))

  /**
    * @param bucketStart different events will be grouped by a bucket defined by the user. For example,
    *                    If group or bucket events by hour, the bucketStart will be the left bound of the range
    *                    [2024-07-26 14:00:00, 2024-07-26 15:00:00) for all event timestamps in this range.
    *                    If group events by minute, the bucketStart will be the left bound of the range
    *                    [2024-07-26 14:03:00, 2024-07-26 14:04:00) for all event timestamps in this range.
    *
    *                    Note that the more granular the bucket, the more buckets will be created, and the more
    *                    accurate the result will be, and the more memory will be used!
    * @param bucketSum   the sum of all values from events in time bucket defined by the bucketStart
    */
  case class SumByBucket(bucketStart: Timestamp, var bucketSum: Long)

  /**
    * @param id         the primary key
    * @param bucketSums a list of unexpired {@link SumByBucket}s in a sliding window as the bucket start for the range
    *                   (current_processing_event_timestamp - windowSize, current_processing_event_timestamp]
    *
    *                   !!! The input list MUST be sorted by {@link SumByBucket.bucketStart} in an ascending order
    *                   to let the code generate the right results!!!
    */
  case class State(id: String, bucketSums: ListBuffer[SumByBucket], runningSum: Long)

  /**
    * @param id          the primary key
    * @param bucketStart Same as {@link SumByBucket.bucketStart}
    * @param bucketSum   Same as {@link SumByBucket.bucketSum}
    */
  case class StateInputFromBootstrap(id: String, bucketStart: Timestamp, bucketSum: Long)

  val bootstrapTable: DataFrame = spark.table(bootstrapTableName)
  val bootstrapHighWatermark: Timestamp = bootstrapTable.agg(max(colTimestamp)).collect()(0)(0).asInstanceOf[Timestamp]
  /**
    * All bootstrap data >= bootstrapCutOffBucketStart will be discarded, and the last bucket from the bootstrap table
    * will be complete.
    * Only events >= bootstrapCutOffEpoch will be processed so that the incremental updates can seamlessly connect
    * with the bootstrap data.
    */
  val bootstrapCutOffBucketStart: Timestamp = SeasUtil.getBucketStartFromEpoch(
    bootstrapHighWatermark.getTime - bootstrapBackoffSeconds * 1000, bucketWidth)

  private val bootstrapAgg = bootstrapTable
    .where(col(colTimestamp).lt(bootstrapCutOffBucketStart))
    .withColumn(colBucketStart, SeasUtil.getBucketStartTimestampColumn(col(colTimestamp), bucketWidth))
    .groupBy(columnId, colBucketStart)
    .agg(sum(colValue).as("bucketSum"))

  // Load the initial state from bootstrapAgg
  val bootstrapState: Dataset[State] = bootstrapAgg
    .as[StateInputFromBootstrap](Encoders.product[StateInputFromBootstrap])
    .groupByKey(_.id)(Encoders.STRING)
    .mapGroups((id, rows) => {
      var runningSum = 0L
      val bucketSums: ListBuffer[SumByBucket] = ListBuffer.empty[SumByBucket]
      while (rows.hasNext) {
        val row = rows.next()
        runningSum += row.bucketSum
        bucketSums += SumByBucket(row.bucketStart, row.bucketSum)
      }
      // Sort bucketSums and initialize the State
      State(id,
        bucketSums.sortWith((x, y) => x.bucketStart.before(y.bucketStart)),
        runningSum
      )
    })(Encoders.product[State])
  bootstrapState.show(truncate = false)
  val initialState: KeyValueGroupedDataset[String, State] = bootstrapState.groupByKey(_.id)(Encoders.STRING)

  val outputDf: Dataset[OutputEvent] = readKafkaTopic(sourceTopicName)
    .select(from_json(col(colValue).cast("string"), schema).as("payload"))
    .selectExpr("payload.*")
    .where(col(colTimestamp).geq(bootstrapCutOffBucketStart)) // only accept events >= the bootstrap cut-off timestamp
    .withWatermark(colTimestamp, "0 seconds") // do not accept late arrivals
    .select(
      col(columnId),
      col(colTimestamp),
      SeasUtil.getBucketStartTimestampColumn(col(colTimestamp), bucketWidth).as(colBucketStart),
      col(colValue).cast("long").as(colValue)
    )
    .as[StateUpdateKafkaEvent](Encoders.product[StateUpdateKafkaEvent])
    .groupByKey(_.id)(Encoders.STRING)
    .flatMapGroupsWithState(
      OutputMode.Update(),
      GroupStateTimeout.NoTimeout,
      initialState
    )(new StateUpdater(slidingWindowSize, bucketWidth).updateStateWithEvents)(
      Encoders.product[State], Encoders.product[OutputEvent])

  val query: StreamingQuery = outputDf.writeStream
    .outputMode(OutputMode.Update)
    .format("console")
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .start()

  query.awaitTermination()
}

object main extends App {
  private def utcTimestamp(dateTimeString: String): Timestamp = {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSSZ")
    // Parse the string to a LocalDateTime
    val localDateTime: LocalDateTime = LocalDateTime.parse(dateTimeString, formatter)
    Timestamp.from(localDateTime.atZone(ZoneOffset.UTC).toInstant)
  }

  assert(SeasUtil.getBucketStartFromTimestamp(utcTimestamp("2024-07-25 23:51:07.000000000+0000"), 1) == utcTimestamp("2024-07-25 23:51:07.000000000+0000"))
  assert(SeasUtil.getBucketStartFromTimestamp(utcTimestamp("2024-07-25 23:51:07.000000000+0000"), 5) == utcTimestamp("2024-07-25 23:51:05.000000000+0000"))
  assert(SeasUtil.getBucketStartFromTimestamp(utcTimestamp("2024-07-25 23:51:07.000000000+0000"), 10) == utcTimestamp("2024-07-25 23:51:00.000000000+0000"))
  assert(SeasUtil.getBucketStartFromTimestamp(utcTimestamp("2024-07-25 23:51:07.000000000+0000"), 30) == utcTimestamp("2024-07-25 23:51:00.000000000+0000"))
  assert(SeasUtil.getBucketStartFromTimestamp(utcTimestamp("2024-07-25 23:51:07.000000000+0000"), 60) == utcTimestamp("2024-07-25 23:51:00.000000000+0000"))
  assert(SeasUtil.getBucketStartFromTimestamp(utcTimestamp("2024-07-25 23:51:07.000000000+0000"), 300) == utcTimestamp("2024-07-25 23:50:00.000000000+0000"))
  assert(SeasUtil.getBucketStartFromTimestamp(utcTimestamp("2024-07-25 23:51:07.000000000+0000"), 600) == utcTimestamp("2024-07-25 23:50:00.000000000+0000"))
  assert(SeasUtil.getBucketStartFromTimestamp(utcTimestamp("2024-07-25 23:51:07.000000000+0000"), 1800) == utcTimestamp("2024-07-25 23:30:00.000000000+0000"))
  assert(SeasUtil.getBucketStartFromTimestamp(utcTimestamp("2024-07-25 23:51:07.000000000+0000"), 3600) == utcTimestamp("2024-07-25 23:00:00.000000000+0000"))
  assert(SeasUtil.getBucketStartFromTimestamp(utcTimestamp("2024-07-25 23:51:07.000000000+0000"), 3600 * 4) == utcTimestamp("2024-07-25 20:00:00.000000000+0000"))
  assert(SeasUtil.getBucketStartFromTimestamp(utcTimestamp("2024-07-25 23:51:07.000000000+0000"), 3600 * 6) == utcTimestamp("2024-07-25 18:00:00.000000000+0000"))
  assert(SeasUtil.getBucketStartFromTimestamp(utcTimestamp("2024-07-25 23:51:07.000000000+0000"), 3600 * 8) == utcTimestamp("2024-07-25 16:00:00.000000000+0000"))
  assert(SeasUtil.getBucketStartFromTimestamp(utcTimestamp("2024-07-25 23:51:07.000000000+0000"), 3600 * 12) == utcTimestamp("2024-07-25 12:00:00.000000000+0000"))
  assert(SeasUtil.getBucketStartFromTimestamp(utcTimestamp("2024-07-25 23:51:07.000000000+0000"), 86400) == utcTimestamp("2024-07-25 00:00:00.000000000+0000"))

  private def buildTestEvent(id: String, timestamp: Timestamp, value: Long, bucketWidth: Int): StateUpdateKafkaEvent = {
    StateUpdateKafkaEvent(id, timestamp, SeasUtil.getBucketStartFromTimestamp(timestamp, bucketWidth), value)
  }

  private val windowSize = 3600
  private val bucketWidth = 1800
  // window size two times the bucketWidth, so only two buckets will be kept in the state
  private val StateUpdater2 = new StateUpdater(windowSize, bucketWidth)

  // Initialize with an empty state
  val output0 = StateUpdater2.processEvents("1",
    List(
      buildTestEvent("1", utcTimestamp("2024-07-25 00:00:01.000000000+0000"), 1L, bucketWidth),
      buildTestEvent("1", utcTimestamp("2024-07-25 00:01:01.000000000+0000"), 2L, bucketWidth),
      buildTestEvent("1", utcTimestamp("2024-07-25 00:31:01.000000000+0000"), 10L, bucketWidth),
      buildTestEvent("1", utcTimestamp("2024-07-25 00:32:01.000000000+0000"), 2L, bucketWidth),
      buildTestEvent("1", utcTimestamp("2024-07-25 01:01:01.000000000+0000"), 5L, bucketWidth),
    ).iterator,
    None
  )
  assert(output0._1 == State("1", ListBuffer(
    SumByBucket(utcTimestamp("2024-07-25 00:30:00.000000000+0000"), 12L),
    SumByBucket(utcTimestamp("2024-07-25 01:00:00.000000000+0000"), 5L),
  ), 17))
  assert(output0._2.toList == List(
    OutputEvent("1", utcTimestamp("2024-07-25 00:00:01.000000000+0000"), 1L),
    OutputEvent("1", utcTimestamp("2024-07-25 00:01:01.000000000+0000"), 3L),
    OutputEvent("1", utcTimestamp("2024-07-25 00:31:01.000000000+0000"), 13L),
    OutputEvent("1", utcTimestamp("2024-07-25 00:32:01.000000000+0000"), 15L),
    OutputEvent("1", utcTimestamp("2024-07-25 01:01:01.000000000+0000"), 17L), // 17 = 15 + 5 - 3
  ))

  var currentState = State("1", ListBuffer(
    SumByBucket(utcTimestamp("2024-07-25 00:00:00.000000000+0000"), 3L),
    SumByBucket(utcTimestamp("2024-07-25 00:30:00.000000000+0000"), 12L),
    SumByBucket(utcTimestamp("2024-07-25 01:00:00.000000000+0000"), 5L),
  ), 20L)

  // 1st batch of events
  val output1 = StateUpdater2.processEvents("1",
    // a late arrival event
    List(buildTestEvent("1", utcTimestamp("2024-07-25 00:51:01.000000000+0000"), 1L, bucketWidth)).iterator,
    Some(currentState)
  )
  currentState = State("1", ListBuffer(
    SumByBucket(utcTimestamp("2024-07-25 00:00:00.000000000+0000"), 3L),
    SumByBucket(utcTimestamp("2024-07-25 00:30:00.000000000+0000"), 13L),
    SumByBucket(utcTimestamp("2024-07-25 01:00:00.000000000+0000"), 5L),
  ), 21L)
  assert(output1._1 == currentState)
  assert(output1._2.toList == List(
    /**
      * this is the output for a late arrival event, all buckets later than this event cannot be included!
      * 16 = (3) + (13 + 1)
      * - 3 is from the first bucket: ["2024-07-25 00:00:00.000000000+0000", "2024-07-25 00:30:00.000000000+0000")
      * - 13 is from the second bucket: ["2024-07-25 00:30:00.000000000+0000", "2024-07-25 01:00:00.000000000+0000")
      * - 1 is from the event itself
      * - 5 cannot be aggregated into the running sum because it belongs to a future bucket for
      * current late arrival event
      */
    OutputEvent("1", utcTimestamp("2024-07-25 00:51:01.000000000+0000"), 16L)
  ))

  // 2nd batch of events
  val output2 = StateUpdater2.processEvents("1",
    List(buildTestEvent("1", utcTimestamp("2024-07-25 01:01:01.000000000+0000"), 1L, bucketWidth)).iterator,
    Some(currentState)
  )
  currentState = State("1", ListBuffer(
    SumByBucket(utcTimestamp("2024-07-25 00:30:00.000000000+0000"), 13L),
    SumByBucket(utcTimestamp("2024-07-25 01:00:00.000000000+0000"), 6L),
  ), 19L)
  assert(output2._1 == currentState)
  assert(output2._2.toList == List(
    OutputEvent("1", utcTimestamp("2024-07-25 01:01:01.000000000+0000"), 19L)
  ))

  // 3rd batch of events
  val output3 = StateUpdater2.processEvents("1",
    List(
      buildTestEvent("1", utcTimestamp("2024-07-25 01:11:01.000000000+0000"), 1L, bucketWidth),
      buildTestEvent("1", utcTimestamp("2024-07-25 01:31:01.000000000+0000"), 1L, bucketWidth)
    ).iterator,
    Some(currentState)
  )
  currentState = State("1", ListBuffer(
    SumByBucket(utcTimestamp("2024-07-25 01:00:00.000000000+0000"), 7L),
    SumByBucket(utcTimestamp("2024-07-25 01:30:00.000000000+0000"), 1L),
  ), 8L)
  assert(output3._1 == currentState)
  assert(output3._2.toList == List(
    OutputEvent("1", utcTimestamp("2024-07-25 01:11:01.000000000+0000"), 20L),
    OutputEvent("1", utcTimestamp("2024-07-25 01:31:01.000000000+0000"), 8L)
  ))

  // 4th batch of events
  val output4 = StateUpdater2.processEvents("1",
    List(
      // test out-of-order incoming events and make sure sorting works for nano-seconds
      buildTestEvent("1", utcTimestamp("2024-07-25 03:32:01.000000003+0000"), 8L, bucketWidth),
      buildTestEvent("1", utcTimestamp("2024-07-25 03:32:01.000000001+0000"), 4L, bucketWidth),
      buildTestEvent("1", utcTimestamp("2024-07-25 03:32:01.000000002+0000"), 2L, bucketWidth),
      buildTestEvent("1", utcTimestamp("2024-07-25 03:01:01.000000000+0000"), 1L, bucketWidth),
      buildTestEvent("1", utcTimestamp("2024-07-25 00:31:01.000000000+0000"), 99L, bucketWidth), // late arrival
    ).iterator,
    Some(currentState)
  )
  currentState = State("1", ListBuffer(
    SumByBucket(utcTimestamp("2024-07-25 03:00:00.000000000+0000"), 1L),
    SumByBucket(utcTimestamp("2024-07-25 03:30:00.000000000+0000"), 14L),
  ), 15L)
  assert(output4._1 == currentState)
  assert(output4._2.toList == List(
    // events with earlier timestamp will be processed first even if they are sent out of order

    // the late arrival event creates a new bucketSum state for its own
    OutputEvent("1", utcTimestamp("2024-07-25 00:31:01.000000000+0000"), 99L),
    OutputEvent("1", utcTimestamp("2024-07-25 03:01:01.000000000+0000"), 1L),
    OutputEvent("1", utcTimestamp("2024-07-25 03:32:01.000000001+0000"), 5L),
    OutputEvent("1", utcTimestamp("2024-07-25 03:32:01.000000002+0000"), 7L),
    OutputEvent("1", utcTimestamp("2024-07-25 03:32:01.000000003+0000"), 15L)
  ))

  // 5th batch of events
  // Check the case when there is no incoming event
  val output5 = StateUpdater2.processEvents("1", List().iterator, Some(currentState))
  assert(output5._1 == currentState)
  assert(output5._2.isEmpty)
}