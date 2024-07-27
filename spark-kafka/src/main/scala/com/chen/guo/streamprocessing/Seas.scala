package com.chen.guo.streamprocessing

import com.chen.guo.streamprocessing.Seas.{State, SumByBucket}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode, Trigger}
import org.apache.spark.sql.types._

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset}
import scala.collection.mutable.ListBuffer

object SeasUtils {
  private val TIME_BUCKET_LONG_PATTERN = "yyyyMMddHHmmss"
  private val TIME_BUCKET_LONG_FORMATTER: DateTimeFormatter = DateTimeFormatter.ofPattern(TIME_BUCKET_LONG_PATTERN).withZone(ZoneOffset.UTC)

  /**
    * @param inputColumn the input column to use to derive the bucket start timestamp based on the granularity
    * @param granularity the granularity of the time bucket in seconds
    *                    For example,
    *                    * use 3600 seconds to group events by hour
    *                    * use 60 seconds to group events by minute
    * @return the start timestamp of the time bucket in seconds in the format of yyyyMMddHHmmss of long type
    */
  def getTimeBucketStartTimestamp(inputColumn: Column, granularity: Int): Column =
    date_format(from_unixtime(floor(unix_timestamp(col("timestamp")) / granularity) * granularity), TIME_BUCKET_LONG_PATTERN)
      .cast("long")

  /**
    * Convert a timestamp to a long value in the format of yyyyMMddHHmmss
    *
    * @param timestamp   the timestamp to convert
    * @param granularity the granularity of the time bucket in seconds
    *                    See doc of {@link Seas.SumByBucket.bucketStartTimestamp}
    */
  def timestampToFormattedLong(timestamp: Timestamp, granularity: Int): Long =
    epochToFormattedLong(timestamp.getTime, granularity)

  /**
    * Convert an epoch timestamp long value to a long value in the format of yyyyMMddHHmmss
    */
  def epochToFormattedLong(epoch: Long, granularity: Int): Long = {
    if (epoch < 20000000000L) {
      throw new IllegalArgumentException("Please use milliseconds for this function!")
    }

    if (granularity > 86400)
      throw new IllegalArgumentException("You will get unexpected results if the granularity is larger than a day!")

    val bucketStart: Long = epoch / 1000 / granularity * granularity
    val bucketStartTimestamp: Timestamp = new Timestamp(bucketStart * 1000)
    TIME_BUCKET_LONG_FORMATTER.format(bucketStartTimestamp.toInstant).toLong
  }
}

/**
  * The input event from Kafka which triggers the state update
  *
  * @param id                   the primary key
  * @param timestamp            the original timestamp of the incoming event
  * @param bucketStartTimestamp Same as {@link SumByBucket.bucketStartTimestamp}
  * @param value                the value of the incoming event
  */
case class StateUpdateKafkaEvent(id: String, timestamp: Timestamp, bucketStartTimestamp: Long, value: Long)

/**
  * The output event to be emitted after the state update
  *
  * @param id         : the primary key
  * @param timestamp  : the timestamp of the incoming event that triggered the state update
  * @param runningSum : the running sum of all values in the sliding window for the primary key
  */
case class OutputEvent(id: String, timestamp: Timestamp, runningSum: Long)

/**
  * @param windowSize  the size of the sliding window measured in seconds. The sliding window is defined by the range
  *                    (current_event_timestamp - windowSize, current_event_timestamp].
  *                    The current event timestamp is in the format of yyyyMMddHHmmss.
  *                    Old state out of the sliding window will be dropped.
  * @param granularity the granularity of the time bucket in seconds
  *                    See doc of {@link Seas.SumByBucket.bucketStartTimestamp}
  */
class StateUpdater(val windowSize: Long, val granularity: Int) extends Serializable {
  /**
    * Emit one output event to indicate current running sum for each input event.
    *
    * @param id       the primary key
    * @param events   the incoming events for the primary key that will trigger the state update.
    *                 The events will be sorted by timestamp in ascending order, then processed inside the function.
    * @param oldState the old state for the primary key.
    * @return an iterator of output events for each input event
    */
  def updateStateWithEvents(id: String, events: Iterator[StateUpdateKafkaEvent], oldState: GroupState[State])
  : Iterator[OutputEvent] = {
    if (events.isEmpty) {
      // this can happen
      return Iterator.empty
    }

    val (newState, iterator) = internalUpdate(id, events, oldState.getOption)

    oldState.update(newState)
    iterator
  }

  def internalUpdate(id: String, events: Iterator[StateUpdateKafkaEvent], currentStateOption: Option[State]):
  (State, Iterator[OutputEvent]) = {
    val currentState: State = currentStateOption match {
      case Some(state) => state
      case None => State(id, ListBuffer.empty[SumByBucket], 0L) // initialize an empty state for the new key
    }

    val outputEvents = ListBuffer[OutputEvent]()
    val newBucketSums: ListBuffer[SumByBucket] = currentState.bucketSums
    var newRunningSum: Long = currentState.runningSum
    // Sort the incoming events and iterate through them in order
    for (event <- events.toArray.sortBy(_.bucketStartTimestamp)) {
      if (newBucketSums.isEmpty) {
        // If see an event for the ID for the first time, initialize a new bucket
        newBucketSums += SumByBucket(event.bucketStartTimestamp, event.value)
        newRunningSum = event.value
      } else {
        // Step1: calculate the cut-off timestamp, and drop old state out of the sliding window
        val cutOffTimeEpoch = event.timestamp.getTime / 1000 - windowSize
        val cutOffTimeInclusive = SeasUtils.epochToFormattedLong(cutOffTimeEpoch * 1000, granularity)
        while (newBucketSums.nonEmpty && newBucketSums.head.bucketStartTimestamp <= cutOffTimeInclusive) {
          val dropped = newBucketSums.remove(0)
          newRunningSum -= dropped.bucketSum
        }

        // Step2: update the state with the new event
        newBucketSums.lastOption match {
          case Some(lastBucket) =>
            if (lastBucket.bucketStartTimestamp < event.bucketStartTimestamp) {
              // append a new bucket at the end at constant time
              newBucketSums += SumByBucket(event.bucketStartTimestamp, event.value)
              newRunningSum += event.value
            } else {
              // iterate through the newBucketSums in reverse order to find the right bucket to update or insert
              var done = false
              val bucketIterator = newBucketSums.reverseIterator
              var insertIndex = newBucketSums.length

              while (bucketIterator.hasNext && !done) {
                val currentBucket = bucketIterator.next()
                if (currentBucket.bucketStartTimestamp == event.bucketStartTimestamp) {
                  // update the existing bucket in place
                  currentBucket.bucketSum += event.value
                  newRunningSum += event.value
                  done = true
                } else if (currentBucket.bucketStartTimestamp < event.bucketStartTimestamp) {
                  // Find the right place to insert a new bucket at O(n) time
                  // Ideally, a doubly-linked list will be more efficient for insertion in the middle. However,
                  // such collection is not available in Scala and java.util.LinkedList is not support in Spark.
                  // A good thing is that insertion in the middle will not happen often when there are no late arrival
                  // events, or we set a low watermark threshold to drop most late arrival events.
                  newBucketSums.insert(insertIndex, SumByBucket(event.bucketStartTimestamp, event.value))
                  newRunningSum += event.value
                  done = true
                }
                insertIndex -= 1
              }
            }
          case None =>
            // all states have been dropped, initialize a new bucket
            newBucketSums += SumByBucket(event.bucketStartTimestamp, event.value)
            newRunningSum = event.value
        }
      }

      outputEvents += OutputEvent(id, event.timestamp, newRunningSum)
    }

    val newState = State(id, newBucketSums, newRunningSum)
    (newState, outputEvents.iterator)
  }
}

/**
  * A Spark streaming job that reads events from Kafka, sum up all values of events in the lookback window whose size
  * is determined by {@link Seas.accumulativeWindowSize}. The running sum update is emitted for each input event.
  * 1. The job first reads the initial state from the bootstrap table.
  * 2. Then it reads events from Kafka, and updates the state with the incoming events.
  * TODO: add bootstrap filtering logic
  * - The state out of the lookback window will be dropped, where the lookback window is defined by the range
  * (current_processing_event_timestamp - windowSize, current_processing_event_timestamp].
  * 3. The job emits the running sum for each input event.
  */
object Seas {
  val bootstrapTable = "users.chen_guo.sample"
  val accumulativeWindowSize = 3600
  val granularity = 1800
  val broker = "xxx"
  val spark = SparkSession.builder.appName("emit-aggregated-upon-receival").getOrCreate()

  def readKafkaTopic(topicName: String): DataFrame = {
    spark
      .readStream
      .format("kafka")
      .option("subscribe", topicName)
      .load()
  }

  val schema: StructType = StructType(Array(
    StructField("id", IntegerType),
    StructField("timestamp", TimestampType),
    StructField("value", IntegerType)
  ))

  /**
    * @param bucketStartTimestamp timestamp in seconds in the format of yyyyMMddHHmmss
    *                             Different events should be grouped by a bucketStartTimestamp defined by the user.
    *                             For example,
    *                             If group events by hour, then the bucketStartTimestamp should be like
    *                             20240726140000  to represent the time bucket [2024-07-26 14:00:00, 2024-07-26 15:00:00).
    *                             If group events by minute, then the bucketStartTimestamp should be like
    *                             20240726140300 to represent the time bucket [2024-07-26 14:03:00, 2024-07-26 14:04:00).
    *
    *                             Note that the more granular the bucket, the more buckets will be created, and the more
    *                             accurate the result will be, and the more memory will be used!
    * @param bucketSum            the sum of all values from events in time bucket defined by the bucketStartTimestamp
    */
  case class SumByBucket(bucketStartTimestamp: Long, var bucketSum: Long)

  /**
    * @param id         the primary key
    * @param bucketSums the list of valid SumByBuckets in a sliding window defined
    *                   (current_event_timestamp - windowSize, current_event_timestamp]
    *
    *                   !!!The value MUST be sorted by bucketStartTimestamp in ascending order when it's passed in!!!
    *
    *                   Use ListBuffer implementation to allow efficient removal of the head element, and
    *                   efficient appending of new elements.
    *                   https://docs.scala-lang.org/overviews/collections-2.13/concrete-mutable-collection-classes.html#list-buffers
    */
  case class State(id: String, bucketSums: ListBuffer[SumByBucket], runningSum: Long)

  /**
    * @param id                   the primary key
    * @param bucketStartTimestamp Same as {@link SumByBucket.bucketStartTimestamp}
    * @param bucketSum            Same as {@link SumByBucket.bucketSum}
    */
  case class StateInputFromBootstrap(id: String, bucketStartTimestamp: Long, bucketSum: Long)

  private val bootstrapAgg = spark.table(bootstrapTable)
    .withColumn("bucketStartTimestamp", SeasUtils.getTimeBucketStartTimestamp(col("timestamp"), granularity))
    .groupBy("id", "bucketStartTimestamp")
    .agg(sum("value").as("bucketSum"))

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
        bucketSums += SumByBucket(row.bucketStartTimestamp, row.bucketSum)
      }
      // Sort bucketSums and initialize the State
      State(id, bucketSums.sortBy(_.bucketStartTimestamp), runningSum)
    }
    )(Encoders.product[State])

  bootstrapState.show(truncate = false)
  val initialState: KeyValueGroupedDataset[String, State] = bootstrapState.groupByKey(_.id)(Encoders.STRING)

  val outputDf: Dataset[OutputEvent] = readKafkaTopic("test_topic-1")
    .select(from_json(col("value").cast("string"), schema).as("payload"))
    .selectExpr("payload.*")
    .withWatermark("timestamp", "0 seconds")
    .select(
      col("id"),
      col("timestamp"),
      SeasUtils.getTimeBucketStartTimestamp(col("timestamp"), granularity).as("bucketStartTimestamp"),
      col("value").cast("long").as("value")
    )
    .as[StateUpdateKafkaEvent](Encoders.product[StateUpdateKafkaEvent])
    .groupByKey(_.id)(Encoders.STRING)
    .flatMapGroupsWithState(
      OutputMode.Update(),
      GroupStateTimeout.NoTimeout,
      initialState
    )(new StateUpdater(accumulativeWindowSize, granularity).updateStateWithEvents)(Encoders.product[State], Encoders.product[OutputEvent])


  val query = outputDf.writeStream
    .outputMode(OutputMode.Update)
    .format("console")
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .start()

  query.awaitTermination()
}

object main extends App {
  private def utcTimestamp(dateTimeString: String): Timestamp = {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssZ")
    // Parse the string to a LocalDateTime
    val localDateTime = LocalDateTime.parse(dateTimeString, formatter)
    Timestamp.from(localDateTime.atZone(ZoneOffset.UTC).toInstant)
  }

  assert(SeasUtils.timestampToFormattedLong(utcTimestamp("2024-07-25 23:51:07+0000"), 1) == 20240725235107L)
  assert(SeasUtils.timestampToFormattedLong(utcTimestamp("2024-07-25 23:51:07+0000"), 5) == 20240725235105L)
  assert(SeasUtils.timestampToFormattedLong(utcTimestamp("2024-07-25 23:51:07+0000"), 10) == 20240725235100L)
  assert(SeasUtils.timestampToFormattedLong(utcTimestamp("2024-07-25 23:51:07+0000"), 30) == 20240725235100L)
  assert(SeasUtils.timestampToFormattedLong(utcTimestamp("2024-07-25 23:51:07+0000"), 60) == 20240725235100L)
  assert(SeasUtils.timestampToFormattedLong(utcTimestamp("2024-07-25 23:51:07+0000"), 300) == 20240725235000L)
  assert(SeasUtils.timestampToFormattedLong(utcTimestamp("2024-07-25 23:51:07+0000"), 600) == 20240725235000L)
  assert(SeasUtils.timestampToFormattedLong(utcTimestamp("2024-07-25 23:51:07+0000"), 1800) == 20240725233000L)
  assert(SeasUtils.timestampToFormattedLong(utcTimestamp("2024-07-25 23:51:07+0000"), 3600) == 20240725230000L)
  assert(SeasUtils.timestampToFormattedLong(utcTimestamp("2024-07-25 23:51:07+0000"), 3600 * 4) == 20240725200000L)
  assert(SeasUtils.timestampToFormattedLong(utcTimestamp("2024-07-25 23:51:07+0000"), 3600 * 6) == 20240725180000L)
  assert(SeasUtils.timestampToFormattedLong(utcTimestamp("2024-07-25 23:51:07+0000"), 3600 * 8) == 20240725160000L)
  assert(SeasUtils.timestampToFormattedLong(utcTimestamp("2024-07-25 23:51:07+0000"), 3600 * 12) == 20240725120000L)
  assert(SeasUtils.timestampToFormattedLong(utcTimestamp("2024-07-25 23:51:07+0000"), 86400) == 20240725000000L)

  private def buildTestEvent(id: String, timestamp: Timestamp, value: Long, granularity: Int): StateUpdateKafkaEvent = {
    StateUpdateKafkaEvent(id, timestamp, SeasUtils.timestampToFormattedLong(timestamp, granularity), value)
  }

  private val windowSize = 3600
  private val granularity = 1800
  private val stateUpdater = new StateUpdater(windowSize, granularity)

  private var currentState = State("1", ListBuffer(
    SumByBucket(20240725000000L, 3L),
    SumByBucket(20240725003000L, 12L),
    SumByBucket(20240725010000L, 5L),
  ), 20L)
  val output1 = stateUpdater.internalUpdate("1",
    List(buildTestEvent("1", utcTimestamp("2024-07-25 00:51:01+0000"), 1L, granularity)).iterator,
    Some(currentState)
  )

  currentState = State("1", ListBuffer(
    SumByBucket(20240725000000L, 3L),
    SumByBucket(20240725003000L, 13L),
    SumByBucket(20240725010000L, 5L),
  ), 21L)
  assert(output1._1 == currentState)
  assert(output1._2.toList == List(
    OutputEvent("1", utcTimestamp("2024-07-25 00:51:01+0000"), 21L)
  ))

  val output2 = stateUpdater.internalUpdate("1",
    List(buildTestEvent("1", utcTimestamp("2024-07-25 01:01:01+0000"), 1L, granularity)).iterator,
    Some(currentState)
  )
  currentState = State("1", ListBuffer(
    SumByBucket(20240725003000L, 13L),
    SumByBucket(20240725010000L, 6L),
  ), 19L)
  assert(output2._1 == currentState)
  assert(output2._2.toList == List(
    OutputEvent("1", utcTimestamp("2024-07-25 01:01:01+0000"), 19L)
  ))
}