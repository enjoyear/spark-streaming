package chen.guo.udaf

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}

// Define a case class for the input data type
case class InputRow(order: Long, data: Double)

// Define a case class for the buffer schema
case class Buffer(var maxOrder: Long, var maxData: Double)

object LatestEventByKey extends Aggregator[InputRow, Buffer, Double] {

  val spark = SparkSession.builder.appName("LatestEventByKey").getOrCreate()

  import spark.implicits._

  // A zero value for this aggregation. Should satisfy the property that any b + zero = b.
  def zero: Buffer = Buffer(0L, Double.MinValue)

  // Combine two values to produce a new value.
  // For performance, the function may modify b and return it instead of constructing new object for b.
  def reduce(buffer: Buffer, input: InputRow): Buffer = {
    if (input.order > buffer.maxOrder) {
      buffer.maxOrder = input.order
      buffer.maxData = input.data
    }
    buffer
  }

  // Merge two intermediate values.
  def merge(b1: Buffer, b2: Buffer): Buffer = {
    if (b2.maxOrder > b1.maxOrder) {
      b1.maxOrder = b2.maxOrder
      b1.maxData = b2.maxData
    }
    b1
  }

  // Transform the output of the reduction.
  def finish(reduction: Buffer): Double = reduction.maxData

  // Specify the Encoder for intermediate value type (buffer schema)
  def bufferEncoder: Encoder[Buffer] = Encoders.product[Buffer]

  // Specify the Encoder for final output value type
  def outputEncoder: Encoder[Double] = Encoders.scalaDouble

  val df = Seq(
    (1, 10L, 100.0),
    (1, 20L, 200.0),
    (1, 15L, 150.0),
    (2, 10L, 400.0),
    (2, 25L, 100.0),
    (2, 5L, 300.0)
  ).toDF("id", "order", "data")

  /**
    * This solution below will fail due to serialization issue.
    */
  //val latestEventAggregator = LatestEventByKey.toColumn.name("max_order_data")
  //df.groupBy("id").agg(latestEventAggregator($"order", $"data")).show()

  df.createOrReplaceGlobalTempView("events")
  spark.udf.register("latest_event", functions.udaf(LatestEventByKey))
  spark.sql("SELECT id, latest_event(order, data) FROM global_temp.events group by id").show()
}

