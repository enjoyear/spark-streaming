package chen.guo.udaf

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}

val spark = SparkSession.builder.appName("MaxOrderDataUDAF").getOrCreate()
import spark.implicits._

case class Average(var sum: Long, var count: Long)

object MyAverage extends Aggregator[Long, Average, Double] {
  // A zero value for this aggregation. Should satisfy the property that any b + zero = b
  def zero: Average = Average(0L, 0L)

  // Combine two values to produce a new value. For performance, the function may modify `buffer`
  // and return it instead of constructing a new object
  def reduce(buffer: Average, data: Long): Average = {
    buffer.sum += data
    buffer.count += 1
    buffer
  }

  // Merge two intermediate values
  def merge(b1: Average, b2: Average): Average = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }

  // Transform the output of the reduction
  def finish(reduction: Average): Double = reduction.sum.toDouble / reduction.count

  // Specifies the Encoder for the intermediate value type
  def bufferEncoder: Encoder[Average] = Encoders.product

  // Specifies the Encoder for the final output value type
  def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

// Register the function to access it
spark.udf.register("my_average", functions.udaf(MyAverage))

Seq(
  ("1", 10L),
  ("1", 20L),
  ("1", 15L),
  ("2", 10L),
  ("2", 25L),
  ("2", 5L)
).toDF("name", "salary").createOrReplaceGlobalTempView("employees")

spark.sql("SELECT my_average(salary) as average_salary FROM global_temp.employees").show()
spark.sql("SELECT name, my_average(salary) FROM global_temp.employees group by name").show()
