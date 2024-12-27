package chen.guo.utils

import com.amazonaws.services.dynamodbv2.{AcquireLockOptions, AmazonDynamoDBLockClient, AmazonDynamoDBLockClientOptions, LockItem}
import org.apache.spark.sql.SparkSession
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient

import java.io.Closeable
import java.util.Optional
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.DurationInt

class DynamoDBDistributedLockManager extends Closeable {
  private val tableName = "databricks_cluster_deployment_locks"
  private val region: Region = Region.US_EAST_1
  private val DEFAULT_LOCK_LEASE_SECONDS = 22L
  // The maximum time a lock can be held without heartbeats before it is considered lost from the
  // coordinator's(DynamoDB) perspective
  private val DEFAULT_LOCK_HOLD_ALLOWED_SECONDS_WITHOUT_HEARTBEATS = DEFAULT_LOCK_LEASE_SECONDS - 2L
  // How long to wait before trying to acquire the lock again
  // e.g. if set to 3 seconds, the follower, or new-lock acquirer, would attempt to acquire the lock so every 3 seconds
  // Set to 3 seconds for the follower to quickly take over the lock during blue-green deployment
  // When DEFAULT_LOCK_LEASE_SECONDS is set to 22 seconds, the follower can become the leader within maximum (22 + 3) seconds
  private val DEFAULT_LOCK_ACQUIRE_RETRY_GAP_SECONDS: Long = 3.seconds.toSeconds
  // How long the follower waits in addition to the lease duration when trying to acquire the lock
  // If set to 10 minutes, follower will try to acquire a lock for about (10 minutes + DEFAULT_LOCK_LEASE_SECONDS)
  // before giving up and throwing the exception
  private val DEFAULT_LOCK_ACQUIRE_FOLLOWER_ADDITIONAL_TOTAL_RETRY_SECONDS: Long = 10.minutes.toSeconds
  // Set to 6 seconds to reduce DynamoDB read costs
  private val DEFAULT_LOCK_HOLD_HEARTBEAT_SECONDS = 6.seconds.toSeconds

  private val ddbclient: DynamoDbClient = DynamoDbClient.builder().region(region).build()
  private var lockClient: AmazonDynamoDBLockClient = _

  def tryAcquireLock(lockKey: String): Optional[LockItem] = {
    tryAcquireLock(lockKey, None)
  }

  /**
    * A blocking call trying to acquire the distributed lock identified by "lockKey"
    * The blocking duration is bounded by "lockLeaseSeconds" + "lockAcquireFollowerAdditionalTotalRetrySeconds"
    *
    * To call such function in PySpark, you can use the following code:
    *
    * {{{
    *   jvm = spark._jvm
    *   lock_manager = jvm.chen.guo.utils.DynamoDBDistributedLockManager()
    *   # 1) Get the Scala 'None$' class object
    *   none_class = getattr(jvm.scala, "None$")
    *   # 2) Retrieve the singleton instance stored in the MODULE$ field
    *   scala_none = getattr(none_class, "MODULE$")
    *
    *   lock_item = lock_manager.tryAcquireLock("bbb", scala_none, 22, 6, 20, 3, 600)
    *   if lock_item.isPresent:
    *     print("Lock acquired")
    *   else:
    *     print("Failed to acquire lock")
    * }}}
    *
    * @param lockKey the path to the file that needs to be locked. This will be used as the DDB partition key.
    *
    * */
  def tryAcquireLock(lockKey: String,
                     sparkSession: Option[SparkSession],
                     lockLeaseSeconds: Long = DEFAULT_LOCK_LEASE_SECONDS,
                     lockHoldHeartbeatSeconds: Long = DEFAULT_LOCK_HOLD_HEARTBEAT_SECONDS,
                     lockHoldAllowedSecondsWithoutHeartbeats: Long = DEFAULT_LOCK_HOLD_ALLOWED_SECONDS_WITHOUT_HEARTBEATS,
                     lockAcquireRetryGapSeconds: Long = DEFAULT_LOCK_ACQUIRE_RETRY_GAP_SECONDS,
                     lockAcquireFollowerAdditionalTotalRetrySeconds: Long = DEFAULT_LOCK_ACQUIRE_FOLLOWER_ADDITIONAL_TOTAL_RETRY_SECONDS
                    ): Optional[LockItem] = {
    if (lockClient == null) {
      val options = AmazonDynamoDBLockClientOptions.builder(ddbclient, tableName)
        .withTimeUnit(TimeUnit.SECONDS)
        .withLeaseDuration(lockLeaseSeconds)
        .withHeartbeatPeriod(lockHoldHeartbeatSeconds)
        .withCreateHeartbeatBackgroundThread(true)
        // When DynamoDB service is unavailable, all threads will get the same exception and no threads will have the lock.
        .withHoldLockOnServiceUnavailable(false)
        .build()
      lockClient = new AmazonDynamoDBLockClient(options)
    }

    val lockEnterDangerZoneCallback: Runnable = new Runnable() {
      override def run(): Unit = {
        println("Lock is entering the danger zone")
        if (sparkSession.isDefined) {
          sparkSession.get.close()
        }
        Runtime.getRuntime.exit(1)
      }
    }

    println(s"Trying to acquire the lock for '$lockKey'")
    val lockItem: Optional[LockItem] = lockClient.tryAcquireLock(AcquireLockOptions
      .builder(lockKey)
      .withTimeUnit(TimeUnit.SECONDS)
      .withRefreshPeriod(lockAcquireRetryGapSeconds)
      .withAdditionalTimeToWaitForLock(lockAcquireFollowerAdditionalTotalRetrySeconds)
      .withSessionMonitor(lockHoldAllowedSecondsWithoutHeartbeats, Optional.of(lockEnterDangerZoneCallback))
      .build()
    )

    lockItem
  }

  override def close(): Unit = {
    if (lockClient != null) {
      lockClient.close()
    }
    if (ddbclient != null) {
      ddbclient.close()
    }
  }
}

object UseLockManager {
  def main(args: Array[String]): Unit = {
    val lockManager = new DynamoDBDistributedLockManager()
    val lockItem = lockManager.tryAcquireLock("lockKey", null)
    if (lockItem.isPresent) {
      println("Lock acquired")
    } else {
      println("Failed to acquire lock")
    }
    lockManager.close()
  }
}