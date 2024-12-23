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
  private val DEFAULT_LEASE_SECONDS = 20L
  private val DEFAULT_ALLOWED_SECONDS_WITHOUT_HEARTBEATS = DEFAULT_LEASE_SECONDS - 2L
  // How long to wait before trying to get the lock again
  // e.g. if set to 10 seconds, it would attempt to do so every 10 seconds
  private val DEFAULT_LOCK_ACQUIRE_REFRESH_SECONDS: Long = 5.seconds.toSeconds
  // How long the follower waits in addition to the lease duration when trying to acquire the lock
  // If set to 10 minutes, follower will try to acquire a lock for at least 10 minutes before giving up and
  // throwing the exception
  private val DEFAULT_FOLLOWER_ADDITIONAL_LOCK_ACQUIRE_SECONDS: Long = 10.minutes.toSeconds
  private val DEFAULT_HEARTBEAT_SECONDS = 5.seconds.toSeconds

  private val ddbclient: DynamoDbClient = DynamoDbClient.builder().region(region).build()
  private var lockClient: AmazonDynamoDBLockClient = _

  /**
    * @param stirPath the path to the file that needs to be locked. This will be used as the DDB partition key.
    */
  def tryAcquireLock(stirPath: String, sparkSession: SparkSession): Optional[LockItem] = {
    if (lockClient == null) {
      val options = AmazonDynamoDBLockClientOptions.builder(ddbclient, tableName)
        .withTimeUnit(TimeUnit.SECONDS)
        .withLeaseDuration(DEFAULT_LEASE_SECONDS)
        .withHeartbeatPeriod(DEFAULT_HEARTBEAT_SECONDS)
        .withCreateHeartbeatBackgroundThread(true)
        // When DynamoDB service is unavailable, all threads will get the same exception and no threads will have the lock.
        .withHoldLockOnServiceUnavailable(false)
        .build()
      lockClient = new AmazonDynamoDBLockClient(options)
    }

    val lockEnterDangerZoneCallback: Runnable = new Runnable() {
      override def run(): Unit = {
        println("Executing lockEnterDangerZoneCallback")
        if (sparkSession != null) {
          sparkSession.close()
        }
        Runtime.getRuntime.exit(1)
      }
    }

    println(s"Try acquiring lock for $stirPath")
    val lockItem: Optional[LockItem] = lockClient.tryAcquireLock(AcquireLockOptions
      .builder(stirPath)
      .withTimeUnit(TimeUnit.SECONDS)
      .withRefreshPeriod(DEFAULT_LOCK_ACQUIRE_REFRESH_SECONDS)
      .withAdditionalTimeToWaitForLock(DEFAULT_FOLLOWER_ADDITIONAL_LOCK_ACQUIRE_SECONDS)
      .withSessionMonitor(DEFAULT_ALLOWED_SECONDS_WITHOUT_HEARTBEATS, Optional.of(lockEnterDangerZoneCallback))
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
