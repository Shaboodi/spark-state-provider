package com.shaboodi.state.redis


import com.shaboodi.state.redis.RedisStateStoreProvider._
import org.apache.hadoop.conf.Configuration
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.streaming.state.{StateStore, StateStoreConf, StateStoreId, StateStoreMetrics, StateStoreProvider, UnsafeRowPair}
import org.apache.spark.sql.types.StructType
import redis.clients.jedis.Jedis

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
 * An implementation of [[StateStoreProvider]] and [[StateStore]] in which all the data is backed
 * by files in a HDFS-compatible file system using Redis key-value storage format.
 * All updates to the store has to be done in sets transactionally, and each set of updates
 * increments the store's version. These versions can be used to re-execute the updates
 * (by retries in RDD operations) on the correct version of the store, and regenerate
 * the store version.
 *
 * Usage:
 * To update the data in the state store, the following order of operations are needed.
 *
 * // get the right store
 * - val store = StateStore.get(
 * StateStoreId(checkpointLocation, operatorId, partitionId), ..., version, ...)
 * - store.put(...)
 * - store.remove(...)
 * - store.commit()    // commits all the updates to made; the new version will be returned
 * - store.iterator()  // key-value data after last commit as an iterator
 * - store.updates()   // updates made in the last commit as an iterator
 *
 *
 */
class RedisStateStoreProvider extends StateStoreProvider with Logging {

  @volatile
  private var redisConf: RedisConf = _
  @volatile
  private var stateStoreId_ : StateStoreId = _
  @volatile
  private var keySchema: StructType = _
  @volatile
  private var valueSchema: StructType = _
  @volatile
  private var storeConf: StateStoreConf = _
  @volatile
  private var hadoopConf: Configuration = _

  override def init(stateStoreId: StateStoreId, keySchema: StructType, valueSchema: StructType,
      keyIndexOrdinal: Option[Int], storeConf: StateStoreConf, hadoopConf: Configuration): Unit = {

    this.stateStoreId_ = stateStoreId
    this.keySchema = keySchema
    this.valueSchema = valueSchema
    this.storeConf = storeConf
    this.hadoopConf = hadoopConf
    this.redisConf = setRedisConf(this.storeConf.confs)
    log.info("Initializing the redis state store")
  }

  override def stateStoreId: StateStoreId = stateStoreId_

  override def close(): Unit = {}

  override def getStore(version: Long): StateStore = synchronized {
    require(version >= 0, "Version cannot be less than 0")
    val store = new RedisStateStore(version, redisConf)
    logInfo(s"Retrieved version $version of ${RedisStateStoreProvider.this} for update")
    store
  }

  override def toString: String = {
    s"RedisStateStoreProvider[id = (op=${stateStoreId.operatorId},part=${stateStoreId.partitionId}),redis=$redisConf]"
  }

  private def verify(condition: => Boolean, msg: String): Unit = {
    if (!condition) {
      throw new IllegalStateException(msg)
    }
  }

  class RedisStateStore(val version: Long, redisConf: RedisConf) extends StateStore {

    trait STATE

    case object UPDATING extends STATE

    case object COMMITTED extends STATE

    case object ABORTED extends STATE

    private val newVersion = version + 1

    @volatile
    private var state: STATE = UPDATING

    private lazy val commitableClient = new Jedis(redisConf.host, redisConf.port)
    private lazy val commitableTransaction = commitableClient.multi()

    override def id: StateStoreId = RedisStateStoreProvider.this.stateStoreId

    override def get(key: UnsafeRow): UnsafeRow = {
      val getClient = new Jedis(redisConf.host, redisConf.port)
      val getTransaction = getClient.multi()
      val transactionData = getTransaction.get(redisConf.getBytePrefix(version) ++ key.getBytes)
      getTransaction.exec()
      val valueBytes = transactionData.get()
      val value = new UnsafeRow(valueSchema.fields.length)
      if (valueBytes == null) return null
      value.pointTo(valueBytes, valueBytes.length)
      value
    }

    override def put(key: UnsafeRow, value: UnsafeRow): Unit = {
      verify(state == UPDATING, "Cannot put after already committed or aborted")
      commitableTransaction.set(redisConf.getBytePrefix(newVersion) ++ key.getBytes, value.getBytes)
    }

    override def remove(unsafeKey: UnsafeRow): Unit = {}

    override def commit(): Long = {
      verify(state == UPDATING, "Cannot commit after already committed or aborted")
      commitableTransaction.exec()
      state = COMMITTED
      log.info(s"Committed version $newVersion for $this")
      newVersion
    }

    override def abort(): Unit = {
      commitableTransaction.discard()
    }

    override def iterator(): Iterator[UnsafeRowPair] = {
      val iterClient = new Jedis(redisConf.host, redisConf.port)
      val keysIter = iterClient.keys(redisConf.getBytePrefix(version)).iterator().asScala
      keysIter.map { redisKey =>
        val key = new UnsafeRow(keySchema.fields.length)
        val keyBytes = redisKey
        key.pointTo(keyBytes, keyBytes.length)

        val value = new UnsafeRow(valueSchema.fields.length)
        val valueBytes = iterClient.get(redisKey)
        value.pointTo(valueBytes, valueBytes.length)
        new UnsafeRowPair(key, value)
      }
    }

    override def metrics: StateStoreMetrics = {
      StateStoreMetrics(0, 0, Map())
    }

    override def hasCommitted: Boolean = {
      state == COMMITTED
    }
  }
}

object RedisStateStoreProvider {

  final val DEFAULT_REDIS_HOST: String = "localhost"
  final val DEFAULT_REDIS_PORT: String = "6379"

  final val REDIS_HOST: String = "spark.sql.streaming.stateStore.redis.host"
  final val REDIS_PORT: String = "spark.sql.streaming.stateStore.redis.port"
  final val REDIS_PREFIX: String = "spark.sql.streaming.stateStore.redis.prefix"

  private def setRedisConf(conf: Map[String, String]): RedisConf = {
    val host = Try(conf.getOrElse(REDIS_HOST, DEFAULT_REDIS_HOST)) match {
      case Success(value) => value
      case Failure(e) => throw new IllegalArgumentException(e)
    }
    val port = Try(conf.getOrElse(REDIS_PORT, DEFAULT_REDIS_PORT).toInt) match {
      case Success(value) => value
      case Failure(e) => throw new IllegalArgumentException(e)
    }
    val prefix = Try(conf.getOrElse(REDIS_PREFIX, conf("spark.app.name"))) match {
      case Success(value) => value
      case Failure(e) => throw new IllegalArgumentException(e)
    }
    RedisConf(host, port, prefix)
  }
}
