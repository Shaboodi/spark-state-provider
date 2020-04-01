package com.shaboodi.state.aerospike

import com.aerospike.client.policy.ClientPolicy
import com.aerospike.client._
import org.apache.hadoop.conf.Configuration
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.streaming.state.{StateStore, StateStoreConf, StateStoreId, StateStoreMetrics, StateStoreProvider, UnsafeRowPair}
import org.apache.spark.sql.types.StructType
import com.shaboodi.state.aerospike.AerospikeStateStoreProvider._
import scala.util.{Failure, Success, Try}

class AerospikeStateStoreProvider extends StateStoreProvider with Logging {


  @volatile
  private var aeropikeConf: AerospikeConf = _
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

  override def init(stateStoreId: StateStoreId, keySchema: StructType, valueSchema: StructType, keyIndexOrdinal: Option[Int],
      storeConfs: StateStoreConf, hadoopConf: Configuration): Unit = {
    this.stateStoreId_ = stateStoreId
    this.keySchema = keySchema
    this.valueSchema = valueSchema
    this.storeConf = storeConfs
    this.hadoopConf = hadoopConf
    this.aeropikeConf = setAerospikeConf(this.storeConf.confs)
    log.info("Initializing the aerospike state store")
  }

  override def stateStoreId: StateStoreId = this.stateStoreId_

  override def close(): Unit = {}

  override def getStore(version: Long): StateStore = synchronized {
    require(version >= 0, "Version cannot be less than 0")
    val store = new AerospikeStateStore(version, aeropikeConf)
    logInfo(s"Retrieved version $version of ${AerospikeStateStoreProvider.this} for update")
    store
  }

  private def verify(condition: => Boolean, msg: String): Unit = {
    if (!condition) {
      throw new IllegalStateException(msg)
    }
  }

  class AerospikeStateStore(val version: Long, aeropsikeConf: AerospikeConf) extends StateStore {

    private val binValueName = "value"

    trait STATE

    case object UPDATING extends STATE

    case object COMMITTED extends STATE

    case object ABORTED extends STATE

    private val newVersion = version + 1

    @volatile
    private var state: STATE = UPDATING

    private val clientPolicy: ClientPolicy = new ClientPolicy()
    clientPolicy.failIfNotConnected = true
    clientPolicy.writePolicyDefault.sendKey = true
    clientPolicy.readPolicyDefault.sendKey = true
    clientPolicy.scanPolicyDefault.sendKey = true

    private lazy val client = new AerospikeClient(clientPolicy, aeropikeConf.host, aeropikeConf.port)

    override def id: StateStoreId = AerospikeStateStoreProvider.this.stateStoreId

    override def get(key: UnsafeRow): UnsafeRow = {
      val client = new AerospikeClient(clientPolicy, aeropikeConf.host, aeropikeConf.port)
      val record = client.get(null, aeropikeConf.getByteKey(version, key))
      if (record == null) return null
      val valueBytes = record.getValue(binValueName).asInstanceOf[Array[Byte]]
      val value = new UnsafeRow(valueSchema.fields.length)
      value.pointTo(valueBytes, valueBytes.length)
      value
    }

    override def put(key: UnsafeRow, value: UnsafeRow): Unit = {
      verify(state == UPDATING, "Cannot put after already committed or aborted")
      client.put(null, aeropikeConf.getByteKey(version, key), Seq(new Bin(binValueName, value.getBytes)):_*)
    }

    override def remove(unsafeKey: UnsafeRow): Unit = {
      client.delete(null, aeropikeConf.getByteKey(version, unsafeKey))
    }

    override def commit(): Long = {
      verify(state == UPDATING, "Cannot commit after already committed or aborted")
      state = COMMITTED
      log.info(s"Committed version $newVersion for $this")
      newVersion
    }

    override def abort(): Unit = {
    }

    override def iterator(): Iterator[UnsafeRowPair] = {
      var res: Iterator[UnsafeRowPair] = Iterator()
      val client =  new AerospikeClient(clientPolicy, aeropikeConf.host, aeropikeConf.port)
      client.scanAll(null, aeropikeConf.namespace, aeropikeConf.setName, new ScanCallback {
        override def scanCallback(key: Key, record: Record): Unit = {
          if (key.userKey.toString.contains(aeropikeConf.getByteKey(version).userKey.toString)) {
            val unsafeRowKey = new UnsafeRow(keySchema.fields.length)
            val keyBytes = key.userKey.asInstanceOf[Array[Byte]]
            unsafeRowKey.pointTo(keyBytes, keyBytes.length)
            val unsafeRowValue = new UnsafeRow(valueSchema.fields.length)
            val bytes = record.getValue(binValueName).asInstanceOf[Array[Byte]]
            unsafeRowValue.pointTo(bytes, bytes.length)
            res = res ++ Seq(new UnsafeRowPair(unsafeRowKey, unsafeRowValue))
          }
        }
      })
      res
    }

    override def metrics: StateStoreMetrics = {
      StateStoreMetrics(0, 0, Map())
    }

    override def hasCommitted: Boolean = {
      state == COMMITTED
    }
  }
}

object AerospikeStateStoreProvider {

  final val SET_NAME: String = "SparkStateSet"
  final val DEFAULT_AEROSPIKE_HOST: String = "localhost"
  final val DEFAULT_AEROSPIKE_PORT: String = "3000"

  final val AEROSPIKE_HOST: String = "spark.sql.streaming.stateStore.aerospike.host"
  final val AEROSPIKE_PORT: String = "spark.sql.streaming.stateStore.aerospike.port"
  final val AEROSPIKE_PREFIX: String = "spark.sql.streaming.stateStore.aerospike.prefix"
  final val AEROSPIKE_NAMESPACE: String = "spark.sql.streaming.stateStore.aerospike.namespace"
  final val AEROSPIKE_SET: String = "spark.sql.streaming.stateStore.aerospike.set"

  private def setAerospikeConf(conf: Map[String, String]): AerospikeConf = {
    val host = Try(conf.getOrElse(AEROSPIKE_HOST, DEFAULT_AEROSPIKE_HOST)) match {
      case Success(value) => value
      case Failure(e) => throw new IllegalArgumentException(e)
    }
    val port = Try(conf.getOrElse(AEROSPIKE_PORT, DEFAULT_AEROSPIKE_PORT).toInt) match {
      case Success(value) => value
      case Failure(e) => throw new IllegalArgumentException(e)
    }
    val prefix = Try(conf.getOrElse(AEROSPIKE_PREFIX, conf("spark.app.name"))) match {
      case Success(value) => value
      case Failure(e) => throw new IllegalArgumentException(e)
    }
    val ns = Try(conf(AEROSPIKE_NAMESPACE)) match {
      case Success(value) => value
      case Failure(e) => throw new IllegalArgumentException(e)
    }
    val set = Try(conf.getOrElse(AEROSPIKE_SET, SET_NAME)) match {
      case Success(value) => value
      case Failure(e) => throw new IllegalArgumentException(e)
    }
    AerospikeConf(host, port, prefix, ns, set)
  }
}
