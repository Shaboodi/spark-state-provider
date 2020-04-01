package com.shaboodi.state.aerospike

import org.apache.spark.sql.SparkSession.Builder
import org.apache.spark.sql.internal.SQLConf

object implicits extends Serializable {

  implicit class SessionImplicits(sparkSessionBuilder: Builder) {

    def useAerospikeDBStateStore(host: String, port: String, prefix: String, namespace: String, set: String): Builder = {
      sparkSessionBuilder.config(SQLConf.STATE_STORE_PROVIDER_CLASS.key, classOf[AerospikeStateStoreProvider].getCanonicalName)
      sparkSessionBuilder.config(AerospikeStateStoreProvider.AEROSPIKE_HOST, host)
      sparkSessionBuilder.config(AerospikeStateStoreProvider.AEROSPIKE_PORT, port)
      sparkSessionBuilder.config(AerospikeStateStoreProvider.AEROSPIKE_NAMESPACE, namespace)
      sparkSessionBuilder.config(AerospikeStateStoreProvider.AEROSPIKE_SET, set)
      sparkSessionBuilder.config(AerospikeStateStoreProvider.AEROSPIKE_PREFIX, prefix)
    }
  }
}