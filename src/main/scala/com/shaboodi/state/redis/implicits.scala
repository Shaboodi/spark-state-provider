package com.shaboodi.state.redis

import org.apache.spark.sql.SparkSession.Builder
import org.apache.spark.sql.internal.SQLConf

object implicits extends Serializable {

  implicit class SessionImplicits(sparkSessionBuilder: Builder) {

    def useRedisDBStateStore(host: String, port: String, prefix: String): Builder = {
      sparkSessionBuilder.config(SQLConf.STATE_STORE_PROVIDER_CLASS.key, classOf[RedisStateStoreProvider].getCanonicalName)
      sparkSessionBuilder.config(RedisStateStoreProvider.REDIS_HOST, host)
      sparkSessionBuilder.config(RedisStateStoreProvider.REDIS_PORT, port)
      sparkSessionBuilder.config(RedisStateStoreProvider.REDIS_PREFIX, prefix)
    }
  }
}