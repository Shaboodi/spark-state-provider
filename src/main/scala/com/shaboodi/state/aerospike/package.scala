package com.shaboodi.state

import com.aerospike.client.Key
import org.apache.spark.sql.catalyst.expressions.UnsafeRow

package object aerospike {

  case class AerospikeConf(host: String, port: Int, prefix: String, namespace: String, setName: String) {

    def getByteKey(version: Long, key: UnsafeRow = null): Key = {
      if (key != null) {
        new Key(namespace, setName, s"$prefix:$version:".getBytes ++ key.getBytes)
      } else {
        new Key(namespace, setName, s"$prefix:$version:".getBytes)
      }
    }

    override def toString: String = {
      s"AerospikeConfiguration[host=$host,port=$port,prefix=$prefix]"
    }
  }
}