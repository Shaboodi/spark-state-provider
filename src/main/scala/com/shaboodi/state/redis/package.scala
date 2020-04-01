package com.shaboodi.state

package object redis {

  case class RedisConf(host: String, port: Int, prefix: String) {

    def getBytePrefix(version: Long): Array[Byte] = s"$prefix:$version:".getBytes

    override def toString: String = {
      s"RedisConfiguration[host=$host,port=$port,prefix=$prefix]"
    }
  }
}