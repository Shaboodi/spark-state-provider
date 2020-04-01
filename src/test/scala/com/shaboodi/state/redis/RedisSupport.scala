package com.shaboodi.state.redis

import org.scalatest.{BeforeAndAfterAll, Suite}
import redis.embedded.RedisServer

trait RedisSupport extends BeforeAndAfterAll {
  self: Suite =>

  val redisServer: RedisServer = new RedisServer(6379)

  override def beforeAll(): Unit = {
    redisServer.start()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    redisServer.stop()
    super.afterAll()
  }
}
