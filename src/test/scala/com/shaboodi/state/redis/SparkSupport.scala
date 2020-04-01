package com.shaboodi.state.redis

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}
import com.shaboodi.state.redis.implicits._

trait SparkSupport extends BeforeAndAfterAll {
  self: Suite =>

  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
        .appName("redistate-spark-test")
        .master("local[*]")
        .useRedisDBStateStore("localhost", "6379", "redistate")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()

    super.beforeAll()
  }

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }
}
