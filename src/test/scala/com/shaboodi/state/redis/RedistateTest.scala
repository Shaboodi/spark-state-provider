package com.shaboodi.state.redis

import java.nio.file.Files

import com.shaboodi.state.tests._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode, StreamingQuery}
import org.apache.spark.sql.{Dataset, SQLContext}
import org.scalatest.FunSuite

class RedistateTest extends FunSuite with SparkSupport with RedisSupport with Logging {

  lazy val checkpointLocation: String = Files.createTempDirectory("redistate-test").toString
  var query: StreamingQuery = _

  test("execute stateful operation") {

    spark.sparkContext.setLogLevel("INFO")

    implicit val sqlCtx: SQLContext = spark.sqlContext
    import sqlCtx.implicits._

    val visitsStream = MemoryStream[PageVisit]

    val pageVisitsTypedStream: Dataset[PageVisit] = visitsStream.toDS()

    val noTimeout = GroupStateTimeout.NoTimeout
    val userStatisticsStream = pageVisitsTypedStream
        .groupByKey(_.id)
        .mapGroupsWithState(noTimeout)(updateUserStatistics)

    query = userStatisticsStream.writeStream
        .outputMode(OutputMode.Update())
        .option("checkpointLocation", checkpointLocation)
        .format("memory")
        .queryName("redistate_updates")
        .start()

    spark.sql("select * from redistate_updates").show()

    val initialBatch = Seq(
      generateEvent(1),
      generateEvent(2),
      generateEvent(3)
    )

    visitsStream.addData(initialBatch)

    processDataWithLock(query)
    spark.sql("select * from redistate_updates").show()

    val additionalBatch = Seq(
      generateEvent(3),
      generateEvent(3),
      generateEvent(4)
    )

    visitsStream.addData(additionalBatch)

    processDataWithLock(query)

    spark.sql("select * from redistate_updates").show()

  }


  def printBatch(batchData: Dataset[UserStatistics], batchId: Long): Unit = {
    log.info(s"Started working with batch id $batchId")
    batchData.show()
    log.info(s"Successfully finished working with batch id $batchId, dataset size: ${batchData.count()}")
  }

  def processDataWithLock(query: StreamingQuery): Unit = {
    while (query.status.message != "Waiting for data to arrive") {
      log.info(s"Waiting for the query to finish processing, current status is ${query.status.message}")
      Thread.sleep(1)
    }
    log.info("Locking the thread for another 5 seconds for state operations cleanup")
    Thread.sleep(5000)
  }
}
