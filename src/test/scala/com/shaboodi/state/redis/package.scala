package com.shaboodi.state

import java.sql.Timestamp
import java.time.Instant

import org.apache.spark.sql.streaming.GroupState
import org.apache.spark.sql.{Encoder, Encoders}

package object redis {

  val random = new scala.util.Random

  case class PageVisit(id: Int, url: String, timestamp: Timestamp = Timestamp.from(Instant.now()))

  case class UserStatistics(userId: Int, visits: Seq[PageVisit], totalVisits: Int)

  case class UserGroupState(groupState: UserStatistics)

  implicit val pageVisitEncoder: Encoder[PageVisit] = Encoders.product[PageVisit]
  implicit val userStatisticsEncoder: Encoder[UserStatistics] = Encoders.product[UserStatistics]

  def generateEvent(id: Int): PageVisit = {
    PageVisit(
      id = id,
      url = s"https://www.my-service.org/${generateBetween(100, 200)}"
    )
  }

  def generateBetween(start: Int = 0, end: Int = 100): Int = {
    start + random.nextInt((end - start) + 1)
  }

  def updateUserStatistics(
      id: Int,
      newEvents: Iterator[PageVisit],
      oldState: GroupState[UserStatistics]): UserStatistics = {

    var state: UserStatistics = if (oldState.exists) oldState.get else UserStatistics(id, Seq.empty, 0)

    for (event <- newEvents) {
      state = state.copy(visits = state.visits ++ Seq(event), totalVisits = state.totalVisits + 1)
      oldState.update(state)
    }
    state
  }
}