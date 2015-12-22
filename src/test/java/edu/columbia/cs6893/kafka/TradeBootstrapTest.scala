package edu.columbia.cs6893.kafka

import edu.columbia.cs6893.model.{HoldingKey, Trade}
import edu.columbia.cs6893.schedule.TradeBootstrap
import org.junit.Test

import org.json4s._
import org.json4s.jackson.Serialization._

class TradeBootstrapTest {

  implicit val formats = DefaultFormats

  @Test
  def testGetHoldings() = println(TradeBootstrap.getHoldings())

  @Test
  def testTradePublisher() = {
    //val jsonString = write(List(Trade(1L, HoldingKey(1L, 1L), 100L),Trade(2L, HoldingKey(1L, 2L), 100L)))
    val jsonString = write(List(
      Trade(1L, HoldingKey("C3P0", "MSFT"), -300L),
      //      Trade(1L, HoldingKey("C3P0", "MSFT"), 300L),
      //      Trade(1L, HoldingKey("C3P0", "MSFT"), 300L),
      //      Trade(1L, HoldingKey("C3P0", "MSFT"), 300L),
      //      Trade(1L, HoldingKey("C3P0", "MSFT"), 300L),
      //      Trade(1L, HoldingKey("C3P0", "MSFT"), 300L),
      //      Trade(1L, HoldingKey("C3P0", "MSFT"), 300L),
      Trade(1L, HoldingKey("C3P0", "IBM"), 500L)))
    println(jsonString)
    new KafkaProducer().send("test-ks-trades", List(jsonString))
  }

}
