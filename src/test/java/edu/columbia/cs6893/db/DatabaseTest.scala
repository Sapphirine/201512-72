package edu.columbia.cs6893.db

import edu.columbia.cs6893.model.{Holding, Trade}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization._
import org.junit.Test

class DatabaseTest {

  implicit val formats = DefaultFormats

  @Test
  def testPortfolioHoldingKeys() {
    val holdings = write(Database.getHoldingKeys(1).collect { case holdingKey => Trade(0L, holdingKey, 0) })
    println(holdings)
  }

  @Test
  def testPortfolioHoldings() {
    val holdings = write(Database.getHoldings(1).collect { case holding => Trade(0L, holding.holdingKey, holding.quantity.getOrElse(0)) })
    println(holdings)
  }

  @Test
  def testProductHoldingKeys(): Unit = {
    val jsonString = write(Map(
      "MarketData" -> Map(
        "MSFT" -> 10.0,
        "IBM" -> 20.0
      )
    ))
    val marketData = read[Map[String, Map[String, _]]](jsonString).getOrElse("MarketData", Map.empty)
    val holdings = marketData.flatMap {
      case (productId: String, price: Double) =>
        val holdingKeys = Database.getHoldingKeys(productId)
        holdingKeys.zip(List.fill(holdingKeys.size)(price))
    }.map { case (holdingKey, price) => holdingKey -> Holding(holdingKey, Option(0L), Option(price)) }
    println(holdings)
  }

}
