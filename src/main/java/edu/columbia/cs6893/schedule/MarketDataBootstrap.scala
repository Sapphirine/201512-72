package edu.columbia.cs6893.schedule

import edu.columbia.cs6893.db.Database
import edu.columbia.cs6893.kafka.KafkaProducer
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization._

object MarketDataBootstrap {

  def getSoDPrices(): String =
    write(Map("MarketData" ->
      Database.getHoldings(1)
        .collect { case holding => holding.holdingKey.productId -> holding.price }
        .groupBy(_._1).map { case (k, v) => v.head }
    ))(DefaultFormats)

  def main(args: Array[String]): Unit =
    new KafkaProducer().send("test-ks-prices", List(getSoDPrices()))

}
