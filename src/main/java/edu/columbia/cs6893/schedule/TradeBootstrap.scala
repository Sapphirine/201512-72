package edu.columbia.cs6893.schedule

import edu.columbia.cs6893.db.Database
import edu.columbia.cs6893.kafka.KafkaProducer
import edu.columbia.cs6893.model.Trade
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization._

object TradeBootstrap {

  def getHoldings(): String =
    write(Database.getHoldings(1).collect { case holding => Trade(0L, holding.holdingKey, holding.quantity.getOrElse(0L)) })(DefaultFormats)

  def main(args: Array[String]): Unit =
    new KafkaProducer().send("test-ks-trades", List(getHoldings()))

}
