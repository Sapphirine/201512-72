package edu.columbia.cs6893.handler

import edu.columbia.cs6893.db.Database
import edu.columbia.cs6893.kafka.KafkaProducer
import edu.columbia.cs6893.model.Trade
import io.vertx.core.Handler
import io.vertx.core.http.ServerWebSocket
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization._

import scala.collection._

class WebSocketHandler extends Handler[ServerWebSocket] {

  val kafkaProducer = new KafkaProducer()
  var peers: mutable.Set[ServerWebSocket] = mutable.Set.empty

  implicit val formats = DefaultFormats

  override def handle(ws: ServerWebSocket) = {
    peers += ws
    ws.closeHandler(new Handler[Void] {
      override def handle(event: Void) = {
        peers -= ws
      }
    })

    kafkaProducer.send("test-ks-trades", List(write(Database.getHoldingKeys(1).collect { case holdingKey => Trade(0L, holdingKey, 0) })))
  }

  def notifyAll(msg: String) = peers.foreach(ws => ws.writeFinalTextFrame(msg))
}
