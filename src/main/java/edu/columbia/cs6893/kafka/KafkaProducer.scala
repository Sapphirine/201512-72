package edu.columbia.cs6893.kafka

import java.util.Properties

import kafka.producer.{KeyedMessage, ProducerConfig, Producer}

class KafkaProducer {

  val props = new Properties()
  props.put("metadata.broker.list", "localhost:9092")
  props.put("serializer.class", "kafka.serializer.StringEncoder")
  val config = new ProducerConfig(props)

  val producer = new Producer[String, String](config)

  def send(topic: String, msgs: List[String]) =
    producer.send(msgs.map(msg => new KeyedMessage[String, String](topic, msg)): _*)

}

object KafkaProducer {
  var TEST_MESSAGE: String =
    s"""
       |[
       |{
       |  \"accountId\": \"C3P0\",
       |  \"productId\": \"MSFT\",
       |  \"quantity\": 250
       |},
       |{
       |  \"accountId\": \"C3P0\",
       |  \"accountName\": \"Science Center\",
       |  \"productId\": \"IBM\",
       |  \"productName\": \"Microsoft Common Stock\",
       |  \"quantity\": 750
       |}
       |]
     """.stripMargin

  def main(args: Array[String]) = new KafkaProducer().send("test-ks-holdings", List(TEST_MESSAGE))
}
