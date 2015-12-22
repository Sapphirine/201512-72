package edu.columbia.cs6893.kafka

import java.util.Properties
import java.util.concurrent.Executors

import edu.columbia.cs6893.handler.WebSocketHandler
import kafka.consumer.{Consumer, ConsumerConfig}
import kafka.utils.Logging

object KafkaConsumerFactory {
  def startConsumer(webSocketHandler: WebSocketHandler) = Executors.newFixedThreadPool(1).submit(new KafkaConsumer(webSocketHandler))
}

class KafkaConsumer(webSocketHandler: WebSocketHandler) extends Logging with Runnable {
  def run(): Unit = {
    val config = {
      val props = new Properties()
      props.put("zookeeper.connect", "localhost:2181")
      props.put("group.id", System.getProperty("user.name"))
      new ConsumerConfig(props)
    }
    val consumer = Consumer.create(config)
    val consumerMap = consumer.createMessageStreams(Map("test-ks-holdings" -> 1))
    val stream = consumerMap("test-ks-holdings").head

    val it = stream.iterator()
    while (it.hasNext()) {
      webSocketHandler.notifyAll(new String(it.next().message()))
    }
  }
}
