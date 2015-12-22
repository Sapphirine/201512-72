package edu.columbia.cs6893.kafka

import edu.columbia.cs6893.handler.WebSocketHandler
import org.junit.Test

class KafkaConsumerFactoryTest {

  @Test
  def testKafkaConsumer(): Unit = {
    KafkaConsumerFactory.startConsumer(new WebSocketHandler())
  }
}
