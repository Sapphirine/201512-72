package edu.columbia.cs6893.spark

import java.util.Properties

import edu.columbia.cs6893.db.Database._
import edu.columbia.cs6893.kafka.KafkaProducer
import edu.columbia.cs6893.util.SparkLoggingUtil

import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations

import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

import org.json4s._
import org.json4s.jackson.Serialization._

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._

object TwitterSentimentCalculator {

  implicit val formats = DefaultFormats
  val producer = new KafkaProducer()

  def main(args: Array[String]) {
    SparkLoggingUtil.setStreamingLogLevels()

    val cb = new ConfigurationBuilder
    cb.setOAuthConsumerKey(args(1))
    cb.setOAuthConsumerSecret(args(2))
    cb.setOAuthAccessToken(args(3))
    cb.setOAuthAccessTokenSecret(args(4))
    val authorization = new OAuthAuthorization(cb.build)

    val productId = args(5)

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("TwitterSentimentCalculator")

    val ssc = new StreamingContext(sparkConf, Seconds(10))
    ssc.checkpoint("checkpoint")

    val productTweets = TwitterUtils.createStream(ssc, Option(authorization), List(productId))
    val sectorTweets = TwitterUtils.createStream(ssc, Option(authorization), List(sectors(productId)))
    val marketTweets = TwitterUtils.createStream(ssc, Option(authorization), List("NYSE", "Bloomberg"))

    val productSentiment = productTweets.map { t => {
      productId -> detectSentiment(t.getText)
    }
    }
    val sectorSentiment = sectorTweets.map { t => {
      productId -> detectSentiment(t.getText)
    }
    }
    val marketSentiment = marketTweets.map { t => {
      productId -> detectSentiment(t.getText)
    }
    }

    productSentiment.join(sectorSentiment).join(marketSentiment).foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        partitionOfRecords.foreach {
          case (_, ((productSentiment, sectorSentiment), marketSentiment)) =>
            val sentiment =
              if (productSentiment == 0) {
                productSentiment + sectorSentiment + marketSentiment / 3
              } else if (productSentiment < 3 && (sectorSentiment > 3 && marketSentiment > 3)) {
                productSentiment
              } else if (productSentiment > 3 && (sectorSentiment < 3 && marketSentiment < 3)) {
                productSentiment
              } else {
                productSentiment + sectorSentiment + marketSentiment / 3
              }
            producer.send("test-ts-sentiments", List(write(Map(productId -> sentiment))))

        }
      })
    })

    ssc.start()
    ssc.awaitTermination()

  }

  val pipeline = new StanfordCoreNLP({
    val props = new Properties()
    props.setProperty("annotators", "tokenize, ssplit, pos, lemma, parse, sentiment")
    props
  })

  def detectSentiment(message: String) = {

    val annotation = pipeline.process(message)
    var sentiments: ListBuffer[Int] = ListBuffer()

    for (sentence <- annotation.get(classOf[CoreAnnotations.SentencesAnnotation])) {
      val tree = sentence.get(classOf[SentimentCoreAnnotations.AnnotatedTree])
      val sentiment = RNNCoreAnnotations.getPredictedClass(tree)
      sentiments += sentiment
    }

    if (sentiments.nonEmpty) sentiments.sum / sentiments.size else -1
  }

}