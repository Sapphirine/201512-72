package edu.columbia.cs6893

import edu.columbia.cs6893.spark.HoldingCalculator
import io.vertx.core.Vertx

object Main {
  // Convenience method so you can run it in your IDE
  def main(args: Array[String]) = {
    Vertx.vertx.deployVerticle(classOf[WebServerVerticle].getName)
    HoldingCalculator.main(Array("192.168.0.109:2181", "holdingsCalculator", "test-ks-trades,test-ks-prices", "1"))
  }
}
