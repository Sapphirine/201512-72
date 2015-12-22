package edu.columbia.cs6893.model

case class Holding(holdingKey: HoldingKey, quantity: Option[Long] = None, price: Option[Double] = None /*SoD*/)

object Holding {
  implicit def long2Option(quantity: Long) = Option(quantity)

  implicit def int2LongOption(quantity: Int) = Option(quantity.toLong)
  implicit def double2Option(price: Double) = Option(price)
  implicit def int2DoubleOption(price: Int) = Option(price.toDouble)
}
