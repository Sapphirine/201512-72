package edu.columbia.cs6893.db

import edu.columbia.cs6893.model.{Holding, HoldingKey}
import edu.columbia.cs6893.model.Holding._

object Database {

  val accounts = Map(
    "C3P0" -> "Science Center",
    "A1B2C3D4" -> "A1 Pension Fund",
    "A11B22C33" -> "XYZ Personal Account"
  )

  val products = Map(
    "MSFT" -> "Microsoft Common Stock",
    "BRK.B" -> "Berkshire Hathaway B",
    "CVC" -> "Cablevision Systems Co A",
    "KO" -> "Coca-Cola Co",
    "GS" -> "Goldman Sachs Group Inc",
    "IBM" -> "IBM Common Stock",
    "AA" -> "Alcoa Inc",
    "AMZN" -> "Amazon.com Inc",
    "V" -> "Visa Inc",
    "SBUX" -> "Starbucks Corp",
    "QCOM" -> "QUALCOMM Inc"
  )

  val sectors = Map(
    "MSFT" -> "Tech",
    "BRK.B" -> "Finance",
    "CVC" -> "Services",
    "KO" -> "Commodities",
    "GS" -> "Finance",
    "IBM" -> "Tech",
    "AA" -> "Services",
    "AMZN" -> "Tech",
    "V" -> "Finance",
    "SBUX" -> "Services",
    "QCOM" -> "Tech"
  )

  val portfolio = Map(1 ->
    Seq(
      Holding(HoldingKey("C3P0", "MSFT"), 100, 10),
      Holding(HoldingKey("C3P0", "BRK.B"), 170, 20),
      Holding(HoldingKey("C3P0", "CVC"), 300, 30),
      Holding(HoldingKey("C3P0", "KO"), 425, 40),
      Holding(HoldingKey("C3P0", "GS"), 500, 50),

      Holding(HoldingKey("A1B2C3D4", "MSFT"), 100, 10),
      Holding(HoldingKey("A1B2C3D4", "BRK.B"), 100, 20),
      Holding(HoldingKey("A1B2C3D4", "CVC"), 100, 30),
      Holding(HoldingKey("A1B2C3D4", "KO"), 100, 40),
      Holding(HoldingKey("A1B2C3D4", "IBM"), 100, 60),

      Holding(HoldingKey("A11B22C33", "MSFT"), 50, 10),
      Holding(HoldingKey("A11B22C33", "AA"), 50, 70),
      Holding(HoldingKey("A11B22C33", "AMZN"), 50, 70),
      Holding(HoldingKey("A11B22C33", "V"), 50, 70),
      Holding(HoldingKey("A11B22C33", "SBUX"), 50, 70),
      Holding(HoldingKey("A11B22C33", "QCOM"), 50, 70)
    )
  )

  def getHoldingKeys(portfolioId: Int) = portfolio.getOrElse(portfolioId, Seq.empty).map(_.holdingKey)

  def getHoldingKeys(productId: String) = portfolio.values.flatten.collect { case holding => holding.holdingKey }.filter(_.productId == productId)

  def getHoldings(portfolioId: Int) = portfolio.getOrElse(portfolioId, Seq.empty)

}

