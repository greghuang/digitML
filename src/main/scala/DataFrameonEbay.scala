import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by GregHuang on 5/4/16.
 * Data source: http://www.modelingonlineauctions.com/datasets
 */

case class Auction(auctionid: String, bid: Float, bidtime: Float, bidder: String, bidderrate: Int, openbid: Float, price: Float)

object DataFrameonEbay extends App {
  val sparkConf = new SparkConf(false)
    .setMaster("local[*]")
    .setAppName("LogisticRegressionML")
    .set("spark.driver.port", "7777")
    .set("spark.driver.host", "localhost")

  val sc = new SparkContext(sparkConf)
  val sqlCtx = new SQLContext(sc)

  // this is used to implicitly convert an RDD to a DataFrame.
  import sqlCtx.implicits._

  def loadCSVWithoutSchema(path: String): DataFrame = {

    val ebayData = sc.textFile(path).collect().drop(1)
    val ebayRDD = sc.parallelize(ebayData)

    val auction = ebayRDD.map(_.split(",")).map(p =>
    Auction(
      p(0),
      p(1).toFloat,
      p(2).toFloat,
      p(3),
      p(4).toInt,
      p(5).toFloat,
      p(6).toFloat))

    auction.toDF()
  }

  def loadCSVwithSchema(path: String): DataFrame = {
    val df = sqlCtx.read
      .format("com.databricks.spark.csv")
      .options(Map("header" -> "true", "inferSchema" -> "true"))
      .load(path)
    df.toDF()
  }

  //val auctionDF = loadCSVWithoutSchema("data/Cartier+7-day+auctions.csv")
  val auctionDF = loadCSVwithSchema("data/Cartier+7-day+auctions.csv")
  auctionDF.show()
//  +----------+------+----------+-----------------+----------+-------+------+
//  | auctionid|   bid|   bidtime|           bidder|bidderrate|openbid| price|
//  +----------+------+----------+-----------------+----------+-------+------+
//  |1638843936| 500.0|0.47836804|        kona-java|       181|  500.0|1625.0|
//  |1638843936| 800.0| 0.8263889|           doc213|        60|  500.0|1625.0|

  auctionDF.printSchema()
//  root
//  |-- auctionid: string (nullable = true)
//  |-- bid: float (nullable = false)
//  |-- bidtime: float (nullable = false)
//  |-- bidder: string (nullable = true)
//  |-- bidderrate: integer (nullable = false)
//  |-- openbid: float (nullable = false)
//  |-- price: float (nullable = false)

  // How many auctions were held?
  val count = auctionDF.select("auctionid").distinct.count
  println(count)

  // How many bids per item?
  auctionDF.groupBy("auctionid").count.filter($"auctionid" === "1638843936").show
//  +----------+-----+
//  | auctionid|count|
//  +----------+-----+
//  |1638843936|    7|
//  +----------+-----+

  // What's the min number of bids per item? what's the average? what's the max?
  auctionDF.groupBy("auctionid").count.agg(min("count"), avg("count"), max("count")).show
//  +----------+------------------+----------+
//  |min(count)|        avg(count)|max(count)|
//  +----------+------------------+----------+
//  |         2|13.896907216494846|        46|
//  +----------+------------------+----------+

  auctionDF.registerTempTable("auction")

  val result = sqlCtx.sql("SELECT auctionid, bidder, count(bid) as count FROM auction GROUP BY auctionid, bidder")
  result.show

  sc.stop()

}
