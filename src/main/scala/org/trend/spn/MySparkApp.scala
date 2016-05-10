package org.trend.spn

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * Created by greghuang on 5/7/16.
  */
trait MySparkApp extends App {
  final val sc = new SparkContext(new SparkConf(false)
    .setMaster("local[*]")
    .setAppName("MySpark")
    .set("spark.driver.port", "7777")
    .set("spark.driver.host", "localhost")
    .set("spark.driver.memory", "8g")
    .set("spark.executor.memory", "4g"))
  final val sqlCtx = new SQLContext(sc)

  val testingSamples: DataFrame = {
    val raw = Array(0, 0, 0, 0, 0, 0, 0, 0, 0, 0) ++: Array(1, 1, 1, 1, 1, 1, 1, 1, 1, 1) ++: Array(0, 0, 0, 0, 0, 0, 0, 0, 0, 0) ++: Array(3, 3, 3, 3, 3, 3, 3, 3, 3, 3) ++: Array(4, 4, 4, 4, 4, 4, 4, 4, 4, 4) ++: Array(0, 0, 0, 0, 0, 0, 0, 0, 0, 0) ++: Array(6, 6, 6, 6, 6, 6, 6, 6, 6, 6) ++: Array(7, 7, 7, 7, 7, 7, 7, 7, 7, 7) ++: Array(8, 8, 8, 8, 8, 8, 8, 8, 8, 8) ++: Array(0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
    val draw = raw.map(_.toDouble)
    var buf = ListBuffer[(Double, Vector)]()
    buf += Tuple2(0.0, Vectors.dense(draw))
    sqlCtx.createDataFrame(buf).toDF("lable", "features")
  }
}
