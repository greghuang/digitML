package org.trend.spn

import java.util.concurrent.TimeUnit.{NANOSECONDS => NANO}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
  * Created by greghuang on 4/22/16.
  */
case class FeatureImage(name: String, label: Double, features: Vector)

object MyMLUtil {
//  def loadCVS(sqlCtx: SQLContext, path: String): DataFrame = {
//    import sqlCtx.implicits._
//    val df = sqlCtx.read
//      .format("com.databricks.spark.csv")
//      .options(Map("header" -> "false", "inferSchema" -> "true"))
//      .load(path)
//    df.toDF()
//  }
  def loadLabelFeatures(sqlCtx: SQLContext, path: String, sep: String = " "): DataFrame = {
    import sqlCtx.implicits._
    val df = sqlCtx.read.text(path)
    df.map(row => {
      val splitted = row.getString(0).split(sep)
      val feature = splitted.drop(2).map(_.toDouble)
      FeatureImage(splitted(0), splitted(1).toDouble, Vectors.dense(feature))
    }).toDF()
  }

  def loadLabelData(sqlCtx: SQLContext, path: String): DataFrame = {
    val source = Source.fromFile(path)
    val vectorArray = source.getLines()
      .map(_.split(" ")
        .drop(1)
        .map(_.toDouble))
      .toArray

    var vectorBuf = new ListBuffer[(Double, Vector)]()
    vectorArray.foreach(sample => {
      val features = sample
        .drop(1)
        //.map(f => f / 255.0)
      val t = (sample.apply(0), Vectors.dense(features))
      vectorBuf += t
    })

    val data = sqlCtx.createDataFrame(vectorBuf.toSeq)
    source.close()
    data
  }

  def convertMultiClassesToSingleClass(sqlCtx: SQLContext, path: String, targetLabel: Double): DataFrame = {
    val df = sqlCtx.read.format("libsvm").load(path)
//    val rdd = raw.map(row => row.getDouble(0) match {
//      case `targetLabel` => Row(1.0, row(1))
//      case _ => Row(0.0, row(1))
//    })

    val rdd = df.map {
      case Row(label: Double, features: Vector) => label match {
        case `targetLabel` => Row(1.0, features)
        case _ => Row(0.0, features)
      }
    }

    val data = sqlCtx.createDataFrame(rdd, df.schema)
    data.toDF("label", "features")
  }

  def time[R](block: => R): (Long, R) = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    (NANO.toSeconds(t1 - t0), result)
  }

  def showDataFrame(df : DataFrame): Unit = {
    println("Schema::" + df.schema)
    df.collect().foreach(println)
  }

  // For testing
  def main(args: Array[String]) {
    val sparkConf = new SparkConf(false)
      .setMaster("local[*]")
      .setAppName("MySpark")
      .set("spark.driver.port", "7777")
      .set("spark.driver.host", "localhost")

    val sc = new SparkContext(sparkConf)
    val sqlCtx = new SQLContext(sc)

    val df = MyMLUtil.loadLabelFeatures(sqlCtx, "data/output/train2.txt")
    println(df.first())

    sc.stop
  }
}
