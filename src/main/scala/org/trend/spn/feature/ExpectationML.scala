package org.trend.spn.feature

import org.apache.spark.ml.feature.ExpectationScaler
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.trend.spn.MyMLUtil

/**
  * Created by greghuang on 4/27/16.
  */
object ExpectationML extends  App {
  val sparkConf = new SparkConf(false)
    .setMaster("local[*]")
    .setAppName("MySpark")
    .set("spark.driver.port", "7777")
    .set("spark.driver.host", "localhost")

  val sc = new SparkContext(sparkConf)
  val sqlCtx = new SQLContext(sc)

  val data = MyMLUtil.loadLabelData(sqlCtx, "data/output/train1.txt").toDF("label", "features")

  val scaler = new ExpectationScaler().setInputCol("features").setOutputCol("scaledFeatures")
  val scaleModel = scaler.fit(data)
  scaleModel.transform(data).collect().foreach {
    case Row(label: Double, features: Vector, scaleF: Vector) =>
      println(s"($label) -> $scaleF")
  }

  sc.stop()
}
