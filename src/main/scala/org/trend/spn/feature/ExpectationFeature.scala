package org.trend.spn.feature

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.trend.spn.MyMLUtil

/**
  * Created by greghuang on 4/27/16.
  */
object ExpectationFeature extends App {
  val sparkConf = new SparkConf(false)
    .setMaster("local[*]")
    .setAppName("MySpark")
    .set("spark.driver.port", "7777")
    .set("spark.driver.host", "localhost")

  val sc = new SparkContext(sparkConf)
  val sqlCtx = new SQLContext(sc)

//  val data = MyMLUtil.loadLabelData(sqlCtx, "data/train/training_data.txt").toDF("label", "features")
  val data = MyMLUtil.loadLabelFeatures(sqlCtx, "data/train/training_60000.txt")

  val expect = new ExpectationScaler()
    .setInputCol("features")
    .setOutputCol("scaledFeatures")
    .setThreshold(0.0)

  val normalizer = new Normalizer()
    .setInputCol("scaledFeatures")
    .setOutputCol("expFeatures")

  val model = new Pipeline().setStages(Array(expect, normalizer)).fit(data)
  val transformData = model.transform(data).select("name", "label", "expFeatures")
  //MyMLUtil.showDataFrame(transformData)
  transformData.write.parquet("data/train/features/expFeat_60000.parquet")
  sc.stop()
}
