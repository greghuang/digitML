package org.trend.spn.feature

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
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

//  val data = MyMLUtil.loadLabelData(sqlCtx, "data/output/train1.txt").toDF("label", "features")
  val data = MyMLUtil.loadLabelData(sqlCtx, "data/output/training_data.txt").toDF("label", "features")

  val expect = new ExpectationScaler().setInputCol("features").setOutputCol("scaledFeatures")
  val normalizer = new Normalizer()
    .setInputCol("scaledFeatures")
    .setOutputCol("normFeatures")

  val p1 = new Pipeline().setStages(Array(expect, normalizer)).fit(data)
  val transformData = p1.transform(data).select("label", "normFeatures").cache()
//  MyMLUtil.showDataFrame(transformData)
  transformData.write.parquet("data/train/expectData.parquet")
  sc.stop()
}
