package org.trend.spn.feature

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.trend.spn.dataset.{MnistDataSet, TrainDataSet}
import org.trend.spn.{MyMLUtil, MySparkApp}

/**
  * Created by greghuang on 4/27/16.
  */
object ExpectationFeature extends MySparkApp with TrainDataSet with MnistDataSet{
//  MyMLUtil.deleteFolder("data/train/features/expFeat_20x20_all.parquet")
//  MyMLUtil.deleteFolder("data/mnist/features/expFeat_20x20_all.parquet")

//  val test = MyMLUtil.loadLabelFeatures(sqlCtx, "data/test/testing_20x20_all.txt")

  val expect = new ExpectationScaler()
    .setInputCol("features")
    .setOutputCol("scaledFeatures")
    .setThreshold(0.0)

  val normalizer = new Normalizer()
    .setInputCol("scaledFeatures")
    .setOutputCol("expFeatures")

  val model = new Pipeline().setStages(Array(expect, normalizer)).fit(training)
  val expTrain = model.transform(training).select("name", "label", "expFeatures")
  val expMnist = model.transform(mnist).select("name", "label", "expFeatures")

  //transformData.show()
  println("Count:" + expTrain.select("expFeatures").distinct().count())

  expTrain.write.parquet("data/train/features/expFeat_80000_countour.parquet")
//  expMnist.write.parquet("data/mnist/features/expFeat_60000_20x20.parquet")
  sc.stop()
}
