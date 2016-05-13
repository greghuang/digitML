package org.trend.spn.feature

import org.apache.spark.ml.feature.{PCA, ProjectTransformer}
import org.trend.spn.dataset.{MnistDataSet, TrainDataSet}
import org.trend.spn.{MyMLUtil, MySparkApp}

/**
  * Created by greghuang on 5/7/16.
  */
object ProjectFeature extends MySparkApp with MnistDataSet with TrainDataSet {

  val data = training.cache()

  data.printSchema()
  println("Count:" + data.select("features").distinct().count())

  val transformer = new ProjectTransformer()
    .setInputCol("features")
    .setOutputCol("projFeatures")

  val projTrans = transformer.transform(training)
  val projMnint = transformer.transform(mnist)
  println("Count:" + projTrans.select("projFeatures").distinct().count())
  projTrans.select("name", "label", "projFeatures").show()


  import sqlCtx.implicits._

  projTrans
    .select($"name", $"label", $"projFeatures")
    .write.parquet("data/train/features/projectFeat_80000_countour.parquet")

//  projMnint
//    .select($"name", $"label", $"projFeatures")
//    .write.parquet("data/mnist/features/projectFeat_60000_20x20.parquet")

  sc.stop()
}
