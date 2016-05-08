package org.trend.spn.feature

import org.apache.spark.ml.feature.{PCA, ProjectTransformer}
import org.trend.spn.{MyMLUtil, MySparkApp}

/**
  * Created by greghuang on 5/7/16.
  */
object ProjectFeature extends MySparkApp {

  //  val df = MyMLUtil.loadLabelFeatures(sqlCtx, "data/train/features/projectFeatures.txt").toDF("name", "label", "projFeature")

  val data = MyMLUtil.loadLabelFeatures(sqlCtx, "data/train/training_60000.txt")
  // val data = MyMLUtil.loadLabelFeatures(sqlCtx, "data/test/testing_20x20.txt")
  data.printSchema()
//  println("Count:" + data.select("name", "features").distinct().count())

    val transformer = new ProjectTransformer()
      .setInputCol("features")
      .setOutputCol("projFeatures")

    val trans = transformer.transform(data)
    println(trans.select("projFeatures").first)
    println("Count:" + trans.select("projFeatures").distinct().count())
    trans.select("name", "label", "projFeatures").show()


  import sqlCtx.implicits._

    trans
      .select($"name", $"label", $"projFeatures")
      .write.parquet("data/train/features/projectFeat_60000_20x20.parquet")

  //  df
  //    .select($"name", $"projFeature")
  //    .write.parquet("data/train/features/proj_feature.parquet")

  sc.stop()
}
