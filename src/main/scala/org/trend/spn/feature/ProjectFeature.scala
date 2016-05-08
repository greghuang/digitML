package org.trend.spn.feature

import org.apache.spark.ml.feature.{PCA, ProjectTransformer}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.trend.spn.{MyMLUtil, MySpark}

/**
  * Created by greghuang on 5/7/16.
  */
object ProjectFeature extends App with MySpark {

//  val df = MyMLUtil.loadLabelFeatures(sqlCtx, "data/train/features/projectFeatures.txt").toDF("name", "label", "projFeature")

  val data = MyMLUtil.loadLabelFeatures(sqlCtx, "data/train/training_60000.txt")
  data.printSchema()

  val transformer = new ProjectTransformer()
    .setInputCol("features")
    .setOutputCol("projFeatures")

  val trans = transformer.transform(data)
  println(trans.select("projFeatures").first)
  println("Count:" + trans.select("projFeatures").distinct().count())


  import sqlCtx.implicits._
  trans
    .select($"name", $"label", $"projFeatures")
    .write.parquet("data/train/features/projectFeat_60000_20x20.parquet")

//  df
//    .select($"name", $"projFeature")
//    .write.parquet("data/train/features/proj_feature.parquet")

  sc.stop()
}
