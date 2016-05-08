package org.trend.spn

/**
  * Created by greghuang on 5/8/16.
  */
trait MyTraining60000 extends MySparkApp {

  val expectionFeatData = sqlCtx.read.parquet("data/train/features/expFeat_60000_20x20.parquet").toDF("name", "label", "expectFeat")
  val projectFeatData = sqlCtx.read.parquet("data/train/features/projectFeat_60000_20x20.parquet").toDF("name2", "label2", "projFeat")

  import sqlCtx.implicits._
  val training60000 = expectionFeatData
    .join(projectFeatData, $"name" === $"name2")
    .withColumn("features", TupleUDF.mergeCol($"expectFeat", $"projFeat"))
//    .join(data3, $"name" === $"name3")
//    .withColumn("features", TupleUDF.merge8Col($"expFeatures", $"proFeatures", $"convol_emboss", $"convol_sobelH", $"convol_sobelV", $"convol_gradientV", $"convol_gradientH", $"convol_edge"))
    .select("name", "label", "features")
    .repartition(3)
}
