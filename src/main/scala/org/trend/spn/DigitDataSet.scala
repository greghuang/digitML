package org.trend.spn

/**
  * Created by greghuang on 5/8/16.
  */
trait DigitDataSet extends MySparkApp {

  // Training
  val expectionFeatTrainingData = sqlCtx.read.parquet("data/train/features/expFeat_60000_20x20.parquet").toDF("name", "label", "expectFeat")
  val projectFeatTrainingData = sqlCtx.read.parquet("data/train/features/projectFeat_60000_20x20_2.parquet").toDF("name2", "label2", "projFeat")
//  val convolFeatTraningData = sqlCtx.read.parquet("data/train/features/convol3filter.parquet").toDF("name3", "label3", "convol_emboss", "convol_edgeH", "convol_edgeV")

  // Testing
  val expectionFeatTestingData = sqlCtx.read.parquet("data/test/features/expFeat_20x20_all.parquet").toDF("name", "label", "expectFeat")
  val projectFeatTestingData = sqlCtx.read.parquet("data/test/features/projectFeat_20x20_all.parquet").toDF("name2", "label2", "projFeat")

  import sqlCtx.implicits._
  val TrainData = expectionFeatTrainingData
    .join(projectFeatTrainingData, $"name" === $"name2")
    .withColumn("features", TupleUDF.mergeCol($"expectFeat", $"projFeat"))
//    .join(convolFeatTraningData, $"name" === $"name3")
//    .withColumn("features", TupleUDF.merge5Col($"expectFeat", $"projFeat", $"convol_emboss", $"convol_edgeV", $"convol_edgeH"))
    .select("name", "label", "features")
    .repartition(2)

//  val TestingData = expectionFeatTestingData
//    .join(projectFeatTestingData, $"name" === $"name2")
//    .withColumn("features", TupleUDF.mergeCol($"expectFeat", $"projFeat"))
//    //    .join(data3, $"name" === $"name3")
//    //    .withColumn("features", TupleUDF.merge8Col($"expFeatures", $"proFeatures", $"convol_emboss", $"convol_sobelH", $"convol_sobelV", $"convol_gradientV", $"convol_gradientH", $"convol_edge"))
//    .select("name", "features")
//    .repartition(3)
}
