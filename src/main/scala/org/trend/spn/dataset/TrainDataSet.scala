package org.trend.spn.dataset

import org.apache.spark.sql.SQLContext
import org.trend.spn.{MyMLUtil, MySparkApp, TupleUDF}

/**
  * Created by greghuang on 5/8/16.
  */
trait TrainDataSet {
  implicit val sqlCtx: SQLContext

  //  lazy val training = MyMLUtil.loadLabelFeatures(sqlCtx, "data/train/training_80000.txt").cache()
  lazy val training = MyMLUtil.loadLabelFeatures(sqlCtx, "data/train/train_countour.txt").select("name", "features").toDF("name4", "countour").cache()

  // Training
  lazy private val expectionFeatTrainingData = sqlCtx.read.parquet("data/train/features/expFeat_80000_20x20.parquet").toDF("name", "label", "expectFeat")
  lazy private val projectFeatTrainingData = sqlCtx.read.parquet("data/train/features/projectFeat_80000_20x20.parquet").toDF("name2", "label2", "projFeat")
  lazy private val convolFeatTraningData = sqlCtx.read.parquet("data/train/features/convol1filter_pca.parquet").toDF("name3", "label3", "convol_emboss")

  import sqlCtx.implicits._

  lazy val TrainData = expectionFeatTrainingData
    .join(projectFeatTrainingData, $"name" === $"name2")
    //    .withColumn("features", TupleUDF.mergeCol($"expectFeat", $"projFeat"))
    .join(training, $"name" === $"name4")
    .withColumn("features", TupleUDF.merge3Col($"expectFeat", $"projFeat", $"countour"))
    //    .join(convolFeatTraningData, $"name" === $"name3")
    //    .withColumn("features", TupleUDF.merge3Col($"expectFeat", $"projFeat", $"convol_emboss"))
    //    .withColumn("features", TupleUDF.merge5Col($"expectFeat", $"projFeat", $"convol_emboss", $"convol_edgeV", $"convol_edgeH"))
    .select("name", "label", "features")
    .repartition(2)
    .cache()
}
