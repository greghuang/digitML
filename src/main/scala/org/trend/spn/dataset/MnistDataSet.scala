package org.trend.spn.dataset

import org.apache.spark.sql.SQLContext
import org.trend.spn.{MyMLUtil, TupleUDF}
import org.trend.spn.feature.ExpectationFeature._

/**
  * Created by greghuang on 5/11/16.
  */
trait MnistDataSet {
  implicit val sqlCtx: SQLContext

  import sqlCtx.implicits._

  lazy val mnist = MyMLUtil.loadLabelFeatures(sqlCtx, "data/mnist/mnist_60000_20x20.txt")
  private lazy val expectionFeatTrainingData = sqlCtx.read.parquet("data/mnist/features/expFeat_60000_20x20.parquet").toDF("name", "label", "expectFeat")
  private lazy val projectFeatTrainingData = sqlCtx.read.parquet("data/mnist/features/projectFeat_60000_20x20.parquet").toDF("name2", "label2", "projFeat")
  private lazy val convolFeatTraningData = sqlCtx.read.parquet("data/mnist/features/convol1filter_pca.parquet").toDF("name3", "label3", "convol_emboss")

  lazy val TestingData = expectionFeatTrainingData
    .join(projectFeatTrainingData, $"name" === $"name2")
    //    .withColumn("features", TupleUDF.mergeCol($"expectFeat", $"projFeat"))
    .join(convolFeatTraningData, $"name" === $"name3")
    .withColumn("features", TupleUDF.merge3Col($"expectFeat", $"projFeat", $"convol_emboss"))
    //    .withColumn("features", TupleUDF.merge5Col($"expectFeat", $"projFeat", $"convol_emboss", $"convol_edgeV", $"convol_edgeH"))
    .select("name", "label", "features")
    .repartition(2)
    .cache()
}
