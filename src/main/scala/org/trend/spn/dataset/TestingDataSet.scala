package org.trend.spn.dataset

import org.apache.spark.sql.SQLContext
import org.trend.spn.{MyMLUtil, TupleUDF}

/**
  * Created by greghuang on 5/11/16.
  */
trait TestingDataSet {
  implicit val sqlCtx: SQLContext

  import sqlCtx.implicits._

  lazy val testing = MyMLUtil.loadLabelFeatures(sqlCtx, "data/test/testing_20x20_all.txt").cache()
  lazy val expectionFeatTrainingData = sqlCtx.read.parquet("data/mnist/features/expFeat_60000_20x20.parquet").toDF("name", "label", "expectFeat")
  lazy val projectFeatTrainingData = sqlCtx.read.parquet("data/mnist/features/projectFeat_60000_20x20.parquet").toDF("name2", "label2", "projFeat")

  lazy val TestingData = expectionFeatTrainingData
    .join(projectFeatTrainingData, $"name" === $"name2")
    .withColumn("features", TupleUDF.mergeCol($"expectFeat", $"projFeat"))
    //    .join(convolFeatTraningData, $"name" === $"name3")
    //    .withColumn("features", TupleUDF.merge5Col($"expectFeat", $"projFeat", $"convol_emboss", $"convol_edgeV", $"convol_edgeH"))
    .select("name", "label", "features")
    .repartition(2)
}
