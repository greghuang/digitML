package org.trend.spn.feature

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{ConvolutionTransformer, Normalizer, PCA}
import org.apache.spark.mllib.linalg.{Matrices, Vector, Vectors}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.trend.spn.{MyMLUtil, MySparkApp}
import org.trend.spn.convolution.filter.Kernel
import org.trend.spn.dataset.{MnistDataSet, TrainDataSet}

import scala.collection.mutable.ListBuffer

/**
  * Created by greghuang on 5/7/16.
  */
object ConvolutionFeature extends MySparkApp with MnistDataSet with TrainDataSet {

  import sqlCtx.implicits._

  //  val data = Array(
  //    Vectors.sparse(5, Seq((1, 1.0), (3, 7.0))),
  //    Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
  //    Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0)
  //  )
  //  val df = sqlContext.createDataFrame(data.map(Tuple1.apply)).toDF("features")

  val blockSize = 5

  val convolFilter1 = new ConvolutionTransformer()
    .setInputCol("features")
    .setOutputCol("convol_emboss")
    .setKernel(Kernel("emboss3x3"))
    .setBlockSize(blockSize)

  val convolFilter2 = new ConvolutionTransformer()
    .setInputCol("features")
    .setOutputCol("convol_sobelH")
    .setKernel(Kernel("sobelH3x3"))
    .setBlockSize(blockSize)

  val convolFilter3 = new ConvolutionTransformer()
    .setInputCol("features")
    .setOutputCol("convol_sobelV")
    .setKernel(Kernel("sobelV3x3"))
    .setBlockSize(blockSize)

  val convolFilter4 = new ConvolutionTransformer()
    .setInputCol("features")
    .setOutputCol("convol_gradientV")
    .setKernel(Kernel("gradientV3x3"))
    .setBlockSize(blockSize)

  val convolFilter5 = new ConvolutionTransformer()
    .setInputCol("features")
    .setOutputCol("convol_gradientH")
    .setKernel(Kernel("gradientH3x3"))
    .setBlockSize(blockSize)

  val convolFilter6 = new ConvolutionTransformer()
    .setInputCol("features")
    .setOutputCol("convol_edge")
    .setKernel(Kernel("edge3x3"))
    .setBlockSize(blockSize)

  val convolFilter7 = new ConvolutionTransformer()
    .setInputCol("features")
    .setOutputCol("convol_edgeH")
    .setKernel(Kernel("edgeH3x3"))
    .setBlockSize(blockSize)

  val convolFilter8 = new ConvolutionTransformer()
    .setInputCol("features")
    .setOutputCol("convol_edgeV")
    .setKernel(Kernel("edgeV3x3"))
    .setBlockSize(blockSize)


  //  val normalizer = new Normalizer()
  //    .setInputCol("convol_emboss")
  //    .setOutputCol("nconvol_emboss")

  //  val trans = normalizer.transform(convolFilter.transform(df)).cache()

  //  val trans1 = convolFilter1.transform(df).cache()
  //  val trans = convolFilter2.transform(trans).cache()

  val pipeline = new Pipeline()
    //    .setStages(Array(convolFilter1, convolFilter2, convolFilter3, convolFilter4, convolFilter5, convolFilter6))
    //    .setStages(Array(convolFilter1, convolFilter7, convolFilter8))
    .setStages(Array(convolFilter1))

  val convolModel = pipeline.fit(training)
  val convolTrain = convolModel.transform(training)
  val convolTest = convolModel.transform(mnist)

  val doPCA = true
  if (!doPCA) {
    convolTrain
      .select($"name", $"label", $"convol_emboss", $"convol_edgeH", $"convol_edgeV")
      .write.parquet("data/train/features/convol3filter.parquet")
    convolTest
      .select($"name", $"label", $"convol_emboss", $"convol_edgeH", $"convol_edgeV")
      .write.parquet("data/mnist/features/convol3filter.parquet")
  }
  else {
    val sampleSize = 60
    val pca1 = new PCA()
      .setInputCol("convol_emboss")
      .setOutputCol("pca_convol_emboss")
      .setK(sampleSize)
      .fit(convolTrain)

//    val pca2 = new PCA()
//      .setInputCol("convol_edgeH")
//      .setOutputCol("pca_convol_edgeH")
//      .setK(sampleSize)
//      .fit(convolTrain)

//    val pca3 = new PCA()
//      .setInputCol("convol_edgeV")
//      .setOutputCol("pca_convol_edgeV")
//      .setK(sampleSize)
//      .fit(convolTrain)

    //    val pca4 = new PCA()
    //      .setInputCol("convol_gradientV")
    //      .setOutputCol("pca_convol_gradientV")
    //      .setK(sampleSize)
    //      .fit(trans)
    //
    //    val pca5 = new PCA()
    //      .setInputCol("convol_gradientH")
    //      .setOutputCol("pca_convol_gradientH")
    //      .setK(sampleSize)
    //      .fit(trans)
    //
    //    val pca6 = new PCA()
    //      .setInputCol("convol_edge")
    //      .setOutputCol("pca_convol_edge")
    //      .setK(sampleSize)
    //      .fit(trans)

    val pipeline2 = new Pipeline()
      .setStages(Array(pca1))

    val model2 = pipeline2.fit(convolTrain)
    val pcaTrain = model2.transform(convolTrain)
    val pcaTest = model2.transform(convolTest)

    pcaTrain
      .select($"name", $"label", $"pca_convol_emboss")
      .write.parquet("data/train/features/convol1filter_pca.parquet")

    pcaTest
      .select($"name", $"label", $"pca_convol_emboss")
      .write.parquet("data/mnist/features/convol1filter_pca.parquet")
  }

  //  trans.select($"convol_emboss").map(row =>
  //    row(0).asInstanceOf[Vector].toArray).collect().foreach { rw =>
  //    val m = Matrices.dense(20, 20, rw)
  //    println(m.toString(20, 400))
  //    println
  //  }

  //  pcaDF.select($"pca_convol_emboss").map(row =>
  //    row(0).asInstanceOf[Vector].toArray).collect().foreach { rw =>
  //    rw.foreach(print)
  //    println
  //  }


  //  trans.select($"nconvol_blur").map(row =>
  //    row(0).asInstanceOf[Vector].toArray).collect().foreach { rw =>
  //    val m = Matrices.dense(20, 20, rw)
  //    println(m.toString(20, 400))
  //    println
  //  }

  //  trans
  //    .select($"name", $"convol_emboss", $"convol_sobelH", $"convol_sobelV", $"convol_gradientV", $"convol_gradientH", $"convol_edge")
  //    .write.parquet("data/train/features/convol6filter.parquet")
  sc.stop()
}
