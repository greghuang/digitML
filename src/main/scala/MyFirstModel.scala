import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{Binarizer, MinMaxScaler}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.{Row, SQLContext}

import scala.collection.mutable.ListBuffer

/**
  * Created by greghuang on 4/12/16.
  */

object MyFirstModel extends App {
  val source = Source.fromFile("data/output/training_test.txt")
  val vectorArray = source.getLines()
    .map(_.split(" ")
//      .drop(1)
      .map(_.toDouble))
    .toArray

  var vectorBuf = new ListBuffer[(Double, Vector)]()
  vectorArray.foreach(sample => {
    val t = (sample.apply(0), Vectors.dense(sample.drop(1)))
    vectorBuf += t
  })

  val sparkConf = new SparkConf(false)
    .setMaster("local[*]")
    .setAppName("MySpark")
    .set("spark.driver.port", "7777")
    .set("spark.driver.host", "localhost")

  val sc = new SparkContext(sparkConf)
  val sqlCtx = new SQLContext(sc)

  val data = sqlCtx.createDataFrame(vectorBuf.toSeq).toDF("label", "features").cache()

  data.select("label", "features").collect().foreach {
    case Row(label: Double, features: Vector) =>
      println(s"($label) -> $features")
  }

  val scaler = new MinMaxScaler()
    .setInputCol("features").setOutputCol("scaledFeatures")

  val scaleModel = scaler.fit(data)

  println("Max:" + scaleModel.getMax + " Min:" + scaleModel.getMin)
  val scaleData = scaleModel.transform(data)

  scaleData.select("label", "scaledFeatures").collect().foreach {
    case Row(label: Double, features: Vector) =>
      println(s"($label) -> $features")
  }
  //  val Array(trainingData, testingData) = data.randomSplit(Array(0.01, 0.7))
  //
  //  data.printSchema()
  //
  //  val data2 = Seq(
  //    Vectors.dense(0.0, 1.0, -2.0, 3.0),
  //    Vectors.dense(-1.0, 2.0, 4.0, -7.0),
  //    Vectors.dense(14.0, -2.0, -5.0, 1.0))
  //
  //  val df = sqlCtx.createDataFrame(data2.map(Tuple1.apply)).toDF("features")
  //
  //  df.printSchema()
  //  df.select("features").collect().foreach(println)

  //  val binarizer = new Binarizer()
  //    .setInputCol("features")
  //    .setOutputCol("binary_feat")
  //    .setThreshold(10)
  //
  //  val binData = binarizer.transform(trainingData)
  //  val binFeatures = binData.select("binary_feat").show(1)

  //  val kmeans = new KMeans().setK(10).setMaxIter(10).setFeaturesCol("features").setPredictionCol("prediction")
  //  val model = kmeans.fit(trainingData)
  //  val predResult = model.transform(testingData)
  //
  //  // Shows the result
  //  predResult.select("label", "prediction").show(20)
  //
  //  println("Final Centers: ")
  //  model.clusterCenters.foreach(println)

  sc.stop()
  //  source.close()
}
