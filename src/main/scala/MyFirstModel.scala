import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.SQLContext

import scala.collection.mutable.ListBuffer
/**
  * Created by greghuang on 4/12/16.
  */

object MyFirstModel extends  App {
  val source = Source.fromFile("data/output/training_data.txt")
  val vectorArray = source.getLines().map(_.split(" ").drop(1).map(_.toDouble)).toArray


  var vectorBuf = new ListBuffer[(Double, Vector)]()
  vectorArray.foreach(sample => {
    val t = (sample.apply(0), Vectors.dense(sample.drop(1)))
    vectorBuf += t
  })

  val seq = vectorBuf.toSeq
  println(seq.apply(0))

  val sparkConf = new SparkConf(false)
    .setMaster("local[*]")
    .setAppName("MySpark")
    .set("spark.driver.port", "7777")
    .set("spark.driver.host", "localhost")

  val sc = new SparkContext(sparkConf)
  val sqlCtx = new SQLContext(sc)

  val data = sqlCtx.createDataFrame(seq).toDF("label","features").cache()
  val Array(trainingData, testingData) = data.randomSplit(Array(0.01, 0.7))
  trainingData.select("label").show(100)

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
  source.close()
}