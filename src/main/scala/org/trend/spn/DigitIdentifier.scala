package org.trend.spn

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by greghuang on 4/24/16.
  */
object DigitIdentifier {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf(false)
      .setMaster("local[*]")
      .setAppName("MySpark")
      .set("spark.driver.port", "7777")
      .set("spark.driver.host", "localhost")

    val sc = new SparkContext(sparkConf)
    val sqlCtx = new SQLContext(sc)

    val data = sqlCtx.read.parquet("data/train/expectData.parquet").toDF("label", "features").cache()

    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(data)

    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(10)
      .fit(data)

    val Array(trainingData, testingData) = data.randomSplit(Array(0.9, 0.1), seed = 1234L)

    val rf = new RandomForestClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setNumTrees(500)

    val labelConvertor = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, rf, labelConvertor))
    val pipelineModel = pipeline.fit(trainingData)
    //  pipelineModel.transform(data).collect().foreach {
    //    case Row(label: Double, features: Vector, scaleF: Vector, normF: Vector) =>
    //      println(s"($label) -> $scaleF   $normF")
    //  }

    val predictions = pipelineModel.transform(testingData)
    predictions.select("predictedLabel", "label", "probability").show(40)

    val evaluator1 = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("precision")

    val accuracy = evaluator1.evaluate(predictions)

    val evaluator2 = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("recall")

    val recall = evaluator2.evaluate(predictions)

    println("Test Error = " + (1.0 - accuracy))
    println("Recall:" + recall)

    sc.stop()

  }
}
