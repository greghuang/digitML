package org.trend.spn

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.ml.tuning.{CrossValidatorModel, ParamGridBuilder, CrossValidator}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by greghuang on 4/24/16.
 * [5/5 12:18] Test Error = 0.25855130784708247, Recall:0.7414486921529175
 * [5/5 12:37] Test Error = 0.15182186234817818, Recall:0.8481781376518218, expF + proF(28+28) + 100 trees
 * [5/5 12:49] Test Error = 0.14473684210526316, Recall:0.8552631578947368, expF + proF(14+14) + 100 trees
 * [5/5 12:51] Test Error = 0.13056680161943324, Recall:0.8694331983805668, expF + proF(14+14) + 300 trees
 */
object DigitClassifierInRF {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf(false)
      .setMaster("local[*]")
      .setAppName("MySpark")
      .set("spark.driver.port", "7777")
      .set("spark.driver.host", "localhost")

    val sc = new SparkContext(sparkConf)
    val sqlCtx = new SQLContext(sc)

    import sqlCtx.implicits._

    val data1 = sqlCtx.read.parquet("data/features/expectData.parquet").cache()
    val data2 = MyMLUtil.loadLabelFeatures(sqlCtx, "data/features/projectFeature_14_14.txt").toDF("name2", "lable2", "proFeatures").cache()

    val data = data1.join(data2, $"name" === $"name2")
      .withColumn("features", TupleUDF.mergeCol($"expFeatures", $"proFeatures"))
      //      .withColumn("intLabel", $"label".cast(IntegerType))
      .select("name", "label", "features")
      .cache()

    //    data.printSchema()
    //    println(data.first())
    //    sc.stop()
    //    return

    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(data)

    //    val featureIndexer = new VectorIndexer()
    //      .setInputCol("features")
    //      .setOutputCol("indexedFeatures")
    //      .setMaxCategories(10)
    //      .fit(data)

    val Array(trainingData, testingData) = data.randomSplit(Array(0.9, 0.1), seed = 1234L)

    //    val rf = new RandomForestClassifier()
    //      .setLabelCol("indexedLabel")
    //      .setFeaturesCol("indexedFeatures")
    //      .setNumTrees(100)

    val rf = new RandomForestClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("features")
      .setNumTrees(100)


    val labelConvertor = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    val pipeline = new Pipeline().setStages(Array(labelIndexer, rf, labelConvertor))
    //val pipelineModel = pipeline.fit(trainingData)

    //  pipelineModel.transform(data).collect().foreach {
    //    case Row(label: Double, features: Vector, scaleF: Vector, normF: Vector) =>
    //      println(s"($label) -> $scaleF   $normF")
    //  }

    //val predictions = pipelineModel.transform(testingData)


    val gridParam = new ParamGridBuilder()
      .addGrid(rf.numTrees, Array(50, 100))
      .addGrid(rf.maxDepth, Array(5, 10, 20))
      .addGrid(rf.minInstancesPerNode, Array(1, 5, 10))
      .build()

    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new MulticlassClassificationEvaluator)
      .setEstimatorParamMaps(gridParam)
      .setNumFolds(5)

    val (trainingDuration, cvModel) = MyMLUtil.time(cv.fit(trainingData))

    cvModel.save("data/model/rf-model")

    //val cvModel = CrossValidatorModel.load("data/model/lr-model")

    val (predictionDuration, predictions) = MyMLUtil.time(cvModel.transform(testingData))

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

    val result = predictions.select("name", "predictedLabel", "label", "prediction", "probability")
    MyMLUtil.showDataFrame(result)

    println(s"Training Time ${trainingDuration} sec\n")
    println(s"Prediction Time ${predictionDuration} sec\n")
    println("Test Error = " + (1.0 - accuracy))
    println("Recall:" + recall)

    // Save results
//    val predLabelMapping = result.select($"predictedLabel", $"prediction").distinct
//    predLabelMapping
//      .coalesce(1)
//      .write
//      .format("com.databricks.spark.csv")
//      .options(Map("header" -> "true", "inferSchema" -> "true"))
//      .save("data/mapping_csv")
//
//    result
//      .coalesce(1)
//      .write
//      .format("com.databricks.spark.csv")
//      .options(Map("header" -> "true", "inferSchema" -> "true"))
//      .save("data/rf_csv")

    sc.stop()

  }
}
