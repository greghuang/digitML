import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.{MulticlassClassificationEvaluator, RegressionEvaluator}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.ml.regression.{DecisionTreeRegressionModel, DecisionTreeRegressor}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by greghuang on 4/14/16.
  */
object DecisionTreeModel extends App {
  val sparkConf = new SparkConf(false)
    .setMaster("local[*]")
    .setAppName("DecisionTreeModel")
    .set("spark.driver.port", "7777")
    .set("spark.driver.host", "localhost")

  val sc = new SparkContext(sparkConf)
  val sqlCtx = new SQLContext(sc)

  val data = sqlCtx.read.format("libsvm").load("data/output/training_all_libsvm.txt").cache()

  val labelIndexer = new StringIndexer()
    .setInputCol("label")
    .setOutputCol("indexedLabel")
    .fit(data)

  val featureIndexer = new VectorIndexer()
    .setInputCol("features")
    .setOutputCol("indexedFeatures")
    .setMaxCategories(10)
    .fit(data)

  val Array(trainingData, testingData) = data.randomSplit(Array(0.9, 0.1), seed = 11L)

  val dt = new DecisionTreeClassifier()
    .setLabelCol("indexedLabel")
    .setFeaturesCol("indexedFeatures")

  val labelConvertor = new IndexToString()
    .setInputCol("prediction")
    .setOutputCol("predictedLabel")
    .setLabels(labelIndexer.labels)

  val piple = new Pipeline()
    .setStages(Array(labelIndexer, featureIndexer, dt, labelConvertor))

  val model = piple.fit(trainingData)

  val predictions = model.transform(testingData)

  predictions.select("predictedLabel", "label", "probability").show(40)

  val evaluator = new MulticlassClassificationEvaluator()
    .setLabelCol("indexedLabel")
    .setPredictionCol("prediction")
    .setMetricName("precision")

  val accuracy = evaluator.evaluate(predictions)
  println("Test Error = " + (1.0 - accuracy))

  //  println("Root Mean Squared Error (RMSE) on test data = " + rmse)
//
//  val treeModel = model.stages(2).asInstanceOf[DecisionTreeClassificationModel]
//  println("Learned classification tree model:\n" + treeModel.toDebugString)

  sc.stop()
}
