import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorIndexer
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

  val data = sqlCtx.read.format("libsvm").load("data/output/training_libsvm_data.txt")
  val Array(trainingData, testingData) = data.randomSplit(Array(0.5, 0.1));

  val featureIndexer = new VectorIndexer()
    .setInputCol("features")
    .setOutputCol("indexedFeatures")
    .setMaxCategories(4)
    .fit(data)


  val dt = new DecisionTreeRegressor()
    .setLabelCol("label")
    .setFeaturesCol("indexedFeatures")

  val piple = new Pipeline()
    .setStages(Array(featureIndexer, dt))

  val model = piple.fit(trainingData)

  val prediction = model.transform(testingData)

  prediction.select("label", "prediction").show(20)

  val evaluator = new RegressionEvaluator()
    .setLabelCol("label")
    .setPredictionCol("prediction")
    .setMetricName("rmse")

  val rmse = evaluator.evaluate(prediction)
  println("Root Mean Squared Error (RMSE) on test data = " + rmse)

  val treeModel = model.stages(1).asInstanceOf[DecisionTreeRegressionModel]
  println("Learned regression tree model:\n" + treeModel.toDebugString)

  sc.stop()
}
