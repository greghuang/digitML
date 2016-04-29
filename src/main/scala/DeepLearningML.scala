import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.trend.spn.MyMLUtil

/**
  * Created by greghuang on 4/22/16.
  */
object DeepLearningML extends App {
  val sparkConf = new SparkConf(false)
    .setMaster("local[*]")
    .setAppName("DeepLearningML")
    .set("spark.driver.port", "7777")
    .set("spark.driver.host", "localhost")

  val sc = new SparkContext(sparkConf)
  val sqlCtx = new SQLContext(sc)

  //  val data = sqlCtx.read.format("libsvm").load("data/output/training_all_libsvm.txt").cache()
  val data = MyMLUtil.loadLabelData(sqlCtx, "data/output/training_data.txt").toDF("label", "features")


  val Array(trainingData, testingData) = data.randomSplit(Array(0.01, 0.4), seed = 1234L)

  val layer = Array[Int](784, 100, 30, 10)

  val trainer = new MultilayerPerceptronClassifier()
    .setLayers(layer)
    .setLabelCol("label")
    .setFeaturesCol("features")
    .setBlockSize(128)
    .setSeed(1234L)
    .setMaxIter(100)

  val model = trainer.fit(trainingData)
  val result = model.transform(testingData)

  println(result.schema)
  val predictionAndLabels = result.select("prediction", "label")
  predictionAndLabels.show(50)
  val evaluator = new MulticlassClassificationEvaluator()
    .setMetricName("precision")
  val accuracy = evaluator.evaluate(predictionAndLabels);
  println("Precision: " + accuracy)

  evaluator.setMetricName("recall")
  val recall = evaluator.evaluate(predictionAndLabels)

  println("Recall: " + recall)

  sc.stop()
}
