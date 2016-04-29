import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.sql.functions.max
import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression, OneVsRest}
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.trend.spn.MyMLUtil

/**
  * Created by greghuang on 4/21/16.
  */
object LogisticRegressionML extends App {
  val sparkConf = new SparkConf(false)
    .setMaster("local[*]")
    .setAppName("LogisticRegressionML")
    .set("spark.driver.port", "7777")
    .set("spark.driver.host", "localhost")

  val sc = new SparkContext(sparkConf)
  val sqlCtx = new SQLContext(sc)

  // val data = sqlCtx.read.format("libsvm").load("data/sample_libsvm_data.txt")
  // val data = sqlCtx.read.format("libsvm").load("data/output/training_1_libsvm.txt")
  // val data = sqlCtx.read.format("libsvm").load("data/output/training_all_libsvm.txt")
  val data = MyMLUtil.convertMultiClassesToSingleClass(sqlCtx, "data/output/training_all_libsvm.txt", 6.0)
  val Array(trainingData, testingData) = data.randomSplit(Array(0.8, 0.2), seed = 11L)

  trainingData.cache()

  val lr = new LogisticRegression()
    .setMaxIter(50)
    .setRegParam(0.05)
    .setElasticNetParam(0.8)
    .setTol(1E-6)
    .setThreshold(0.5)
    .setFitIntercept(true)

  // Pipeline with CrossValidation
  val  pipeline = new Pipeline().setStages(Array(lr))

  val gridParam = new ParamGridBuilder()
    .addGrid(lr.regParam, Array(0.1, 0.01))
    .addGrid(lr.elasticNetParam, Array(0.7, 0.9))
    .build()

  val cv = new CrossValidator()
    .setEstimator(pipeline)
    .setEvaluator(new BinaryClassificationEvaluator)
    .setEstimatorParamMaps(gridParam)
    .setNumFolds(5)

  val (trainingDuration, cvModel) = MyMLUtil.time(cv.fit(trainingData))

//  cvModel.save("data/model/lr-model")
//
//  val cvModel = CrossValidatorModel.load("data/model/lr-model")

  val (predictionDuration, predictions) = MyMLUtil.time(cvModel.transform(testingData))

  // OneVsRest
  //  val ovr = new OneVsRest()
  //    .setClassifier(lr)
  //
  //  val (trainingDuration, ovrModel) = MyMLUtil.time(ovr.fit(trainingData))
  //  val (predictionDuration, predictions) = MyMLUtil.time(ovrModel.transform(testingData))
  //
  //  println(predictions.schema)
  //  val predictionAndLabels = predictions.select("prediction", "label")
  //    .map(row => (row.getDouble(0), row.getDouble(1)))
  //
  //  val metrics = new MulticlassMetrics(predictionAndLabels)
  //  val confusionMatrix = metrics.confusionMatrix
  //  val predictionColSchema = predictions.schema("prediction")
  //  val numClasses = MetadataUtils.getNumClasses(predictionColSchema).get
  //  val fprs = Range(0, numClasses).map(p => (p, metrics.falsePositiveRate(p.toDouble)))
  //
  //  println(s" Training Time ${trainingDuration} sec\n")
  //
  //  println(s" Prediction Time ${predictionDuration} sec\n")
  //
  //  println(s" Confusion Matrix\n ${confusionMatrix.toString}\n")
  //
  //  println(s"Precision: ${metrics.precision}\n")
  //
  //  println("label\tfpr")
  //
  //  println(fprs.map {case (label, fpr) => label + "\t" + fpr}.mkString("\n"))
  // $example off$

  //////////////////////////
//  val (trainingDuration, lrModel) = MyMLUtil.time(lr.fit(trainingData))
//
//  println(s"Coefficients: ${lrModel.coefficients}")
//  println(s"Intercept: ${lrModel.intercept}")
//
//  val trainingSummary = lrModel.summary
//
//  val objectiveHistory = trainingSummary.objectiveHistory
//  objectiveHistory.foreach(loss => println("loss:" + loss))
//  val binarySummary = trainingSummary.asInstanceOf[BinaryLogisticRegressionSummary]
//  val roc = binarySummary.roc
//  roc.show()
//  println(binarySummary.areaUnderROC)
//
//  val fMeasure = binarySummary.fMeasureByThreshold
//  val maxFMeasure = fMeasure.select(max("F-Measure")).head().getDouble(0)
//  val bestThreshold = fMeasure.where(fMeasure.col("F-Measure").equalTo(maxFMeasure))
//    .select("threshold").head().getDouble(0)
//  println("Best:" + bestThreshold)
//
//  lrModel.setThreshold(bestThreshold)
//  testingData.cache()
//  val (predictionDuration, predictions) = MyMLUtil.time(lrModel.transform(testingData))

//  predictions.select("prediction", "label", "probability").show(30)
//    .collect().foreach {
//      case Row(predict: Double, label: Double, prob: Vector) =>
//        println(s"($label) -> prediction=$predict, prob=$prob")
//    }

  val evaluator = new BinaryClassificationEvaluator()
    .setLabelCol("label")
    .setRawPredictionCol("rawPrediction")
    .setMetricName("areaUnderROC")

  val accuracy = evaluator.evaluate(predictions)

  println("ROC:" + accuracy)
  //  println("Precision:" + evaluator.setMetricName("precision").evaluate(predictions))
  //println("Test error:" + (1.0 - accuracy))

//  println(s"Training Time ${trainingDuration} sec\n")
  println(s"Prediction Time ${predictionDuration} sec\n")

  sc.stop()
}

