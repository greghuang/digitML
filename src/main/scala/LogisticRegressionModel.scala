import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by greghuang on 4/21/16.
  */
object LogisticRegressionModel extends App {
  val sparkConf = new SparkConf(false)
    .setMaster("local[*]")
    .setAppName("LogisticRegressionModel")
    .set("spark.driver.port", "7777")
    .set("spark.driver.host", "localhost")

  val sc = new SparkContext(sparkConf)
  val data = MLUtils.loadLibSVMFile(sc, "data/output/training_all_libsvm.txt")

  val Array(training, testing) = data.randomSplit(Array(0.8, 0.2), seed = 11L)
  training.cache()

  // Run training algorithm to build the model
  val model = new LogisticRegressionWithLBFGS()
    .setNumClasses(10).run(training)

  val predictionResult = testing.map {
    case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
  }

  predictionResult.foreach(println)

  // Instantiate metrics object
  val metrics = new MulticlassMetrics(predictionResult)

  println("Confusion matrix:")
  println(metrics.confusionMatrix)

  // Overall Statistics
  val precision = metrics.precision
  val recall = metrics.recall // same as true positive rate
  val f1Score = metrics.fMeasure
  println("Summary Statistics")
  println(s"Precision = $precision")
  println(s"Recall = $recall")
  println(s"F1 Score = $f1Score")

  // Precision by label
  val labels = metrics.labels
  labels.foreach { l =>
    println(s"Precision($l) = " + metrics.precision(l))
  }

  // Recall by label
  labels.foreach { l =>
    println(s"Recall($l) = " + metrics.recall(l))
  }

  // False positive rate by label
  labels.foreach { l =>
    println(s"FPR($l) = " + metrics.falsePositiveRate(l))
  }

  // F-measure by label
  labels.foreach { l =>
    println(s"F1-Score($l) = " + metrics.fMeasure(l))
  }

  // Weighted stats
  println(s"Weighted precision: ${metrics.weightedPrecision}")
  println(s"Weighted recall: ${metrics.weightedRecall}")
  println(s"Weighted F1 score: ${metrics.weightedFMeasure}")
  println(s"Weighted false positive rate: ${metrics.weightedFalsePositiveRate}")
  // $example off$
}
