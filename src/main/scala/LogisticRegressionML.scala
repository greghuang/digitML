import org.apache.spark.sql.functions.max
import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

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

  val data = sqlCtx.read.format("libsvm").load("data/output/training_all_libsvm.txt")

  val rdd = data.map(t => t(0) match {
    case 1 => Row(1.0, t(1))
    case _ => Row(0.0, t(1))
  })

  val binaryData = sqlCtx.createDataFrame(rdd, data.schema)

  val Array(trainingData, testingData) = binaryData.randomSplit(Array(0.6, 0.4));

  trainingData.cache()

  val lr = new LogisticRegression()
    .setMaxIter(10)
    .setRegParam(0.3)
    .setElasticNetParam(0.8)

  val lrModel = lr.fit(trainingData)

//  println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

  val trainingSummary = lrModel.summary
  val objectiveHistory = trainingSummary.objectiveHistory
  objectiveHistory.foreach(loss => println("loss:" + loss))
  val binarySummary = trainingSummary.asInstanceOf[BinaryLogisticRegressionSummary]
  val roc = binarySummary.roc
  roc.show()
  println(binarySummary.areaUnderROC)

//  val fMeasure = binarySummary.fMeasureByThreshold
//  val maxFMeasure = fMeasure.select(max("F-Measure")).head().getDouble(0)
//  val bestThreshold = fMeasure.where($"F-Measure" === maxFMeasure)
//    .select("threshold").head().getDouble(0)
//  lrModel.setThreshold(bestThreshold)

  sc.stop()
}
