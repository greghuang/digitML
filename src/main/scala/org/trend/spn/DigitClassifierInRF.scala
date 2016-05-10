package org.trend.spn

import org.apache.spark.examples.mllib.AbstractParams
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

/**
  * Created by greghuang on 4/24/16.
  * [5/5 12:18] Test Error = 0.25855130784708247, Recall:0.7414486921529175
  * [5/5 12:37] Test Error = 0.15182186234817818, Recall:0.8481781376518218, expF + proF(28+28) + 100 trees
  * [5/5 12:49] Test Error = 0.14473684210526316, Recall:0.8552631578947368, expF + proF(14+14) + 100 trees
  * [5/5 12:51] Test Error = 0.13056680161943324, Recall:0.8694331983805668, expF + proF(14+14) + 300 trees
  * [5/6 11:14] Test Error = 0.14473684210526316, Recall:0.8552631578947368, Training Time 31 sec, expF + proF(14+14) + 100 trees, max_depth=default, min_node=default
  * [5/6 11:18] Test Error = 0.1406882591093117,  Recall:0.8593117408906883, Training Time 28 sec, expF + proF(14+14) + 100 trees, max_depth=5, min_node=10
  * [5/6 11:10] Test Error = 0.10323886639676116, Recall:0.8967611336032388, Training Time 141 sec, expF + proF(14+14) + 100 trees, max_depth=10, min_node=10
  * [5/7 02:01] Test Error = 0.09109311740890691, Recall:0.9089068825910931, Training Time 338 sec, expF + proF(14+14) + 100 trees, max_depth=20, min_node=10
  * [5/7 18:41] Test Error = 0.08502024291497978, Recall:0.9149797570850202, Training Time 519 sec, expF + proF(14+14) + 1 convol(pca 100) + 100 trees, max_depth=10, min_node=10
  * [5/7 19:24] Test Error = 0.0748987854251012,  Recall:0.9251012145748988, Training Time 768 sec, expF + proF(14+14) + 3 convol(pca 100) + 100 trees, max_depth=10, min_node=10
  * [5/7 20:58] Test Error = 0.05711318795430942, Recall:0.9428868120456906, Training Time 80 sec, expF + proF(14+14) + 3 convol(pca 300) + 100 trees, max_depth=10, min_node=10
  * [5/7 23:02] Test Error = 0.06645898234683278, Recall:0.9335410176531672, Training Time 201 sec,  expF + proF(28+28) + 6 convol(pca 300) + 150 trees, max_depth=30, min_node=10
  * [5/7 23:02] Test Error = 0.061266874350986544, Recall:0.9387331256490135, Training Time 181 sec,  expF + proF(28+28) + 6 convol + 150 trees, max_depth=30, min_node=10
  * ==== 60000 training data ====
  * [5/8 03:00] Test Error = 0.007518941579169991, Recall:0.99248105842083, Training Time 1038 sec
  * [5/10 23:48] Test Error = 0.00790483351649407, Recall:0.9920951664835059, Training Time 870 sec, expF20x20 + proF20x20-2 + 100 trees, max_depth=30, min_node=5
  * [5/11 01:00] Test Error = 0.03140725316675341, Recall:0.9685927468332466, Training Time 251 sec, expF20x20 + proF20x20-2 + 100 trees, max_depth=15, min_node=5
  * Best Result:
  *
  * -Xms10240m -Xmx10240m
  */
object DigitClassifierInRF extends DigitDataSet {

  case class Params(crossValidation: Boolean = false,
                    singleModel: Boolean = true,
                    saveModel: Boolean = false,
                    savePredictions: Boolean = false,
                    isRealTesting: Boolean = false,
                    maxDepth: Int = 15,
                    minLeafNodes: Int = 5,
                    numTrees: Int = 150
                   ) extends AbstractParams[Params]

//  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("DigitClassifierInRF") {
      head("DigitClassifierInRF: an classifier for digit by RandomForest.")
            opt[Int]("maxDepth")
              .text(s"maxDepth, default: ${defaultParams.maxDepth}")
              .action((x, c) => c.copy(maxDepth = x))
            opt[Int]("numTrees")
              .text(s"max number of tree, default: ${defaultParams.numTrees}")
              .action((x, c) => c.copy(numTrees = x))
            opt[Int]("minLeafNodes")
              .text(s"minimun of leaf nodes, default: ${defaultParams.minLeafNodes}")
              .action((x, c) => c.copy(minLeafNodes = x))
      note(
        """
          |Example command line to run this app:
          |
          | bin/spark-submit --class org.trend.spn.DigitClassifierInRF \
          |  target/scala-2.10/digitML-assembly-1.0.jar \
          |  --maxDepth 15
          |  --numTrees 100
          |  --minLeafNodes 5
        """.stripMargin)
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    } getOrElse {
      System.exit(1)
    }
//  }

  def run(params: Params) {

//    val data1 = sqlCtx.read.parquet("data/train/features/expectData.parquet").cache()
//    val data2 = MyMLUtil.loadLabelFeatures(sqlCtx, "data/train/features/projectFeature_14_14.txt").toDF("name2", "lable2", "proFeatures").cache()
//    val data2 = sqlCtx.read.parquet("data/train/features/proj_feature.parquet").toDF("name2", "proFeatures").cache()
    //val data3 = sqlCtx.read.parquet("data/train/features/convol1filter_pca.parquet/").toDF("name3", "convol_emboss").cache()
    //val data3 = sqlCtx.read.parquet("data/train/features/convol6filter_pca300.parquet/").toDF("name3", "convol_emboss", "convol_sobelH", "convol_sobelV", "convol_gradientV", "convol_gradientH", "convol_edge").cache()
//    val data3 = sqlCtx.read.parquet("data/train/features/convol6filter.parquet/").toDF("name3", "convol_emboss", "convol_sobelH", "convol_sobelV", "convol_gradientV", "convol_gradientH", "convol_edge").cache()

//    val data = data1
//      .join(data2, $"name" === $"name2")
//      //.withColumn("features1", TupleUDF.mergeCol($"expFeatures", $"proFeatures"))
//      .join(data3, $"name" === $"name3")
//      .withColumn("features", TupleUDF.merge8Col($"expFeatures", $"proFeatures", $"convol_emboss", $"convol_sobelH", $"convol_sobelV", $"convol_gradientV", $"convol_gradientH", $"convol_edge"))
//      .select("name", "label", "features")
//      .repartition(5)
//      .cache()

    import sqlCtx.implicits._

    TrainData.printSchema()
    //println(training.first())

    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(TrainData)

//    val featureIndexer = new VectorIndexer()
//      .setInputCol("features")
//      .setOutputCol("indexedFeatures")
//      .setMaxCategories(2)
//      .fit(data)

    val Array(splitTrain, splitTest) = TrainData.randomSplit(Array(0.8, 0.2), seed = 1234L)

    val training = if (params.isRealTesting) TrainData.cache() else splitTrain
    val testing = splitTest
//    val testing = if (params.isRealTesting) TestingData.cache() else splitTest

    //    val rf = new RandomForestClassifier()
    //      .setLabelCol("indexedLabel")
    //      .setFeaturesCol("indexedFeatures")
    //      .setNumTrees(100)

    val rf = new RandomForestClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("features")
      .setNumTrees(params.numTrees)
      .setMaxDepth(params.maxDepth)
      .setMinInstancesPerNode(params.minLeafNodes)
      .setMaxMemoryInMB(2024)
//      .setMinInfoGain(0.05)


    val labelConvertor = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    val pipeline = new Pipeline().setStages(Array(labelIndexer, rf, labelConvertor))

    if (params.singleModel) {
      val (trainingDuration, predModel) = MyMLUtil.time(pipeline.fit(training))
      val (predictionDuration, predictions) = MyMLUtil.time(predModel.transform(testing))

//      if (params.saveModel) predModel.save("data/model/rf-model-20160509-01")
//        pipelineModel.transform(data).collect().foreach {
//          case Row(label: Double, features: Vector, scaleF: Vector, normF: Vector) =>
//            println(s"($label) -> $scaleF   $normF")
//        }

      if (!params.isRealTesting)
        evaluate(trainingDuration, predictionDuration, predictions)

      val result = predictions.select("name", "predictedLabel", "prediction", "probability")
//      result.show()
      if (params.savePredictions) saveResult(sqlCtx, result)
    }
    else {
      val gridParam = new ParamGridBuilder()
        .addGrid(rf.numTrees, Array(100, 200))
        .addGrid(rf.maxDepth, Array(10, 20, 30))
        .addGrid(rf.minInstancesPerNode, Array(5, 10))
        .build()

      if (params.crossValidation) {
        val cv = new CrossValidator()
          .setEstimator(pipeline)
          .setEvaluator(new MulticlassClassificationEvaluator)
          .setEstimatorParamMaps(gridParam)
          .setNumFolds(3)

        val (trainingDuration, cvModel) = MyMLUtil.time(cv.fit(training))

        //val cvModel = CrossValidatorModel.load("data/model/lr-model")
        if (params.saveModel) cvModel.save("data/model/rf-model")

        val (predictionDuration, predictions) = MyMLUtil.time(cvModel.transform(testing))

        println("Best Model:")
        println(cvModel.bestModel.explainParams)

        evaluate(trainingDuration, predictionDuration, predictions)

        val result = predictions.select("name", "predictedLabel", "label", "prediction", "probability")
        MyMLUtil.showDataFrame(result)

        if (params.savePredictions) saveResult(sqlCtx, result)
      }
      else {
        val trainValidationSplit = new TrainValidationSplit()
          .setEstimator(pipeline)
          .setEvaluator(new MulticlassClassificationEvaluator)
          .setEstimatorParamMaps(gridParam)
          // 80% of the data will be used for training and the remaining 20% for validation.
          .setTrainRatio(0.8)

        val (trainingDuration, predModel) = MyMLUtil.time(trainValidationSplit.fit(training))
        val (predictionDuration, predictions) = MyMLUtil.time(predModel.transform(testing))

        println("Best Model:")
        println(predModel.bestModel.explainParams())

        evaluate(trainingDuration, predictionDuration, predictions)

        val result = predictions.select("name", "predictedLabel", "label", "prediction", "probability")
        //      MyMLUtil.showDataFrame(result)

        if (params.savePredictions) saveResult(sqlCtx, result)
      }
    }
    sc.stop()
  }

  def evaluate(trainingDuration: Long, predictionDuration: Long, predictions: DataFrame) {
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


    print("Test Error = " + (1.0 - accuracy))
    print(", Recall:" + recall)
    println(s", Training Time ${trainingDuration} sec")
  }

  def saveResult(sqlCtx: SQLContext, result: DataFrame) {
    import sqlCtx.implicits._

    // Save results
    val predLabelMapping = result.select($"predictedLabel", $"prediction").distinct

    predLabelMapping
      .coalesce(1)
      .write
      .format("com.databricks.spark.csv")
      .options(Map("header" -> "true", "inferSchema" -> "true"))
      .save("data/test/mapping_csv")

    result
      .coalesce(1)
      .write
      .format("com.databricks.spark.csv")
      .options(Map("header" -> "true", "inferSchema" -> "true"))
      .save("data/test/rf_csv")
  }
}
