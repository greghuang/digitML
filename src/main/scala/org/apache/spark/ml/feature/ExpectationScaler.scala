package org.apache.spark.ml.feature

import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.util._
import org.apache.spark.mllib.linalg.{Matrix, Matrices, Vector, VectorUDT, Vectors}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{StructField, StructType}
import org.trend.spn.MyMLUtil

/**
  * Created by greghuang on 4/27/16.
  */
private[feature] trait ExpectationScalerParam extends Params with HasInputCol with HasOutputCol {
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputCol)).dataType
    require(inputType.isInstanceOf[VectorUDT],
      s"Input column ${$(inputCol)} must be a vector column")
    require(!schema.fieldNames.contains($(outputCol)),
      s"Output column ${$(outputCol)} already exists.")
    val outputFields = schema.fields :+ StructField($(outputCol), new VectorUDT, false)
    StructType(outputFields)
  }
}


class ExpectationScaler(override val uid: String)
  extends Estimator[ExpectationScalerModel] with DefaultParamsWritable with ExpectationScalerParam {
  def this() = this(Identifiable.randomUID("expectationScaler"))

  var threshold: Double = 0.0

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  def setThreshold(value: Double): this.type = {
    threshold = value
    this
  }

//  val isValid: PartialFunction[Double, Double] = {
//    case x if x > threshold  => x
//    case _ => 0.0
//  }

  def isValid(x: Double) = if (x > threshold) x else 0.0

  case class ImageData(label : Double, features: Vector)

  def normalizeVector(v : Vector, count: Long): Vector = {
    val values = v.toArray
    var i = 0
    val size = values.length
    while (i < size) {
      values(i) = values(i) / count
      i += 1
    }
    Vectors.dense(values)
  }

  override def fit(dataset: DataFrame): ExpectationScalerModel = {
    transformSchema(dataset.schema, logging = true)
//    val r = dataset.collect().groupBy(_.get(0))
//      .values.foreach(f =>
//      f.map{case Row(l: Double, v: Vector) => v}.
//
//    )
//      .map{case Row(l: Double, v: Vector) => v}
//      .flatten
      //.foreach(println)
    //val groups = dataset.groupBy("label").agg(max("features"))

//    MyMLUtil.showDataFrame(groups)
//    dataset.show(2)

    val buf = scala.collection.mutable.ArrayBuffer.empty[Vector]
    val dinctLabel = dataset.select("label").distinct().map{_.getDouble(0)}.collect()
    for (i <- dinctLabel) {
      val input = dataset.filter(dataset("label").equalTo(i)).select($(inputCol))
      val summary = Statistics.colStats(input.map { case Row(v: Vector) => v })
      val v = normalizeVector(summary.numNonzeros, summary.count)
      buf += v
    }

    val array = buf.toArray
      .map(v => v.toArray)
      .transpose
      .map(r =>
        r.map(isValid(_)) // Filter by threshold
      )

    val mat = Matrices.dense(array(0).length, array.length, array.flatten)

    // For testing
//    val dv = Vectors.dense(1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0)
//    println(mat)
//    println(mat.multiply(dv))
//    val input = dataset.select($(inputCol)).map { case Row(v: Vector) => v }
//    val summary = Statistics.colStats(input)

    copyValues(new ExpectationScalerModel(uid, mat).setParent(this))
  }

  override def copy(extra: ParamMap): ExpectationScaler = defaultCopy(extra)


  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }
}

object ExpectationScaler extends DefaultParamsReadable[ExpectationScaler] {
  @Since("1.6.0")
  override def load(path: String): ExpectationScaler = super.load(path)
}

class ExpectationScalerModel private[ml] (
      override val uid: String,
      val meanMatrix: Matrix)
  extends Model[ExpectationScalerModel] with MLWritable with ExpectationScalerParam  {

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)


  override def copy(extra: ParamMap): ExpectationScalerModel = {
    val copied = new ExpectationScalerModel(uid, meanMatrix)
    copyValues(copied, extra).setParent(parent)
  }

  @org.apache.spark.annotation.Since("1.6.0")
  override def write: MLWriter = null

  override def transform(dataset: DataFrame): DataFrame = {
    val reScale = udf { (v : Vector) =>
      val expectation = meanMatrix.multiply(v)
      expectation
//      val values = v.toArray
//      val nnz = numNonezeros.toArray
//      var i = 0
//      val size = values.length
//      while (i < size) {
//        val newValue = nnz(i) / count
//        values(i) = newValue
//        i += 1
//      }
//      Vectors.dense(values)
    }
    dataset.withColumn($(outputCol), reScale(col($(inputCol))))
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }
}
