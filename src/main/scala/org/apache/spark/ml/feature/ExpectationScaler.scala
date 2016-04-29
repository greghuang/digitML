package org.apache.spark.ml.feature

import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.util._
import org.apache.spark.mllib.linalg.{Vector, VectorUDT, Vectors}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{StructField, StructType}

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

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)


  override def fit(dataset: DataFrame): ExpectationScalerModel = {
    transformSchema(dataset.schema, logging = true)
    val input = dataset.select($(inputCol)).map { case Row(v: Vector) => v }
    val summary = Statistics.colStats(input)
    copyValues(new ExpectationScalerModel(uid, summary.numNonzeros, summary.count).setParent(this))
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
      val numNonezeros: Vector,
      val count: Long) extends Model[ExpectationScalerModel] with MLWritable with ExpectationScalerParam  {

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)


  override def copy(extra: ParamMap): ExpectationScalerModel = {
    val copied = new ExpectationScalerModel(uid, numNonezeros, count)
    copyValues(copied, extra).setParent(parent)
  }

  @org.apache.spark.annotation.Since("1.6.0")
  override def write: MLWriter = null

  override def transform(dataset: DataFrame): DataFrame = {
    val reScale = udf { (v : Vector) =>
      val values = v.toArray
      val nnz = numNonezeros.toArray
      var i = 0
      val size = values.length
      while (i < size) {
        val newValue = nnz(i) / count
        values(i) = newValue
        i += 1
      }
      Vectors.dense(values)
    }
    dataset.withColumn($(outputCol), reScale(col($(inputCol))))
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }
}
