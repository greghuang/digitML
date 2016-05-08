package org.apache.spark.ml.feature

import breeze.linalg.{DenseMatrix, DenseVector, max}
import org.apache.spark.annotation.Since
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.mllib.linalg.{Matrices, Vector, VectorUDT, Vectors}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}
import org.trend.spn.convolution.filter.{ConvolFilter, Kernel, MatrixUtil}

/**
  * Created by greghuang on 5/8/16.
  */
private[feature] trait ProjectTransformerBase extends Params with HasInputCol with HasOutputCol {
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    /** Validates and transforms the input schema. */
    val inputType = schema($(inputCol)).dataType
    require(inputType.isInstanceOf[VectorUDT],
      s"Input column ${$(inputCol)} must be a vector column")
    require(!schema.fieldNames.contains($(outputCol)),
      s"Output column ${$(outputCol)} already exists.")
    val outputFields = schema.fields :+ StructField($(outputCol), new VectorUDT, false)
    StructType(outputFields)
  }
}

class ProjectTransformer(override val uid: String) extends Transformer
  with ProjectTransformerBase with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("projectTransformer"))

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
//  def setKernel(kernel : Kernel): this.type = {
//    convKernel = kernel
//    this
//  }
//
//  def setBlockSize(size: Int): this.type = {
//    bkSize = size
//    this
//  }

  override def transformSchema(schema: StructType): StructType = {
    if (schema.fieldNames.contains($(inputCol))) {
      validateAndTransformSchema(schema)
    } else {
      // If the input column does not exist during transformation, we skip.
      schema
    }
  }

  override def transform(dataset: DataFrame): DataFrame = {
    val projectFt = udf { (input: Vector) =>
      val dimension = math.sqrt(input.size).toInt
      val data = input.toArray.transform(f => if(f > 0.0) 1 else 0)
      val mat = Matrices.dense(dimension, dimension, data.toArray).transpose
      val bdm = MatrixUtil.toBreeze(mat)

      val matX = DenseMatrix.ones[Double](1, dimension)
      val matY = DenseMatrix.ones[Double](dimension, 1)

      val projX = matX * bdm
      val projY = bdm * matY

      var resX = projX.toDenseVector
      var resY = projY.toDenseVector

      // Normalization
      val maxX = max(projX.toDenseVector)
      val maxY = max(projY.toDenseVector)
      resX = resX :/ maxX
      resY = resY :/ maxY

      Vectors.dense(DenseVector.vertcat(resX, resY).toArray)
    }

    dataset.withColumn($(outputCol), projectFt(dataset($(inputCol))))
  }

  override def copy(extra: ParamMap): ProjectTransformer = {
    defaultCopy(extra)
  }
}

@Since("1.6.0")
object ProjectTransformer extends DefaultParamsReadable[ProjectTransformer] {

  @Since("1.6.0")
  override def load(path: String): ProjectTransformer = super.load(path)
}