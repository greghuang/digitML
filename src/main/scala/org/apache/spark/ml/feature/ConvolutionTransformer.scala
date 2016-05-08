package org.apache.spark.ml.feature

import org.apache.spark.annotation.Since
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util._
import org.apache.spark.mllib.linalg.{Matrices, Vector, VectorUDT, Vectors}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.functions._
import org.trend.spn.convolution.filter.{ConvolFilter, Kernel, MatrixUtil}


/**
  * Created by greghuang on 5/7/16.
  */
private[feature] trait ConvolutionTransformerBase extends Params with HasInputCol with HasOutputCol {
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

class ConvolutionTransformer(override val uid: String) extends Transformer
  with ConvolutionTransformerBase with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("convolutionIdx"))

  var convKernel: Kernel = _
  var bkSize : Int = 7

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  def setKernel(kernel : Kernel): this.type = {
    convKernel = kernel
    this
  }

  def setBlockSize(size: Int): this.type = {
    bkSize = size
    this
  }

  override def transformSchema(schema: StructType): StructType = {
    if (schema.fieldNames.contains($(inputCol))) {
      validateAndTransformSchema(schema)
    } else {
      // If the input column does not exist during transformation, we skip.
      schema
    }
  }

  override def transform(dataset: DataFrame): DataFrame = {
    val cf = new ConvolFilter(convKernel)

    val convolutionft = udf { (input: Vector) =>
      val dimension = math.sqrt(input.size).toInt
      //println(s"Dimension:$dimension")
      val rawMat = Matrices.dense(dimension, dimension, input.toArray).transpose
      val bdm = MatrixUtil.toBreeze(rawMat)
      var buf = scala.collection.mutable.ArrayBuffer.empty[Double]

      for (j <- 0 to rawMat.numRows / bkSize - 1)
        for (i <- 0 to rawMat.numCols / bkSize - 1) {
          val bk = bdm(j * bkSize to (j + 1) * bkSize - 1, i * bkSize to (i + 1) * bkSize - 1)
          val bkm = Matrices.dense(bk.rows, bk.cols, bk.toArray)
          val res = cf.filter3x3(bkm).toArray
            .map { x =>
              math.min(math.abs(x), 255.0) / 255.0
            }
          buf ++= res
        }
      Vectors.dense(buf.toArray)
    }

    //    dataset.select(col("*"), convolutionft(dataset($(inputCol))).as($(outputCol)))
    dataset.withColumn($(outputCol), convolutionft(dataset($(inputCol))))
  }

  override def copy(extra: ParamMap): ConvolutionTransformer = {
    defaultCopy(extra)
  }
}

@Since("1.6.0")
object ConvolutionTransformer extends DefaultParamsReadable[ConvolutionTransformer] {

  @Since("1.6.0")
  override def load(path: String): ConvolutionTransformer = super.load(path)
}


