package org.trend.spn.convolution.filter

import org.apache.spark.mllib.linalg.{Matrix, Matrices, Vector, Vectors}
import breeze.linalg.{DenseMatrix => BDM}

/**
  * Created by GregHuang on 5/5/16.
  */
object ConvolFilter {
  def main(args: Array[String]) {
    val input: Vector = Vectors.dense(
      1, 2, 3, 4, 5,
      6, 7, 8, 9, 10,
      11, 12, 13, 14, 15,
      16, 17, 18, 19, 20,
      21, 22, 23, 24, 25)

    val rawMat = Matrices.dense(5, 5, input.toArray).transpose
    println("Data")
    println(rawMat)
    println()

    //    println(Kernel.sharp3x3)

    //    val a1: Array[Double] = Array(1, 3, 1, 3)
    //    val a2: Array[Double] = Array(1, 2, 3, 4)
    //    val m1 = Matrices.dense(2, 2, a1)
    //    val m2 = Matrices.dense(2, 2, a2)
    //
    //    val sum = MatrixUtil.convolOp(a1, a2)
    //    println("Sum1:" + sum)
    //
    //    val sum2 = MatrixUtil.convolOp(m1, m2)
    //    println("Sum2:" + sum2)


    println("Apply edge filter")
    var output = new ConvolFilter(Kernel("edge3x3")).filter3x3(rawMat)
    println(output)

    println("\nApply blur filter")
    output = new ConvolFilter(Kernel("blur3x3")).filter3x3(rawMat)
    println(output)
  }
}

class ConvolFilter(val kernel: Kernel) extends java.io.Serializable {

  def filter3x3(data: Array[Double], rows: Int, cols: Int): Matrix = {
    filter3x3(Matrices.dense(rows, cols, data))
  }

  def filter3x3(data: Matrix): Matrix = {
    val bdm = BDM.zeros[Double](data.numRows - 2, data.numCols - 2)
    val covMat = kernel.matrix

    for (j <- 1 to (data.numRows - 2))
      for (i <- 1 to (data.numCols - 2)) {
        val sliceMat = MatrixUtil.slice3x3Block(data, j, i)
        bdm(j - 1, i - 1) = MatrixUtil.convolOp(sliceMat, covMat) / kernel.factor
      }
    MatrixUtil.fromBreeze(bdm)
  }
}
