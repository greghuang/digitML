package org.trend.spn.convolution.filter

import org.apache.spark.mllib.linalg._
import breeze.linalg.{DenseMatrix => BDM}

/**
  * Created by GregHuang on 5/5/16.
  */
object MatrixUtil {
  def slice3x3Block(raw: Vector, numRow: Int, numCol: Int, center: Int): Matrix = {
    val data = raw.toArray
    val buf = scala.collection.mutable.ArrayBuffer.empty[Double]
    for (j <- -1 to 1) {
      val index = j * numCol + center
      buf += data(index - 1)
      buf += data(index)
      buf += data(index + 1)
    }
    Matrices.dense(3, 3, buf.toArray).transpose
  }

  def slice3x3Block(raw: Matrix, row: Int, col: Int): Matrix = {
    val bm = toBreeze(raw)
    val sm = bm(row - 1 to row + 1, col - 1 to col + 1)
    Matrices.dense(sm.rows, sm.cols, sm.toArray)
  }

  def convolOp(source: Array[Double], kernel: Array[Double]): Double = {
    val sum = (source, kernel).zipped.map(_ * _)
    sum.sum
  }

  def convolOp(source: Matrix, kernel: Matrix): Double = {
    val brzS = toBreeze(source)
    val brzK = toBreeze(kernel)
    val res = brzS :* brzK
    res.sum
  }

  /**
    * Convert a local matrix into a dense breeze matrix.
    * TODO: use breeze sparse matrix if local matrix is sparse
    */
  def toBreeze(A: Matrix): BDM[Double] = {
    new BDM[Double](A.numRows, A.numCols, A.toArray)
  }

  /**
    * Convert from dense breeze matrix to local dense matrix.
    */
  def fromBreeze(dm: BDM[Double]): Matrix = {
    new DenseMatrix(dm.rows, dm.cols, dm.toArray, dm.isTranspose)
  }


}
