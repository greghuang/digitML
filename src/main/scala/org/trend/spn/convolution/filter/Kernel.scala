package org.trend.spn.convolution.filter

import org.apache.spark.mllib.linalg.{Matrix, Matrices, Vector, Vectors}
import breeze.linalg.{DenseMatrix => BDM, DenseVector}

/**
  * Created by GregHuang on 5/5/16.
  */

trait Kernel {
  def matrix: Matrix
}

object Kernel {

  private class Sharp3x3 extends Kernel {
    override def matrix: Matrix = {
      val bm: BDM[Double] = BDM((0.0, -1.0, 0.0), (-1.0, 5.0, -1.0), (0.0, -1.0, 0.0))
      Matrices.dense(bm.rows, bm.cols, bm.toArray)
    }
  }

  def apply(s: String): Kernel = {
    if (s == "sharp3x3") return new Sharp3x3
    else return null
  }
}
