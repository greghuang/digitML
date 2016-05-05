package org.trend.spn.convolution.filter

import org.apache.spark.mllib.linalg.{Matrix, Matrices, Vector, Vectors}
import breeze.linalg.{DenseMatrix => BDM, DenseVector}

/**
 * Created by GregHuang on 5/5/16.
 */

object Kernel {
  final def sharp3x3: Matrix = {
    val bm: BDM[Double] = BDM((0.0, -1.0, 0.0),(-1.0, 5.0, -1.0), (0.0, -1.0, 0.0))
    println("kernel")
    println(bm)
    println
    Matrices.dense(bm.rows, bm.cols, bm.toArray)
  }
}
