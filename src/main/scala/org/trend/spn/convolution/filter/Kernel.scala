package org.trend.spn.convolution.filter

import org.apache.spark.mllib.linalg.{Matrix, Matrices, Vector, Vectors}

/**
 * Created by GregHuang on 5/5/16.
 */
object Kernel {

  final def sharp3x3: Matrix = {
    val v: Array[Double] = Array(0, -1, 0, -1, 5, -1, 0, -1, 0)
    Matrices.dense(3, 3, v)
  }
}
