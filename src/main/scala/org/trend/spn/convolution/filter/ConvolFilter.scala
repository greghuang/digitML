package org.trend.spn.convolution.filter

import org.apache.spark.mllib.linalg.{Matrix, Matrices, Vector, Vectors}

/**
 * Created by GregHuang on 5/5/16.
 */
object ConvolFilter {
  def main (args: Array[String]) {
    val input: Vector = Vectors.dense(
      1, 2, 3, 4, 5,
      6, 7, 8, 9, 10,
      11,12,13,14,15,
      16,17,18,19,20,
      21,22,23,24,25)

    println(Kernel.sharp3x3)

    val a1: Array[Double] = Array(1, 3, 1, 3)
    val a2: Array[Double] = Array(1, 2, 3, 4)
    val m1 = Matrices.dense(2, 2, a1)
    val m2 = Matrices.dense(2, 2, a2)

    val sum = MatrixUtil.convolOp(a1, a2)
    println("Sum1:" + sum)

    val sum2 = MatrixUtil.convolOp(m1, m2)
    println("Sum2:" + sum2)

    val filter = new ConvolFilter(Kernel.sharp3x3)
    val output = filter.setDimension(5, 5).filter(input)

    println(output)
  }
}

class ConvolFilter(val kernel : Matrix) {

  var row: Int = _
  var col: Int = _

  def setDimension(row: Int, col: Int): this.type = {
    this.row = row
    this.col = col
    this
  }

  def filter(data: Vector): Vector = {
    val buf = scala.collection.mutable.ArrayBuffer.empty[Double]

    for (j <- 1 to (row-2))
      for (i <- 1 to (col-2)) {
        val center = j*col + i
        val source = MatrixUtil.Mat3x3Block(data, row, col, center)
        val res =
        buf += data(center)
        println(source)
        println
      }
    Vectors.dense(buf.toArray)
  }
}
